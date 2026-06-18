package com.resourcebox.AeronPublisher.service;

import com.resourcebox.MediaDriver.client.DataPublisher;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

@Service
public class DataPublisherService implements SmartLifecycle {

    private static final String AERON_DIR = "PARCAeron";
    private static final int STREAM_ID = 10;
    private static final int CAPACITY = 1024;
    private static final int ALIGNMENT = 16;

    // 초당 발행을 원하는 '횟수' (목표치)
    private static final long TARGET_PUBLISH_PER_SEC = 1_000_000;
    // 발행 간격을 나노초(ns)로 계산
    private static final long INTERVAL_NS = 1_000_000_000L / TARGET_PUBLISH_PER_SEC;

    private DataPublisher publisher;
    private Thread workerThread;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public DataPublisherService() {}

    @Override
    public void start() {
        if (running.compareAndSet(false, true)) {
            workerThread = new Thread(this::publishingLoop, "Aeron-Publisher-Worker");
            workerThread.setPriority(Thread.MAX_PRIORITY);
            workerThread.setDaemon(false);
            workerThread.start();
        }
    }

    @Override
    public void stop() {
        if (running.compareAndSet(true, false)) {
            try {
                if (workerThread != null) {
                    workerThread.join(5000);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            if (publisher != null) {
                publisher.close();
            }
        }
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE;
    }

    private void publishingLoop() {
        publisher = new DataPublisher(AERON_DIR, STREAM_ID, CAPACITY, ALIGNMENT);

        int[] ids = {0, 1, 2, 3, 4};
        double[] values = {0.0, 1.5, 3.0, 4.5, 6.0};

        long totalRows = 0;
        int publishCount = 0;

        long startTime = System.nanoTime();
        long lastReportTime = startTime;
        long lastRows = 0;

        // 다음 발행 예약 시간 설정
        long nextPublishTime = System.nanoTime();

        while (running.get()) {
            long now = System.nanoTime();

            // 1. 예약된 발행 시간이 도래했는지 확인
            if (now >= nextPublishTime) {

                // 데이터 발행
                if (publisher.publishListData("2026-06-15T10:00:00.000Z", ids, values)) {
                    totalRows += 5; // 한 번 발행 시 5개의 Row 포함
                    publishCount++;
                }

                // 다음 발행 시간 예약
                // (현재 시간인 now가 아니라 nextPublishTime에 더해야 미세한 오차가 누적되지 않음)
                nextPublishTime += INTERVAL_NS;

                // 시스템 과부하 등으로 인해 처리가 너무 밀려서 과거로 뒤처진 경우 영점 보정
                if (now > nextPublishTime + INTERVAL_NS) {
                    nextPublishTime = now;
                }

            } else {
                // 2. 대기 로직 (Hybrid Wait)
                long sleepNs = nextPublishTime - now;

                if (sleepNs > 100_000) {
                    // 남은 시간이 0.1ms 이상일 경우: OS 레벨 대기 (CPU 양보)
                    // (깨어나는 시간 오차를 감안해 목표 시간보다 조금 일찍 깨어나도록 보정)
                    LockSupport.parkNanos(sleepNs - 50_000);
                } else {
                    // 남은 시간이 0.1ms 미만일 경우: Spin-Wait (초정밀 대기)
                    Thread.onSpinWait(); // Java 9 이상 권장. Java 8 환경이라면 비워두거나 yield() 사용
                }
            }

            // 3. 1초 단위 모니터링 리포트
            now = System.nanoTime();
            if (now - lastReportTime >= 1_000_000_000L) {
                long durationSeconds = (now - lastReportTime) / 1_000_000_000L;
                long rowsThisSecond = totalRows - lastRows;

                System.out.printf(
                        "Target: %,d pub/sec | Actual: %,d pub/sec | Throughput: %,d rows/sec | Total: %,d rows%n",
                        TARGET_PUBLISH_PER_SEC,
                        publishCount / durationSeconds,
                        rowsThisSecond / durationSeconds,
                        totalRows
                );

                lastReportTime = now;
                lastRows = totalRows;
                publishCount = 0; // 초당 발행 횟수 측정용 초기화
            }
        }
    }
}