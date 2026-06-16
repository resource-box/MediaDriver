package com.resourcebox.AeronPublisher.service;

import com.resourcebox.MediaDriver.client.DataPublisher;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicBoolean;

@Service
public class DataPublisherService implements SmartLifecycle {

    private static final String AERON_DIR = "PARCAeron";
    private static final int STREAM_ID = 10;
    private static final int CAPACITY = 1024;
    private static final int ALIGNMENT = 16;

    private DataPublisher publisher;
    private Thread workerThread;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public DataPublisherService() {}

    @Override
    public void start() {
        if (running.compareAndSet(false, true)) {
            workerThread = new Thread(this::publishingLoop, "Aeron-Publisher-Worker");
            workerThread.setPriority(Thread.MAX_PRIORITY); // 추가
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
        // Client Conductor는 생성자에서 설정한 AERON_CLIENT_CORE(14)를 따름
        publisher = new DataPublisher(AERON_DIR, STREAM_ID, CAPACITY, ALIGNMENT);

        int[] ids = {0, 1, 2, 3, 4};
        double[] values = {0.0, 1.5, 3.0, 4.5, 6.0};

        long totalRows = 0;
        long startTime = System.nanoTime();
        long lastReportTime = startTime;
        long lastRows = 0;

        while (running.get()) {
            publisher.publishListData("2026-06-15T10:00:00.000Z", ids, values);
            totalRows += 5;

            long now = System.nanoTime();
            if (now - lastReportTime >= 1_000_000_000L) {
                long durationSeconds = (now - lastReportTime) / 1_000_000_000L;
                long rowsThisSecond = totalRows - lastRows;
                System.out.printf("Throughput: %,d rows/sec | Total: %,d%n", rowsThisSecond / durationSeconds, totalRows);

                lastReportTime = now;
                lastRows = totalRows;
            }
        }
    }
}