package com.resourcebox.MediaDriver.server;

import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

public class MediaDriverServer {

    private final String aeronDir;
    private final int senderCpuAffinity;
    private final int receiverCpuAffinity;
    private final int conductorCpuAffinity;
    private final Logger log = LoggerFactory.getLogger(MediaDriverServer.class);

    public MediaDriverServer(String aeronDirName, int senderCpuAffinity, int receiverCpuAffinity, int conductorCpuAffinity) {
        this.aeronDir = Path.of(System.getProperty("java.io.tmpdir"), aeronDirName).toAbsolutePath().toString();
        this.senderCpuAffinity = senderCpuAffinity;
        this.receiverCpuAffinity = receiverCpuAffinity;
        this.conductorCpuAffinity = conductorCpuAffinity;
    }

    public void start() {
        // CPU 코어 고정 (Affinity) 설정
        System.setProperty("aeron.sender.cpu.affinity", String.valueOf(senderCpuAffinity));
        System.setProperty("aeron.receiver.cpu.affinity", String.valueOf(receiverCpuAffinity));
        System.setProperty("aeron.conductor.cpu.affinity", String.valueOf(conductorCpuAffinity));

        // DEDICATED 스레드 모드로 설정하여 3개의 스레드가 각자 전용으로 실행
        final MediaDriver.Context ctx = new MediaDriver.Context()
                .aeronDirectoryName(aeronDir)
                .threadingMode(ThreadingMode.DEDICATED)
//                .conductorIdleStrategy(BusySpinIdleStrategy.INSTANCE)
//                .senderIdleStrategy(BusySpinIdleStrategy.INSTANCE)
//                .receiverIdleStrategy(BusySpinIdleStrategy.INSTANCE)
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true);

        log.info("Starting Media Driver with DEDICATED threading mode...");

        try (MediaDriver ignored = MediaDriver.launch(ctx)) {
            log.info("Media Driver started successfully.");
            log.info("Sender CPU affinity: " + senderCpuAffinity);
            log.info("Receiver CPU affinity: " + receiverCpuAffinity);
            log.info("Conductor CPU affinity: " + conductorCpuAffinity);

            // 종료 시그널(Ctrl+C) 대기
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("Shutting down Media Driver...");
                ctx.close();
            }));

            // 메인 스레드 무한 대기
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Media Driver server interrupted.");
        }
    }

}
