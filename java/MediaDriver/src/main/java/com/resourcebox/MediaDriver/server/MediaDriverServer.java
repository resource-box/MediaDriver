package com.resourcebox.MediaDriver.server;

import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

public class MediaDriverServer {

    private final String aeronDir;
    private final int ipcTermBufferMB;
    private final int socketBufferMB;

    private final Logger log = LoggerFactory.getLogger(MediaDriverServer.class);

    public MediaDriverServer(String aeronDirName, int ipcTermBufferMB, int socketBufferMB) {
        this.aeronDir = Path.of(System.getProperty("java.io.tmpdir"), aeronDirName).toAbsolutePath().toString();
        this.ipcTermBufferMB = ipcTermBufferMB;
        this.socketBufferMB = socketBufferMB;
    }

    public void start() {
        // DEDICATED 스레드 모드로 설정하여 3개의 스레드가 각자 전용으로 실행
        final MediaDriver.Context ctx = new MediaDriver.Context()
                .aeronDirectoryName(aeronDir)
                .threadingMode(ThreadingMode.DEDICATED)
                .conductorIdleStrategy(new BackoffIdleStrategy())
                .senderIdleStrategy(new YieldingIdleStrategy())
                .receiverIdleStrategy(new YieldingIdleStrategy())
                .ipcTermBufferLength(ipcTermBufferMB * 1024 * 1024)
                .socketRcvbufLength(socketBufferMB * 1024 * 1024)
                .socketSndbufLength(socketBufferMB * 1024 * 1024)
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true);

        log.info("Starting Media Driver with DEDICATED threading mode...");

        try (MediaDriver ignored = MediaDriver.launch(ctx)) {
            log.info("Media Driver started successfully.");

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
