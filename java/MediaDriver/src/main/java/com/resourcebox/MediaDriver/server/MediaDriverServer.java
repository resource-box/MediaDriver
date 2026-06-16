package com.resourcebox.MediaDriver.server;

import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

/**
 * Media Driver 서버를 실행하는 클래스입니다.
 * 서버용 OS 및 하드웨어에 적용하기 위한 별도의 IdleStrategy 설정이 포함되어 있습니다.
 */
public class MediaDriverServer {

    // Aeron
    private final String aeronDir;
    private final int ipcTermBufferMB;
    private final int socketBufferMB;

    // Logger
    private final Logger log = LoggerFactory.getLogger(MediaDriverServer.class);

    public MediaDriverServer(String aeronDirName, int ipcTermBufferMB, int socketBufferMB) {
        this.aeronDir = Path.of(System.getProperty("java.io.tmpdir"), aeronDirName).toAbsolutePath().toString();
        this.ipcTermBufferMB = ipcTermBufferMB;
        this.socketBufferMB = socketBufferMB;
    }

    /**
     * Media Driver 서버를 실행합니다.
     */
    public void start() {
        log.info("Starting Media Driver with DEDICATED threading mode...");

        // Context Setup
        final MediaDriver.Context ctx = new MediaDriver.Context()
                .aeronDirectoryName(aeronDir)
                .threadingMode(ThreadingMode.DEDICATED)
                .conductorIdleStrategy(new BackoffIdleStrategy()) // CPU Switching 환경
                .senderIdleStrategy(new YieldingIdleStrategy()) // CPU Switching 환경
                .receiverIdleStrategy(new YieldingIdleStrategy()) // CPU Switching 환경
                .ipcTermBufferLength(ipcTermBufferMB * 1024 * 1024)
                .socketRcvbufLength(socketBufferMB * 1024 * 1024)
                .socketSndbufLength(socketBufferMB * 1024 * 1024)
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true);

        // Media Driver 실행
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
