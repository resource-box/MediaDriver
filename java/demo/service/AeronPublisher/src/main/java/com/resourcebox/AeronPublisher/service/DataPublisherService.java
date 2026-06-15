package com.resourcebox.AeronPublisher.service;

import com.resourcebox.MediaDriver.client.DataPublisher;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicBoolean;

@Service
public class DataPublisherService {

    private static final String AERON_DIR = "PARCAeron";
    private static final int STREAM_ID = 10;
    private DataPublisher PUBLISHER;

    @PostConstruct
    private void run() {
        // Start
        PUBLISHER = new DataPublisher(AERON_DIR, STREAM_ID);


        AtomicBoolean running = new AtomicBoolean(true);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> running.set(false)));

        System.out.println("Starting high-speed ingestion test...");

        long totalRows = 0;
        long startTime = System.nanoTime();
        long lastReportTime = startTime;
        long lastRows = 0;

        // Pre-allocate arrays for testing ListData
        int[] ids = new int[5];
        double[] values = new double[5];
        for (int i = 0; i < 5; i++) {
            ids[i] = i;
            values[i] = i * 1.5;
        }

        while (running.get()) {
            // Primitive data publication, zero object allocation in this loop
            PUBLISHER.publishListData("2026-06-15T10:00:00.000Z", ids, values);
            totalRows += 5; // 5 rows per message

            long now = System.nanoTime();
            if (now - lastReportTime >= 1_000_000_000L) { // 1 second
                long durationSeconds = (now - lastReportTime) / 1_000_000_000L;
                long rowsThisSecond = totalRows - lastRows;
                System.out.printf("Throughput: %,d rows/sec | Total: %,d%n", rowsThisSecond / durationSeconds, totalRows);

                lastReportTime = now;
                lastRows = totalRows;
            }
        }

    }

    @PreDestroy
    private void stop() {
        PUBLISHER.close();
    }

}
