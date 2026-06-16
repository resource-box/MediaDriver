package com.resourcebox.demo;

import com.resourcebox.MediaDriver.client.DataPublisher;

import java.util.concurrent.atomic.AtomicBoolean;

public class DemoPublisher {
    private static final String AERON_DIR = "PARCAeron";
    private static final int STREAM_ID = 10;
    private static final int CAPACITY = 1024;
    private static final int ALIGNMENT = 16;

    public static void main(String[] args) throws Exception {
        System.out.println("Starting Demo Publisher...");

        DataPublisher publisher = new DataPublisher(AERON_DIR, STREAM_ID, CAPACITY, ALIGNMENT);

        AtomicBoolean running = new AtomicBoolean(true);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            running.set(false);
            publisher.close();
            System.out.println("Demo Publisher Shutdown");
        }));

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
            publisher.publishListData("2026-06-15T10:00:00.000Z", ids, values);
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
}
