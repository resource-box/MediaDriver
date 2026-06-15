package com.resourcebox.demo;

import com.resourcebox.MediaDriver.client.DataPublisher;
import com.resourcebox.MediaDriver.client.DataReceiver;
import com.resourcebox.MediaDriver.server.MediaDriverServer;
import java.util.concurrent.atomic.AtomicBoolean;

public class NewValidationApp {
    private static final String AERON_DIR = "PARCAeron";
    private static final int STREAM_ID = 10;

    public static void main(String[] args) throws Exception {
        System.out.println("Starting Validation App...");

        // 1. Start Media Driver in a background thread
        Thread driverThread = new Thread(() -> {
            MediaDriverServer server = new MediaDriverServer("PARCAeron", 1, 2, 3);
            server.start();
        });
        driverThread.setDaemon(true);
        driverThread.start();

        // 2. Start Receiver
        DataReceiver receiver = new DataReceiver(AERON_DIR, STREAM_ID);

        // 3. Start Publisher
        DataPublisher publisher = new DataPublisher(AERON_DIR, STREAM_ID);

        AtomicBoolean running = new AtomicBoolean(true);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> running.set(false)));

        // 4. Telemetry and Zero-Allocation High-Speed Data Generation Loop
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

        System.out.println("Shutting down validation app...");
        publisher.close();
        receiver.close();
    }
}
