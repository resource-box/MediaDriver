package com.resourcebox.demo;

import com.resourcebox.MediaDriver.client.DataReceiver;

import java.util.concurrent.atomic.AtomicBoolean;

public class DemoReceiver {
    private static final String AERON_DIR = "PARCAeron";
    private static final int STREAM_ID = 10;

    public static void main(String[] args) throws Exception {
        System.out.println("Starting Demo Receiver...");

        com.resourcebox.MediaDriver.client.DataMessageListener listener = new com.resourcebox.MediaDriver.client.DataMessageListener() {
            @Override
            public void onSingleDataReceived(com.resourcebox.sbe.SingleDataMessageDecoder decoder) {
                int id = decoder.id();
                String value = decoder.value();
                String timestamp = decoder.timestamp();
                // System.out.println("Received SingleData: id=" + id + ", value=" + value + ", ts=" + timestamp);
            }
            
            @Override
            public void onListDataReceived(com.resourcebox.sbe.ListDataMessageDecoder decoder) {
                com.resourcebox.sbe.ListDataMessageDecoder.TimestampDecoder timestampDecoder = decoder.timestamp();
                String timestamp = "";
                if (timestampDecoder.hasNext()) {
                    timestamp = timestampDecoder.next().value();
                }

                com.resourcebox.sbe.ListDataMessageDecoder.EntriesDecoder entries = decoder.entries();
                // System.out.println("Received ListData: ts=" + timestamp + ", count=" + entries.count());
                while (entries.hasNext()) {
                    entries.next();
                    int id = entries.id();
                    double value = entries.value();
                    // System.out.println("  Entry - id=" + id + ", value=" + value);
                }
            }
            
            @Override
            public void onListStatusReceived(com.resourcebox.sbe.ListStatusMessageDecoder decoder) {
                com.resourcebox.sbe.ListStatusMessageDecoder.TimestampDecoder timestampDecoder = decoder.timestamp();
                String timestamp = "";
                if (timestampDecoder.hasNext()) {
                    timestamp = timestampDecoder.next().value();
                }

                com.resourcebox.sbe.ListStatusMessageDecoder.EntriesDecoder entries = decoder.entries();
                // System.out.println("Received ListStatus: ts=" + timestamp + ", count=" + entries.count());
                while (entries.hasNext()) {
                    entries.next();
                    int id = entries.id();
                    String value = entries.value();
                    // System.out.println("  Entry - id=" + id + ", value=" + value);
                }
            }
        };
        
        DataReceiver receiver = new DataReceiver(AERON_DIR, STREAM_ID, listener);

        AtomicBoolean running = new AtomicBoolean(true);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            running.set(false);
            receiver.close();
            System.out.println("Demo Receiver Shutdown");
        }));

        System.out.println("Receiver is running in background Agent. Press Ctrl+C to exit.");
        // 메인 스레드 대기 (AgentRunner가 백그라운드에서 동작하므로)
        Thread.currentThread().join();
    }
}
