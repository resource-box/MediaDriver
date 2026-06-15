package com.resourcebox.AeronSubscriber.service;

import com.resourcebox.MediaDriver.client.DataReceiver;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.stereotype.Service;

@Service
public class DataReceiverService {

    private static final String AERON_DIR = "PARCAeron";
    private static final int STREAM_ID = 10;
    private DataReceiver RECEIVER;

    @PostConstruct
    private void run() {
        // Start
        RECEIVER = new DataReceiver(AERON_DIR, STREAM_ID);
    }

    @PreDestroy
    private void stop() {
        RECEIVER.close();
    }

}
