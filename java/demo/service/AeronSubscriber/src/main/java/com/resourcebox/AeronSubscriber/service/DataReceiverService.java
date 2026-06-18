package com.resourcebox.AeronSubscriber.service;

import com.resourcebox.MediaDriver.client.DataMessageListener;
import com.resourcebox.MediaDriver.client.DataReceiver;
import com.resourcebox.sbe.ListDataMessageDecoder;
import com.resourcebox.sbe.ListStatusMessageDecoder;
import com.resourcebox.sbe.SingleDataMessageDecoder;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class DataReceiverService {

    private static final String AERON_DIR = "PARCAeron";
    private static final int STREAM_ID = 10;

    private DataReceiver dataReceiver;
    private final Logger log = LoggerFactory.getLogger(DataReceiverService.class);

    @PostConstruct
    private void run() {
        log.trace("Starting DataReceiverService...");

        // 비즈니스 로직만 정의하여 DataReceiver에 주입
        DataMessageListener listener = new DataMessageListener() {
            @Override
            public void onSingleDataReceived(SingleDataMessageDecoder decoder) {
                // 필요시 구현
            }

            @Override
            public void onListDataReceived(ListDataMessageDecoder decoder) {
                dataMessageBusinessLogic(decoder);
            }

            @Override
            public void onListStatusReceived(ListStatusMessageDecoder decoder) {
                statusMessageBusinessLogic(decoder);
            }
        };

        // 라이브러리 객체 생성 (내부적으로 수신 및 Ring Buffer 처리 후 Listener 호출)
        this.dataReceiver = new DataReceiver(AERON_DIR, STREAM_ID, listener);
    }

    private void dataMessageBusinessLogic(ListDataMessageDecoder decoder) {
        // TIMESTAMP 처리
        ListDataMessageDecoder.TimestampDecoder timestampDecoder = decoder.timestamp();
        if (timestampDecoder.hasNext()) {
            String timestamp = timestampDecoder.next().value();
        }

        // TAG 데이터 처리
        ListDataMessageDecoder.EntriesDecoder entries = decoder.entries();
        while (entries.hasNext()) {
            entries.next();
            int id = entries.id();
            double value = entries.value();
        }
    }

    private void statusMessageBusinessLogic(ListStatusMessageDecoder decoder) {
        // TIMESTAMP 처리
        ListStatusMessageDecoder.TimestampDecoder timestampDecoder = decoder.timestamp();
        if (timestampDecoder.hasNext()) {
            String timestamp = timestampDecoder.next().value();
        }

        // TAG 데이터 처리
        ListStatusMessageDecoder.EntriesDecoder entries = decoder.entries();
        while (entries.hasNext()) {
            entries.next();
            int id = entries.id();
            String value = entries.value();
        }
    }

    @PreDestroy
    public void stop() {
        log.trace("Shutting down DataReceiverService...");
        if (dataReceiver != null) {
            dataReceiver.close(); // Aeron 및 Worker Thread 안전 종료
        }
    }
}