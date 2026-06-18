package com.resourcebox.AeronSubscriber.service;

import com.resourcebox.MediaDriver.client.DataMessageListener;
import com.resourcebox.MediaDriver.client.DataReceiver;
import com.resourcebox.sbe.ListDataMessageDecoder;
import com.resourcebox.sbe.ListStatusMessageDecoder;
import com.resourcebox.sbe.SingleDataMessageDecoder;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.agrona.concurrent.ringbuffer.RingBufferDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

@Service
public class DataReceiverService {

    // Aeron
    private static final String AERON_DIR = "PARCAeron";
    private static final int STREAM_ID = 10;

    // Agrona Ring Buffer
    private static final int BUFFER_CAPACITY = 16 * 1024 * 1024 + RingBufferDescriptor.TRAILER_LENGTH;
    private final RingBuffer ringBuffer = new ManyToOneRingBuffer(
            new UnsafeBuffer(ByteBuffer.allocateDirect(BUFFER_CAPACITY))
    );

    // Message Type ID
    private static final int MSG_TYPE_SINGLE_DATA = 100;
    private static final int MSG_TYPE_LIST_DATA = 200;
    private static final int MSG_TYPE_LIST_STATUS = 300;

    // Worker Thread
    private final AtomicBoolean running = new AtomicBoolean(true);
    private Thread workerThread;

    // Logger
    private final Logger log = LoggerFactory.getLogger(DataReceiverService.class);

    @PostConstruct
    private void run() {
        log.trace("Starting Receiver with Agrona RingBuffer...");

        // Worker Thread 시작
        startWorkerThread();

        DataMessageListener listener = new DataMessageListener() {
            @Override
            public void onSingleDataReceived(SingleDataMessageDecoder decoder) {}

            @Override
            public void onListDataReceived(ListDataMessageDecoder decoder) {
                // Timestamp 순회
                ListDataMessageDecoder.TimestampDecoder timestampDecoder = decoder.timestamp();
                while (timestampDecoder.hasNext()) {
                    timestampDecoder.next();
                }

                // Entries 순회
                ListDataMessageDecoder.EntriesDecoder entries = decoder.entries();
                while (entries.hasNext()) {
                    entries.next();
                }

                // Encoded Length 계산
                DirectBuffer underlyingBuffer = decoder.buffer();
                int offset = decoder.offset();
                int fullEncodedLength = decoder.encodedLength();

                // Ring Buffer 스레드로 메세지 전송
                while (!ringBuffer.write(MSG_TYPE_LIST_DATA, underlyingBuffer, offset, fullEncodedLength)) {
                    Thread.onSpinWait();
                }
            }

            @Override
            public void onListStatusReceived(ListStatusMessageDecoder decoder) {
                // Timestamp 순회
                ListStatusMessageDecoder.TimestampDecoder timestampDecoder = decoder.timestamp();
                while (timestampDecoder.hasNext()) {
                    timestampDecoder.next();
                }

                // Entries 순회
                ListStatusMessageDecoder.EntriesDecoder entries = decoder.entries();
                while (entries.hasNext()) {
                    entries.next();
                }

                // Encoded Length 계산
                DirectBuffer underlyingBuffer = decoder.buffer();
                int offset = decoder.offset();
                int fullEncodedLength = decoder.encodedLength();

                // Ring Buffer 스레드로 메세지 전송
                while (!ringBuffer.write(MSG_TYPE_LIST_STATUS, underlyingBuffer, offset, fullEncodedLength)) {
                    Thread.onSpinWait();
                }
            }
        };

        // Aeron Data Receiver 시작
        DataReceiver receiver = new DataReceiver(AERON_DIR, STREAM_ID, listener);

        // JVM 종료 시 리소스 정리
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            stop();
            receiver.close();
            log.trace("Receiver Shutdown");
        }));
    }

    /**
     * ListDataMessage 데이터의 비즈니스 로직 (사용자 구현 필요)
     */
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

    /**
     * ListStatusMessage 데이터의 비즈니스 로직 (사용자 구현 필요)
     */
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

    private void startWorkerThread() {
        final MessageHandler messageHandler = getMessageHandler();

        final IdleStrategy idleStrategy = new BusySpinIdleStrategy();

        workerThread = new Thread(() -> {
            while (running.get()) {
                int messagesRead = ringBuffer.read(messageHandler);
                idleStrategy.idle(messagesRead);
            }
        }, "aeron-worker-thread");

        workerThread.start();
    }

    private MessageHandler getMessageHandler() {
        final ListStatusMessageDecoder workerDecoderStatus = new ListStatusMessageDecoder();
        final ListDataMessageDecoder workerDecoderData = new ListDataMessageDecoder();

        final MessageHandler messageHandler = (msgTypeId, buffer, offset, length) -> {
            if (msgTypeId == MSG_TYPE_LIST_DATA) {
                workerDecoderData.wrap(
                        buffer,
                        offset,
                        workerDecoderData.sbeBlockLength(),
                        workerDecoderData.sbeSchemaVersion()
                );

                dataMessageBusinessLogic(workerDecoderData);
            } else if (msgTypeId == MSG_TYPE_LIST_STATUS) {
                workerDecoderStatus.wrap(
                        buffer,
                        offset,
                        workerDecoderStatus.sbeBlockLength(),
                        workerDecoderStatus.sbeSchemaVersion()
                );

                statusMessageBusinessLogic(workerDecoderStatus);
            }
        };
        return messageHandler;
    }

    @PreDestroy
    public void stop() {
        running.set(false);
        if (workerThread != null) {
            try {
                workerThread.join(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}