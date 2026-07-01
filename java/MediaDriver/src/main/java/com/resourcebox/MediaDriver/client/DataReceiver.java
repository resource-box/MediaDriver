package com.resourcebox.MediaDriver.client;

import com.resourcebox.sbe.*;
import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import io.aeron.FragmentAssembler;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.*;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.agrona.concurrent.ringbuffer.RingBufferDescriptor;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

/**
 * Media Driver 환경에서 메시지를 수신하고 비즈니스 로직을 수행하는 클래스입니다.
 */
public class DataReceiver implements AutoCloseable {

    // Aeron
    private final Aeron aeron;
    private final Subscription subscription;
    private final AgentRunner agentRunner;
    private final DataMessageListener listener;

    // Agrona Ring Buffer & Worker
    private static final int BUFFER_CAPACITY = 16 * 1024 * 1024 + RingBufferDescriptor.TRAILER_LENGTH;
    private final RingBuffer ringBuffer;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private Thread workerThread;

    // SBE
    private final SingleDataMessageDecoder singleDataDecoder = new SingleDataMessageDecoder();
    private final ListDataMessageDecoder listDataDecoder = new ListDataMessageDecoder();
    private final ListStatusMessageDecoder listStatusDecoder = new ListStatusMessageDecoder();
    private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();

    // Logger
    private final Logger log = Logger.getLogger(DataReceiver.class.getName());

    public DataReceiver(String aeronDirName, int streamId, DataMessageListener listener) {
        log.info(">> INITIALIZING RECEIVER..");

        // Listener 주입
        this.listener = listener;

        // Media Driver 연결 및 구독
        Aeron.Context ctx = new Aeron.Context()
                .aeronDirectoryName(Path.of(System.getProperty("java.io.tmpdir"), aeronDirName).toAbsolutePath().toString());
        this.aeron = Aeron.connect(ctx);
        this.subscription = aeron.addSubscription("aeron:ipc", streamId);

        log.info("MEDIA DRIVER CONNECTED - Channel: aeron:ipc, StreamId: " + streamId);

        // Ring Buffer 초기화 및 Worker Thread 시작
        this.ringBuffer = new ManyToOneRingBuffer(
                new UnsafeBuffer(ByteBuffer.allocateDirect(BUFFER_CAPACITY))
        );
        startWorkerThread();

        log.info("WORKER THREAD STARTED - RingBuffer Capacity: " + BUFFER_CAPACITY);

        // Agent Runner 초기화 및 시작
        IdleStrategy idleStrategy = new BusySpinIdleStrategy();
        FragmentAssembler assembler = new FragmentAssembler(this::onFragment);
        ReceiverAgent agent = new ReceiverAgent(subscription, assembler);
        this.agentRunner = new AgentRunner(idleStrategy, Throwable::printStackTrace, null, agent);
        AgentRunner.startOnThread(agentRunner);

        log.info("AGENT RUNNER STARTED");
    }

    private void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
        headerDecoder.wrap(buffer, offset);
        int templateId = headerDecoder.templateId();

        // ** <중요> ENCODED_LENGTH 파라미터를 사용해 순회 없이 length 계산
        int messageOffset = offset + MessageHeaderDecoder.ENCODED_LENGTH;
        int messageLength = length - MessageHeaderDecoder.ENCODED_LENGTH;

        // ** <중요> Ring Buffer Write
        while (!ringBuffer.write(templateId, buffer, messageOffset, messageLength)) {
            Thread.onSpinWait();
        }
    }

    private void startWorkerThread() {
        final MessageHandler messageHandler = (msgTypeId, buffer, offset, length) -> {
            switch (msgTypeId) {
                case SingleDataMessageDecoder.TEMPLATE_ID:
                    singleDataDecoder.wrap(buffer, offset, singleDataDecoder.sbeBlockLength(), singleDataDecoder.sbeSchemaVersion());
                    if (listener != null) listener.onSingleDataReceived(singleDataDecoder);
                    break;
                case ListDataMessageDecoder.TEMPLATE_ID:
                    listDataDecoder.wrap(buffer, offset, listDataDecoder.sbeBlockLength(), listDataDecoder.sbeSchemaVersion());
                    if (listener != null) listener.onListDataReceived(listDataDecoder);
                    break;
                case ListStatusMessageDecoder.TEMPLATE_ID:
                    listStatusDecoder.wrap(buffer, offset, listStatusDecoder.sbeBlockLength(), listStatusDecoder.sbeSchemaVersion());
                    if (listener != null) listener.onListStatusReceived(listStatusDecoder);
                    break;
                default:
                    log.warning("Unknown msgTypeId from RingBuffer: " + msgTypeId);
            }
        };

        workerThread = new Thread(() -> {
            final IdleStrategy idleStrategy = new BusySpinIdleStrategy();
            while (running.get()) {
                int messagesRead = ringBuffer.read(messageHandler);
                idleStrategy.idle(messagesRead);
            }
        }, "data-receiver-worker");
        workerThread.start();
    }

    @Override
    public void close() {
        log.info(">> CLOSING RECEIVER..");
        running.set(false);
        if (agentRunner != null) agentRunner.close();
        if (subscription != null) subscription.close();
        if (aeron != null) aeron.close();

        if (workerThread != null) {
            try {
                workerThread.join(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        log.info("DATA RECEIVER CLOSED");
    }

    private static class ReceiverAgent implements Agent {
        private final Subscription subscription;
        private final FragmentHandler fragmentHandler;
        private static final int FRAGMENT_LIMIT = 10;

        public ReceiverAgent(Subscription subscription, FragmentHandler fragmentHandler) {
            this.subscription = subscription;
            this.fragmentHandler = fragmentHandler;
        }

        @Override
        public int doWork() {
            return subscription.poll(fragmentHandler, FRAGMENT_LIMIT);
        }

        @Override
        public String roleName() {
            return "data-receiver-agent";
        }
    }

}