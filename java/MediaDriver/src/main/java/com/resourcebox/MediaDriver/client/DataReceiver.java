package com.resourcebox.MediaDriver.client;

import com.resourcebox.sbe.*;
import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import java.nio.file.Path;

public class DataReceiver implements AutoCloseable {
    private final Aeron aeron;
    private final Subscription subscription;
    private final AgentRunner agentRunner;

    // SBE Decoders (객체 재사용을 통한 Zero-Allocation)
    private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
    private final SingleDataMessageDecoder singleDataDecoder = new SingleDataMessageDecoder();
    private final ListDataMessageDecoder listDataDecoder = new ListDataMessageDecoder();
    private final ListStatusMessageDecoder listStatusDecoder = new ListStatusMessageDecoder();
    private final DataMessageListener listener;

    public DataReceiver(String aeronDirName, int streamId, DataMessageListener listener) {
        this.listener = listener;
        System.out.println("Connecting DataReceiver to Aeron Media Driver...");
        Aeron.Context ctx = new Aeron.Context()
                .aeronDirectoryName(Path.of(System.getProperty("java.io.tmpdir"), aeronDirName).toAbsolutePath().toString());
        this.aeron = Aeron.connect(ctx);
        String channel = "aeron:ipc";
        this.subscription = aeron.addSubscription(channel, streamId);

        // 지속적 수신을 위한 Agent 및 Runner 설정 (별도 스레드에서 무한 Polling)
        IdleStrategy idleStrategy = new BusySpinIdleStrategy();
        ReceiverAgent agent = new ReceiverAgent(subscription, this::onFragment);
        
        this.agentRunner = new AgentRunner(idleStrategy, Throwable::printStackTrace, null, agent);
        AgentRunner.startOnThread(agentRunner);
        
        System.out.println("DataReceiver initialized and running.");
    }

    /**
     * 수신된 바이너리 버퍼를 파싱하여 적절한 처리 로직으로 라우팅합니다.
     */
    private void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
        headerDecoder.wrap(buffer, offset);
        int templateId = headerDecoder.templateId();
        int actingBlockLength = headerDecoder.blockLength();
        int actingVersion = headerDecoder.version();
        int messageOffset = offset + MessageHeaderDecoder.ENCODED_LENGTH;

        switch (templateId) {
            case SingleDataMessageDecoder.TEMPLATE_ID:
                handleSingleData(buffer, messageOffset, actingBlockLength, actingVersion);
                break;
            case ListDataMessageDecoder.TEMPLATE_ID:
                handleListData(buffer, messageOffset, actingBlockLength, actingVersion);
                break;
            case ListStatusMessageDecoder.TEMPLATE_ID:
                handleListStatus(buffer, messageOffset, actingBlockLength, actingVersion);
                break;
            default:
                System.out.println("Unknown template id: " + templateId);
                break;
        }
    }

    private void handleSingleData(DirectBuffer buffer, int offset, int actingBlockLength, int actingVersion) {
        singleDataDecoder.wrap(buffer, offset, actingBlockLength, actingVersion);
        if (listener != null) {
            listener.onSingleDataReceived(singleDataDecoder);
        }
    }

    private void handleListData(DirectBuffer buffer, int offset, int actingBlockLength, int actingVersion) {
        listDataDecoder.wrap(buffer, offset, actingBlockLength, actingVersion);
        if (listener != null) {
            listener.onListDataReceived(listDataDecoder);
        }
    }

    private void handleListStatus(DirectBuffer buffer, int offset, int actingBlockLength, int actingVersion) {
        listStatusDecoder.wrap(buffer, offset, actingBlockLength, actingVersion);
        if (listener != null) {
            listener.onListStatusReceived(listStatusDecoder);
        }
    }

    @Override
    public void close() {
        if (agentRunner != null) agentRunner.close();
        if (subscription != null) subscription.close();
        if (aeron != null) aeron.close();
        System.out.println("DataReceiver closed.");
    }

    /**
     * Aeron Subscription을 지속적으로 Polling하는 Agrona Agent 구현체
     */
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
