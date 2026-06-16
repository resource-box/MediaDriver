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
import java.util.logging.Logger;

/**
 * Media Driver 환경에서 메시지를 수신하는 클래스입니다.
 */
public class DataReceiver implements AutoCloseable {

    // Aeron
    private final Aeron aeron;
    private final Subscription subscription;
    private final AgentRunner agentRunner;

    // SBE
    private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
    private final SingleDataMessageDecoder singleDataDecoder = new SingleDataMessageDecoder();
    private final ListDataMessageDecoder listDataDecoder = new ListDataMessageDecoder();
    private final ListStatusMessageDecoder listStatusDecoder = new ListStatusMessageDecoder();
    private final DataMessageListener listener;

    // Logger
    private final Logger log = Logger.getLogger(DataReceiver.class.getName());

    public DataReceiver(String aeronDirName, int streamId, DataMessageListener listener) {
        log.info("Connecting DataReceiver to Aeron Media Driver...");

        // Context Setup
        Aeron.Context ctx = new Aeron.Context()
                .aeronDirectoryName(Path.of(System.getProperty("java.io.tmpdir"), aeronDirName).toAbsolutePath().toString());
        this.aeron = Aeron.connect(ctx);
        String channel = "aeron:ipc";
        this.subscription = aeron.addSubscription(channel, streamId);

        // Listener
        this.listener = listener;

        // Agent & Runner
        IdleStrategy idleStrategy = new BusySpinIdleStrategy();
        ReceiverAgent agent = new ReceiverAgent(subscription, this::onFragment);
        this.agentRunner = new AgentRunner(idleStrategy, Throwable::printStackTrace, null, agent);
        AgentRunner.startOnThread(agentRunner);
        
        log.info("DataReceiver initialized and running.");
    }

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
     * Aeron Subscription Polling 작업을 수행하는 Agrona 에이전트입니다.
     * (해당 클래스의 Record 전환 시 성능 저하 발생)
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
