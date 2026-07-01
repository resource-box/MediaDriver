package com.resourcebox.MediaDriver.client;

import com.resourcebox.sbe.ListDataMessageEncoder;
import com.resourcebox.sbe.ListStatusMessageEncoder;
import com.resourcebox.sbe.MessageHeaderEncoder;
import com.resourcebox.sbe.SingleDataMessageEncoder;
import io.aeron.Aeron;
import io.aeron.Publication;
import org.agrona.BufferUtil;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.ByteBuffer;
import java.nio.file.Path;

/**
 * Media Driver 환경으로 메세지 데이터를 발행하는 클래스입니다.
 */
public class DataPublisher implements AutoCloseable {

    private final Aeron aeron;
    private final Publication publication;

    private final String aeronDir;
    private final int capacity;
    private final int alignment;
    private final int streamId;

    // SBE Encoders and Buffer wrapped in ThreadLocal for zero-allocation Thread-Safety
    private static class PublisherState {
        final UnsafeBuffer buffer;
        final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
        final SingleDataMessageEncoder singleDataEncoder = new SingleDataMessageEncoder();
        final ListDataMessageEncoder listDataEncoder = new ListDataMessageEncoder();
        final ListStatusMessageEncoder listStatusEncoder = new ListStatusMessageEncoder();

        PublisherState(int capacity, int alignment) {
            ByteBuffer byteBuffer = BufferUtil.allocateDirectAligned(capacity, alignment);
            this.buffer = new UnsafeBuffer(byteBuffer);
        }
    }

    private final ThreadLocal<PublisherState> threadState;

    // Aeron offer 재시도를 위한 IdleStrategy (Zero-Allocation 및 Park 방지를 위해 BusySpin 사용)
    private final IdleStrategy idleStrategy = new BusySpinIdleStrategy();

    // Logger
    private final Logger log = LoggerFactory.getLogger(DataPublisher.class);

    public DataPublisher(String aeronDir, int streamId, int capacity, int alignment) {
        log.info(">> CONNECTING TO MEDIA DRIVER..");

        this.aeronDir = aeronDir;
        this.streamId = streamId;
        this.capacity = capacity;
        this.alignment = alignment;

        // Context Setup
        Aeron.Context ctx = new Aeron.Context()
                .aeronDirectoryName(Path.of(System.getProperty("java.io.tmpdir"), this.aeronDir).toAbsolutePath().toString());
        this.aeron = Aeron.connect(ctx);
        this.publication = aeron.addPublication("aeron:ipc", this.streamId);

        this.threadState = ThreadLocal.withInitial(() -> new PublisherState(capacity, alignment));

        log.info("DATA PUBLISHER CONNECTED - StreamId: {}, Capacity: {}, Alignment: {}", streamId, capacity, alignment);
    }

    /**
     * SingleData 발행
     */
    public boolean publishSingleData(int id, String value, String timestamp) {
        PublisherState state = threadState.get();
        state.singleDataEncoder.wrapAndApplyHeader(state.buffer, 0, state.headerEncoder)
                .id(id)
                .value(value)
                .timestamp(timestamp);

        // 메세지 데이터 발행
        int length = MessageHeaderEncoder.ENCODED_LENGTH + state.singleDataEncoder.encodedLength();
        return offerToPublication(state.buffer, length);
    }

    /**
     * ListDataMessage 발행
     */
    public boolean publishListData(String timestamp, int[] ids, double[] values) {
        if (ids.length != values.length) {
            throw new IllegalArgumentException("ids and values array length must match");
        }

        PublisherState state = threadState.get();
        state.listDataEncoder.wrapAndApplyHeader(state.buffer, 0, state.headerEncoder);

        // Timestamp
        state.listDataEncoder.timestampCount(1).next().value(timestamp);

        // Entries
        ListDataMessageEncoder.EntriesEncoder entriesEncoder = state.listDataEncoder.entriesCount(ids.length);
        for (int i = 0; i < ids.length; i++) {
            entriesEncoder.next()
                    .id(ids[i])
                    .value(values[i]);
        }

        // 메세지 데이터 발행
        int length = MessageHeaderEncoder.ENCODED_LENGTH + state.listDataEncoder.encodedLength();
        return offerToPublication(state.buffer, length);
    }

    /**
     * ListStatusMessage 발행
     */
    public boolean publishListStatus(String timestamp, int[] ids, String[] values) {
        if (ids.length != values.length) {
            throw new IllegalArgumentException("IDS SIZE != VALUES SIZE");
        }

        PublisherState state = threadState.get();
        state.listStatusEncoder.wrapAndApplyHeader(state.buffer, 0, state.headerEncoder);

        // Timestamp
        state.listStatusEncoder.timestampCount(1).next().value(timestamp);

        // Entries
        ListStatusMessageEncoder.EntriesEncoder entriesEncoder = state.listStatusEncoder.entriesCount(ids.length);
        for (int i = 0; i < ids.length; i++) {
            entriesEncoder.next()
                    .id(ids[i])
                    .value(values[i]);
        }

        // 메세지 데이터 발행
        int length = MessageHeaderEncoder.ENCODED_LENGTH + state.listStatusEncoder.encodedLength();
        return offerToPublication(state.buffer, length);
    }

    private boolean offerToPublication(UnsafeBuffer buffer, int length) {
        // 루프 시작 전 IdleStrategy 상태 초기화
        idleStrategy.reset();

        while (true) {
            long result = publication.offer(buffer, 0, length);

            // SUCCEED
            if (result >= 0) {
                return true;
            }
            // ADMIN ACTION :: Retry
            else if (result == Publication.ADMIN_ACTION) {
                idleStrategy.idle();
            }
            // BACK PRESSURE & NOT CONNECTED & CLOSED :: Drop
            else {
                log.warn(">> FAILED TO PUBLISH at STREAM ID {} : {}", streamId, result);
                return false;
            }
        }
    }

    @Override
    public void close() {
        if (publication != null) publication.close();
        if (aeron != null) aeron.close();
        log.info("DataPublisher closed.");
    }
}
