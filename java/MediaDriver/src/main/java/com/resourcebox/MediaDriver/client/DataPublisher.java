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

    // Aeron
    private final Aeron aeron;
    private final Publication publication;
    private final UnsafeBuffer buffer;

    // SBE
    private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
    private final SingleDataMessageEncoder singleDataEncoder = new SingleDataMessageEncoder();
    private final ListDataMessageEncoder listDataEncoder = new ListDataMessageEncoder();
    private final ListStatusMessageEncoder listStatusEncoder = new ListStatusMessageEncoder();
    
    // Aeron offer 재시도를 위한 IdleStrategy (Zero-Allocation 및 Park 방지를 위해 BusySpin 사용)
    private final IdleStrategy idleStrategy = new BusySpinIdleStrategy();

    // Logger
    private final Logger log = LoggerFactory.getLogger(DataPublisher.class);

    public DataPublisher(String aeronDir, int streamId, int capacity, int alignment) {
        log.info("Connecting to Aeron Media Driver...");

        // Context Setup
        Aeron.Context ctx = new Aeron.Context()
                .aeronDirectoryName(Path.of(System.getProperty("java.io.tmpdir"), aeronDir).toAbsolutePath().toString());
        this.aeron = Aeron.connect(ctx);
        String channel = "aeron:ipc";
        this.publication = aeron.addPublication(channel, streamId);
        
        // ** 메시지 조립용 Off-heap 버퍼 : Zero-Allocation을 위해 재사용 **
        ByteBuffer byteBuffer = BufferUtil.allocateDirectAligned(capacity, alignment);
        this.buffer = new UnsafeBuffer(byteBuffer);

        log.info("DataPublisher initialized.");
    }

    /**
     * SingleData 발행
     */
    public void publishSingleData(int id, String value, String timestamp) {
        singleDataEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
                .id(id)
                .value(value)
                .timestamp(timestamp);

        // 메세지 데이터 발행
        int length = MessageHeaderEncoder.ENCODED_LENGTH + singleDataEncoder.encodedLength();
        offerToPublication(length);
    }

    /**
     * ListDataMessage 발행
     */
    public void publishListData(String timestamp, int[] ids, double[] values) {
        if (ids.length != values.length) {
            throw new IllegalArgumentException("ids and values array length must match");
        }

        listDataEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder);
        
        // Timestamp
        listDataEncoder.timestampCount(1).next().value(timestamp);
        
        // Entries
        ListDataMessageEncoder.EntriesEncoder entriesEncoder = listDataEncoder.entriesCount(ids.length);
        for (int i = 0; i < ids.length; i++) {
            entriesEncoder.next()
                    .id(ids[i])
                    .value(values[i]);
        }

        // 메세지 데이터 발행
        int length = MessageHeaderEncoder.ENCODED_LENGTH + listDataEncoder.encodedLength();
        offerToPublication(length);
    }

    /**
     * ListStatusMessage 발행
     */
    public void publishListStatus(String timestamp, int[] ids, String[] values) {
        if (ids.length != values.length) {
            throw new IllegalArgumentException("ids and values array length must match");
        }

        listStatusEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder);
        
        // Timestamp
        listStatusEncoder.timestampCount(1).next().value(timestamp);
        
        // Entries
        ListStatusMessageEncoder.EntriesEncoder entriesEncoder = listStatusEncoder.entriesCount(ids.length);
        for (int i = 0; i < ids.length; i++) {
            entriesEncoder.next()
                    .id(ids[i])
                    .value(values[i]);
        }

        // 메세지 데이터 발행
        int length = MessageHeaderEncoder.ENCODED_LENGTH + listStatusEncoder.encodedLength();
        offerToPublication(length);
    }

    private void offerToPublication(int length) {
        idleStrategy.reset();
        while (true) {
            long result = publication.offer(buffer, 0, length);
            // Succeed
            if (result >= 0) { break; }
            // Failed - Backpressure, Admin Action, Not Connected
            else if (result == Publication.BACK_PRESSURED ||
                       result == Publication.ADMIN_ACTION || 
                       result == Publication.NOT_CONNECTED) {
                idleStrategy.idle();
            } else if (result == Publication.CLOSED || result == Publication.MAX_POSITION_EXCEEDED) {
                throw new IllegalStateException("Publication failed with code: " + result);
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
