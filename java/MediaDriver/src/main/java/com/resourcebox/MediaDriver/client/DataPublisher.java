package com.resourcebox.MediaDriver.client;

import com.resourcebox.sbe.ListDataMessageEncoder;
import com.resourcebox.sbe.ListStatusMessageEncoder;
import com.resourcebox.sbe.MessageHeaderEncoder;
import com.resourcebox.sbe.SingleDataMessageEncoder;
import io.aeron.Aeron;
import io.aeron.Publication;
import org.agrona.BufferUtil;

import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

public class DataPublisher implements AutoCloseable {
    private final Aeron aeron;
    private final Publication publication;
    private final UnsafeBuffer buffer;

    private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
    private final SingleDataMessageEncoder singleDataEncoder = new SingleDataMessageEncoder();
    private final ListDataMessageEncoder listDataEncoder = new ListDataMessageEncoder();
    private final ListStatusMessageEncoder listStatusEncoder = new ListStatusMessageEncoder();
    
    // Aeron offer 재시도를 위한 IdleStrategy (Zero-Allocation 및 Park 방지를 위해 BusySpin 사용)
    private final IdleStrategy idleStrategy = new org.agrona.concurrent.BusySpinIdleStrategy();

    public DataPublisher(String aeronDirName, int streamId) {
        System.out.println("Connecting to Aeron Media Driver...");

        Aeron.Context ctx = new Aeron.Context()
                .aeronDirectoryName(Path.of(System.getProperty("java.io.tmpdir"), aeronDirName).toAbsolutePath().toString());
        this.aeron = Aeron.connect(ctx);
        String channel = "aeron:ipc";
        this.publication = aeron.addPublication(channel, streamId);
        
        // 메시지 조립용 Off-heap 버퍼 (Zero-Allocation을 위해 재사용)
        ByteBuffer byteBuffer = BufferUtil.allocateDirectAligned(4096, 64);
        this.buffer = new UnsafeBuffer(byteBuffer);
        System.out.println("DataPublisher initialized.");
    }

    /**
     * SingleData 발행
     * Aeron Publication의 offer는 Non-blocking이며 호출 스레드를 멈추지 않습니다.
     * Backpressure 발생 시에만 IdleStrategy를 통해 짧은 시간 재시도합니다.
     */
    public void publishSingleData(int id, String value, String timestamp) {
        singleDataEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
                .id(id)
                .value(value)
                .timestamp(timestamp);

        int length = MessageHeaderEncoder.ENCODED_LENGTH + singleDataEncoder.encodedLength();
        offerToPublication(length);
    }

    /**
     * ListDataMessage 발행
     * 객체 할당(Allocation)을 피하기 위해 primitive 배열 사용
     */
    public void publishListData(String timestamp, int[] ids, double[] values) {
        if (ids.length != values.length) {
            throw new IllegalArgumentException("ids and values array length must match");
        }

        listDataEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder);
        
        // Timestamp 그룹 (SBE 구조상 count를 받으므로 1개 설정 후 입력)
        listDataEncoder.timestampCount(1).next().value(timestamp);
        
        // Entries 그룹
        ListDataMessageEncoder.EntriesEncoder entriesEncoder = listDataEncoder.entriesCount(ids.length);
        for (int i = 0; i < ids.length; i++) {
            entriesEncoder.next()
                    .id(ids[i])
                    .value(values[i]);
        }

        int length = MessageHeaderEncoder.ENCODED_LENGTH + listDataEncoder.encodedLength();
        offerToPublication(length);
    }

    /**
     * ListStatusMessage 발행
     * 객체 할당(Allocation)을 피하기 위해 primitive/String 배열 사용
     */
    public void publishListStatus(String timestamp, int[] ids, String[] values) {
        if (ids.length != values.length) {
            throw new IllegalArgumentException("ids and values array length must match");
        }

        listStatusEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder);
        
        // Timestamp 그룹
        listStatusEncoder.timestampCount(1).next().value(timestamp);
        
        // Entries 그룹
        ListStatusMessageEncoder.EntriesEncoder entriesEncoder = listStatusEncoder.entriesCount(ids.length);
        for (int i = 0; i < ids.length; i++) {
            entriesEncoder.next()
                    .id(ids[i])
                    .value(values[i]);
        }

        int length = MessageHeaderEncoder.ENCODED_LENGTH + listStatusEncoder.encodedLength();
        offerToPublication(length);
    }

    private void offerToPublication(int length) {
        idleStrategy.reset();
        while (true) {
            long result = publication.offer(buffer, 0, length);
            if (result >= 0) {
                break; // 발행 성공, 즉시 리턴하여 호출자의 다음 작업 방해 안 함
            } else if (result == Publication.BACK_PRESSURED || 
                       result == Publication.ADMIN_ACTION || 
                       result == Publication.NOT_CONNECTED) {
                // Backpressure 등의 이유로 실패 시 짧게 대기 후 재시도
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
        System.out.println("DataPublisher closed.");
    }
}
