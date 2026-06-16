package com.resourcebox.MediaDriver.client;

import com.resourcebox.sbe.ListDataMessageDecoder;
import com.resourcebox.sbe.ListStatusMessageDecoder;
import com.resourcebox.sbe.SingleDataMessageDecoder;

/**
 * DataReceiver에서 수신된 메시지를 처리하기 위한 콜백 메서드를 정의합니다.
 */
public interface DataMessageListener {

    void onSingleDataReceived(SingleDataMessageDecoder decoder);
    void onListDataReceived(ListDataMessageDecoder decoder);
    void onListStatusReceived(ListStatusMessageDecoder decoder);

}
