package com.resourcebox.MediaDriver.client;

import com.resourcebox.sbe.ListDataMessageDecoder;
import com.resourcebox.sbe.ListStatusMessageDecoder;
import com.resourcebox.sbe.SingleDataMessageDecoder;

public interface DataMessageListener {
    // 디코더 객체(포인터)를 직접 전달
    void onSingleDataReceived(SingleDataMessageDecoder decoder);
    void onListDataReceived(ListDataMessageDecoder decoder);
    void onListStatusReceived(ListStatusMessageDecoder decoder);
}
