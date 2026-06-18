package com.resourcebox.AeronSubscriber.service;


import com.resourcebox.Disruptors.Event;
import com.resourcebox.Disruptors.Handler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;

public class RecordsHandler extends Handler<byte[]> {

    // Logger
    private static final Logger log = LoggerFactory.getLogger(RecordsHandler.class);



    public RecordsHandler() {

    }
    
    /**
     * LMAX Disruptor 이벤트 발생 시점에 호출되는 메서드입니다.
     * @param event 수신된 데이터 이벤트 객체
     */
    @Override
    protected void process(Event<byte[]> event) {

    }

}
