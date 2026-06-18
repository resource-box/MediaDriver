package com.resourcebox.AeronSubscriber.service;

import com.resourcebox.Disruptors.Handler;
import com.resourcebox.Disruptors.StreamAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import java.util.List;

/**
 * LMAX Disruptor 기반 스트림 처리를 위한 Spring Configuration 클래스입니다.
 * 핸들러 빈과 스트림 자동 구성 빈을 정의하여 LMAX Disruptor와의 통합을 지원합니다.
 */
@Configuration
@Import(StreamAutoConfiguration.class)
public class RecordsStreamConfig {

    @Bean
    public List<Handler<byte[]>> handlers() {
        return List.of(new RecordsHandler());
    }

    @Bean
    public StreamAutoConfiguration<byte[]> streamAutoConfiguration() {
        return new StreamAutoConfiguration<>();
    }

}