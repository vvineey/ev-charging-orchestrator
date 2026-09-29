package com.evcharging.mqttadapter.adapter.mqtt;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.ProducerListener;

@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = {"telemetry.kafka-topic", "spring.kafka.bootstrap-servers"})
public class TelemetryKafkaConfiguration {

    @Bean
    ProducerListener<Object, Object> telemetryProducerListener() {
        // The publisher observes the Future; the default listener would log record contents a second time.
        return new ProducerListener<>() { };
    }
}
