package com.evcharging.ocppgateway;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.ProducerListener;

@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = "ocpp.ingress.enabled", havingValue = "true")
class OcppKafkaConfiguration {
    @Bean
    ProducerListener<Object, Object> ocppProducerListener() {
        // The publisher observes the Future; the default listener may print record contents.
        return new ProducerListener<>() { };
    }
}
