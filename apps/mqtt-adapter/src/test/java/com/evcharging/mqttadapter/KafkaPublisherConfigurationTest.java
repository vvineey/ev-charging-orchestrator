package com.evcharging.mqttadapter;

import com.evcharging.mqttadapter.adapter.mqtt.KafkaTelemetryEventPublisher;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.core.KafkaTemplate;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=localhost:9092",
        "telemetry.kafka-topic=charger.telemetry"
})
class KafkaPublisherConfigurationTest {

    @Autowired
    private ApplicationContext context;

    @Test
    void configuresRealKafkaTemplateAndPublisherWithoutMqttConnection() {
        assertThat(context.getBeansOfType(KafkaTemplate.class)).hasSize(1);
        assertThat(context.getBeansOfType(KafkaTelemetryEventPublisher.class)).hasSize(1);
        assertThat(context.containsBean("mqttTelemetryFlow")).isFalse();
    }
}
