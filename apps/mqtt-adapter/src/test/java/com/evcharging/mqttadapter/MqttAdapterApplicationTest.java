package com.evcharging.mqttadapter;

import com.evcharging.mqttadapter.adapter.mqtt.MqttTelemetryMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
class MqttAdapterApplicationTest {

    @Autowired
    private ApplicationContext context;

    @Test
    void startsWithoutBrokerSettings() {
        assertThat(context.getBeansOfType(MqttTelemetryMapper.class)).hasSize(1);
        assertThat(context.containsBean("mqttTelemetryFlow")).isFalse();
        assertThat(context.containsBean("mqttClientFactory")).isFalse();
        assertThat(context.containsBean("kafkaTelemetryEventPublisher")).isFalse();
    }
}
