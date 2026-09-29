package com.evcharging.telemetryworker;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Import;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = {
        "mqtt.url=tcp://localhost:1883",
        "mqtt.topic=charger/telemetry",
        "mqtt.client-id=worker-must-not-connect",
        "spring.kafka.bootstrap-servers=localhost:9092",
        "telemetry.kafka-topic=charger.telemetry"
})
@Import(PostgreSqlTestConfiguration.class)
class TelemetryWorkerApplicationTest {

    @Autowired
    private ApplicationContext context;

    @Test
    void startsWithoutMqttIngressEvenWhenLegacySettingsArePresent() {
        assertThat(context.containsBean("mqttTelemetryFlow")).isFalse();
        assertThat(context.containsBean("mqttClientFactory")).isFalse();
        assertThat(context.containsBean("mqttTelemetryMapper")).isFalse();
        assertThat(context.containsBean("kafkaTelemetryEventPublisher")).isFalse();
    }
}
