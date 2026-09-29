package com.evcharging.mqttadapter;

import com.evcharging.mqttadapter.adapter.mqtt.KafkaTelemetryEventPublisher;
import com.evcharging.messaging.contract.ChargerTelemetryPayload;
import com.evcharging.messaging.contract.ChargerTelemetryReceived;
import com.evcharging.messaging.contract.EvseId;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.NotEnoughReplicasException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.LoggingProducerListener;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.test.util.ReflectionTestUtils;
import tools.jackson.databind.ObjectMapper;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=localhost:9092",
        "telemetry.kafka-topic=charger.telemetry",
        "logging.level.org.springframework.kafka=DEBUG"
})
@ExtendWith(OutputCaptureExtension.class)
class KafkaPublisherConfigurationTest {

    @Autowired
    private ApplicationContext context;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Test
    void configuresRealKafkaTemplateAndPublisherWithoutMqttConnection() {
        assertThat(context.getBeansOfType(KafkaTemplate.class)).hasSize(1);
        assertThat(context.getBeansOfType(KafkaTelemetryEventPublisher.class)).hasSize(1);
        assertThat(context.containsBean("mqttTelemetryFlow")).isFalse();
        assertThat(context.getBeansOfType(ProducerListener.class)).hasSize(1);
        assertThat(ReflectionTestUtils.getField(kafkaTemplate, "producerListener"))
                .isSameAs(context.getBean("telemetryProducerListener"))
                .isNotInstanceOf(LoggingProducerListener.class);
        var producerConfig = new ProducerConfig(kafkaTemplate.getProducerFactory().getConfigurationProperties());
        assertThat(producerConfig.getString(ProducerConfig.ACKS_CONFIG)).isEqualTo("-1");
    }

    @Test
    @SuppressWarnings("unchecked")
    void realKafkaTemplateFailureUsesOneSafePublisherLog(CapturedOutput output) {
        MockProducer<String, String> producer = new MockProducer<>(false, null, new StringSerializer(), new StringSerializer());
        ProducerFactory<String, String> factory = mock(ProducerFactory.class);
        when(factory.createProducer()).thenReturn(producer);
        KafkaTemplate<String, String> template = new KafkaTemplate<>(factory);
        template.setProducerListener((ProducerListener<String, String>) context.getBean("telemetryProducerListener"));
        ObjectMapper mapper = mock(ObjectMapper.class);
        Instant timestamp = Instant.parse("2024-01-01T00:00:00Z");
        var event = ChargerTelemetryReceived.create(UUID.randomUUID(), timestamp, timestamp,
                "sample-key-not-for-logs", new EvseId(1), null,
                new ChargerTelemetryPayload(timestamp.toEpochMilli(), true,
                        BigDecimal.ONE, BigDecimal.ONE, BigDecimal.ONE));
        when(mapper.writeValueAsString(event)).thenReturn("sample-value-not-for-logs");
        var publisher = new KafkaTelemetryEventPublisher(template, mapper, "charger.telemetry");

        publisher.publish(event);
        assertThat(producer.errorNext(new NotEnoughReplicasException("sample-error-not-for-logs"))).isTrue();

        assertThat(output.getAll().lines().filter(line -> line.contains("telemetry_kafka_publication")))
                .singleElement().asString().contains("stage=async", "eventId=" + event.eventId(),
                        "causeType=NotEnoughReplicasException");
        assertThat(output.getAll()).doesNotContain("sample-key-not-for-logs", "sample-value-not-for-logs",
                "sample-error-not-for-logs", "Exception thrown when sending a message");
    }
}
