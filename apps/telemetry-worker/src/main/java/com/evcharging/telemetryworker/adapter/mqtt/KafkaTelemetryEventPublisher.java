package com.evcharging.telemetryworker.adapter.mqtt;

import com.evcharging.messaging.contract.ChargerTelemetryPayload;
import com.evcharging.messaging.contract.DomainEventEnvelope;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(prefix = "telemetry", name = "kafka-topic")
public class KafkaTelemetryEventPublisher implements TelemetryEventPublisher {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String topic;

    public KafkaTelemetryEventPublisher(
            KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper,
            TelemetryWorkerProperties properties
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.topic = properties.kafkaTopic();
    }

    @Override
    public void publish(DomainEventEnvelope<ChargerTelemetryPayload> event) {
        try {
            String json = objectMapper.writeValueAsString(event);
            kafkaTemplate.send(topic, event.stationId(), json);
        } catch (JacksonException exception) {
            throw new IllegalStateException("failed to serialize telemetry event", exception);
        }
    }
}
