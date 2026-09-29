package com.evcharging.mqttadapter.adapter.mqtt;

import com.evcharging.messaging.contract.ChargerTelemetryPayload;
import com.evcharging.messaging.contract.DomainEventEnvelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

@Component
@ConditionalOnProperty(name = {"telemetry.kafka-topic", "spring.kafka.bootstrap-servers"})
public class KafkaTelemetryEventPublisher implements TelemetryEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(KafkaTelemetryEventPublisher.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String topic;

    public KafkaTelemetryEventPublisher(
            KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper,
            @Value("${telemetry.kafka-topic}") String topic
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.topic = topic;
    }

    @Override
    public void publish(DomainEventEnvelope<ChargerTelemetryPayload> event) {
        String json;
        try {
            json = objectMapper.writeValueAsString(event);
        } catch (JacksonException exception) {
            logFailure(event, "json_serialization", exception);
            throw new IllegalStateException("failed to serialize telemetry event", exception);
        }

        CompletableFuture<SendResult<String, String>> future;
        try {
            future = kafkaTemplate.send(topic, event.stationId(), json);
        } catch (RuntimeException exception) {
            logFailure(event, "send", exception);
            throw exception;
        }

        future.whenComplete((result, exception) -> {
            if (exception != null) {
                logFailure(event, "async", exception);
                return;
            }
            var metadata = result.getRecordMetadata();
            log.info("telemetry_kafka_publication outcome=completed eventId={} topic={} partition={} offset={}",
                    event.eventId(), metadata.topic(), metadata.partition(), metadata.offset());
        });
    }

    private void logFailure(DomainEventEnvelope<ChargerTelemetryPayload> event, String stage, Throwable exception) {
        Throwable cause = exception;
        Throwable rootCause = exception;
        boolean deliveryUnknown = false;
        Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        while (cause != null && visited.add(cause)) {
            rootCause = cause;
            if (cause instanceof org.apache.kafka.common.errors.TimeoutException
                    || cause instanceof java.util.concurrent.TimeoutException) {
                deliveryUnknown = true;
            }
            cause = cause.getCause();
        }
        // Exception messages and stack traces can contain the record or source payload.
        log.warn("telemetry_kafka_publication outcome={} stage={} eventId={} topic={} errorType={} causeType={}",
                deliveryUnknown ? "unknown" : "failed", stage, event.eventId(), topic,
                exception.getClass().getSimpleName(), rootCause.getClass().getSimpleName());
    }
}
