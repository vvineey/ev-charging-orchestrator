package com.evcharging.ocppgateway;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.nio.charset.StandardCharsets;

@Component
@ConditionalOnProperty(name = "ocpp.ingress.enabled", havingValue = "true")
class TransactionKafkaPublisher {
    private static final Logger log = LoggerFactory.getLogger(TransactionKafkaPublisher.class);

    private final KafkaTemplate<String, String> template;
    private final ObjectMapper mapper;
    private final String topic;

    TransactionKafkaPublisher(KafkaTemplate<String, String> template, ObjectMapper mapper,
            @Value("${ocpp.kafka-topic}") String topic) {
        this.template = template;
        this.mapper = mapper;
        this.topic = topic;
    }

    void publish(TransactionEventMapper.TransactionRecord record) {
        try {
            String json = mapper.writeValueAsString(record.json());
            var result = template.send(topic, record.stationId(), json).get(10, TimeUnit.SECONDS);
            var metadata = result.getRecordMetadata();
            log.info("ocpp_transaction_publication outcome=completed eventId={} topic={} partition={} offset={} bytes={}",
                    record.eventId(), metadata.topic(), metadata.partition(), metadata.offset(),
                    json.getBytes(StandardCharsets.UTF_8).length);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            logFailure(record, "unknown", exception);
            throw new PublicationFailure(exception);
        } catch (ExecutionException | TimeoutException | RuntimeException exception) {
            logFailure(record, deliveryUnknown(exception) ? "unknown" : "failed", exception);
            throw new PublicationFailure(exception);
        }
    }

    private boolean deliveryUnknown(Throwable exception) {
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        for (Throwable cause = exception; cause != null && seen.add(cause); cause = cause.getCause()) {
            if (cause instanceof TimeoutException || cause instanceof org.apache.kafka.common.errors.TimeoutException) {
                return true;
            }
        }
        return false;
    }

    private void logFailure(TransactionEventMapper.TransactionRecord record, String outcome, Exception exception) {
        // Exception messages and stack traces may contain the submitted record or credentials.
        log.warn("ocpp_transaction_publication outcome={} eventId={} topic={} errorType={}",
                outcome, record.eventId(), topic, exception.getClass().getSimpleName());
    }

    static final class PublicationFailure extends RuntimeException {
        PublicationFailure(Throwable cause) {
            super("transaction publication failed", cause);
        }
    }
}
