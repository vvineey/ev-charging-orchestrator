package com.evcharging.telemetryworker.adapter.kafka;

import com.evcharging.telemetryworker.application.StoreLatestTelemetryService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Component
@ConditionalOnProperty(name = {"telemetry.kafka-topic", "spring.kafka.bootstrap-servers"})
public class TelemetryRecordListener {
    public static final String GROUP_ID = "telemetry-current-state-v1";
    private static final Logger log = LoggerFactory.getLogger(TelemetryRecordListener.class);
    private final TelemetryRecordDecoder decoder;
    private final StoreLatestTelemetryService service;

    public TelemetryRecordListener(TelemetryRecordDecoder decoder, StoreLatestTelemetryService service) {
        this.decoder = decoder;
        this.service = service;
    }

    @KafkaListener(id = "telemetry-current-state", groupId = GROUP_ID,
            topics = "${telemetry.kafka-topic}", containerFactory = "telemetryKafkaListenerContainerFactory")
    public void consume(ConsumerRecord<String, String> record) {
        try {
            var event = decoder.decode(record.key(), record.value());
            try {
                // This separate proxied service returns only after the DB transaction commits.
                service.store(event);
            } catch (RuntimeException exception) {
                throw new TelemetryConsumptionException("database", exception.getClass().getSimpleName(), event.eventId());
            }
        } catch (TelemetryConsumptionException exception) {
            log.error("telemetry_consume outcome=failed stage={} topic={} partition={} offset={} eventId={} failedAt={} reason={}",
                    exception.stage(), record.topic(), record.partition(), record.offset(),
                    exception.eventId(), Instant.now(), exception.reason());
            throw exception;
        }
    }
}
