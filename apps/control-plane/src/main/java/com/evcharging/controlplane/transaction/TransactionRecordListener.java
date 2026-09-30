package com.evcharging.controlplane.transaction;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Component
@ConditionalOnProperty(name = "transaction.ingress.enabled", havingValue = "true")
class TransactionRecordListener {
    static final String GROUP_ID = "transaction-inbox-v1";
    private static final Logger log = LoggerFactory.getLogger(TransactionRecordListener.class);
    private final TransactionRecordDecoder decoder;
    private final StoreTransactionObservation service;

    TransactionRecordListener(TransactionRecordDecoder decoder, StoreTransactionObservation service) {
        this.decoder = decoder;
        this.service = service;
    }

    @KafkaListener(id = "transaction-inbox", groupId = GROUP_ID,
            topics = "${transaction.kafka-topic}", containerFactory = "transactionKafkaListenerContainerFactory")
    public void consume(ConsumerRecord<String, String> record) {
        ObservedTransactionRecord event;
        try {
            event = decoder.decode(record.key(), record.value());
        } catch (TransactionRecordDecoder.TransactionContractException exception) {
            log.error("transaction_consume outcome=failed stage=contract topic={} partition={} offset={} failedAt={} reason={}",
                    record.topic(), record.partition(), record.offset(), Instant.now(), exception.getMessage());
            throw exception;
        }
        try {
            // The separate proxied service returns only after its PostgreSQL transaction commits.
            service.store(event);
        } catch (RuntimeException exception) {
            log.error("transaction_consume outcome=failed stage=database topic={} partition={} offset={} failedAt={} errorType={}",
                    record.topic(), record.partition(), record.offset(), Instant.now(), exception.getClass().getSimpleName());
            throw exception;
        }
    }
}
