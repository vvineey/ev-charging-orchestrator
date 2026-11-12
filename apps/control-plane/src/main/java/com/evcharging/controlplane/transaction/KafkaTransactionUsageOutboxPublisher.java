package com.evcharging.controlplane.transaction;

import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

@Component
@ConditionalOnProperty(name = {"transaction.ingress.enabled", "transaction.outbox.relay-enabled"}, havingValue = "true")
class KafkaTransactionUsageOutboxPublisher implements TransactionUsageOutboxPublisher, AutoCloseable {
    private final KafkaProducer<String, String> producer;
    private final Duration sendTimeout;

    KafkaTransactionUsageOutboxPublisher(ProducerFactory<Object, Object> factory,
            @Value("${transaction.outbox.send-timeout:10s}") Duration sendTimeout) {
        if (sendTimeout.toMillis() < 1 || sendTimeout.toMillis() > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("outbox send timeout must fit a positive millisecond integer");
        }
        this.sendTimeout = sendTimeout;
        var properties = new HashMap<String, Object>(factory.getConfigurationProperties());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 0);
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, (int) sendTimeout.toMillis());
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, (int) sendTimeout.toMillis());
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, sendTimeout.toMillis());
        producer = new KafkaProducer<>(properties);
    }

    @Override
    public void publish(JdbcTransactionUsageOutbox.ClaimedEvent event) throws Exception {
        // Waiting on this Future does not cancel an outstanding send after a timeout.
        producer.send(new ProducerRecord<>(event.topic(), event.messageKey(), event.payload()))
                .get(sendTimeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    @PreDestroy
    @Override
    public void close() {
        producer.close(sendTimeout);
    }
}
