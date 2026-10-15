package com.evcharging.controlplane.transaction;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/** Isolated test-only Spring afterCommit publisher; no production closeout path uses it. */
public final class Exp0004AfterCommitProcess {
    private Exp0004AfterCommitProcess() {}

    public static void main(String[] args) {
        if (args.length != 6) throw new IllegalArgumentException("mode, jdbcUrl, brokers, topic, transactionId, expectedWh");
        String mode = args[0];
        if (!"normal".equals(mode) && !"in-callback-before-send".equals(mode)) {
            throw new IllegalArgumentException("unknown mode " + mode);
        }
        var dataSource = new DriverManagerDataSource(args[1], "postgres", "postgres");
        var transaction = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        transaction.executeWithoutResult(status -> {
            UUID eventId;
            try {
                eventId = Exp0004DirectPublishProcess.insertTestCloseout(
                        DataSourceUtils.getConnection(dataSource), args[4], new BigDecimal(args[5]));
            } catch (SQLException failure) {
                throw new IllegalStateException("test closeout rejected", failure);
            }
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    if ("in-callback-before-send".equals(mode)) {
                        System.out.println("AFTER_COMMIT_ENTERED_BEFORE_SEND " + eventId);
                        System.out.flush();
                        Runtime.getRuntime().halt(137);
                    }
                    String payload = Exp0004DirectPublishProcess.eventPayload(eventId, args[4], args[5]);
                    try (var producer = new KafkaProducer<String, String>(Map.of(
                            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, args[2],
                            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                            ProducerConfig.ACKS_CONFIG, "all"))) {
                        producer.send(new ProducerRecord<>(args[3], "ST-TEST", payload))
                                .get(10, TimeUnit.SECONDS);
                    } catch (Exception failure) {
                        throw new IllegalStateException("afterCommit publish failed", failure);
                    }
                    System.out.println("AFTER_COMMIT_PUBLISHED " + eventId);
                }
            });
        });
    }
}
