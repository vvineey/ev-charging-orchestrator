package com.evcharging.controlplane.transaction;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.util.Map;

/** Separate test JVM so halt(137) crosses a real process boundary. */
public final class Exp0003PollingCrashProcess {
    private Exp0003PollingCrashProcess() {}

    public static void main(String[] args) throws Exception {
        var dataSource = new DriverManagerDataSource(args[1], "postgres", "postgres");
        if (args[0].equals("after-commit-before-send")) {
            Exp0003OutboxFixture.commitWork(dataSource, args[4], false);
            Runtime.getRuntime().halt(137);
        }
        if (args[0].equals("after-kafka-ack-before-delivery-commit")) {
            try (var producer = new KafkaProducer<String, String>(Map.of(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, args[2],
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                    ProducerConfig.ACKS_CONFIG, "all"))) {
                Exp0003OutboxFixture.pollBatch(dataSource, producer, args[3],
                        Integer.parseInt(args[5]), () -> Runtime.getRuntime().halt(137));
            }
        }
        throw new IllegalArgumentException("Unknown crash mode: " + args[0]);
    }
}
