package com.evcharging.controlplane.transaction;

import com.evcharging.controlplane.ControlPlaneApplication;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.support.TransactionTemplate;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

/** Test-only JVM for comparing three consumers against the same external Kafka/PostgreSQL testbed. */
public final class TransactionCandidateRestartProcess {
    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws InterruptedException {
        var app = new SpringApplication(ControlPlaneApplication.class);
        app.setWebApplicationType(WebApplicationType.NONE);
        var context = app.run(args);
        var factory = (ConcurrentKafkaListenerContainerFactory<String, String>)
                context.getBean("transactionKafkaListenerContainerFactory");
        var decoder = context.getBean(TransactionRecordDecoder.class);
        var listener = context.getBean(TransactionRecordListener.class);
        var inbox = context.getBean(JdbcTransactionInbox.class);
        var rules = context.getBean(TransactionRecoveryRules.class);
        var transactions = context.getBean(TransactionTemplate.class);
        var jdbc = context.getBean(JdbcClient.class);
        int windowSeconds = context.getEnvironment().getProperty("transaction.test.window-seconds",
                Integer.class, 30);
        boolean restoreFromInbox = context.getEnvironment().getProperty(
                "transaction.test.restore-from-inbox", Boolean.class, false);
        boolean pauseAfterStart = context.getEnvironment().getProperty(
                "transaction.test.pause-after-start", Boolean.class, false);
        String bRestorePrefix = context.getEnvironment().getProperty(
                "transaction.test.b-restore-prefix", "");
        String windowMarker = context.getEnvironment().getProperty(
                "transaction.test.window-marker-file", "");
        for (String mode : new String[] {"A", "B", "C"}) {
            String topic = context.getEnvironment().getRequiredProperty(
                    "transaction.test.topic-" + mode.toLowerCase());
            var container = factory.createContainer(topic);
            container.getContainerProperties().setGroupId(topic);
            container.getContainerProperties().setPollTimeout(100);
            MessageListener<String, String> delegate;
            if ("C".equals(mode)) {
                delegate = listener::consume;
            } else {
                var adapter = new TransactionRecoveryOnlinePerformanceTest.CandidateAdapter(
                        mode, windowSeconds, inbox, rules, transactions, jdbc, restoreFromInbox,
                        transactionId -> {
                            if (windowMarker.isBlank()) return;
                            try {
                                Files.writeString(Path.of(windowMarker), transactionId + "\n",
                                        StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                            } catch (IOException exception) {
                                throw new UncheckedIOException(exception);
                            }
                        });
                Map<String, Long> candidateNanos = new ConcurrentHashMap<>();
                adapter.restorePendingWindows(bRestorePrefix, candidateNanos);
                delegate = record -> adapter.accept(
                        decoder.decode(record.key(), record.value()), candidateNanos);
            }
            container.getContainerProperties().setMessageListener((MessageListener<String, String>) record -> {
                delegate.onMessage(record);
                if (pauseAfterStart && decoder.decode(record.key(), record.value()).seqNo() == 0) {
                    // The database transaction has committed, but RECORD ack has not run yet.
                    try {
                        new CountDownLatch(1).await();
                    } catch (InterruptedException exception) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException("test listener interrupted before offset commit", exception);
                    }
                }
            });
            container.start();
        }
        new CountDownLatch(1).await();
    }
}
