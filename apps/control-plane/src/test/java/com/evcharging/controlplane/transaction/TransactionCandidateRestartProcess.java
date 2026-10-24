package com.evcharging.controlplane.transaction;

import com.evcharging.controlplane.ControlPlaneApplication;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.transaction.support.TransactionTemplate;

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
        int windowSeconds = context.getEnvironment().getProperty("transaction.test.window-seconds",
                Integer.class, 30);
        for (String mode : new String[] {"A", "B", "C"}) {
            String topic = context.getEnvironment().getRequiredProperty(
                    "transaction.test.topic-" + mode.toLowerCase());
            var container = factory.createContainer(topic);
            container.getContainerProperties().setGroupId(topic);
            container.getContainerProperties().setPollTimeout(100);
            if ("C".equals(mode)) {
                container.getContainerProperties().setMessageListener(
                        (MessageListener<String, String>) listener::consume);
            } else {
                var adapter = new TransactionRecoveryOnlinePerformanceTest.CandidateAdapter(
                        mode, windowSeconds, inbox, rules, transactions);
                Map<String, Long> finalizedNanos = new ConcurrentHashMap<>();
                container.getContainerProperties().setMessageListener((MessageListener<String, String>)
                        record -> adapter.accept(decoder.decode(record.key(), record.value()), finalizedNanos));
            }
            container.start();
        }
        new CountDownLatch(1).await();
    }
}
