package com.evcharging.controlplane.transaction;

import com.evcharging.controlplane.ControlPlaneApplication;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.listener.MessageListener;

import java.util.concurrent.CountDownLatch;

/** Test-only child JVM. Halt after the real listener commits PostgreSQL, before its Kafka offset commit. */
public final class CrashAfterTransactionCommitProcess {
    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws InterruptedException {
        var app = new SpringApplication(ControlPlaneApplication.class);
        app.setWebApplicationType(WebApplicationType.NONE);
        var context = app.run(args);
        var factory = (ConcurrentKafkaListenerContainerFactory<String, String>)
                context.getBean("transactionKafkaListenerContainerFactory");
        var listener = context.getBean(TransactionRecordListener.class);
        var container = factory.createContainer(context.getEnvironment().getRequiredProperty("transaction.kafka-topic"));
        container.getContainerProperties().setGroupId(
                context.getEnvironment().getRequiredProperty("transaction.test.group-id"));
        container.getContainerProperties().setPollTimeout(100);
        container.getContainerProperties().setMessageListener((MessageListener<String, String>) record -> {
            listener.consume(record);
            Runtime.getRuntime().halt(137);
        });
        container.start();
        new CountDownLatch(1).await();
    }
}
