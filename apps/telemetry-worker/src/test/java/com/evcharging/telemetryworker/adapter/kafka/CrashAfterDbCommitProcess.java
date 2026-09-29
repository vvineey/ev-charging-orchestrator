package com.evcharging.telemetryworker.adapter.kafka;

import com.evcharging.telemetryworker.TelemetryWorkerApplication;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.listener.MessageListener;

import java.util.concurrent.CountDownLatch;

/** Test-only fork: the real listener returns after DB commit; halt prevents its offset commit. */
public final class CrashAfterDbCommitProcess {
    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws InterruptedException {
        var app = new SpringApplication(TelemetryWorkerApplication.class);
        app.setWebApplicationType(WebApplicationType.NONE);
        var context = app.run(args);
        var factory = (ConcurrentKafkaListenerContainerFactory<String, String>)
                context.getBean("telemetryKafkaListenerContainerFactory");
        var listener = context.getBean(TelemetryRecordListener.class);
        var container = factory.createContainer(context.getEnvironment().getRequiredProperty("telemetry.kafka-topic"));
        container.getContainerProperties().setGroupId(TelemetryRecordListener.GROUP_ID);
        container.getContainerProperties().setMessageListener((MessageListener<String, String>) record -> {
            listener.consume(record);
            Runtime.getRuntime().halt(137);
        });
        container.start();
        new CountDownLatch(1).await();
    }
}
