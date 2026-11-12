package com.evcharging.controlplane.transaction;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

@Configuration(proxyBeanMethods = false)
@EnableScheduling
@ConditionalOnProperty(name = {"transaction.ingress.enabled", "transaction.outbox.relay-enabled"}, havingValue = "true")
class TransactionUsageOutboxConfiguration {
}
