package com.evcharging.controlplane.transaction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
@ConditionalOnProperty(name = {"transaction.ingress.enabled", "transaction.outbox.relay-enabled"}, havingValue = "true")
class TransactionUsageOutboxRelay {
    private static final Logger log = LoggerFactory.getLogger(TransactionUsageOutboxRelay.class);
    private final JdbcTransactionUsageOutbox outbox;
    private final TransactionUsageOutboxPublisher publisher;
    private final Duration claimLease;

    TransactionUsageOutboxRelay(JdbcTransactionUsageOutbox outbox, TransactionUsageOutboxPublisher publisher,
            @Value("${transaction.outbox.claim-lease:30s}") Duration claimLease,
            @Value("${transaction.outbox.send-timeout:10s}") Duration sendTimeout) {
        if (sendTimeout.toMillis() < 1 || claimLease.compareTo(sendTimeout.multipliedBy(2)) <= 0) {
            throw new IllegalArgumentException("outbox claim lease must exceed twice the send timeout");
        }
        this.outbox = outbox;
        this.publisher = publisher;
        this.claimLease = claimLease;
    }

    @Scheduled(fixedDelayString = "${transaction.outbox.poll-interval:1000}")
    public void publishNext() {
        // One claim per invocation keeps the lease from expiring while earlier batch sends wait.
        for (var event : outbox.claim(claimLease, 1)) {
            try {
                publisher.publish(event);
            } catch (Exception exception) {
                if (exception instanceof InterruptedException) Thread.currentThread().interrupt();
                // No automatic business retries until their policy is decided separately.
                boolean blocked = outbox.block(event, exception.getClass().getSimpleName());
                log.warn("usage_outbox_publication outcome=unconfirmed eventId={} blocked={} errorType={}",
                        event.eventId(), blocked, exception.getClass().getSimpleName());
                return;
            }
            // A DB failure here leaves an expiring claim. The same event may be sent again.
            boolean completed = outbox.markPublished(event);
            log.info("usage_outbox_publication outcome={} eventId={}",
                    completed ? "acknowledged" : "claim_lost", event.eventId());
        }
    }
}
