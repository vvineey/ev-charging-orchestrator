package com.evcharging.controlplane.transaction;

interface TransactionUsageOutboxPublisher {
    void publish(JdbcTransactionUsageOutbox.ClaimedEvent event) throws Exception;
}
