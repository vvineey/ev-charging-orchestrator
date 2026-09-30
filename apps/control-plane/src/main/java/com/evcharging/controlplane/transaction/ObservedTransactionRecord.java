package com.evcharging.controlplane.transaction;

import tools.jackson.databind.JsonNode;

import java.time.Instant;
import java.util.UUID;

record ObservedTransactionRecord(
        UUID eventId,
        String stationId,
        String chargingStationId,
        String transactionId,
        int seqNo,
        String eventType,
        Instant occurredAt,
        Instant receivedAt,
        String sourceJson,
        JsonNode payload) {
}
