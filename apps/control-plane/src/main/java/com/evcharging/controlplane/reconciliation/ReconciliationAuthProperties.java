package com.evcharging.controlplane.reconciliation;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

@ConfigurationProperties(prefix = "reconciliation.auth")
public record ReconciliationAuthProperties(
        boolean enabled,
        String clientUri,
        String issuerId,
        List<String> stationIds,
        List<String> actions
) {
}
