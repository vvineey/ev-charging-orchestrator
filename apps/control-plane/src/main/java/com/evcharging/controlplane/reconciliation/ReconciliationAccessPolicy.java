package com.evcharging.controlplane.reconciliation;

import java.util.Set;

/** A narrow, explicit allowlist until the independent authority registry is available. */
public final class ReconciliationAccessPolicy {
    public enum Action { SUBMIT_EVIDENCE, REQUEST_CLOSEOUT }

    private final String clientUri;
    private final String issuerId;
    private final Set<String> stationIds;
    private final Set<Action> actions;

    public ReconciliationAccessPolicy(ReconciliationAuthProperties properties) {
        clientUri = required(properties.clientUri(), "client-uri");
        issuerId = required(properties.issuerId(), "issuer-id");
        if (!clientUri.startsWith("urn:ev-charging:reconciliation:")) {
            throw new IllegalArgumentException("reconciliation.auth.client-uri must be a reconciliation URI");
        }
        if (properties.stationIds() == null || properties.stationIds().isEmpty()
                || properties.stationIds().stream().anyMatch(value -> value == null || value.isBlank() || "*".equals(value))) {
            throw new IllegalArgumentException("reconciliation.auth.station-ids requires explicit station IDs");
        }
        stationIds = Set.copyOf(properties.stationIds());
        if (properties.actions() == null || properties.actions().isEmpty()) {
            throw new IllegalArgumentException("reconciliation.auth.actions requires explicit actions");
        }
        actions = properties.actions().stream().map(Action::valueOf).collect(java.util.stream.Collectors.toUnmodifiableSet());
    }

    public boolean allows(String authenticatedClientUri, String evidenceIssuerId, String stationId, Action action) {
        return clientUri.equals(authenticatedClientUri)
                && issuerId.equals(evidenceIssuerId)
                && stationIds.contains(stationId)
                && actions.contains(action);
    }

    public boolean recognizes(String authenticatedClientUri) {
        return clientUri.equals(authenticatedClientUri);
    }

    private static String required(String value, String name) {
        if (value == null || value.isBlank() || "*".equals(value)) {
            throw new IllegalArgumentException("reconciliation.auth." + name + " is required");
        }
        return value;
    }
}
