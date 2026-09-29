package com.evcharging.controlplane.telemetry;

final class TelemetryQueryException extends RuntimeException {
    private final boolean invalidQuery;

    private TelemetryQueryException(boolean invalidQuery) {
        this.invalidQuery = invalidQuery;
    }

    static TelemetryQueryException invalid() {
        return new TelemetryQueryException(true);
    }

    static TelemetryQueryException unavailable() {
        return new TelemetryQueryException(false);
    }

    boolean invalidQuery() {
        return invalidQuery;
    }
}
