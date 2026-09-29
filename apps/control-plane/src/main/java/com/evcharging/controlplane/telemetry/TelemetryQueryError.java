package com.evcharging.controlplane.telemetry;

public record TelemetryQueryError(String code, String message) {
    static final TelemetryQueryError INVALID = new TelemetryQueryError(
            "INVALID_TELEMETRY_QUERY", "stationId와 양의 정수 evseId를 확인하세요.");
    static final TelemetryQueryError NOT_FOUND = new TelemetryQueryError(
            "TELEMETRY_NOT_FOUND", "저장된 telemetry 관측이 없습니다.");
    static final TelemetryQueryError UNAVAILABLE = new TelemetryQueryError(
            "TELEMETRY_QUERY_UNAVAILABLE", "telemetry를 조회할 수 없습니다.");
}
