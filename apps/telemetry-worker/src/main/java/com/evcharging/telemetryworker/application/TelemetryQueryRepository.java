package com.evcharging.telemetryworker.application;

import java.util.List;
import java.util.Optional;

public interface TelemetryQueryRepository {
    List<TelemetryObservation> findByStation(String stationId);

    Optional<TelemetryObservation> findByEvse(String stationId, int evseId);
}
