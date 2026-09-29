package com.evcharging.telemetryworker.application;

public interface LatestTelemetryRepository {

    /** Returns true only when a new or more recent observation is stored. */
    boolean upsert(LatestTelemetry telemetry);
}
