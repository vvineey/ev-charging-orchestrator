package com.evcharging.ocppgateway;

import java.util.Set;

record StationRegistration(String chargingStationId, String stationId, Set<Integer> allowedEvseIds,
        String password) {
    StationRegistration {
        if (chargingStationId == null || !chargingStationId.matches("[A-Za-z0-9._-]{1,64}")
                || stationId == null || !stationId.matches("[A-Za-z0-9._-]{1,128}")
                || allowedEvseIds == null || allowedEvseIds.isEmpty()
                || allowedEvseIds.stream().anyMatch(id -> id == null || id <= 0)
                || password == null || password.length() < 16) {
            throw new IllegalArgumentException("invalid OCPP station registration");
        }
        allowedEvseIds = Set.copyOf(allowedEvseIds);
    }
}
