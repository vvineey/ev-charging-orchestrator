package com.evcharging.telemetryworker.application;

import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

@Service
public class QueryTelemetryService {
    private final TelemetryQueryRepository repository;

    public QueryTelemetryService(TelemetryQueryRepository repository) {
        this.repository = repository;
    }

    public List<TelemetryObservation> list(String stationId) {
        validateStation(stationId);
        return repository.findByStation(stationId);
    }

    public Optional<TelemetryObservation> detail(String stationId, String evseId) {
        validateStation(stationId);
        int id;
        try {
            id = Integer.parseInt(evseId);
        } catch (NumberFormatException exception) {
            throw new InvalidQueryException();
        }
        if (id <= 0) {
            throw new InvalidQueryException();
        }
        return repository.findByEvse(stationId, id);
    }

    private static void validateStation(String stationId) {
        if (stationId == null || stationId.isBlank()) {
            throw new InvalidQueryException();
        }
    }

    public static final class InvalidQueryException extends RuntimeException {
    }
}
