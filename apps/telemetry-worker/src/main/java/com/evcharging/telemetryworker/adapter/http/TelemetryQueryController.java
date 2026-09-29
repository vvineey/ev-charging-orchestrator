package com.evcharging.telemetryworker.adapter.http;

import com.evcharging.telemetryworker.application.QueryTelemetryService;
import com.evcharging.telemetryworker.application.TelemetryObservation;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/internal/v1/stations")
public class TelemetryQueryController {
    private final QueryTelemetryService service;

    public TelemetryQueryController(QueryTelemetryService service) {
        this.service = service;
    }

    @GetMapping("/{stationId}/telemetry/latest")
    public StationTelemetry list(@PathVariable String stationId) {
        return new StationTelemetry(stationId, service.list(stationId));
    }

    @GetMapping("/{stationId}/evses/{evseId}/telemetry/latest")
    public ResponseEntity<?> detail(@PathVariable String stationId, @PathVariable String evseId) {
        return service.detail(stationId, evseId)
                .<ResponseEntity<?>>map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.status(404).body(new TelemetryQueryError(
                        "TELEMETRY_NOT_FOUND", "저장된 telemetry 관측이 없습니다.")));
    }

    // Accept both preserved and normalized forms of an empty station path segment.
    @GetMapping({"//telemetry/latest", "//evses/{evseId}/telemetry/latest",
            "/telemetry/latest", "/evses/{evseId}/telemetry/latest"})
    public StationTelemetry missingStation() {
        throw new QueryTelemetryService.InvalidQueryException();
    }

    public record StationTelemetry(String stationId, List<TelemetryObservation> evses) {
    }
}
