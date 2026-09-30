package com.evcharging.controlplane.telemetry;

import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
@RequestMapping("/api/v1/stations")
public class OperatorTelemetryController {
    private final TelemetryWorkerQueryClient worker;

    public OperatorTelemetryController(TelemetryWorkerQueryClient worker) {
        this.worker = worker;
    }

    @GetMapping("/{stationId}/telemetry/latest")
    public ResponseEntity<String> list(@PathVariable String stationId) {
        return reply(worker.list(stationId));
    }

    @GetMapping("/{stationId}/evses/{evseId}/telemetry/latest")
    public ResponseEntity<String> detail(@PathVariable String stationId, @PathVariable String evseId) {
        return reply(worker.detail(stationId, evseId));
    }

    @GetMapping({"//telemetry/latest", "//evses/{evseId}/telemetry/latest",
            "/telemetry/latest", "/evses/{evseId}/telemetry/latest"})
    public ResponseEntity<String> missingStation() {
        throw TelemetryQueryException.invalid();
    }

    private ResponseEntity<String> reply(TelemetryWorkerQueryClient.QueryResult result) {
        return ResponseEntity.status(result.status()).contentType(MediaType.APPLICATION_JSON).body(result.json());
    }
}
