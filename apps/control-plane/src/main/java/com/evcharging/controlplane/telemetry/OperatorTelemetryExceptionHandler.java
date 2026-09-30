package com.evcharging.controlplane.telemetry;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice(assignableTypes = OperatorTelemetryController.class)
public class OperatorTelemetryExceptionHandler {
    @ExceptionHandler(TelemetryQueryException.class)
    public ResponseEntity<TelemetryQueryError> handle(TelemetryQueryException exception) {
        if (exception.invalidQuery()) {
            return ResponseEntity.badRequest().body(TelemetryQueryError.INVALID);
        }
        return ResponseEntity.status(503).body(TelemetryQueryError.UNAVAILABLE);
    }
}
