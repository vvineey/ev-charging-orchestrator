package com.evcharging.telemetryworker.adapter.http;

import com.evcharging.telemetryworker.application.QueryTelemetryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice(assignableTypes = TelemetryQueryController.class)
public class TelemetryQueryExceptionHandler {
    private static final Logger log = LoggerFactory.getLogger(TelemetryQueryExceptionHandler.class);

    @ExceptionHandler(QueryTelemetryService.InvalidQueryException.class)
    public ResponseEntity<TelemetryQueryError> invalid() {
        return ResponseEntity.badRequest().body(new TelemetryQueryError(
                "INVALID_TELEMETRY_QUERY", "stationId와 양의 정수 evseId를 확인하세요."));
    }

    @ExceptionHandler(DataAccessException.class)
    public ResponseEntity<TelemetryQueryError> unavailable(DataAccessException exception) {
        log.warn("telemetry_query_unavailable reason={}", exception.getClass().getSimpleName());
        return ResponseEntity.status(503).body(new TelemetryQueryError(
                "TELEMETRY_QUERY_UNAVAILABLE", "telemetry를 조회할 수 없습니다."));
    }
}
