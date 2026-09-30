package com.evcharging.controlplane.telemetry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.core.StreamReadFeature;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.ObjectReader;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
public class TelemetryWorkerQueryClient implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(TelemetryWorkerQueryClient.class);

    private final URI baseUrl;
    private final Duration deadline;
    private final HttpClient http;
    private final ObjectMapper mapper;
    private final ObjectReader strictReader;

    public TelemetryWorkerQueryClient(
            @Value("${telemetry.worker.base-url}") String baseUrl,
            @Value("${telemetry.worker.trust-store}") String trustStore,
            @Value("${telemetry.worker.trust-store-password}") String trustStorePassword,
            @Value("${telemetry.worker.deadline}") Duration deadline,
            ObjectMapper mapper) throws Exception {
        this.baseUrl = URI.create(baseUrl);
        if (!"https".equalsIgnoreCase(this.baseUrl.getScheme()) || this.baseUrl.getHost() == null
                || this.baseUrl.getUserInfo() != null || this.baseUrl.getQuery() != null
                || this.baseUrl.getFragment() != null || !("".equals(this.baseUrl.getPath())
                || "/".equals(this.baseUrl.getPath()))) {
            throw new IllegalArgumentException("telemetry.worker.base-url must be an HTTPS origin");
        }
        if (deadline.isZero() || deadline.isNegative()) {
            throw new IllegalArgumentException("telemetry.worker.deadline must be positive");
        }
        this.deadline = deadline;
        this.mapper = mapper;
        this.strictReader = mapper.reader().with(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
                .with(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
        var store = KeyStore.getInstance("PKCS12");
        try (var input = Files.newInputStream(Path.of(trustStore))) {
            store.load(input, trustStorePassword.toCharArray());
        }
        var trustManagers = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagers.init(store);
        var tls = SSLContext.getInstance("TLS");
        tls.init(null, trustManagers.getTrustManagers(), null);
        http = HttpClient.newBuilder().sslContext(tls).connectTimeout(deadline)
                .followRedirects(HttpClient.Redirect.NEVER).build();
    }

    public QueryResult list(String stationId) {
        validateStation(stationId);
        String path = "/internal/v1/stations/" + encode(stationId) + "/telemetry/latest";
        return fetch(path, stationId, null);
    }

    public QueryResult detail(String stationId, String evseId) {
        validateStation(stationId);
        int id;
        try {
            id = Integer.parseInt(evseId);
        } catch (NumberFormatException exception) {
            throw TelemetryQueryException.invalid();
        }
        if (id <= 0) {
            throw TelemetryQueryException.invalid();
        }
        String path = "/internal/v1/stations/" + encode(stationId)
                + "/evses/" + id + "/telemetry/latest";
        return fetch(path, stationId, id);
    }

    private QueryResult fetch(String path, String stationId, Integer evseId) {
        var request = HttpRequest.newBuilder(baseUrl.resolve(path))
                .timeout(deadline).GET().build();
        var future = http.sendAsync(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        HttpResponse<String> response;
        try {
            // Completion includes the response body, not just its headers.
            response = future.get(deadline.toNanos(), TimeUnit.NANOSECONDS);
        } catch (TimeoutException exception) {
            future.cancel(true);
            log.warn("telemetry_worker_query_unavailable stage=deadline");
            throw TelemetryQueryException.unavailable();
        } catch (InterruptedException exception) {
            future.cancel(true);
            Thread.currentThread().interrupt();
            log.warn("telemetry_worker_query_unavailable stage=interrupted");
            throw TelemetryQueryException.unavailable();
        } catch (ExecutionException exception) {
            log.warn("telemetry_worker_query_unavailable stage=transport reason={}",
                    exception.getCause() == null ? "unknown" : exception.getCause().getClass().getSimpleName());
            throw TelemetryQueryException.unavailable();
        } catch (CancellationException exception) {
            log.warn("telemetry_worker_query_unavailable stage=cancelled");
            throw TelemetryQueryException.unavailable();
        }
        try {
            if (response.statusCode() == 200) {
                validateSuccess(response.body(), stationId, evseId);
                return new QueryResult(200, response.body());
            }
            if (response.statusCode() == 400 && validError(response.body(), "INVALID_TELEMETRY_QUERY")) {
                return new QueryResult(400, mapper.writeValueAsString(TelemetryQueryError.INVALID));
            }
            if (response.statusCode() == 404 && evseId != null && validError(response.body(), "TELEMETRY_NOT_FOUND")) {
                return new QueryResult(404, mapper.writeValueAsString(TelemetryQueryError.NOT_FOUND));
            }
        } catch (JacksonException | IllegalArgumentException exception) {
            log.warn("telemetry_worker_query_unavailable stage=contract");
            throw TelemetryQueryException.unavailable();
        }
        log.warn("telemetry_worker_query_unavailable stage=upstream_response status={}", response.statusCode());
        throw TelemetryQueryException.unavailable();
    }

    private boolean validError(String body, String expectedCode) {
        JsonNode root = strictReader.readTree(body);
        return root != null && root.isObject() && root.properties().size() == 2
                && string(root, "code") != null && expectedCode.equals(string(root, "code").asString())
                && string(root, "message") != null && !string(root, "message").asString().isBlank();
    }

    private void validateSuccess(String body, String stationId, Integer evseId) {
        JsonNode root = strictReader.readTree(body);
        if (evseId != null) {
            validateObservation(root, stationId, evseId);
            return;
        }
        if (root == null || !root.isObject() || root.properties().size() != 2
                || !equalsString(root, "stationId", stationId)
                || root.get("evses") == null || !root.get("evses").isArray()) {
            throw new IllegalArgumentException("invalid list");
        }
        int previous = 0;
        for (JsonNode evse : root.get("evses")) {
            int id = positiveId(evse);
            if (id <= previous) {
                throw new IllegalArgumentException("evses must be ordered");
            }
            validateObservation(evse, stationId, id);
            previous = id;
        }
    }

    private void validateObservation(JsonNode node, String stationId, int evseId) {
        if (node == null || !node.isObject() || node.properties().size() != 10
                || !equalsString(node, "stationId", stationId) || positiveId(node) != evseId
                || node.get("charging") == null || !node.get("charging").isBoolean()
                || !number(node, "power") || !number(node, "voltage") || !number(node, "current")
                || !utcInstant(node, "occurredAt") || !utcInstant(node, "receivedAt")
                || !utcInstant(node, "updatedAt") || string(node, "lastEventId") == null) {
            throw new IllegalArgumentException("invalid observation");
        }
        UUID.fromString(node.get("lastEventId").asString());
    }

    private int positiveId(JsonNode node) {
        JsonNode value = node == null ? null : node.get("evseId");
        if (value == null || !value.isIntegralNumber() || !value.canConvertToInt() || value.intValue() <= 0) {
            throw new IllegalArgumentException("invalid evseId");
        }
        return value.intValue();
    }

    private boolean number(JsonNode object, String field) {
        return object.get(field) != null && object.get(field).isNumber();
    }

    private boolean utcInstant(JsonNode object, String field) {
        JsonNode value = string(object, field);
        if (value == null || !value.asString().endsWith("Z")) {
            return false;
        }
        try {
            return Instant.parse(value.asString()).getNano() % 1_000 == 0;
        } catch (DateTimeParseException exception) {
            return false;
        }
    }

    private boolean equalsString(JsonNode object, String field, String expected) {
        JsonNode value = string(object, field);
        return value != null && expected.equals(value.asString());
    }

    private JsonNode string(JsonNode object, String field) {
        JsonNode value = object.get(field);
        return value != null && value.isString() ? value : null;
    }

    private static void validateStation(String stationId) {
        if (stationId == null || stationId.isBlank()) {
            throw TelemetryQueryException.invalid();
        }
    }

    private static String encode(String segment) {
        return URLEncoder.encode(segment, StandardCharsets.UTF_8).replace("+", "%20");
    }

    @Override
    public void close() {
        http.close();
    }

    public record QueryResult(int status, String json) {
    }
}
