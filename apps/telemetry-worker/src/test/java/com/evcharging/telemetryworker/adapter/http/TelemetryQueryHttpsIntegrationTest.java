package com.evcharging.telemetryworker.adapter.http;

import com.evcharging.telemetryworker.PostgreSqlTestConfiguration;
import com.evcharging.telemetryworker.TelemetryWorkerApplication;
import com.evcharging.telemetryworker.application.TelemetryObservation;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContextException;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.Environment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import tools.jackson.databind.ObjectMapper;
import org.testcontainers.postgresql.PostgreSQLContainer;

import javax.net.ssl.SSLHandshakeException;
import javax.sql.DataSource;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "server.address=127.0.0.1", "spring.datasource.hikari.maximum-pool-size=2",
        "spring.datasource.hikari.connection-timeout=250", "spring.jdbc.template.query-timeout=1s"
})
@Import(PostgreSqlTestConfiguration.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class TelemetryQueryHttpsIntegrationTest {
    static final TestTlsMaterial TLS = new TestTlsMaterial();
    static final String LIST = "/internal/v1/stations/query-fixture/telemetry/latest";
    static final String DETAIL = "/internal/v1/stations/query-fixture/evses/1/telemetry/latest";
    static final HttpClient CLIENT = HttpClient.newBuilder().sslContext(TLS.trustedContext)
            .connectTimeout(Duration.ofSeconds(2)).build();

    @Autowired JdbcTemplate jdbc;
    @Autowired DataSource dataSource;
    @Autowired ObjectMapper mapper;
    @Autowired Environment environment;
    @Autowired PostgreSQLContainer postgres;

    @DynamicPropertySource
    static void ssl(DynamicPropertyRegistry registry) {
        registry.add("server.ssl.key-store", () -> TLS.keyStore.toUri().toString());
        registry.add("server.ssl.key-store-password", () -> TLS.password);
    }

    @AfterAll
    static void cleanup() throws Exception {
        CLIENT.close();
        TLS.close();
        assertThat(TLS.directory).doesNotExist();
    }

    @BeforeEach
    void fixture() {
        jdbc.update("DELETE FROM evse_telemetry_latest");
        // Independent, committed SQL fixtures. Insert order, time zone and >5 ID are intentional.
        for (int evse : new int[]{6, 3, 1}) {
            jdbc.update("""
                    INSERT INTO evse_telemetry_latest
                        (station_id, evse_id, charging, power, voltage, current,
                         occurred_at, received_at, updated_at, last_event_id)
                    VALUES ('query-fixture', ?, true, 120.12345678901234567890123, 220.987654321, 0.54321,
                            TIMESTAMPTZ '2025-01-01 09:00:00.123456+09',
                            TIMESTAMPTZ '2025-01-01 09:00:01.654321+09',
                            TIMESTAMPTZ '2025-01-01 09:00:02.111222+09',
                            '11111111-1111-4111-8111-111111111111')
                    """, evse);
        }
        jdbc.update("""
                INSERT INTO evse_telemetry_latest
                    SELECT 'other-fixture', evse_id, false, 999, 1, 1,
                           occurred_at, received_at, last_event_id, updated_at
                    FROM evse_telemetry_latest WHERE evse_id = 1
                """);
    }

    @Test
    void listsOnlyStoredRowsInEvseOrderWithoutChangingAnyObservation() throws Exception {
        var before = snapshot();
        var response = get(LIST);
        assertThat(response.statusCode()).isEqualTo(200);
        var result = mapper.readValue(response.body(), StationResponse.class);
        assertThat(result.stationId()).isEqualTo("query-fixture");
        assertThat(result.evses()).extracting(TelemetryObservation::evseId).containsExactly(1, 3, 6);
        result.evses().forEach(this::assertFixture);
        assertThat(mapper.readTree(response.body()).properties()).hasSize(2);
        assertThat(snapshot()).isEqualTo(before);
    }

    @Test
    void detailKeepsDecimalPrecisionUtcMicrosecondsAndOldChargingObservation() throws Exception {
        var before = snapshot();
        var response = get(DETAIL);
        assertThat(response.statusCode()).isEqualTo(200);
        var node = mapper.readTree(response.body());
        assertThat(node.properties()).hasSize(10);
        assertThat(node.get("power").isNumber()).isTrue();
        assertThat(node.get("charging").isBoolean()).isTrue();
        assertThat(node.get("occurredAt").asText()).isEqualTo("2025-01-01T00:00:00.123456Z");
        assertThat(node.get("receivedAt").asText()).isEqualTo("2025-01-01T00:00:01.654321Z");
        assertThat(node.get("updatedAt").asText()).isEqualTo("2025-01-01T00:00:02.111222Z");
        assertFixture(mapper.readValue(response.body(), TelemetryObservation.class));
        assertThat(snapshot()).isEqualTo(before);
    }

    @Test
    void unknownStationHasAnEmptyListAndMissingEvseHas404() throws Exception {
        var empty = get("/internal/v1/stations/unobserved/telemetry/latest");
        assertThat(empty.statusCode()).isEqualTo(200);
        var list = mapper.readValue(empty.body(), StationResponse.class);
        assertThat(list.stationId()).isEqualTo("unobserved");
        assertThat(list.evses()).isEmpty();
        assertError(get("/internal/v1/stations/unobserved/evses/1/telemetry/latest"),
                404, "TELEMETRY_NOT_FOUND");
        assertError(get("/internal/v1/stations/query-fixture/evses/2/telemetry/latest"),
                404, "TELEMETRY_NOT_FOUND");
    }

    @ParameterizedTest
    @ValueSource(strings = {"0", "-1", "abc", "1.5", "2147483648", "%20"})
    void invalidEvseIdsHave400(String id) throws Exception {
        assertError(get("/internal/v1/stations/query-fixture/evses/" + id + "/telemetry/latest"),
                400, "INVALID_TELEMETRY_QUERY");
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "/internal/v1/stations/%20/telemetry/latest",
            "/internal/v1/stations/%20/evses/1/telemetry/latest",
            "/internal/v1/stations//telemetry/latest",
            "/internal/v1/stations//evses/1/telemetry/latest"
    })
    void blankStationIdsHave400(String path) throws Exception {
        assertError(get(path), 400, "INVALID_TELEMETRY_QUERY");
    }

    @Test
    void stationIsAnExactBoundParameter() throws Exception {
        var response = get("/internal/v1/stations/query-fixture%27%20OR%20%271%27=%271/telemetry/latest");
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(mapper.readValue(response.body(), StationResponse.class).evses()).isEmpty();
    }

    @Test
    void readsOnlyCommittedProjection() throws Exception {
        try (var connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try (var statement = connection.createStatement()) {
                statement.executeUpdate("UPDATE evse_telemetry_latest SET charging=false WHERE station_id='query-fixture'");
            }
            assertFixture(mapper.readValue(get(DETAIL).body(), TelemetryObservation.class));
            connection.rollback();
        }
        assertFixture(mapper.readValue(get(DETAIL).body(), TelemetryObservation.class));
    }

    @Test
    void actualSqlFailureHasSafe503AndRecoversWithoutChangingRows() throws Exception {
        var before = snapshot();
        jdbc.execute("ALTER TABLE evse_telemetry_latest RENAME TO query_fault_fixture");
        try {
            assertError(get(LIST), 503, "TELEMETRY_QUERY_UNAVAILABLE");
            assertError(get(DETAIL), 503, "TELEMETRY_QUERY_UNAVAILABLE");
        } finally {
            jdbc.execute("ALTER TABLE query_fault_fixture RENAME TO evse_telemetry_latest");
        }
        assertThat(get(LIST).statusCode()).isEqualTo(200);
        assertThat(snapshot()).isEqualTo(before);
    }

    @Test
    void unavailableDatabaseConnectionHasSafe503ThenRecovers() throws Exception {
        var before = snapshot();
        try (var first = dataSource.getConnection(); var second = dataSource.getConnection()) {
            assertError(get(LIST), 503, "TELEMETRY_QUERY_UNAVAILABLE");
            assertError(get(DETAIL), 503, "TELEMETRY_QUERY_UNAVAILABLE");
        }
        assertThat(get(LIST).statusCode()).isEqualTo(200);
        assertThat(snapshot()).isEqualTo(before);
    }

    @Test
    void refusesUntrustedCertificate() throws Exception {
        try (var untrusted = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build()) {
            assertThatThrownBy(() -> send(untrusted, "https://localhost:" + port() + LIST))
                    .isInstanceOf(SSLHandshakeException.class);
        }
    }

    @Test
    void refusesHostnameMismatchEvenWithTrustedCertificate() {
        assertThatThrownBy(() -> send(CLIENT, "https://127.0.0.1:" + port() + DETAIL))
                .isInstanceOf(SSLHandshakeException.class);
    }

    @Test
    void plaintextCannotRetrieveTelemetryOrRedirectToAFallback() throws Exception {
        var response = send(CLIENT, "http://localhost:" + port() + LIST);
        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(response.headers().firstValue("location")).isEmpty();
        assertThat(response.body()).doesNotContain("query-fixture", "evses", "lastEventId");
    }

    @Test
    void missingTlsMaterialFailsStartupInsteadOfOpeningPlaintext() {
        var app = new SpringApplication(TelemetryWorkerApplication.class);
        app.addInitializers(context -> context.getEnvironment().getPropertySources().addFirst(
                new MapPropertySource("missing-tls-fixture", Map.of(
                        "spring.datasource.url", postgres.getJdbcUrl(),
                        "spring.datasource.username", postgres.getUsername(),
                        "spring.datasource.password", postgres.getPassword(),
                        "server.port", "0", "server.address", "127.0.0.1",
                        "server.ssl.key-store", "", "server.ssl.key-store-password", "",
                        "spring.main.banner-mode", "off", "logging.level.root", "OFF"))));
        assertThatThrownBy(() -> {
            try (var ignored = app.run()) {
                // Any successful server startup here violates the default TLS requirement.
            }
        }).isInstanceOf(ApplicationContextException.class)
                .hasRootCauseInstanceOf(IllegalStateException.class)
                .hasRootCauseMessage("Location must not be empty or null");
    }

    private void assertFixture(TelemetryObservation observation) {
        assertThat(observation.stationId()).isEqualTo("query-fixture");
        assertThat(observation.charging()).isTrue();
        assertThat(observation.power()).isEqualByComparingTo("120.12345678901234567890123");
        assertThat(observation.voltage()).isEqualByComparingTo("220.987654321");
        assertThat(observation.current()).isEqualByComparingTo("0.54321");
        assertThat(observation.occurredAt()).isEqualTo(Instant.parse("2025-01-01T00:00:00.123456Z"));
        assertThat(observation.receivedAt()).isEqualTo(Instant.parse("2025-01-01T00:00:01.654321Z"));
        assertThat(observation.updatedAt()).isEqualTo(Instant.parse("2025-01-01T00:00:02.111222Z"));
        assertThat(observation.lastEventId()).isEqualTo(UUID.fromString("11111111-1111-4111-8111-111111111111"));
    }

    private void assertError(HttpResponse<String> response, int status, String code) {
        assertThat(response.statusCode()).isEqualTo(status);
        var node = mapper.readTree(response.body());
        assertThat(node.properties()).hasSize(2);
        assertThat(node.get("code").asText()).isEqualTo(code);
        assertThat(node.get("message").asText()).isNotBlank();
        assertThat(response.body()).doesNotContain("SELECT", "evse_telemetry_latest", "jdbc:", "Hikari", "Exception");
    }

    private List<String> snapshot() {
        return jdbc.queryForList("SELECT row_to_json(t)::text FROM evse_telemetry_latest t ORDER BY station_id, evse_id", String.class);
    }

    private int port() {
        return environment.getRequiredProperty("local.server.port", Integer.class);
    }

    private HttpResponse<String> get(String path) throws Exception {
        return send(CLIENT, "https://localhost:" + port() + path);
    }

    private HttpResponse<String> send(HttpClient client, String url) throws Exception {
        return client.send(HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(5)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private record StationResponse(String stationId, List<TelemetryObservation> evses) {
    }
}
