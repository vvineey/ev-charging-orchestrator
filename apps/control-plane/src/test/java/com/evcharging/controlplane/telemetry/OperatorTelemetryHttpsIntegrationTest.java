package com.evcharging.controlplane.telemetry;

import com.evcharging.controlplane.ControlPlaneApplication;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.web.server.servlet.context.ServletWebServerApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import tools.jackson.databind.ObjectMapper;

import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OperatorTelemetryHttpsIntegrationTest {
    private static final String LIST = "/api/v1/stations/api-fixture/telemetry/latest";
    private static final String DETAIL = "/api/v1/stations/api-fixture/evses/1/telemetry/latest";
    private static final String INTERNAL_LIST = "/internal/v1/stations/api-fixture/telemetry/latest";
    private static final String INTERNAL_DETAIL = "/internal/v1/stations/api-fixture/evses/1/telemetry/latest";
    private static final String OBSERVATION = """
            {"stationId":"api-fixture","evseId":1,"charging":true,
             "power":120.12345678901234567890123,"voltage":220.987654321,"current":0.54321,
             "occurredAt":"2025-01-01T00:00:00.123456Z","receivedAt":"2025-01-01T00:00:01.654321Z",
             "updatedAt":"2025-01-01T00:00:02.111222Z","lastEventId":"11111111-1111-4111-8111-111111111111"}
            """.trim();
    private static final String OBSERVATION_3 = OBSERVATION.replace("\"evseId\":1", "\"evseId\":3");
    private static final String LIST_BODY = "{\"stationId\":\"api-fixture\",\"evses\":["
            + OBSERVATION + "," + OBSERVATION_3 + "]}";
    private static final String EMPTY_BODY = "{\"stationId\":\"empty-fixture\",\"evses\":[]}";
    private static final String INVALID_BODY = "{\"code\":\"INVALID_TELEMETRY_QUERY\",\"message\":\"bad input\"}";
    private static final String NOT_FOUND_BODY = "{\"code\":\"TELEMETRY_NOT_FOUND\",\"message\":\"missing\"}";

    private static LocalHttpsWorkerStub stub;
    private static ConfigurableApplicationContext app;
    private static HttpClient browser;
    private static int appPort;
    private static final ObjectMapper JSON = new ObjectMapper();

    @BeforeAll
    static void start() {
        stub = new LocalHttpsWorkerStub();
        app = startApp("https://localhost:" + stub.port(), stub.trustStore);
        appPort = port(app);
        browser = HttpClient.newHttpClient();
    }

    @AfterAll
    static void stop() throws Exception {
        browser.close();
        app.close();
        stub.close();
        assertThat(stub.directory).doesNotExist();
    }

    @BeforeEach
    void reset() {
        stub.calls.set(0);
        stub.lastPath.set(null);
        stub.respond.set(path -> {
            if (INTERNAL_LIST.equals(path)) return LocalHttpsWorkerStub.Reply.json(200, LIST_BODY);
            if (INTERNAL_DETAIL.equals(path)) return LocalHttpsWorkerStub.Reply.json(200, OBSERVATION);
            if (path.endsWith("/stations/empty-fixture/telemetry/latest")) {
                return LocalHttpsWorkerStub.Reply.json(200, EMPTY_BODY);
            }
            return LocalHttpsWorkerStub.Reply.json(404, NOT_FOUND_BODY);
        });
    }

    @Test
    void preservesListAndDetailNumbersTimestampsAndEventId() throws Exception {
        var list = get(appPort, LIST);
        assertThat(list.statusCode()).isEqualTo(200);
        assertThat(list.headers().firstValue("content-type").orElseThrow()).contains("application/json");
        assertThat(list.body()).isEqualTo(LIST_BODY);
        assertThat(get(appPort, DETAIL).body()).isEqualTo(OBSERVATION);
        assertThat(stub.calls.get()).isEqualTo(2);
        assertThat(stub.lastPath.get()).isEqualTo(INTERNAL_DETAIL);
    }

    @Test
    void emptyListAndDefined400And404ArePassedWithCanonicalSafeErrors() throws Exception {
        assertThat(get(appPort, "/api/v1/stations/empty-fixture/telemetry/latest").body())
                .isEqualTo(EMPTY_BODY);
        stub.respond.set(path -> LocalHttpsWorkerStub.Reply.json(400, INVALID_BODY));
        assertError(get(appPort, LIST), 400, "INVALID_TELEMETRY_QUERY");
        stub.respond.set(path -> LocalHttpsWorkerStub.Reply.json(404, NOT_FOUND_BODY));
        assertError(get(appPort, DETAIL), 404, "TELEMETRY_NOT_FOUND");
        assertThat(get(appPort, DETAIL).body()).doesNotContain("missing");
    }

    @ParameterizedTest
    @ValueSource(strings = {"0", "-1", "abc", "1.5", "2147483648", "%20"})
    void rejectsInvalidEvseBeforeContactingWorker(String id) throws Exception {
        assertError(get(appPort, "/api/v1/stations/api-fixture/evses/" + id + "/telemetry/latest"),
                400, "INVALID_TELEMETRY_QUERY");
        assertThat(stub.calls.get()).isZero();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "/api/v1/stations/%20/telemetry/latest", "/api/v1/stations//telemetry/latest",
            "/api/v1/stations/%20/evses/1/telemetry/latest", "/api/v1/stations//evses/1/telemetry/latest"
    })
    void rejectsBlankStationBeforeContactingWorker(String path) throws Exception {
        assertError(get(appPort, path), 400, "INVALID_TELEMETRY_QUERY");
        assertThat(stub.calls.get()).isZero();
    }

    @Test
    void encodesStationAsOnePathSegment() throws Exception {
        stub.respond.set(path -> LocalHttpsWorkerStub.Reply.json(200,
                "{\"stationId\":\"api'+fixture\",\"evses\":[]}"));
        var response = get(appPort, "/api/v1/stations/api%27%2Bfixture/telemetry/latest");
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(stub.lastPath.get()).contains("api%27%2Bfixture");
    }

    @ParameterizedTest
    @ValueSource(ints = {204, 301, 418, 500, 503})
    void rejectsOtherWorkerStatusesWithoutRedirectOrRetry(int status) throws Exception {
        stub.respond.set(path -> LocalHttpsWorkerStub.Reply.json(status, "jdbc:postgresql://private/secret"));
        assertError(get(appPort, LIST), 503, "TELEMETRY_QUERY_UNAVAILABLE");
        assertThat(stub.calls.get()).isEqualTo(1);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "not-json", "{}", "{\"stationId\":\"api-fixture\",\"evses\":null}",
            "{\"stationId\":\"other\",\"evses\":[]}",
            "{\"stationId\":\"api-fixture\",\"stationId\":\"other\",\"evses\":[]}",
            "{\"stationId\":\"api-fixture\",\"evses\":[]} {\"extra\":true}",
            "{\"stationId\":\"api-fixture\",\"evses\":[{}]}",
            "{\"stationId\":\"api-fixture\",\"evses\":[{\"evseId\":3},{\"evseId\":1}]}"
    })
    void rejectsMalformedOrInconsistentListResponse(String body) throws Exception {
        stub.respond.set(path -> LocalHttpsWorkerStub.Reply.json(200, body));
        assertError(get(appPort, LIST), 503, "TELEMETRY_QUERY_UNAVAILABLE");
    }

    @Test
    void rejectsWrongDetailIdOrTooPreciseTimestamp() throws Exception {
        stub.respond.set(path -> LocalHttpsWorkerStub.Reply.json(200,
                OBSERVATION.replace("\"evseId\":1", "\"evseId\":2")));
        assertError(get(appPort, DETAIL), 503, "TELEMETRY_QUERY_UNAVAILABLE");
        stub.respond.set(path -> LocalHttpsWorkerStub.Reply.json(200,
                OBSERVATION.replace(".123456Z", ".123456789Z")));
        assertError(get(appPort, DETAIL), 503, "TELEMETRY_QUERY_UNAVAILABLE");
    }

    @Test
    void rejectsMalformedWorkerTimestampWithSafe503() throws Exception {
        for (String timestamp : new String[]{
                "2025-01-01T00:00:00.123456Z", "2025-01-01T00:00:01.654321Z",
                "2025-01-01T00:00:02.111222Z"
        }) {
            stub.respond.set(path -> LocalHttpsWorkerStub.Reply.json(200,
                    OBSERVATION.replace(timestamp, "not-an-instantZ")));
            assertError(get(appPort, DETAIL), 503, "TELEMETRY_QUERY_UNAVAILABLE");
        }
    }

    @Test
    void rejectsWrongErrorCodeAndUnexpectedList404() throws Exception {
        stub.respond.set(path -> LocalHttpsWorkerStub.Reply.json(400, NOT_FOUND_BODY));
        assertError(get(appPort, LIST), 503, "TELEMETRY_QUERY_UNAVAILABLE");
        stub.respond.set(path -> LocalHttpsWorkerStub.Reply.json(404, NOT_FOUND_BODY));
        assertError(get(appPort, LIST), 503, "TELEMETRY_QUERY_UNAVAILABLE");
    }

    @Test
    void deadlineIncludesDelayedHeadersAndDelayedBody() throws Exception {
        for (boolean delayBody : new boolean[]{false, true}) {
            stub.calls.set(0);
            stub.respond.set(path -> new LocalHttpsWorkerStub.Reply(200, LIST_BODY,
                    delayBody ? 0 : 2_000, delayBody ? 2_000 : 0));
            long start = System.nanoTime();
            assertError(get(appPort, LIST), 503, "TELEMETRY_QUERY_UNAVAILABLE");
            assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofMillis(1_850));
            assertThat(stub.calls.get()).isEqualTo(1);
        }
    }

    @Test
    void refusedConnectionProduces503WithoutCachedSuccess() throws Exception {
        assertThat(get(appPort, DETAIL).statusCode()).isEqualTo(200);
        int freePort;
        try (var socket = new ServerSocket(0)) {
            freePort = socket.getLocalPort();
        }
        try (var disconnected = startApp("https://localhost:" + freePort, stub.trustStore)) {
            assertError(get(port(disconnected), DETAIL), 503, "TELEMETRY_QUERY_UNAVAILABLE");
        }
    }

    @Test
    void untrustedCertificateAndHostnameMismatchProduce503() throws Exception {
        try (var untrusted = startApp("https://localhost:" + stub.port(), stub.untrustedStore)) {
            assertError(get(port(untrusted), DETAIL), 503, "TELEMETRY_QUERY_UNAVAILABLE");
        }
        try (var wrongHost = startApp("https://127.0.0.1:" + stub.port(), stub.trustStore)) {
            assertError(get(port(wrongHost), DETAIL), 503, "TELEMETRY_QUERY_UNAVAILABLE");
        }
    }

    @Test
    void rejectsHttpWorkerOriginAtStartup() {
        assertThatThrownBy(() -> {
            try (var ignored = startApp("http://localhost:" + stub.port(), stub.trustStore)) {
            }
        }).hasStackTraceContaining("must be an HTTPS origin");
    }

    private static ConfigurableApplicationContext startApp(String workerUrl, Path trustStore) {
        return new SpringApplication(ControlPlaneApplication.class).run(
                "--server.address=127.0.0.1", "--server.port=0",
                "--telemetry.worker.base-url=" + workerUrl,
                "--telemetry.worker.trust-store=" + trustStore,
                "--telemetry.worker.trust-store-password=" + stub.password,
                "--telemetry.worker.deadline=1s", "--spring.main.banner-mode=off");
    }

    private static int port(ConfigurableApplicationContext context) {
        return ((ServletWebServerApplicationContext) context).getWebServer().getPort();
    }

    private HttpResponse<String> get(int port, String path) throws Exception {
        return browser.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                        .timeout(Duration.ofSeconds(4)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private void assertError(HttpResponse<String> response, int status, String code) {
        assertThat(response.statusCode()).isEqualTo(status);
        var body = JSON.readTree(response.body());
        assertThat(body.properties()).hasSize(2);
        assertThat(body.get("code").asString()).isEqualTo(code);
        assertThat(body.get("message").asString()).isNotBlank();
        assertThat(response.body()).doesNotContain("jdbc:", "private", "secret", "Exception", "localhost");
    }
}
