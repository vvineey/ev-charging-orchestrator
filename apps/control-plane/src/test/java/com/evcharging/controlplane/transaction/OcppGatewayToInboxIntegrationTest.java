package com.evcharging.controlplane.transaction;

import com.evcharging.ocppgateway.OcppGatewayApplication;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.time.Duration;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {"transaction.ingress.enabled=true",
                "transaction.kafka-topic=charging.transaction.observed.v1"})
@EmbeddedKafka(partitions = 1, topics = "charging.transaction.observed.v1",
        bootstrapServersProperty = "spring.kafka.bootstrap-servers",
        brokerProperties = "offsets.topic.num.partitions=1")
class OcppGatewayToInboxIntegrationTest {
    private static final EmbeddedPostgres POSTGRES;
    static {
        try {
            POSTGRES = EmbeddedPostgres.builder().start();
        } catch (IOException exception) {
            throw new ExceptionInInitializerError(exception);
        }
    }

    @DynamicPropertySource
    static void database(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", () -> POSTGRES.getJdbcUrl("postgres", "postgres"));
        registry.add("spring.datasource.username", () -> "postgres");
        registry.add("spring.datasource.password", () -> "postgres");
    }

    @AfterAll
    static void closePostgres() throws IOException {
        POSTGRES.close();
    }

    @Autowired EmbeddedKafkaBroker broker;
    @Autowired JdbcClient jdbc;
    @Autowired ObjectMapper mapper;
    @TempDir Path directory;

    @Test
    void wssGatewayKafkaAndInboxPreserveEvseAbsenceAndAllMeterGroups() throws Exception {
        var tls = new LocalTls(directory);
        try (var gateway = startGateway(tls, "CS-E2E")) {
            int port = Integer.parseInt(gateway.getEnvironment().getRequiredProperty("local.server.port"));
            var replies = new ArrayBlockingQueue<String>(2);
            try (var client = HttpClient.newBuilder().sslContext(tls.trustedContext).build()) {
                String credentials = Base64.getEncoder().encodeToString(
                        "CS-E2E:test-credential-only".getBytes(StandardCharsets.UTF_8));
                WebSocket socket = client.newWebSocketBuilder().subprotocols("ocpp2.0.1")
                        .header("Authorization", "Basic " + credentials)
                        .connectTimeout(Duration.ofSeconds(5))
                        .buildAsync(URI.create("wss://localhost:" + port + "/ocpp/CS-E2E"),
                                new WebSocket.Listener() {
                                    @Override
                                    public java.util.concurrent.CompletionStage<?> onText(
                                            WebSocket webSocket, CharSequence data, boolean last) {
                                        replies.add(data.toString());
                                        webSocket.request(1);
                                        return null;
                                    }
                                }).get(10, TimeUnit.SECONDS);
                try {
                    socket.sendText("""
                            [2,"start","TransactionEvent",{"eventType":"Started",
                            "timestamp":"2026-01-01T10:00:00Z","triggerReason":"EVConnected","seqNo":0,
                            "transactionInfo":{"transactionId":"TX-E2E"},"evse":{"id":1},
                            "meterValue":[{"timestamp":"2026-01-01T10:00:00Z","sampledValue":[
                            {"value":10000,"context":"Transaction.Begin",
                            "measurand":"Energy.Active.Import.Register",
                            "unitOfMeasure":{"unit":"Wh","multiplier":0}}]}]}]
                            """, true).get(10, TimeUnit.SECONDS);
                    assertReply(replies);
                    socket.sendText("""
                            [2,"end","TransactionEvent",{"eventType":"Ended",
                            "timestamp":"2026-01-01T10:20:00Z","triggerReason":"EVDisconnected","seqNo":1,
                            "transactionInfo":{"transactionId":"TX-E2E"},"meterValue":[
                            {"timestamp":"2026-01-01T10:00:00Z","sampledValue":[
                            {"value":10000,"context":"Transaction.Begin",
                            "measurand":"Energy.Active.Import.Register"}]},
                            {"timestamp":"2026-01-01T10:20:00Z","sampledValue":[
                            {"value":12500,"context":"Transaction.End",
                            "measurand":"Energy.Active.Import.Register"},
                            {"value":230,"measurand":"Voltage"}]},
                            {"timestamp":"2026-01-01T10:20:00Z","sampledValue":[
                            {"value":12.5,"context":"Transaction.End",
                            "measurand":"Energy.Active.Import.Register",
                            "unitOfMeasure":{"unit":"kWh"}}]}]}]
                            """, true).get(10, TimeUnit.SECONDS);
                    assertReply(replies);
                    await().atMost(Duration.ofSeconds(25)).untilAsserted(() -> {
                        assertThat(jdbc.sql("""
                                SELECT status FROM transaction_inbox_state
                                WHERE charging_station_id = 'CS-E2E' AND transaction_id = 'TX-E2E'
                                """).query(String.class).single()).isEqualTo("HOLD");
                        assertThat(jdbc.sql("""
                                SELECT hold_reason FROM transaction_inbox_state
                                WHERE charging_station_id = 'CS-E2E' AND transaction_id = 'TX-E2E'
                                """).query(String.class).single()).isEqualTo("METER_POLICY_PENDING");
                        assertThat(jdbc.sql("""
                                SELECT count(*) FROM transaction_inbox_event
                                WHERE charging_station_id = 'CS-E2E' AND transaction_id = 'TX-E2E'
                                """).query(Long.class).single()).isEqualTo(2);
                    });
                    assertThat(jdbc.sql("""
                            SELECT jsonb_array_length(source_document->'payload'->'meterValue')
                            FROM transaction_inbox_event
                            WHERE charging_station_id = 'CS-E2E' AND transaction_id = 'TX-E2E' AND seq_no = 1
                            """).query(Integer.class).single()).isEqualTo(3);
                    assertThat(jdbc.sql("""
                            SELECT jsonb_exists(source_document->'payload', 'evse')
                            FROM transaction_inbox_event
                            WHERE charging_station_id = 'CS-E2E' AND transaction_id = 'TX-E2E' AND seq_no = 1
                            """).query(Boolean.class).single()).isFalse();
                    assertThat(jdbc.sql("""
                            SELECT count(*) FROM transaction_session_candidate
                            WHERE charging_station_id = 'CS-E2E' AND transaction_id = 'TX-E2E'
                            """).query(Long.class).single()).isZero();
                } finally {
                    socket.sendClose(WebSocket.NORMAL_CLOSURE, "done").get(5, TimeUnit.SECONDS);
                    socket.abort();
                }
            }
        }
    }

    @Test
    void pinnedSapSimulatorTransactionReachesInboxAndStaysOnHold() throws Exception {
        String simulatorDirectory = System.getenv("EVC_SAP_INBOX_SIMULATOR_DIR");
        Assumptions.assumeTrue(simulatorDirectory != null && Files.isDirectory(Path.of(simulatorDirectory)),
                "Pinned SAP simulator is an optional, separately obtained E3 input");
        Path simulator = Path.of(simulatorDirectory);
        Path rootConfig = simulator.resolve("dist/assets/config.json");
        Path template = simulator.resolve("dist/assets/station-templates/boa-keba.station-template.json");
        Path savedStation = simulator.resolve("dist/assets/configurations/"
                + "e9041c294a82a2d6aa194a801c3ba39d6b24d1cb16c0a0b3db4e37c9fe4e80cbb0808843f66a320b3f58df0288c8e98a.json");
        Assumptions.assumeTrue(Files.isRegularFile(rootConfig) && Files.isRegularFile(template)
                && Files.isRegularFile(savedStation), "Expected pinned simulator configuration is absent");

        var tls = new LocalTls(directory);
        Map<Path, byte[]> originals = new HashMap<>();
        Process process = null;
        try (var gateway = startGateway(tls, "CS-KEBA-OCPP2-00001")) {
            int port = Integer.parseInt(gateway.getEnvironment().getRequiredProperty("local.server.port"));
            String url = "wss://localhost:" + port + "/ocpp";
            for (Path path : List.of(rootConfig, template, savedStation)) {
                originals.put(path, Files.readAllBytes(path));
                ObjectNode config = (ObjectNode) mapper.readTree(originals.get(path));
                ObjectNode station = path.equals(savedStation) ? (ObjectNode) config.path("stationInfo") : config;
                station.putArray("supervisionUrls").add(url);
                if (!path.equals(rootConfig)) {
                    station.put("supervisionUrlOcppConfiguration", false);
                    station.put("supervisionUser", "CS-KEBA-OCPP2-00001");
                    station.put("supervisionPassword", "test-credential-only");
                }
                Files.writeString(path, mapper.writeValueAsString(config));
            }
            var launch = new ProcessBuilder("node", "dist/start.js").directory(simulator.toFile())
                    .redirectErrorStream(true).redirectOutput(directory.resolve("sap-simulator.log").toFile());
            launch.environment().put("NODE_EXTRA_CA_CERTS", tls.certificate.toString());
            launch.environment().put("NODE_ENV", "production");
            process = launch.start();

            String completedTransaction = """
                    SELECT transaction_id FROM transaction_inbox_event
                    WHERE charging_station_id = 'CS-KEBA-OCPP2-00001'
                    GROUP BY transaction_id
                    HAVING count(*) BETWEEN 2 AND 3
                       AND min(seq_no) = 0 AND max(seq_no) = count(*) - 1
                       AND count(*) FILTER (WHERE event_type = 'Started' AND seq_no = 0) = 1
                       AND count(*) FILTER (WHERE event_type = 'Ended') = 1
                       AND max(seq_no) FILTER (WHERE event_type = 'Ended') = max(seq_no)
                    ORDER BY transaction_id LIMIT 1
                    """;
            await().atMost(Duration.ofSeconds(70)).untilAsserted(() ->
                    assertThat(jdbc.sql(completedTransaction).query(String.class).optional()).isPresent());
            String transactionId = jdbc.sql(completedTransaction).query(String.class).single();
            await().atMost(Duration.ofSeconds(25)).untilAsserted(() -> {
                assertThat(jdbc.sql("""
                        SELECT status FROM transaction_inbox_state
                        WHERE charging_station_id = 'CS-KEBA-OCPP2-00001' AND transaction_id = :id
                        """).param("id", transactionId).query(String.class).single()).isEqualTo("HOLD");
            });
            assertThat(jdbc.sql("""
                    SELECT hold_reason FROM transaction_inbox_state
                    WHERE charging_station_id = 'CS-KEBA-OCPP2-00001' AND transaction_id = :id
                    """).param("id", transactionId).query(String.class).single())
                    .isEqualTo("METER_POLICY_PENDING");
            assertThat(jdbc.sql("""
                    SELECT jsonb_array_length(source_document->'payload'->'meterValue')
                    FROM transaction_inbox_event
                    WHERE charging_station_id = 'CS-KEBA-OCPP2-00001' AND transaction_id = :id
                      AND event_type = 'Ended'
                    """).param("id", transactionId).query(Integer.class).single()).isEqualTo(3);
            assertThat(jdbc.sql("""
                    SELECT count(*) FROM transaction_inbox_event
                    WHERE charging_station_id = 'CS-KEBA-OCPP2-00001' AND transaction_id = :id
                      AND seq_no IN (1, 2)
                      AND jsonb_exists(source_document->'payload', 'evse')
                    """).param("id", transactionId).query(Long.class).single()).isZero();
            assertThat(jdbc.sql("""
                    SELECT count(*) FROM transaction_session_candidate
                    WHERE charging_station_id = 'CS-KEBA-OCPP2-00001' AND transaction_id = :id
                    """).param("id", transactionId).query(Long.class).single()).isZero();
        } finally {
            if (process != null) {
                process.destroyForcibly();
                process.waitFor(10, TimeUnit.SECONDS);
            }
            for (var entry : originals.entrySet()) Files.write(entry.getKey(), entry.getValue());
        }
    }

    private ConfigurableApplicationContext startGateway(LocalTls tls, String chargingStationId) {
        return new SpringApplicationBuilder(OcppGatewayApplication.class)
                .web(WebApplicationType.SERVLET)
                .properties("server.address=127.0.0.1", "server.port=0", "server.ssl.enabled=true",
                        "server.ssl.key-store=" + tls.keyStore.toUri(),
                        "server.ssl.key-store-password=" + tls.password,
                        "ocpp.ingress.enabled=true", "ocpp.station.charging-station-id=" + chargingStationId,
                        "ocpp.station.station-id=ST-E2E", "ocpp.station.allowed-evse-ids=1",
                        "ocpp.station.password=test-credential-only",
                        "ocpp.kafka-topic=charging.transaction.observed.v1",
                        "spring.kafka.bootstrap-servers=" + broker.getBrokersAsString(),
                        "spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer",
                        "spring.kafka.producer.value-serializer=org.apache.kafka.common.serialization.StringSerializer")
                .run();
    }

    private void assertReply(ArrayBlockingQueue<String> replies) throws Exception {
        String reply = replies.poll(10, TimeUnit.SECONDS);
        assertThat(reply).isNotNull();
        assertThat(mapper.readTree(reply).get(0).intValue()).isEqualTo(3);
    }

    private static final class LocalTls {
        final Path keyStore;
        final Path certificate;
        final String password = UUID.randomUUID().toString();
        final SSLContext trustedContext;

        LocalTls(Path directory) throws Exception {
            keyStore = directory.resolve("ocpp-gateway.p12");
            certificate = directory.resolve("ocpp-gateway.crt");
            var command = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "keytool")
                    .toString(), "-genkeypair", "-alias", "ocpp-gateway", "-keyalg", "RSA", "-keysize", "2048",
                    "-sigalg", "SHA256withRSA", "-dname", "CN=localhost", "-ext", "SAN=dns:localhost",
                    "-validity", "1", "-storetype", "PKCS12", "-keystore", keyStore.toString(),
                    "-storepass:env", "EVC_TEST_TLS_PASSWORD", "-keypass:env", "EVC_TEST_TLS_PASSWORD")
                    .redirectErrorStream(true).redirectOutput(directory.resolve("keytool.log").toFile());
            command.environment().put("EVC_TEST_TLS_PASSWORD", password);
            Process process = command.start();
            if (!process.waitFor(20, TimeUnit.SECONDS) || process.exitValue() != 0) {
                process.destroyForcibly();
                throw new IllegalStateException("test TLS material could not be created");
            }
            var export = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "keytool")
                    .toString(), "-exportcert", "-rfc", "-alias", "ocpp-gateway", "-keystore",
                    keyStore.toString(), "-storepass:env", "EVC_TEST_TLS_PASSWORD", "-file",
                    certificate.toString()).redirectErrorStream(true)
                    .redirectOutput(directory.resolve("keytool-export.log").toFile());
            export.environment().put("EVC_TEST_TLS_PASSWORD", password);
            Process exportProcess = export.start();
            if (!exportProcess.waitFor(20, TimeUnit.SECONDS) || exportProcess.exitValue() != 0) {
                exportProcess.destroyForcibly();
                throw new IllegalStateException("test TLS certificate could not be exported");
            }
            KeyStore store = KeyStore.getInstance("PKCS12");
            try (var input = Files.newInputStream(keyStore)) {
                store.load(input, password.toCharArray());
            }
            KeyStore trust = KeyStore.getInstance("PKCS12");
            trust.load(null, null);
            trust.setCertificateEntry("ocpp-gateway", store.getCertificate("ocpp-gateway"));
            var managers = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            managers.init(trust);
            trustedContext = SSLContext.getInstance("TLS");
            trustedContext.init(null, managers.getTrustManagers(), null);
        }
    }
}
