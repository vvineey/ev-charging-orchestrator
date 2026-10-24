package com.evcharging.controlplane.transaction;

import com.evcharging.ocppgateway.OcppGatewayApplication;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
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
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.sql.DriverManager;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Base64;
import java.util.ArrayList;
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
    private static final boolean PROVISIONAL_SAP_TEST =
            Boolean.parseBoolean(System.getenv("EVC_SAP_INBOX_PROVISIONAL"));
    private static final boolean CLOSEOUT_WSS_TEST =
            Boolean.parseBoolean(System.getenv("EVC_WSS_CLOSEOUT_TEST"));
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
        registry.add("transaction.recovery.experimental-candidate-calculation-enabled",
                () -> PROVISIONAL_SAP_TEST || CLOSEOUT_WSS_TEST);
    }

    @AfterAll
    static void closePostgres() throws IOException {
        POSTGRES.close();
    }

    @Autowired EmbeddedKafkaBroker broker;
    @Autowired JdbcClient jdbc;
    @Autowired ObjectMapper mapper;
    @Autowired KafkaListenerEndpointRegistry listenerRegistry;
    @TempDir Path directory;

    @Test
    void wssGatewayKafkaAndInboxPreserveEvseAbsenceAndAllMeterGroups() throws Exception {
        Assumptions.assumeFalse(PROVISIONAL_SAP_TEST || CLOSEOUT_WSS_TEST,
                "Opt-in candidate run selects an experimental transaction only");
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
    void fixedWssTransactionKeepsOutboxIntentAcrossWriterCrash() throws Exception {
        Assumptions.assumeTrue(CLOSEOUT_WSS_TEST, "Fixed WSS closeout is an opt-in test-only path");
        String transactionId = "TX-EXP0004-WSS";
        String outputTopic = "exp0004-wss-closeout-" + UUID.randomUUID();
        try (var admin = AdminClient.create(Map.of("bootstrap.servers", broker.getBrokersAsString()))) {
            admin.createTopics(List.of(new NewTopic(outputTopic, 1, (short) 1)))
                    .all().get(10, TimeUnit.SECONDS);
        }
        jdbc.sql("""
                CREATE TABLE exp0004_closeout (
                    charging_station_id TEXT NOT NULL,
                    transaction_id TEXT NOT NULL,
                    station_id TEXT NOT NULL,
                    energy_wh NUMERIC(24,6) NOT NULL,
                    event_id UUID NOT NULL UNIQUE,
                    PRIMARY KEY (charging_station_id, transaction_id)
                )
                """).update();
        jdbc.sql("""
                CREATE TABLE exp0004_outbox (
                    event_id UUID PRIMARY KEY,
                    transaction_id TEXT NOT NULL UNIQUE,
                    station_id TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    published_at TIMESTAMPTZ
                )
                """).update();

        var tls = new LocalTls(directory);
        try (var gateway = startGateway(tls, "CS-TEST", "ST-TEST")) {
            int port = Integer.parseInt(gateway.getEnvironment().getRequiredProperty("local.server.port"));
            var replies = new ArrayBlockingQueue<String>(2);
            try (var client = HttpClient.newBuilder().sslContext(tls.trustedContext).build()) {
                String credentials = Base64.getEncoder().encodeToString(
                        "CS-TEST:test-credential-only".getBytes(StandardCharsets.UTF_8));
                WebSocket socket = client.newWebSocketBuilder().subprotocols("ocpp2.0.1")
                        .header("Authorization", "Basic " + credentials)
                        .connectTimeout(Duration.ofSeconds(5))
                        .buildAsync(URI.create("wss://localhost:" + port + "/ocpp/CS-TEST"),
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
                            [2,"exp0004-start","TransactionEvent",{"eventType":"Started",
                            "timestamp":"2026-01-01T10:00:00Z","triggerReason":"EVConnected","seqNo":0,
                            "transactionInfo":{"transactionId":"TX-EXP0004-WSS"},"evse":{"id":1},
                            "meterValue":[{"timestamp":"2026-01-01T10:00:00Z","sampledValue":[
                            {"value":10000,"context":"Transaction.Begin",
                            "measurand":"Energy.Active.Import.Register",
                            "unitOfMeasure":{"unit":"Wh","multiplier":0}}]}]}]
                            """, true).get(10, TimeUnit.SECONDS);
                    assertReply(replies);
                    socket.sendText("""
                            [2,"exp0004-end","TransactionEvent",{"eventType":"Ended",
                            "timestamp":"2026-01-01T10:20:00Z","triggerReason":"EVDisconnected","seqNo":1,
                            "transactionInfo":{"transactionId":"TX-EXP0004-WSS"},"meterValue":[
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
                } finally {
                    socket.sendClose(WebSocket.NORMAL_CLOSURE, "done").get(5, TimeUnit.SECONDS);
                    socket.abort();
                }
            }

            await().atMost(Duration.ofSeconds(25)).untilAsserted(() -> {
                assertThat(jdbc.sql("""
                        SELECT candidate_status FROM transaction_session_candidate
                        WHERE charging_station_id = 'CS-TEST' AND transaction_id = :id
                        """).param("id", transactionId).query(String.class).single())
                        .isEqualTo("PROVISIONAL");
            });
            assertThat(jdbc.sql("""
                    SELECT energy_wh FROM transaction_session_candidate
                    WHERE charging_station_id = 'CS-TEST' AND transaction_id = :id
                    """).param("id", transactionId).query(BigDecimal.class).single())
                    .isEqualByComparingTo("2500");
            assertThat(jdbc.sql("""
                    SELECT count(*) FROM transaction_inbox_event
                    WHERE charging_station_id = 'CS-TEST' AND transaction_id = :id
                    """).param("id", transactionId).query(Long.class).single()).isEqualTo(2);

            assertThat(runExp0004OutboxChild("write", outputTopic, transactionId, "2501"))
                    .isNotZero();
            assertThat(Files.readString(directory.resolve("exp0004-write.log")))
                    .contains("test closeout precondition failed or duplicate");
            assertThat(exp0004Count("exp0004_closeout", transactionId, null)).isZero();
            assertThat(exp0004Count("exp0004_outbox", transactionId, null)).isZero();
            assertThat(readExp0004Output(outputTopic, 0)).isEmpty();

            assertThat(runExp0004OutboxChild("write-rollback-before-outbox", outputTopic,
                    transactionId)).isNotZero();
            assertThat(Files.readString(directory.resolve("exp0004-write-rollback-before-outbox.log")))
                    .contains("injected rollback before Outbox insert");
            assertThat(exp0004Count("exp0004_closeout", transactionId, null)).isZero();
            assertThat(exp0004Count("exp0004_outbox", transactionId, null)).isZero();

            assertThat(runExp0004OutboxChild("write-then-halt", outputTopic, transactionId)).isEqualTo(137);
            assertThat(Files.readString(directory.resolve("exp0004-write-then-halt.log")))
                    .contains("OUTBOX_COMMITTED_BEFORE_RELAY");
            assertThat(exp0004Count("exp0004_closeout", transactionId, null)).isEqualTo(1);
            assertThat(exp0004Count("exp0004_outbox", transactionId, false)).isEqualTo(1);
            assertThat(readExp0004Output(outputTopic, 0)).isEmpty();

            assertThat(runExp0004OutboxChild("write", outputTopic, transactionId)).isNotZero();
            assertThat(Files.readString(directory.resolve("exp0004-write.log")))
                    .contains("test closeout precondition failed or duplicate");
            assertThat(exp0004Count("exp0004_closeout", transactionId, null)).isEqualTo(1);
            assertThat(exp0004Count("exp0004_outbox", transactionId, false)).isEqualTo(1);
            assertThat(readExp0004Output(outputTopic, 0)).isEmpty();

            assertThat(runExp0004OutboxChild("relay", outputTopic, transactionId)).isZero();
            assertThat(exp0004Count("exp0004_outbox", transactionId, true)).isEqualTo(1);
            var output = readExp0004Output(outputTopic, 1);
            assertThat(output).hasSize(1);
            assertThat(output.getFirst().path("eventId").asText()).isEqualTo(jdbc.sql("""
                    SELECT event_id FROM exp0004_outbox WHERE transaction_id = :id
                    """).param("id", transactionId).query(UUID.class).single().toString());
            assertThat(output.getFirst().path("payload").path("energyWh").decimalValue())
                    .isEqualByComparingTo("2500");
            try (var effectDatabase = EmbeddedPostgres.builder().start()) {
                String effectJdbcUrl = effectDatabase.getJdbcUrl("postgres", "postgres");
                try (var connection = DriverManager.getConnection(effectJdbcUrl, "postgres", "postgres");
                     var statement = connection.createStatement()) {
                    statement.execute("""
                            CREATE TABLE exp0004_effect (
                                event_id UUID PRIMARY KEY,
                                transaction_id TEXT NOT NULL UNIQUE
                            )
                            """);
                }
                String groupId = "exp0004-wss-effect-" + UUID.randomUUID();
                assertThat(runExp0004ConsumerChild(outputTopic, effectJdbcUrl, groupId)).isZero();
                try (var connection = DriverManager.getConnection(effectJdbcUrl, "postgres", "postgres");
                     var statement = connection.prepareStatement("""
                             SELECT count(*) FROM exp0004_effect
                             WHERE event_id = ? AND transaction_id = ?
                             """)) {
                    statement.setObject(1, UUID.fromString(output.getFirst().path("eventId").asText()));
                    statement.setString(2, transactionId);
                    try (var rows = statement.executeQuery()) {
                        rows.next();
                        assertThat(rows.getLong(1)).isEqualTo(1);
                    }
                }
                try (var admin = AdminClient.create(Map.of("bootstrap.servers", broker.getBrokersAsString()))) {
                    assertThat(admin.listConsumerGroupOffsets(groupId)
                            .partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS)
                            .get(new TopicPartition(outputTopic, 0)).offset()).isEqualTo(1);
                }
            }
        }
    }

    @Test
    void pinnedSapSimulatorTransactionDistinguishesSameAndChangedSourceReplays() throws Exception {
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
                    ObjectNode generator = path.equals(savedStation)
                            ? (ObjectNode) config.path("automaticTransactionGenerator")
                            : (ObjectNode) config.path("AutomaticTransactionGenerator");
                    generator.put("enable", true);
                    generator.put("minDuration", 35);
                    generator.put("maxDuration", 35);
                    generator.put("minDelayBetweenTwoTransactions", 1);
                    generator.put("maxDelayBetweenTwoTransactions", 1);
                    generator.put("probabilityOfStart", 1);
                    generator.put("stopAfterHours", 0.1);
                    // This ingress test does not implement the separate OCPP Authorize request.
                    generator.put("requireAuthorize", false);
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
                        """).param("id", transactionId).query(String.class).single())
                        .isEqualTo(PROVISIONAL_SAP_TEST ? "PROVISIONAL" : "HOLD");
            });
            assertThat(jdbc.sql("""
                    SELECT coalesce(hold_reason, '') FROM transaction_inbox_state
                    WHERE charging_station_id = 'CS-KEBA-OCPP2-00001' AND transaction_id = :id
                    """).param("id", transactionId).query(String.class).single())
                    .isEqualTo(PROVISIONAL_SAP_TEST ? "" : "METER_POLICY_PENDING");
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
                    """).param("id", transactionId).query(Long.class).single())
                    .isEqualTo(PROVISIONAL_SAP_TEST ? 1 : 0);
            if (PROVISIONAL_SAP_TEST) {
                BigDecimal expectedWh = sapBoundaryWh(transactionId, "Ended", "Transaction.End")
                        .subtract(sapBoundaryWh(transactionId, "Started", "Transaction.Begin"));
                assertThat(expectedWh).isPositive();
                assertThat(jdbc.sql("""
                        SELECT energy_wh FROM transaction_session_candidate
                        WHERE charging_station_id = 'CS-KEBA-OCPP2-00001' AND transaction_id = :id
                        """).param("id", transactionId).query(BigDecimal.class).single())
                        .isEqualByComparingTo(expectedWh);
                assertThat(jdbc.sql("""
                        SELECT candidate_status FROM transaction_session_candidate
                        WHERE charging_station_id = 'CS-KEBA-OCPP2-00001' AND transaction_id = :id
                        """).param("id", transactionId).query(String.class).single())
                        .isEqualTo("PROVISIONAL");
            }
            process.destroyForcibly();
            process.waitFor(10, TimeUnit.SECONDS);
            process = null;
            assertSameSourceReplayPreservesState(transactionId);
            assertChangedSourceReplayMovesToConflict(transactionId);
        } finally {
            if (process != null) {
                process.destroyForcibly();
                process.waitFor(10, TimeUnit.SECONDS);
            }
            for (var entry : originals.entrySet()) Files.write(entry.getKey(), entry.getValue());
        }
    }

    private BigDecimal sapBoundaryWh(String transactionId, String eventType, String context) throws Exception {
        JsonNode source = mapper.readTree(jdbc.sql("""
                SELECT source_document::text FROM transaction_inbox_event
                WHERE charging_station_id = 'CS-KEBA-OCPP2-00001' AND transaction_id = :id
                  AND event_type = :eventType
                """).param("id", transactionId).param("eventType", eventType)
                .query(String.class).single());
        BigDecimal value = null;
        for (JsonNode meter : source.path("payload").path("meterValue")) {
            for (JsonNode sample : meter.path("sampledValue")) {
                if (!context.equals(sample.path("context").asText())
                        || !"Energy.Active.Import.Register".equals(sample.path("measurand").asText())) continue;
                assertThat(sample.path("unitOfMeasure").path("unit").asText()).isEqualTo("Wh");
                assertThat(sample.path("location").asText()).isEqualTo("Outlet");
                BigDecimal observed = sample.path("value").decimalValue();
                if (value == null) value = observed;
                else assertThat(observed).isEqualByComparingTo(value);
            }
        }
        assertThat(value).as("SAP must provide an unambiguous " + context + " reading").isNotNull();
        return value;
    }

    private void assertSameSourceReplayPreservesState(String transactionId) throws Exception {
        String source = jdbc.sql("""
                SELECT jsonb_build_object(
                    'eventId', first_event_id,
                    'eventType', 'OcppTransactionEventObserved',
                    'schemaVersion', 1,
                    'occurredAt', source_document->'occurredAt',
                    'receivedAt', first_received_at,
                    'stationId', source_document->'stationId',
                    'chargingStationId', source_document->'chargingStationId',
                    'payload', source_document->'payload')::text
                FROM transaction_inbox_event
                WHERE charging_station_id = 'CS-KEBA-OCPP2-00001' AND transaction_id = :id
                  AND event_type = 'Ended'
                """).param("id", transactionId).query(String.class).single();
        long eventCount = jdbc.sql("""
                SELECT count(*) FROM transaction_inbox_event
                WHERE charging_station_id = 'CS-KEBA-OCPP2-00001' AND transaction_id = :id
                """).param("id", transactionId).query(Long.class).single();
        String updatedAt = jdbc.sql("""
                SELECT updated_at::text FROM transaction_inbox_state
                WHERE charging_station_id = 'CS-KEBA-OCPP2-00001' AND transaction_id = :id
                """).param("id", transactionId).query(String.class).single();
        publishAndWaitForInboxOffset(source);
        assertThat(jdbc.sql("""
                SELECT count(*) FROM transaction_inbox_event
                WHERE charging_station_id = 'CS-KEBA-OCPP2-00001' AND transaction_id = :id
                """).param("id", transactionId).query(Long.class).single()).isEqualTo(eventCount);
        assertThat(jdbc.sql("""
                SELECT count(*) FROM transaction_inbox_conflict
                WHERE charging_station_id = 'CS-KEBA-OCPP2-00001' AND transaction_id = :id
                """).param("id", transactionId).query(Long.class).single()).isZero();
        assertThat(jdbc.sql("""
                SELECT count(*) FROM transaction_session_candidate
                WHERE charging_station_id = 'CS-KEBA-OCPP2-00001' AND transaction_id = :id
                """).param("id", transactionId).query(Long.class).single())
                .isEqualTo(PROVISIONAL_SAP_TEST ? 1 : 0);
        assertThat(jdbc.sql("""
                SELECT status FROM transaction_inbox_state
                WHERE charging_station_id = 'CS-KEBA-OCPP2-00001' AND transaction_id = :id
                """).param("id", transactionId).query(String.class).single())
                .isEqualTo(PROVISIONAL_SAP_TEST ? "PROVISIONAL" : "HOLD");
        assertThat(jdbc.sql("""
                SELECT coalesce(hold_reason, '') FROM transaction_inbox_state
                WHERE charging_station_id = 'CS-KEBA-OCPP2-00001' AND transaction_id = :id
                """).param("id", transactionId).query(String.class).single())
                .isEqualTo(PROVISIONAL_SAP_TEST ? "" : "METER_POLICY_PENDING");
        assertThat(jdbc.sql("""
                SELECT updated_at::text FROM transaction_inbox_state
                WHERE charging_station_id = 'CS-KEBA-OCPP2-00001' AND transaction_id = :id
                """).param("id", transactionId).query(String.class).single()).isEqualTo(updatedAt);
    }

    private void assertChangedSourceReplayMovesToConflict(String transactionId) throws Exception {
        String source = jdbc.sql("""
                SELECT jsonb_build_object(
                    'eventId', first_event_id,
                    'eventType', 'OcppTransactionEventObserved',
                    'schemaVersion', 1,
                    'occurredAt', source_document->'occurredAt',
                    'receivedAt', first_received_at,
                    'stationId', source_document->'stationId',
                    'chargingStationId', source_document->'chargingStationId',
                    'payload', source_document->'payload')::text
                FROM transaction_inbox_event
                WHERE charging_station_id = 'CS-KEBA-OCPP2-00001' AND transaction_id = :id
                  AND event_type = 'Ended'
                """).param("id", transactionId).query(String.class).single();
        ObjectNode changed = (ObjectNode) mapper.readTree(source);
        changed.put("eventId", UUID.randomUUID().toString());
        changed.put("receivedAt", OffsetDateTime.now(ZoneOffset.UTC).toString());
        int changedSamples = 0;
        for (JsonNode meter : changed.path("payload").path("meterValue")) {
            for (JsonNode sample : meter.path("sampledValue")) {
                if ("Transaction.End".equals(sample.path("context").asText())
                        && "Energy.Active.Import.Register".equals(sample.path("measurand").asText())) {
                    ObjectNode value = (ObjectNode) sample;
                    value.put("value", sample.path("value").decimalValue().add(BigDecimal.ONE));
                    changedSamples++;
                }
            }
        }
        assertThat(changedSamples).isPositive();

        long originalEventCount = jdbc.sql("""
                SELECT count(*) FROM transaction_inbox_event
                WHERE charging_station_id = 'CS-KEBA-OCPP2-00001' AND transaction_id = :id
                """).param("id", transactionId).query(Long.class).single();
        String changedRecord = mapper.writeValueAsString(changed);
        var listener = listenerRegistry.getListenerContainer("transaction-inbox");
        assertThat(listener).isNotNull();
        listener.stop();
        long committedBefore = inboxOffset();
        long conflictOffset = publish(changedRecord);
        try {
            var builder = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                    "-cp", System.getProperty("transaction.test.runtime-classpath"),
                    CrashAfterTransactionCommitProcess.class.getName(),
                    "--spring.kafka.listener.auto-startup=false",
                    "--transaction.ingress.enabled=true",
                    "--transaction.kafka-topic=charging.transaction.observed.v1",
                    "--transaction.recovery.experimental-candidate-calculation-enabled="
                            + PROVISIONAL_SAP_TEST,
                    "--transaction.test.group-id=" + TransactionRecordListener.GROUP_ID,
                    "--spring.kafka.consumer.properties[session.timeout.ms]=6000",
                    "--spring.kafka.consumer.properties[heartbeat.interval.ms]=1000");
            builder.environment().put("SPRING_DATASOURCE_URL", POSTGRES.getJdbcUrl("postgres", "postgres"));
            builder.environment().put("SPRING_DATASOURCE_USERNAME", "postgres");
            builder.environment().put("SPRING_DATASOURCE_PASSWORD", "postgres");
            builder.environment().put("SPRING_KAFKA_BOOTSTRAPSERVERS", broker.getBrokersAsString());
            builder.redirectErrorStream(true).redirectOutput(directory.resolve("sap-conflict-crash-process.log").toFile());
            Process child = builder.start();
            try {
                assertThat(child.waitFor(45, TimeUnit.SECONDS))
                        .as("child JVM must commit the SAP-derived conflict and halt before offset commit").isTrue();
                assertThat(child.exitValue()).isEqualTo(137);
            } finally {
                if (child.isAlive()) {
                    child.destroyForcibly();
                    child.waitFor(10, TimeUnit.SECONDS);
                }
            }
            assertThat(inboxOffset()).isEqualTo(committedBefore);
        } finally {
            listener.start();
        }
        waitForInboxOffset(conflictOffset);
        assertThat(jdbc.sql("""
                SELECT status FROM transaction_inbox_state
                WHERE charging_station_id = 'CS-KEBA-OCPP2-00001' AND transaction_id = :id
                """).param("id", transactionId).query(String.class).single())
                .isEqualTo(PROVISIONAL_SAP_TEST ? "HOLD_AFTER_PROVISIONAL" : "HOLD_CONFLICT");
        assertThat(jdbc.sql("""
                SELECT hold_reason FROM transaction_inbox_state
                WHERE charging_station_id = 'CS-KEBA-OCPP2-00001' AND transaction_id = :id
                """).param("id", transactionId).query(String.class).single()).isEqualTo("SOURCE_CONFLICT");
        assertThat(jdbc.sql("""
                SELECT count(*) FROM transaction_inbox_event
                WHERE charging_station_id = 'CS-KEBA-OCPP2-00001' AND transaction_id = :id
                """).param("id", transactionId).query(Long.class).single()).isEqualTo(originalEventCount);
        assertThat(jdbc.sql("""
                SELECT count(*) FROM transaction_inbox_conflict
                WHERE charging_station_id = 'CS-KEBA-OCPP2-00001' AND transaction_id = :id
                """).param("id", transactionId).query(Long.class).single()).isEqualTo(1);
        assertThat(jdbc.sql("""
                SELECT count(*) FROM transaction_session_candidate
                WHERE charging_station_id = 'CS-KEBA-OCPP2-00001' AND transaction_id = :id
                """).param("id", transactionId).query(Long.class).single())
                .isEqualTo(PROVISIONAL_SAP_TEST ? 1 : 0);
        if (PROVISIONAL_SAP_TEST) {
            assertThat(jdbc.sql("""
                    SELECT candidate_status FROM transaction_session_candidate
                    WHERE charging_station_id = 'CS-KEBA-OCPP2-00001' AND transaction_id = :id
                    """).param("id", transactionId).query(String.class).single())
                    .isEqualTo("INVALIDATED");
            assertThat(jdbc.sql("""
                    SELECT invalidation_reason FROM transaction_session_candidate
                    WHERE charging_station_id = 'CS-KEBA-OCPP2-00001' AND transaction_id = :id
                    """).param("id", transactionId).query(String.class).single())
                    .isEqualTo("SOURCE_CONFLICT");
        }

        publishAndWaitForInboxOffset(changedRecord);
        assertThat(jdbc.sql("""
                SELECT count(*) FROM transaction_inbox_conflict
                WHERE charging_station_id = 'CS-KEBA-OCPP2-00001' AND transaction_id = :id
                """).param("id", transactionId).query(Long.class).single()).isEqualTo(1);
    }

    private void publishAndWaitForInboxOffset(String record) throws Exception {
        waitForInboxOffset(publish(record));
    }

    private long publish(String record) throws Exception {
        String topic = "charging.transaction.observed.v1";
        try (var producer = new KafkaProducer<String, String>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.ACKS_CONFIG, "all"))) {
            return producer.send(new ProducerRecord<>(topic, 0, "ST-E2E", record))
                    .get(10, TimeUnit.SECONDS).offset();
        }
    }

    private long inboxOffset() throws Exception {
        try (var admin = AdminClient.create(Map.of("bootstrap.servers", broker.getBrokersAsString()))) {
            return admin.listConsumerGroupOffsets(TransactionRecordListener.GROUP_ID)
                    .partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS)
                    .get(new TopicPartition("charging.transaction.observed.v1", 0)).offset();
        }
    }

    private void waitForInboxOffset(long recordOffset) {
        await().atMost(Duration.ofSeconds(25)).untilAsserted(() ->
                assertThat(inboxOffset()).isGreaterThan(recordOffset));
    }

    private int runExp0004OutboxChild(String mode, String topic, String transactionId) throws Exception {
        return runExp0004OutboxChild(mode, topic, transactionId, "2500");
    }

    private int runExp0004OutboxChild(String mode, String topic, String transactionId,
                                     String expectedWh) throws Exception {
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        String classpath = System.getProperty("transaction.test.runtime-classpath");
        assertThat(classpath).isNotBlank();
        var process = new ProcessBuilder(java, "-cp", classpath,
                Exp0004OutboxPollingProcess.class.getName(), mode,
                POSTGRES.getJdbcUrl("postgres", "postgres"), broker.getBrokersAsString(),
                topic, transactionId, expectedWh)
                .redirectErrorStream(true)
                .redirectOutput(directory.resolve("exp0004-" + mode + ".log").toFile())
                .start();
        if (!process.waitFor(30, TimeUnit.SECONDS)) {
            process.destroyForcibly();
            throw new IllegalStateException("test-only Outbox child did not finish");
        }
        return process.exitValue();
    }

    private int runExp0004ConsumerChild(String topic, String effectJdbcUrl, String groupId)
            throws Exception {
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        String classpath = System.getProperty("transaction.test.runtime-classpath");
        assertThat(classpath).isNotBlank();
        var process = new ProcessBuilder(java, "-cp", classpath,
                Exp0004IndependentConsumerProcess.class.getName(), "normal",
                broker.getBrokersAsString(), topic, effectJdbcUrl, groupId, "1")
                .redirectErrorStream(true)
                .redirectOutput(directory.resolve("exp0004-independent-consumer.log").toFile())
                .start();
        if (!process.waitFor(45, TimeUnit.SECONDS)) {
            process.destroyForcibly();
            throw new IllegalStateException("test-only independent consumer did not finish");
        }
        return process.exitValue();
    }

    private long exp0004Count(String table, String transactionId, Boolean published) {
        String sql = switch (table) {
            case "exp0004_closeout" -> "SELECT count(*) FROM exp0004_closeout WHERE transaction_id = :id";
            case "exp0004_outbox" -> "SELECT count(*) FROM exp0004_outbox WHERE transaction_id = :id"
                    + (published == null ? "" : published ? " AND published_at IS NOT NULL"
                    : " AND published_at IS NULL");
            default -> throw new IllegalArgumentException("unexpected test table " + table);
        };
        return jdbc.sql(sql).param("id", transactionId).query(Long.class).single();
    }

    private List<JsonNode> readExp0004Output(String topic, int expected) throws Exception {
        var partition = new TopicPartition(topic, 0);
        try (var consumer = new KafkaConsumer<String, String>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString(),
                ConsumerConfig.GROUP_ID_CONFIG, "exp0004-wss-output-" + UUID.randomUUID(),
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"))) {
            consumer.assign(List.of(partition));
            consumer.seekToBeginning(List.of(partition));
            var found = new ArrayList<JsonNode>();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
            while (System.nanoTime() < deadline) {
                for (var record : consumer.poll(Duration.ofMillis(200))) {
                    assertThat(record.key()).isEqualTo("ST-TEST");
                    found.add(mapper.readTree(record.value()));
                }
                if (expected > 0 && found.size() >= expected) break;
            }
            return found;
        }
    }

    private ConfigurableApplicationContext startGateway(LocalTls tls, String chargingStationId) {
        return startGateway(tls, chargingStationId, "ST-E2E");
    }

    private ConfigurableApplicationContext startGateway(LocalTls tls, String chargingStationId,
                                                      String stationId) {
        return new SpringApplicationBuilder(OcppGatewayApplication.class)
                .web(WebApplicationType.SERVLET)
                .properties("server.address=127.0.0.1", "server.port=0", "server.ssl.enabled=true",
                        "server.ssl.key-store=" + tls.keyStore.toUri(),
                        "server.ssl.key-store-password=" + tls.password,
                        "ocpp.ingress.enabled=true", "ocpp.station.charging-station-id=" + chargingStationId,
                        "ocpp.station.station-id=" + stationId, "ocpp.station.allowed-evse-ids=1",
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
