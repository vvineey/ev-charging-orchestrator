package com.evcharging.controlplane.transaction;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Opt-in real PostgreSQL WAL -> Debezium/Connect -> Kafka comparison path. */
class Exp0003OutboxCdcTest {
    private static final String BOOTSTRAP = "127.0.0.1:19093";
    private static final String TOPIC = "exp0003.outbox";
    private static final String CONNECT = "http://127.0.0.1:18083";
    private final ObjectMapper mapper = new ObjectMapper();
    private final HttpClient http = HttpClient.newHttpClient();

    @Test
    void walRelayResumesAfterConnectStopsWithoutLosingBusinessEffects() throws Exception {
        assumeTrue(Boolean.getBoolean("exp0003.cdc.enabled"),
                "Start the isolated Compose stack and use -Pexp0003Cdc=true");
        var dataSource = new DriverManagerDataSource(
                "jdbc:postgresql://127.0.0.1:15439/exp0003", "postgres", "exp0003_only");
        Exp0003OutboxFixture.createSchema(dataSource);
        Exp0003OutboxFixture.clear(dataSource);
        try (var admin = AdminClient.create(Map.of("bootstrap.servers", BOOTSTRAP))) {
            admin.createTopics(List.of(new NewTopic(TOPIC, 1, (short) 1))).all().get(10, TimeUnit.SECONDS);
        }
        waitForRestReady();
        registerConnector();
        waitForRunning();

        String firstKey = "work-cdc-first";
        UUID firstId = Exp0003OutboxFixture.commitWork(dataSource, firstKey, false);
        var firstRecord = readRecords(1).getFirst();
        assertRecord(firstRecord, firstId, firstKey);
        Exp0003OutboxFixture.applyEffect(dataSource, firstId, firstKey);

        compose("stop", "connect");
        String secondKey = "work-cdc-after-stop";
        UUID secondId = Exp0003OutboxFixture.commitWork(dataSource, secondKey, false);
        assertThat(slotCount(dataSource)).isEqualTo(1);
        assertThat(readRecords(1)).hasSize(1);

        compose("start", "connect");
        waitForRunning();
        var records = readRecords(2);
        assertThat(records).hasSize(2);
        assertRecord(records.get(0), firstId, firstKey);
        assertRecord(records.get(1), secondId, secondKey);
        Exp0003OutboxFixture.applyEffect(dataSource, firstId, firstKey);
        Exp0003OutboxFixture.applyEffect(dataSource, secondId, secondKey);
        assertThat(Exp0003OutboxFixture.count(dataSource, "exp0003_outbox")).isEqualTo(2);
        assertThat(Exp0003OutboxFixture.count(dataSource, "exp0003_effect")).isEqualTo(2);
        assertThat(Exp0003OutboxFixture.missingEffects(dataSource)).isZero();
        assertThat(Exp0003OutboxFixture.ghostEffects(dataSource)).isZero();
    }

    private void assertRecord(ConsumerRecord<String, String> record, UUID id, String key) {
        assertThat(record.key()).isEqualTo(key);
        var payload = mapper.readTree(record.value());
        assertThat(payload.path("eventId").asText()).isEqualTo(id.toString());
        assertThat(payload.path("workKey").asText()).isEqualTo(key);
        assertThat(payload.path("version").asInt()).isEqualTo(1);
    }

    private void registerConnector() throws Exception {
        String body = Files.readString(Path.of(System.getProperty("exp0003.cdc.connector-file")));
        var request = HttpRequest.newBuilder(URI.create(CONNECT + "/connectors"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body)).build();
        var response = http.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 201) {
            throw new IllegalStateException("Connector registration HTTP " + response.statusCode()
                    + ": " + response.body());
        }
    }

    private void waitForRestReady() throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (System.nanoTime() < deadline) {
            try {
                var response = http.send(HttpRequest.newBuilder(URI.create(CONNECT + "/connectors"))
                                .timeout(Duration.ofSeconds(3)).GET().build(),
                        HttpResponse.BodyHandlers.discarding());
                if (response.statusCode() == 200) return;
            } catch (java.io.IOException ignored) {
                // Jetty can answer before the Kafka Connect herder is ready.
            }
            Thread.sleep(250);
        }
        throw new IllegalStateException("Connect REST API did not become ready within 60 seconds");
    }

    private void waitForRunning() throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
        while (System.nanoTime() < deadline) {
            try {
                var response = http.send(HttpRequest.newBuilder(
                                URI.create(CONNECT + "/connectors/exp0003-outbox-cdc/status"))
                                .timeout(Duration.ofSeconds(3)).GET().build(),
                        HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() == 200) {
                    var status = mapper.readTree(response.body());
                    var tasks = status.path("tasks");
                    String taskState = tasks.isArray() && tasks.size() > 0
                            ? tasks.get(0).path("state").asText() : "STARTING";
                    if (status.path("connector").path("state").asText().equals("RUNNING")
                            && taskState.equals("RUNNING")) return;
                    if (status.path("connector").path("state").asText().equals("FAILED")
                            || taskState.equals("FAILED")) {
                        throw new IllegalStateException("Connector failed: " + response.body());
                    }
                }
            } catch (java.io.IOException ignored) {
                // Connect is still starting after the container restart.
            }
            Thread.sleep(500);
        }
        throw new IllegalStateException("Connector did not reach RUNNING within 90 seconds");
    }

    private List<ConsumerRecord<String, String>> readRecords(int expected) {
        var partition = new TopicPartition(TOPIC, 0);
        try (var consumer = new KafkaConsumer<String, String>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP,
                ConsumerConfig.GROUP_ID_CONFIG, "exp0003-cdc-reader-" + UUID.randomUUID(),
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"))) {
            consumer.assign(List.of(partition));
            consumer.seekToBeginning(List.of(partition));
            var found = new ArrayList<ConsumerRecord<String, String>>();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(45);
            while (found.size() < expected && System.nanoTime() < deadline) {
                consumer.poll(Duration.ofMillis(250)).forEach(found::add);
            }
            return found;
        }
    }

    private long slotCount(DriverManagerDataSource dataSource) throws Exception {
        try (Connection connection = dataSource.getConnection();
                var statement = connection.createStatement();
                var rows = statement.executeQuery("""
                        SELECT count(*) FROM pg_replication_slots
                        WHERE slot_name = 'exp0003_outbox_slot'
                        """)) {
            rows.next();
            return rows.getLong(1);
        }
    }

    private void compose(String action, String service) throws Exception {
        var process = new ProcessBuilder("docker", "compose", "-f",
                System.getProperty("exp0003.cdc.compose-file"), action, service)
                .redirectErrorStream(true).start();
        String output = new String(process.getInputStream().readAllBytes());
        if (!process.waitFor(30, TimeUnit.SECONDS) || process.exitValue() != 0) {
            process.destroyForcibly();
            throw new IllegalStateException("docker compose " + action + " failed: " + output);
        }
    }
}
