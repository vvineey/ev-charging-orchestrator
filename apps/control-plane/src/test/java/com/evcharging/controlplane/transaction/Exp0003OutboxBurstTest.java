package com.evcharging.controlplane.transaction;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import tools.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Opt-in burst-drain pilot. Each run requires a fresh isolated Compose stack. */
class Exp0003OutboxBurstTest {
    private static final String BOOTSTRAP = "127.0.0.1:19093";
    private static final String TOPIC = "exp0003.outbox";
    private static final String CONNECT = "http://127.0.0.1:18083";
    private final ObjectMapper mapper = new ObjectMapper();
    private final HttpClient http = HttpClient.newHttpClient();

    @Test
    void drainsTheSamePreloadedBurstAndReconcilesEffects() throws Exception {
        assumeTrue(Boolean.getBoolean("exp0003.burst.enabled"),
                "Run only against a fresh isolated Compose stack with -Pexp0003Burst=true");
        String mode = System.getProperty("exp0003.burst.mode");
        int batchSize = Integer.parseInt(System.getProperty("exp0003.burst.batch-size"));
        int expected = Integer.parseInt(System.getProperty("exp0003.burst.events"));
        if (!mode.equals("POLLING") && !mode.equals("CDC")) throw new IllegalArgumentException(mode);
        if (expected < 1) throw new IllegalArgumentException("events must be positive");

        var dataSource = new DriverManagerDataSource(
                "jdbc:postgresql://127.0.0.1:15439/exp0003", "postgres", "exp0003_only");
        Exp0003OutboxFixture.createSchema(dataSource);
        try (var admin = AdminClient.create(Map.of("bootstrap.servers", BOOTSTRAP))) {
            admin.createTopics(List.of(new NewTopic(TOPIC, 1, (short) 1))).all().get(10, TimeUnit.SECONDS);
        }
        if (mode.equals("CDC")) {
            waitForRestReady();
            registerConnector();
            waitForConnector();
            waitForSlot(dataSource);
            compose("stop", "connect");
        } else {
            compose("stop", "connect");
        }

        // The identical keys yield identical event IDs and payloads in every fresh run.
        for (int index = 0; index < expected; index++) {
            Exp0003OutboxFixture.commitWork(dataSource, "work-%06d".formatted(index), false);
        }
        assertThat(Exp0003OutboxFixture.count(dataSource, "exp0003_outbox")).isEqualTo(expected);
        long start = System.nanoTime();
        Future<Integer> polling = null;
        var selectNs = new LongAdder();
        var ackNs = new LongAdder();
        var commitNs = new LongAdder();
        var batches = new LongAdder();
        var executor = Executors.newSingleThreadExecutor();
        try {
            if (mode.equals("CDC")) {
                compose("start", "connect");
                waitForConnector();
            } else {
                polling = executor.submit(() -> {
                    int published = 0;
                    try (var producer = new KafkaProducer<String, String>(Map.of(
                            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP,
                            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                            ProducerConfig.ACKS_CONFIG, "all"))) {
                        while (published < expected) {
                            published += Exp0003OutboxFixture.pollBatch(dataSource, producer, TOPIC,
                                    batchSize, () -> {}, (events, select, ack, commit) -> {
                                        selectNs.add(select);
                                        ackNs.add(ack);
                                        commitNs.add(commit);
                                        batches.increment();
                                    });
                        }
                    }
                    return published;
                });
            }

            var observedAtMs = new ArrayList<Double>();
            var eventIds = new HashSet<UUID>();
            var keys = new HashSet<String>();
            var partition = new TopicPartition(TOPIC, 0);
            try (var consumer = new KafkaConsumer<String, String>(Map.of(
                    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP,
                    ConsumerConfig.GROUP_ID_CONFIG, "exp0003-burst-" + UUID.randomUUID(),
                    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"))) {
                consumer.assign(List.of(partition));
                consumer.seekToBeginning(List.of(partition));
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(120);
                while (eventIds.size() < expected && System.nanoTime() < deadline) {
                    for (var record : consumer.poll(Duration.ofMillis(200))) {
                        var payload = mapper.readTree(record.value());
                        String key = payload.path("workKey").asText();
                        UUID id = UUID.fromString(payload.path("eventId").asText());
                        assertThat(record.key()).isEqualTo(key);
                        assertThat(payload.path("version").asInt()).isEqualTo(1);
                        assertThat(id).isEqualTo(UUID.nameUUIDFromBytes(
                                ("exp0003:" + key).getBytes(StandardCharsets.UTF_8)));
                        eventIds.add(id);
                        keys.add(key);
                        observedAtMs.add((System.nanoTime() - start) / 1_000_000.0);
                    }
                }
            }
            long drainNs = System.nanoTime() - start;
            if (polling != null) assertThat(polling.get(10, TimeUnit.SECONDS)).isEqualTo(expected);
            assertThat(eventIds).hasSize(expected);
            assertThat(keys).hasSize(expected);
            assertThat(observedAtMs).hasSize(expected);
            if (mode.equals("POLLING")) {
                assertThat(Exp0003OutboxFixture.count(dataSource, "exp0003_delivery")).isEqualTo(expected);
            }
            for (String key : keys) {
                Exp0003OutboxFixture.applyEffect(dataSource, UUID.nameUUIDFromBytes(
                        ("exp0003:" + key).getBytes(StandardCharsets.UTF_8)), key);
            }
            assertThat(Exp0003OutboxFixture.count(dataSource, "exp0003_effect")).isEqualTo(expected);
            assertThat(Exp0003OutboxFixture.missingEffects(dataSource)).isZero();
            assertThat(Exp0003OutboxFixture.ghostEffects(dataSource)).isZero();

            observedAtMs.sort(Double::compareTo);
            var result = new LinkedHashMap<String, Object>();
            result.put("mode", mode);
            result.put("batchSize", mode.equals("POLLING") ? batchSize : null);
            result.put("events", expected);
            result.put("physicalRecords", observedAtMs.size());
            result.put("uniqueEvents", eventIds.size());
            result.put("drainMs", drainNs / 1_000_000.0);
            result.put("deliveredPerSecond", expected * 1_000_000_000.0 / drainNs);
            result.put("visibilityFromRestartP50Ms", percentile(observedAtMs, 0.50));
            result.put("visibilityFromRestartP95Ms", percentile(observedAtMs, 0.95));
            result.put("visibilityFromRestartP99Ms", percentile(observedAtMs, 0.99));
            result.put("maxBacklog", expected);
            if (mode.equals("POLLING")) {
                result.put("pollBatches", batches.longValue());
                result.put("pollSelectMs", selectNs.doubleValue() / 1_000_000.0);
                result.put("pollKafkaAckMs", ackNs.doubleValue() / 1_000_000.0);
                result.put("pollDeliveryCommitMs", commitNs.doubleValue() / 1_000_000.0);
            }
            result.put("finalMissingEffects", 0);
            result.put("finalGhostEffects", 0);
            Path output = Path.of(System.getProperty("exp0003.burst.output"));
            Files.createDirectories(output.getParent());
            Files.writeString(output, mapper.writeValueAsString(result));
        } finally {
            executor.shutdownNow();
        }
    }

    private double percentile(List<Double> sorted, double fraction) {
        int index = Math.max(0, (int) Math.ceil(sorted.size() * fraction) - 1);
        return sorted.get(index);
    }

    private void registerConnector() throws Exception {
        String body = Files.readString(Path.of(System.getProperty("exp0003.cdc.connector-file")));
        var response = http.send(HttpRequest.newBuilder(URI.create(CONNECT + "/connectors"))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(body)).build(),
                HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 201) throw new IllegalStateException(
                "Connector registration HTTP " + response.statusCode() + ": " + response.body());
    }

    private void waitForRestReady() throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (System.nanoTime() < deadline) {
            try {
                var response = http.send(HttpRequest.newBuilder(URI.create(CONNECT + "/connectors"))
                                .timeout(Duration.ofSeconds(3)).GET().build(),
                        HttpResponse.BodyHandlers.discarding());
                if (response.statusCode() == 200) return;
            } catch (IOException ignored) {
                // Jetty can answer before the Kafka Connect herder is ready.
            }
            Thread.sleep(250);
        }
        throw new IllegalStateException("Connect REST API did not become ready within 60 seconds");
    }

    private void waitForConnector() throws Exception {
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
                    if (taskState.equals("FAILED")) throw new IllegalStateException(response.body());
                }
            } catch (IOException ignored) {
                // The HTTP endpoint closes connections while Connect restarts.
            }
            Thread.sleep(500);
        }
        throw new IllegalStateException("Connect did not become RUNNING within 90 seconds");
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

    private void waitForSlot(DriverManagerDataSource dataSource) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < deadline) {
            try (var connection = dataSource.getConnection();
                    var statement = connection.createStatement();
                    var rows = statement.executeQuery("""
                            SELECT count(*) FROM pg_replication_slots
                            WHERE slot_name = 'exp0003_outbox_slot'
                            """)) {
                rows.next();
                if (rows.getLong(1) == 1) return;
            }
            Thread.sleep(200);
        }
        throw new IllegalStateException("CDC slot was not created before the outage fixture");
    }
}
