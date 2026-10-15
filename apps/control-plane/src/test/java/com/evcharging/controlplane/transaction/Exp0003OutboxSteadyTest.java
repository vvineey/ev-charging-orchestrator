package com.evcharging.controlplane.transaction;

import com.sun.management.OperatingSystemMXBean;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import javax.sql.DataSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Opt-in offered-rate experiment. Every run starts with a fresh isolated Compose stack. */
class Exp0003OutboxSteadyTest {
    private static final String BOOTSTRAP = "127.0.0.1:19093";
    private static final String TOPIC = "exp0003.outbox";
    private static final String CONNECT = "http://127.0.0.1:18083";
    private final ObjectMapper mapper = new ObjectMapper();
    private final HttpClient http = HttpClient.newHttpClient();

    @Test
    void measuresFixedRateInputDeliveryAndFinalEffects() throws Exception {
        assumeTrue(Boolean.getBoolean("exp0003.steady.enabled"),
                "Run only against a fresh isolated Compose stack with -Pexp0003Steady=true");
        String mode = System.getProperty("exp0003.steady.mode");
        int rate = Integer.parseInt(System.getProperty("exp0003.steady.rate"));
        int seconds = Integer.parseInt(System.getProperty("exp0003.steady.seconds"));
        String cdcPollMs = System.getProperty("exp0003.steady.cdc-poll-ms", "default");
        if (!mode.equals("POLLING") && !mode.equals("CDC")) throw new IllegalArgumentException(mode);
        if (!cdcPollMs.equals("default") && !cdcPollMs.equals("500") && !cdcPollMs.equals("50")) {
            throw new IllegalArgumentException("Unsupported CDC poll interval: " + cdcPollMs);
        }
        if (rate < 1 || seconds < 1 || (long) rate * seconds > 100_000) {
            throw new IllegalArgumentException("Rate and seconds must produce 1..100000 events");
        }
        int expected = rate * seconds;

        var poolConfig = new HikariConfig();
        poolConfig.setJdbcUrl("jdbc:postgresql://127.0.0.1:15439/exp0003");
        poolConfig.setUsername("postgres");
        poolConfig.setPassword("exp0003_only");
        poolConfig.setMaximumPoolSize(16);
        poolConfig.setMinimumIdle(16);
        poolConfig.setPoolName("exp0003-steady-input");
        var dataSource = new HikariDataSource(poolConfig);
        Exp0003OutboxFixture.createSchema(dataSource);
        try (var admin = AdminClient.create(Map.of("bootstrap.servers", BOOTSTRAP))) {
            admin.createTopics(List.of(new NewTopic(TOPIC, 1, (short) 1))).all().get(10, TimeUnit.SECONDS);
        }
        if (mode.equals("CDC")) {
            waitForRestReady();
            registerConnector(cdcPollMs);
            waitForConnector();
            verifyConnectorPollInterval(cdcPollMs);
            waitForSlot(dataSource);
        } else {
            compose("stop", "connect");
        }

        var committedAt = new ConcurrentHashMap<UUID, Long>();
        var workKeyById = new ConcurrentHashMap<UUID, String>();
        var firstObservedAt = new ConcurrentHashMap<UUID, Long>();
        var committedCount = new AtomicInteger();
        var observedCount = new AtomicInteger();
        var physicalCount = new AtomicInteger();
        var maxObservedBacklog = new AtomicInteger();
        var cpuAtPlannedEndNs = new AtomicLong(-1);
        var maxHeapBytes = new AtomicLong();
        var stopSampler = new AtomicBoolean();
        var samples = Collections.synchronizedList(new ArrayList<Map<String, Object>>());
        var consumerReady = new CountDownLatch(1);
        var startSignal = new CountDownLatch(1);
        var executor = Executors.newFixedThreadPool(3);
        var writers = Executors.newFixedThreadPool(8);
        OperatingSystemMXBean os = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        long startNs = -1;
        try (var producer = new KafkaProducer<String, String>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.ACKS_CONFIG, "all"))) {
            if (mode.equals("POLLING")) producer.partitionsFor(TOPIC);
            Future<?> consumer = executor.submit(() -> {
                var partition = new TopicPartition(TOPIC, 0);
                try (var reader = new KafkaConsumer<String, String>(Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP,
                        ConsumerConfig.GROUP_ID_CONFIG, "exp0003-steady-" + UUID.randomUUID(),
                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"))) {
                    reader.assign(List.of(partition));
                    reader.seekToBeginning(List.of(partition));
                    reader.poll(Duration.ofMillis(1));
                    consumerReady.countDown();
                    startSignal.await();
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds + 120L);
                    while (firstObservedAt.size() < expected && System.nanoTime() < deadline) {
                        for (var record : reader.poll(Duration.ofMillis(100))) {
                            long observedNs = System.nanoTime();
                            var payload = mapper.readTree(record.value());
                            String key = payload.path("workKey").asText();
                            UUID id = UUID.fromString(payload.path("eventId").asText());
                            assertThat(record.key()).isEqualTo(key);
                            assertThat(payload.path("version").asInt()).isEqualTo(1);
                            assertThat(id).isEqualTo(eventId(key));
                            physicalCount.incrementAndGet();
                            if (firstObservedAt.putIfAbsent(id, observedNs) == null) {
                                observedCount.incrementAndGet();
                            }
                        }
                    }
                } catch (Exception exception) {
                    throw new IllegalStateException("Kafka observation failed", exception);
                }
            });
            assertThat(consumerReady.await(20, TimeUnit.SECONDS)).isTrue();

            Future<Integer> polling = null;
            if (mode.equals("POLLING")) {
                polling = executor.submit(() -> {
                    int published = 0;
                    while (published < expected) {
                        int count = Exp0003OutboxFixture.pollBatch(dataSource, producer, TOPIC,
                                16, () -> {});
                        published += count;
                        if (count == 0) LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(5));
                    }
                    return published;
                });
            }

            long cpuStartNs = os.getProcessCpuTime();
            long inputStartedAtEpochMillis = System.currentTimeMillis();
            startNs = System.nanoTime();
            long plannedEndNs = startNs + TimeUnit.SECONDS.toNanos(seconds);
            long measurementStartNs = startNs;
            Future<?> sampler = executor.submit(() -> {
                while (!stopSampler.get()) {
                    long now = System.nanoTime();
                    int committed = committedCount.get();
                    int observed = observedCount.get();
                    int backlog = Math.max(0, committed - observed);
                    maxObservedBacklog.accumulateAndGet(backlog, Math::max);
                    long heap = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
                    maxHeapBytes.accumulateAndGet(heap, Math::max);
                    if (now >= plannedEndNs) cpuAtPlannedEndNs.compareAndSet(-1, os.getProcessCpuTime());
                    var sample = new LinkedHashMap<String, Object>();
                    sample.put("elapsedMs", (now - measurementStartNs) / 1_000_000.0);
                    sample.put("committed", committed);
                    sample.put("observedUnique", observed);
                    sample.put("observedBacklog", backlog);
                    sample.put("testJvmHeapUsedBytes", heap);
                    samples.add(sample);
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
                }
            });
            startSignal.countDown();

            var commitDurationsMs = Collections.synchronizedList(new ArrayList<Double>(expected));
            var scheduleSlipMs = Collections.synchronizedList(new ArrayList<Double>(expected));
            var lastCommitNs = new AtomicLong(-1);
            var writerTasks = new ArrayList<Future<?>>(expected);
            for (int index = 0; index < expected; index++) {
                long scheduledNs = startNs + (index * 1_000_000_000L) / rate;
                while (System.nanoTime() < scheduledNs) {
                    LockSupport.parkNanos(Math.min(scheduledNs - System.nanoTime(),
                            TimeUnit.MILLISECONDS.toNanos(1)));
                }
                int workIndex = index;
                writerTasks.add(writers.submit(() -> {
                    long attemptNs = System.nanoTime();
                    scheduleSlipMs.add(Math.max(0, attemptNs - scheduledNs) / 1_000_000.0);
                    String key = workKey(workIndex);
                    UUID id = Exp0003OutboxFixture.commitWork(dataSource, key, false);
                    long committedNs = System.nanoTime();
                    committedAt.put(id, committedNs);
                    workKeyById.put(id, key);
                    committedCount.incrementAndGet();
                    lastCommitNs.accumulateAndGet(committedNs, Math::max);
                    commitDurationsMs.add((committedNs - attemptNs) / 1_000_000.0);
                    return null;
                }));
            }
            for (Future<?> writerTask : writerTasks) writerTask.get(120, TimeUnit.SECONDS);
            while (System.nanoTime() < plannedEndNs) {
                LockSupport.parkNanos(Math.min(plannedEndNs - System.nanoTime(),
                        TimeUnit.MILLISECONDS.toNanos(1)));
            }
            long walAtInputEnd = mode.equals("CDC") ? retainedWalBytes(dataSource) : -1;
            consumer.get(120, TimeUnit.SECONDS);
            if (polling != null) assertThat(polling.get(30, TimeUnit.SECONDS)).isEqualTo(expected);
            long walAfterDrain = mode.equals("CDC") ? retainedWalBytes(dataSource) : -1;
            stopSampler.set(true);
            sampler.get(5, TimeUnit.SECONDS);

            assertThat(firstObservedAt).hasSize(expected);
            assertThat(committedAt).hasSize(expected);
            assertThat(physicalCount.get()).isEqualTo(expected);
            assertThat(Exp0003OutboxFixture.count(dataSource, "exp0003_work")).isEqualTo(expected);
            assertThat(Exp0003OutboxFixture.count(dataSource, "exp0003_outbox")).isEqualTo(expected);
            if (mode.equals("POLLING")) {
                assertThat(Exp0003OutboxFixture.count(dataSource, "exp0003_delivery")).isEqualTo(expected);
            }

            var visibilityMs = new ArrayList<Double>(expected);
            int negativeLatencies = 0;
            long lastObservedNs = -1;
            for (var entry : committedAt.entrySet()) {
                Long observedNs = firstObservedAt.get(entry.getKey());
                assertThat(observedNs).isNotNull();
                long delta = observedNs - entry.getValue();
                if (delta < 0) negativeLatencies++;
                visibilityMs.add(delta / 1_000_000.0);
                lastObservedNs = Math.max(lastObservedNs, observedNs);
            }
            assertThat(negativeLatencies).as("commit-return→Kafka observation clock order").isZero();
            long committedAtPlannedEnd = committedAt.values().stream()
                    .filter(committedNs -> committedNs <= plannedEndNs).count();
            long observedAtPlannedEnd = firstObservedAt.values().stream()
                    .filter(observedNs -> observedNs <= plannedEndNs).count();
            assertThat(observedAtPlannedEnd).isLessThanOrEqualTo(committedAtPlannedEnd);
            Collections.sort(visibilityMs);
            Collections.sort(commitDurationsMs);
            Collections.sort(scheduleSlipMs);
            Exp0003OutboxFixture.applyEffects(dataSource, workKeyById);
            assertThat(Exp0003OutboxFixture.count(dataSource, "exp0003_effect")).isEqualTo(expected);
            assertThat(Exp0003OutboxFixture.missingEffects(dataSource)).isZero();
            assertThat(Exp0003OutboxFixture.ghostEffects(dataSource)).isZero();
            long brokerEndOffset;
            try (var admin = AdminClient.create(Map.of("bootstrap.servers", BOOTSTRAP))) {
                brokerEndOffset = admin.listOffsets(Map.of(new TopicPartition(TOPIC, 0), OffsetSpec.latest()))
                        .all().get(10, TimeUnit.SECONDS).get(new TopicPartition(TOPIC, 0)).offset();
            }
            assertThat(brokerEndOffset).as("physical records in fresh one-partition topic")
                    .isEqualTo(expected);

            var result = new LinkedHashMap<String, Object>();
            result.put("mode", mode);
            result.put("cdcPollIntervalMs", mode.equals("CDC") ? cdcPollMs : null);
            result.put("inputStartedAtEpochMillis", inputStartedAtEpochMillis);
            result.put("inputPlannedEndAtEpochMillis", inputStartedAtEpochMillis + seconds * 1_000L);
            result.put("targetRatePerSecond", rate);
            result.put("durationSeconds", seconds);
            result.put("logicalStations", 2000);
            result.put("logicalPortsPerStation", 5);
            result.put("scheduledEvents", expected);
            result.put("committedEvents", committedCount.get());
            result.put("observedUniqueEvents", observedCount.get());
            result.put("physicalKafkaRecords", brokerEndOffset);
            result.put("actualCommitRatePerSecond", expected * 1_000_000_000.0
                    / Math.max(TimeUnit.SECONDS.toNanos(seconds), lastCommitNs.get() - startNs));
            result.put("committedAtPlannedEnd", committedAtPlannedEnd);
            result.put("observedAtPlannedEnd", observedAtPlannedEnd);
            result.put("backlogAtPlannedEnd", committedAtPlannedEnd - observedAtPlannedEnd);
            result.put("sampledMaxObservedBacklog", maxObservedBacklog.get());
            result.put("drainAfterPlannedEndMs", Math.max(0, lastObservedNs - plannedEndNs) / 1_000_000.0);
            result.put("commitToKafkaP50Ms", percentile(visibilityMs, 0.50));
            result.put("commitToKafkaP95Ms", percentile(visibilityMs, 0.95));
            result.put("commitToKafkaP99Ms", percentile(visibilityMs, 0.99));
            result.put("commitServiceP95Ms", percentile(commitDurationsMs, 0.95));
            result.put("scheduleSlipP95Ms", percentile(scheduleSlipMs, 0.95));
            result.put("scheduleSlipMaxMs", scheduleSlipMs.getLast());
            result.put("testJvmCpuOneCorePercentDuringInput", 100.0 *
                    (cpuAtPlannedEndNs.get() - cpuStartNs) / TimeUnit.SECONDS.toNanos(seconds));
            result.put("testJvmMaxHeapUsedBytes", maxHeapBytes.get());
            result.put("cdcRetainedWalBytesAtInputEnd", mode.equals("CDC") ? walAtInputEnd : null);
            result.put("cdcRetainedWalBytesAfterDrain", mode.equals("CDC") ? walAfterDrain : null);
            result.put("finalMissingEffects", 0);
            result.put("finalGhostEffects", 0);
            result.put("samples", samples);
            Path output = Path.of(System.getProperty("exp0003.steady.output"));
            Files.createDirectories(output.getParent());
            Files.writeString(output, mapper.writeValueAsString(result));
        } finally {
            stopSampler.set(true);
            startSignal.countDown();
            writers.shutdownNow();
            executor.shutdownNow();
            dataSource.close();
        }
    }

    private String workKey(int index) {
        int portIndex = index % 10_000;
        return "station-%04d-port-%d-work-%06d".formatted(
                portIndex / 5, portIndex % 5 + 1, index);
    }

    private UUID eventId(String key) {
        return UUID.nameUUIDFromBytes(("exp0003:" + key).getBytes(StandardCharsets.UTF_8));
    }

    private double percentile(List<Double> sorted, double fraction) {
        return sorted.get(Math.max(0, (int) Math.ceil(sorted.size() * fraction) - 1));
    }

    private long retainedWalBytes(DataSource dataSource) throws Exception {
        try (Connection connection = dataSource.getConnection();
                var statement = connection.createStatement();
                var rows = statement.executeQuery("""
                        SELECT COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn), 0)::bigint
                        FROM pg_replication_slots WHERE slot_name = 'exp0003_outbox_slot'
                        """)) {
            assertThat(rows.next()).isTrue();
            return rows.getLong(1);
        }
    }

    private void registerConnector(String cdcPollMs) throws Exception {
        var request = (ObjectNode) mapper.readTree(
                Files.readString(Path.of(System.getProperty("exp0003.cdc.connector-file"))));
        if (!cdcPollMs.equals("default")) {
            ((ObjectNode) request.path("config")).put("poll.interval.ms", cdcPollMs);
        }
        String body = mapper.writeValueAsString(request);
        var response = http.send(HttpRequest.newBuilder(URI.create(CONNECT + "/connectors"))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(body)).build(),
                HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 201) throw new IllegalStateException(
                "Connector registration HTTP " + response.statusCode() + ": " + response.body());
    }

    private void verifyConnectorPollInterval(String cdcPollMs) throws Exception {
        if (cdcPollMs.equals("default")) return;
        var response = http.send(HttpRequest.newBuilder(
                        URI.create(CONNECT + "/connectors/exp0003-outbox-cdc/config"))
                        .timeout(Duration.ofSeconds(3)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(mapper.readTree(response.body()).path("poll.interval.ms").asText())
                .as("Applied Connect poll.interval.ms")
                .isEqualTo(cdcPollMs);
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
                // The REST socket may be available before the Connect herder is ready.
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
                // Connect can close the socket while it starts.
            }
            Thread.sleep(500);
        }
        throw new IllegalStateException("Connect did not become RUNNING within 90 seconds");
    }

    private void waitForSlot(DataSource dataSource) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < deadline) {
            try (Connection connection = dataSource.getConnection();
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
        throw new IllegalStateException("CDC slot was not created before input");
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
