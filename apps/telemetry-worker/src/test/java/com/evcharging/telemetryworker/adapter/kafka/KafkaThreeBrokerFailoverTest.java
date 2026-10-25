package com.evcharging.telemetryworker.adapter.kafka;

import tools.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.NotEnoughReplicasAfterAppendException;
import org.apache.kafka.common.errors.NotEnoughReplicasException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Opt-in E4 experiment against load-tests/kafka-failover/compose.yaml. */
class KafkaThreeBrokerFailoverTest {
    private static final String BOOTSTRAP = "127.0.0.1:19101,127.0.0.1:19102,127.0.0.1:19103";
    private static final int EVENTS = 100;
    private static final Path COMPOSE = Path.of(System.getProperty("kafka.failover.compose"));

    @Test
    @EnabledIfEnvironmentVariable(named = "EVC_KAFKA_FAILOVER_TEST", matches = "true")
    void minIsrBoundaryRejectsWritesThenRecoversAfterTheLeaderReturns() throws Exception {
        String suffix = UUID.randomUUID().toString().substring(0, 8);
        String availableTopic = "exp76-minisr2-" + suffix;
        String strictTopic = "exp76-minisr3-" + suffix;
        String baselineId = "exp76-baseline";
        String affectedId = "exp76-after-leader-kill";
        var assignment = Map.of(0, List.of(1, 2, 3));
        int killedLeader = -1;
        boolean brokerKilled = false;
        long started = System.nanoTime();

        try (var admin = AdminClient.create(Map.of("bootstrap.servers", BOOTSTRAP));
             var resilientProducer = producer();
             var failFastProducer = failFastProducer()) {
            assertEquals(3, admin.describeCluster().nodes().get(20, TimeUnit.SECONDS).size());
            admin.createTopics(List.of(
                    new NewTopic(availableTopic, assignment).configs(Map.of("min.insync.replicas", "2")),
                    new NewTopic(strictTopic, assignment).configs(Map.of("min.insync.replicas", "3"))))
                    .all().get(20, TimeUnit.SECONDS);
            var healthyAvailable = awaitPartition(admin, availableTopic, 3, -1, Duration.ofSeconds(30));
            var healthyStrict = awaitPartition(admin, strictTopic, 3, -1, Duration.ofSeconds(30));
            assertEquals(healthyAvailable.leaderId(), healthyStrict.leaderId());
            killedLeader = healthyAvailable.leaderId();
            assertEquals(List.of(1, 2, 3), healthyAvailable.replicaIds());
            assertEquals(List.of(1, 2, 3), healthyStrict.replicaIds());
            long baselineAvailableOffset = resilientProducer.send(
                    new ProducerRecord<>(availableTopic, "station-1", baselineId))
                    .get(15, TimeUnit.SECONDS).offset();
            long baselineStrictOffset = resilientProducer.send(
                    new ProducerRecord<>(strictTopic, "station-1", baselineId))
                    .get(15, TimeUnit.SECONDS).offset();
            assertEquals(0, baselineAvailableOffset);
            assertEquals(0, baselineStrictOffset);

            long killStarted = System.nanoTime();
            docker("kill", "-s", "SIGKILL", "broker" + killedLeader);
            brokerKilled = true;
            var degradedAvailable = awaitPartition(admin, availableTopic, 2, killedLeader, Duration.ofSeconds(30));
            var degradedStrict = awaitPartition(admin, strictTopic, 2, killedLeader, Duration.ofSeconds(30));
            assertFalse(degradedAvailable.isrIds().contains(killedLeader));
            assertFalse(degradedStrict.isrIds().contains(killedLeader));
            assertEquals(java.util.Set.copyOf(degradedAvailable.isrIds()),
                    java.util.Set.copyOf(degradedStrict.isrIds()), "comparison topics must have the same ISR");
            long degradedObserved = System.nanoTime();

            // Disable producer retries here to expose the broker's min ISR decision to the caller.
            long rejectedStarted = System.nanoTime();
            String rejectionType;
            try {
                failFastProducer.send(new ProducerRecord<>(strictTopic, "station-1", affectedId))
                        .get(8, TimeUnit.SECONDS);
                throw new AssertionError("min.insync.replicas=3 unexpectedly acknowledged an ISR=2 write");
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                assertTrue(cause instanceof NotEnoughReplicasException
                                || cause instanceof NotEnoughReplicasAfterAppendException,
                        "expected min ISR rejection, got " + cause.getClass().getName());
                rejectionType = cause.getClass().getSimpleName();
            }
            long rejectedAt = System.nanoTime();

            long availableStarted = System.nanoTime();
            long availableOffset = resilientProducer.send(
                    new ProducerRecord<>(availableTopic, "station-1", affectedId))
                    .get(15, TimeUnit.SECONDS).offset();
            long availableAckAt = System.nanoTime();
            assertEquals(1, availableOffset);

            long restartStarted = System.nanoTime();
            docker("up", "-d", "--wait", "broker" + killedLeader);
            var restoredAvailable = awaitPartition(admin, availableTopic, 3, -1, Duration.ofSeconds(60));
            var restoredStrict = awaitPartition(admin, strictTopic, 3, -1, Duration.ofSeconds(60));
            brokerKilled = false;
            assertTrue(restoredAvailable.isrIds().contains(killedLeader));
            assertTrue(restoredStrict.isrIds().contains(killedLeader));
            long isrRestoredAt = System.nanoTime();
            long recoveryStarted = System.nanoTime();
            long recoveredOffset = resilientProducer.send(
                    new ProducerRecord<>(strictTopic, "station-1", affectedId))
                    .get(15, TimeUnit.SECONDS).offset();
            long recoveryAckAt = System.nanoTime();

            var availableCounts = consumeCounts(availableTopic, admin);
            var strictCounts = consumeCounts(strictTopic, admin);
            var expectedIds = java.util.Set.of(baselineId, affectedId);
            assertEquals(expectedIds, availableCounts.keySet());
            assertEquals(expectedIds, strictCounts.keySet());
            assertEquals(Map.of(baselineId, 1, affectedId, 1), availableCounts);
            assertEquals(Map.of(baselineId, 1, affectedId, 1), strictCounts,
                    "failed attempt followed by recovery must not create a physical duplicate");

            var result = new LinkedHashMap<String, Object>();
            result.put("observedAt", Instant.now().toString());
            result.put("brokerCount", 3);
            result.put("replicationFactor", 3);
            result.put("producerAcks", "all");
            result.put("killedLeader", killedLeader);
            result.put("availableMinIsr", 2);
            result.put("strictMinIsr", 3);
            result.put("healthyIsrIds", healthyAvailable.isrIds());
            result.put("degradedIsrIds", degradedAvailable.isrIds());
            result.put("restoredIsrIds", restoredAvailable.isrIds());
            result.put("killToDegradedObservedMs", millis(degradedObserved - killStarted));
            result.put("strictRejectionType", rejectionType);
            result.put("strictRejectionLatencyMs", millis(rejectedAt - rejectedStarted));
            result.put("availableAckLatencyWhileDegradedMs", millis(availableAckAt - availableStarted));
            result.put("availableAckOffsetWhileDegraded", availableOffset);
            result.put("restartToIsrThreeObservedMs", millis(isrRestoredAt - restartStarted));
            result.put("strictRecoveryAckLatencyMs", millis(recoveryAckAt - recoveryStarted));
            result.put("strictRecoveryAckOffset", recoveredOffset);
            result.put("availableFinalCounts", availableCounts);
            result.put("strictFinalCounts", strictCounts);
            result.put("experimentElapsedMs", millis(System.nanoTime() - started));
            String output = System.getenv().getOrDefault("EVC_KAFKA_MIN_ISR_OUTPUT",
                    "apps/telemetry-worker/build/experiment-results/kafka-min-isr-boundary.json");
            Path path = Path.of(output).toAbsolutePath();
            Files.createDirectories(path.getParent());
            new ObjectMapper().writerWithDefaultPrettyPrinter().writeValue(path.toFile(), result);
            System.out.println("Kafka min ISR boundary result: " + path);
        } finally {
            if (brokerKilled) {
                docker("up", "-d", "--wait", "broker" + killedLeader);
            }
        }
    }

    @Test
    @EnabledIfEnvironmentVariable(named = "EVC_KAFKA_FAILOVER_TEST", matches = "true")
    void leaderKillDuringPublishingRetainsAckedEventsAndReconcilesTheLog() throws Exception {
        String topic = "exp76-failover-" + UUID.randomUUID().toString().substring(0, 8);
        var attempts = Collections.synchronizedList(new ArrayList<Attempt>());
        var firstTen = new CountDownLatch(10);
        long experimentStart = System.nanoTime();
        int originalLeader = -1;
        int newLeader;
        long killStart;
        long killEnd;
        long newLeaderAt;
        long restartStart;
        long restartEnd;
        long recoveredIsrAt;
        boolean brokerKilled = false;

        try (var admin = AdminClient.create(Map.of("bootstrap.servers", BOOTSTRAP));
             var producer = producer();
             var executor = Executors.newSingleThreadExecutor()) {
            assertEquals(3, admin.describeCluster().nodes().get(20, TimeUnit.SECONDS).size());
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 3).configs(Map.of(
                    "min.insync.replicas", "2", "unclean.leader.election.enable", "false"))))
                    .all().get(20, TimeUnit.SECONDS);
            var healthy = awaitPartition(admin, topic, 3, -1, Duration.ofSeconds(30));
            originalLeader = healthy.leaderId();
            assertEquals(3, healthy.replicaCount());

            var sending = executor.submit(() -> {
                for (int i = 0; i < EVENTS; i++) {
                    String id = "exp76-event-" + String.format("%03d", i);
                    send(producer, topic, id, "initial", attempts, experimentStart);
                    firstTen.countDown();
                    try {
                        Thread.sleep(40);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            });
            assertTrue(firstTen.await(30, TimeUnit.SECONDS), "first ten sends did not complete");
            synchronized (attempts) {
                assertEquals(10, attempts.subList(0, 10).stream()
                        .filter(a -> a.status().equals("ACK")).count(), "healthy baseline ACKs");
            }
            killStart = System.nanoTime();
            try {
                docker("kill", "-s", "SIGKILL", "broker" + originalLeader);
                brokerKilled = true;
            } finally {
                killEnd = System.nanoTime();
            }
            var degraded = awaitPartition(admin, topic, 2, originalLeader, Duration.ofSeconds(30));
            newLeaderAt = System.nanoTime();
            newLeader = degraded.leaderId();
            assertEquals(3, degraded.replicaCount());
            assertTrue(newLeader != originalLeader);
            assertFalse(degraded.isrIds().contains(originalLeader), "killed broker stayed in ISR");
            sending.get(120, TimeUnit.SECONDS);

            // A failed/timeout send may have reached Kafka. Retrying by business ID exposes any physical duplicate.
            var acknowledged = acknowledgedIds(attempts);
            for (int i = 0; i < EVENTS; i++) {
                String id = "exp76-event-" + String.format("%03d", i);
                if (!acknowledged.contains(id)) {
                    send(producer, topic, id, "recovery", attempts, experimentStart);
                }
            }
            assertEquals(EVENTS, acknowledgedIds(attempts).size(), "all IDs must eventually be ACKed");

            restartStart = System.nanoTime();
            docker("up", "-d", "--wait", "broker" + originalLeader);
            restartEnd = System.nanoTime();
            brokerKilled = false;
            var restored = awaitPartition(admin, topic, 3, -1, Duration.ofSeconds(60));
            recoveredIsrAt = System.nanoTime();
            assertTrue(restored.isrIds().contains(originalLeader));

            var counts = consumeCounts(topic, admin);
            assertEquals(EVENTS, counts.size(), "missing or unexpected business IDs");
            assertEquals(acknowledgedIds(attempts), counts.keySet(), "ACKed IDs and log IDs differ");
            assertFalse(counts.values().stream().anyMatch(count -> count < 1));
            writeResult(topic, healthy, degraded, restored, originalLeader, newLeader,
                    killStart, killEnd, newLeaderAt,
                    restartStart, restartEnd, recoveredIsrAt, experimentStart, attempts, counts);
        } finally {
            if (brokerKilled) {
                docker("up", "-d", "--wait", "broker" + originalLeader);
            }
        }
    }

    private static KafkaProducer<String, String> producer() {
        var config = new HashMap<String, Object>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        config.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 3000);
        config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 10000);
        config.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        return new KafkaProducer<>(config);
    }

    private static KafkaProducer<String, String> failFastProducer() {
        var config = new HashMap<String, Object>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
        config.put(ProducerConfig.RETRIES_CONFIG, 0);
        config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 3000);
        config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 5000);
        config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5000);
        return new KafkaProducer<>(config);
    }

    private static void send(KafkaProducer<String, String> producer, String topic, String id,
                             String phase, List<Attempt> attempts, long experimentStart) {
        long started = System.nanoTime();
        try {
            var metadata = producer.send(new ProducerRecord<>(topic, "station-1", id))
                    .get(15, TimeUnit.SECONDS);
            attempts.add(new Attempt(id, phase, millis(started - experimentStart),
                    millis(System.nanoTime() - started), "ACK", null, metadata.offset()));
        } catch (Exception e) {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            attempts.add(new Attempt(id, phase, millis(started - experimentStart),
                    millis(System.nanoTime() - started), "FAILED", cause.getClass().getSimpleName(), null));
        }
    }

    private static PartitionState awaitPartition(AdminClient admin, String topic, int isr,
                                                 int excludedLeader, Duration timeout) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        PartitionState last = null;
        while (System.nanoTime() < deadline) {
            try {
                var partition = admin.describeTopics(List.of(topic)).allTopicNames()
                        .get(5, TimeUnit.SECONDS).get(topic).partitions().getFirst();
                last = new PartitionState(partition.leader().id(),
                        partition.isr().stream().map(node -> node.id()).toList(),
                        partition.replicas().stream().map(node -> node.id()).toList());
                if (last.isrCount() == isr && last.leaderId() != excludedLeader) {
                    return last;
                }
            } catch (Exception ignored) {
                // Leadership can be unavailable while the controller elects a new leader.
            }
            Thread.sleep(100);
        }
        throw new AssertionError("partition state timeout: expected ISR=" + isr + ", last=" + last);
    }

    private static Map<String, Integer> consumeCounts(String topic, AdminClient admin) throws Exception {
        var partition = new TopicPartition(topic, 0);
        long end = admin.listOffsets(Map.of(partition,
                        org.apache.kafka.clients.admin.OffsetSpec.latest()))
                .all().get(10, TimeUnit.SECONDS).get(partition).offset();
        var config = Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP,
                ConsumerConfig.GROUP_ID_CONFIG, "exp76-audit-" + UUID.randomUUID(),
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        var counts = new LinkedHashMap<String, Integer>();
        try (var consumer = new KafkaConsumer<String, String>(config)) {
            consumer.assign(List.of(partition));
            consumer.seekToBeginning(List.of(partition));
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (consumer.position(partition) < end && System.nanoTime() < deadline) {
                for (var record : consumer.poll(Duration.ofMillis(200))) {
                    counts.merge(record.value(), 1, Integer::sum);
                }
            }
            assertEquals(end, consumer.position(partition), "audit consumer did not reach high watermark");
            assertEquals(counts.values().stream().mapToInt(Integer::intValue).sum(), end,
                    "physical record count differs from end offset");
        }
        return counts;
    }

    private static java.util.Set<String> acknowledgedIds(List<Attempt> attempts) {
        return attempts.stream().filter(a -> a.status().equals("ACK"))
                .map(Attempt::id).collect(java.util.stream.Collectors.toSet());
    }

    private static void writeResult(String topic, PartitionState healthy, PartitionState degraded,
                                    PartitionState restored, int oldLeader, int newLeader, long killStart,
                                    long killEnd, long newLeaderAt, long restartStart,
                                    long restartEnd, long recoveredIsrAt,
                                    long experimentStart, List<Attempt> attempts,
                                    Map<String, Integer> counts) throws Exception {
        var result = new LinkedHashMap<String, Object>();
        result.put("observedAt", Instant.now().toString());
        result.put("topic", topic);
        result.put("brokerCount", 3);
        result.put("replicationFactor", 3);
        result.put("minInsyncReplicas", 2);
        result.put("producerAcks", "all");
        result.put("producerIdempotence", true);
        result.put("initialIsr", 3);
        result.put("degradedIsr", 2);
        result.put("restoredIsr", 3);
        result.put("replicaIds", healthy.replicaIds());
        result.put("initialIsrIds", healthy.isrIds());
        result.put("degradedIsrIds", degraded.isrIds());
        result.put("restoredIsrIds", restored.isrIds());
        result.put("oldLeader", oldLeader);
        result.put("newLeader", newLeader);
        result.put("killStartMs", millis(killStart - experimentStart));
        result.put("killCommandDurationMs", millis(killEnd - killStart));
        result.put("newLeaderObservedAfterKillStartMs", millis(newLeaderAt - killStart));
        result.put("restartCommandStartAfterKillStartMs", millis(restartStart - killStart));
        result.put("restartCommandDurationMs", millis(restartEnd - restartStart));
        result.put("isrThreeObservedAfterRestartStartMs", millis(recoveredIsrAt - restartStart));
        result.put("isrThreeObservedAfterKillStartMs", millis(recoveredIsrAt - killStart));
        result.put("initialFailures", attempts.stream().filter(a -> a.phase().equals("initial")
                && a.status().equals("FAILED")).count());
        result.put("totalAttempts", attempts.size());
        result.put("ackedUniqueIds", acknowledgedIds(attempts).size());
        result.put("physicalRecords", counts.values().stream().mapToInt(Integer::intValue).sum());
        result.put("consumedUniqueIds", counts.size());
        result.put("duplicateIds", counts.entrySet().stream().filter(e -> e.getValue() > 1)
                .map(Map.Entry::getKey).toList());
        result.put("firstPostKillAckCompletedAfterKillStartMs", attempts.stream()
                .filter(a -> a.status().equals("ACK")
                && a.startedMs() >= millis(killEnd - experimentStart))
                .mapToDouble(a -> a.startedMs() + a.latencyMs() - millis(killStart - experimentStart))
                .min().orElse(-1));
        result.put("attempts", List.copyOf(attempts));
        String output = System.getenv().getOrDefault("EVC_KAFKA_FAILOVER_OUTPUT",
                "apps/telemetry-worker/build/experiment-results/kafka-three-broker-failover.json");
        Path path = Path.of(output).toAbsolutePath();
        Files.createDirectories(path.getParent());
        new ObjectMapper().writerWithDefaultPrettyPrinter().writeValue(path.toFile(), result);
        System.out.println("Kafka failover result: " + path);
    }

    private static double millis(long nanos) {
        return Math.round(nanos / 1000.0) / 1000.0;
    }

    private static void docker(String... args) throws Exception {
        var command = new ArrayList<>(List.of("docker", "compose", "-f", COMPOSE.toString()));
        Collections.addAll(command, args);
        var process = new ProcessBuilder(command).redirectErrorStream(true).start();
        boolean exited = process.waitFor(90, TimeUnit.SECONDS);
        if (!exited) {
            process.destroyForcibly();
            throw new AssertionError("docker command timed out: " + args[0]);
        }
        if (process.exitValue() != 0) {
            throw new AssertionError("docker command failed: " + process.inputReader().lines()
                    .limit(30).toList());
        }
    }

    private record PartitionState(int leaderId, List<Integer> isrIds, List<Integer> replicaIds) {
        int isrCount() { return isrIds.size(); }
        int replicaCount() { return replicaIds.size(); }
    }
    private record Attempt(String id, String phase, double startedMs, double latencyMs,
                           String status, String errorType, Long offset) { }
}
