package com.evcharging.controlplane.transaction;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import tools.jackson.databind.ObjectMapper;

import javax.sql.DataSource;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Files;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = "transaction.ingress.enabled=false")
@EmbeddedKafka(partitions = 1, topics = "exp0003-bootstrap",
        bootstrapServersProperty = "spring.kafka.bootstrap-servers",
        brokerProperties = "offsets.topic.num.partitions=1")
class Exp0003OutboxPollingTest {
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

    @Autowired DataSource dataSource;
    @Autowired EmbeddedKafkaBroker broker;
    @Autowired ObjectMapper mapper;
    @TempDir Path tempDirectory;

    private String topic;
    private AdminClient admin;
    private KafkaProducer<String, String> producer;

    @BeforeEach
    void prepare() throws Exception {
        Exp0003OutboxFixture.createSchema(dataSource);
        Exp0003OutboxFixture.clear(dataSource);
        topic = "exp0003-outbox-" + UUID.randomUUID();
        admin = AdminClient.create(Map.of("bootstrap.servers", broker.getBrokersAsString()));
        admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get(10, TimeUnit.SECONDS);
        producer = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.ACKS_CONFIG, "all"));
    }

    @AfterEach
    void close() {
        if (producer != null) producer.close(Duration.ofSeconds(5));
        if (admin != null) admin.close(Duration.ofSeconds(5));
    }

    @Test
    void workAndOutboxCommitTogetherButRollbackTogether() throws Exception {
        String key = "work-" + UUID.randomUUID();
        UUID first = Exp0003OutboxFixture.commitWork(dataSource, key, false);
        UUID repeated = Exp0003OutboxFixture.commitWork(dataSource, key, false);
        assertThat(repeated).isEqualTo(first);
        assertThat(Exp0003OutboxFixture.count(dataSource, "exp0003_work")).isEqualTo(1);
        assertThat(Exp0003OutboxFixture.count(dataSource, "exp0003_outbox")).isEqualTo(1);

        Exp0003OutboxFixture.commitWork(dataSource, "rollback-" + UUID.randomUUID(), true);
        assertThat(Exp0003OutboxFixture.count(dataSource, "exp0003_work")).isEqualTo(1);
        assertThat(Exp0003OutboxFixture.count(dataSource, "exp0003_outbox")).isEqualTo(1);
        assertThat(Exp0003OutboxFixture.ghostEffects(dataSource)).isZero();
    }

    @Test
    void pollingPublishesTheStableContractAndRecordsAckSeparately() throws Exception {
        String key = "work-" + UUID.randomUUID();
        UUID eventId = Exp0003OutboxFixture.commitWork(dataSource, key, false);

        assertThat(Exp0003OutboxFixture.pollOnce(dataSource, producer, topic, () -> {})).isTrue();
        assertThat(Exp0003OutboxFixture.pollOnce(dataSource, producer, topic, () -> {})).isFalse();
        List<ConsumerRecord<String, String>> records = readRecords(1);
        assertThat(records).hasSize(1);
        assertThat(records.getFirst().key()).isEqualTo(key);
        var payload = mapper.readTree(records.getFirst().value());
        assertThat(payload.path("eventId").asText()).isEqualTo(eventId.toString());
        assertThat(payload.path("workKey").asText()).isEqualTo(key);
        assertThat(payload.path("version").asInt()).isEqualTo(1);

        Exp0003OutboxFixture.applyEffect(dataSource, eventId, key);
        Exp0003OutboxFixture.applyEffect(dataSource, eventId, key);
        assertThat(Exp0003OutboxFixture.count(dataSource, "exp0003_delivery")).isEqualTo(1);
        assertThat(Exp0003OutboxFixture.count(dataSource, "exp0003_effect")).isEqualTo(1);
        assertThat(Exp0003OutboxFixture.missingEffects(dataSource)).isZero();
        assertThat(Exp0003OutboxFixture.ghostEffects(dataSource)).isZero();
    }

    @Test
    void failureAfterKafkaAckBeforeDeliveryCommitRetriesTheSameEvent() throws Exception {
        String key = "work-" + UUID.randomUUID();
        UUID eventId = Exp0003OutboxFixture.commitWork(dataSource, key, false);

        assertThatThrownBy(() -> Exp0003OutboxFixture.pollOnce(dataSource, producer, topic,
                () -> { throw new IllegalStateException("test failpoint after ACK"); }))
                .isInstanceOf(IllegalStateException.class);
        assertThat(Exp0003OutboxFixture.count(dataSource, "exp0003_delivery")).isZero();
        assertThat(Exp0003OutboxFixture.pollOnce(dataSource, producer, topic, () -> {})).isTrue();

        List<ConsumerRecord<String, String>> records = readRecords(2);
        assertThat(records).hasSize(2);
        for (var record : records) {
            var payload = mapper.readTree(record.value());
            assertThat(record.key()).isEqualTo(key);
            assertThat(payload.path("eventId").asText()).isEqualTo(eventId.toString());
            Exp0003OutboxFixture.applyEffect(dataSource, eventId, key);
        }
        assertThat(Exp0003OutboxFixture.count(dataSource, "exp0003_delivery")).isEqualTo(1);
        assertThat(Exp0003OutboxFixture.count(dataSource, "exp0003_effect")).isEqualTo(1);
        assertThat(Exp0003OutboxFixture.missingEffects(dataSource)).isZero();
    }

    @Test
    void anotherEventIdCannotApplyTheSameBusinessEffect() throws Exception {
        String key = "work-" + UUID.randomUUID();
        UUID eventId = Exp0003OutboxFixture.commitWork(dataSource, key, false);
        Exp0003OutboxFixture.applyEffect(dataSource, eventId, key);
        assertThatThrownBy(() -> Exp0003OutboxFixture.applyEffect(dataSource, UUID.randomUUID(), key))
                .isInstanceOf(SQLException.class);
        assertThat(Exp0003OutboxFixture.count(dataSource, "exp0003_effect")).isEqualTo(1);
    }

    @Test
    void separateJvmHaltAfterCommitLeavesAnEventTheRelayCanPublish() throws Exception {
        String key = "work-" + UUID.randomUUID();
        assertThat(crashChild("after-commit-before-send", key)).isEqualTo(137);
        assertThat(Exp0003OutboxFixture.count(dataSource, "exp0003_work")).isEqualTo(1);
        assertThat(Exp0003OutboxFixture.count(dataSource, "exp0003_outbox")).isEqualTo(1);
        assertThat(readRecords(0)).isEmpty();

        assertThat(Exp0003OutboxFixture.pollOnce(dataSource, producer, topic, () -> {})).isTrue();
        var record = readRecords(1).getFirst();
        assertThat(record.key()).isEqualTo(key);
        assertThat(mapper.readTree(record.value()).path("workKey").asText()).isEqualTo(key);
        Exp0003OutboxFixture.applyEffect(dataSource,
                UUID.fromString(mapper.readTree(record.value()).path("eventId").asText()), key);
        assertThat(Exp0003OutboxFixture.missingEffects(dataSource)).isZero();
    }

    @Test
    void separateJvmHaltAfterKafkaAckReplaysOneEffect() throws Exception {
        String key = "work-" + UUID.randomUUID();
        UUID eventId = Exp0003OutboxFixture.commitWork(dataSource, key, false);
        assertThat(crashChild("after-kafka-ack-before-delivery-commit", key)).isEqualTo(137);
        assertThat(Exp0003OutboxFixture.count(dataSource, "exp0003_delivery")).isZero();
        assertThat(readRecords(1)).hasSize(1);

        assertThat(Exp0003OutboxFixture.pollOnce(dataSource, producer, topic, () -> {})).isTrue();
        var records = readRecords(2);
        assertThat(records).hasSize(2);
        for (var record : records) {
            assertThat(mapper.readTree(record.value()).path("eventId").asText())
                    .isEqualTo(eventId.toString());
            Exp0003OutboxFixture.applyEffect(dataSource, eventId, key);
        }
        assertThat(Exp0003OutboxFixture.count(dataSource, "exp0003_effect")).isEqualTo(1);
        assertThat(Exp0003OutboxFixture.missingEffects(dataSource)).isZero();
        assertThat(Exp0003OutboxFixture.ghostEffects(dataSource)).isZero();
    }

    @Test
    void batchSixteenStillReplaysAtMostOneEffectPerWorkKeyAfterJvmHalt() throws Exception {
        var expected = new HashMap<String, UUID>();
        for (int index = 0; index < 16; index++) {
            String key = "batch-%02d".formatted(index);
            expected.put(key, Exp0003OutboxFixture.commitWork(dataSource, key, false));
        }
        assertThat(crashChild("after-kafka-ack-before-delivery-commit", "unused", 16)).isEqualTo(137);
        assertThat(Exp0003OutboxFixture.count(dataSource, "exp0003_delivery")).isZero();
        assertThat(readRecords(16)).hasSize(16);

        assertThat(Exp0003OutboxFixture.pollBatch(dataSource, producer, topic, 16, () -> {}))
                .isEqualTo(16);
        var records = readRecords(32);
        assertThat(records).hasSize(32);
        var physicalByKey = new HashMap<String, Integer>();
        for (var record : records) {
            var payload = mapper.readTree(record.value());
            String key = payload.path("workKey").asText();
            UUID id = UUID.fromString(payload.path("eventId").asText());
            assertThat(record.key()).isEqualTo(key);
            assertThat(id).isEqualTo(expected.get(key));
            physicalByKey.merge(key, 1, Integer::sum);
            Exp0003OutboxFixture.applyEffect(dataSource, id, key);
        }
        assertThat(physicalByKey).hasSize(16);
        assertThat(physicalByKey.values()).allMatch(count -> count == 2);
        assertThat(Exp0003OutboxFixture.count(dataSource, "exp0003_delivery")).isEqualTo(16);
        assertThat(Exp0003OutboxFixture.count(dataSource, "exp0003_effect")).isEqualTo(16);
        assertThat(Exp0003OutboxFixture.missingEffects(dataSource)).isZero();
    }

    @Test
    @EnabledIfEnvironmentVariable(named = "EVC_RELAY_BACKLOG_TEST", matches = "true")
    void relayRestartDrainsTheRemainingBacklogAndReconcilesPhysicalDuplicates() throws Exception {
        final int total = 64;
        final int batchSize = 16;
        String runId = UUID.randomUUID().toString();
        var expectedByKey = new HashMap<String, UUID>();
        for (int index = 0; index < total; index++) {
            String key = "backlog-%02d".formatted(index);
            expectedByKey.put(key, Exp0003OutboxFixture.commitWork(dataSource, key, false));
        }
        assertThat(Exp0003OutboxFixture.count(dataSource, "exp0003_work")).isEqualTo(total);
        assertThat(Exp0003OutboxFixture.count(dataSource, "exp0003_outbox")).isEqualTo(total);
        assertThat(crashChild("after-kafka-ack-before-delivery-commit", "unused", batchSize))
                .isEqualTo(137);
        assertThat(Exp0003OutboxFixture.count(dataSource, "exp0003_delivery")).isZero();
        List<ConsumerRecord<String, String>> beforeRestart = readRecords(batchSize);
        assertThat(beforeRestart).hasSize(batchSize);
        Set<UUID> firstAckedIds = new HashSet<>();
        for (var record : beforeRestart) {
            UUID id = UUID.fromString(mapper.readTree(record.value()).path("eventId").asText());
            assertThat(record.key()).isEqualTo(mapper.readTree(record.value()).path("workKey").asText());
            assertThat(id).isEqualTo(expectedByKey.get(record.key()));
            firstAckedIds.add(id);
        }
        assertThat(firstAckedIds).hasSize(batchSize);

        long restartedAt = System.nanoTime();
        int published = 0;
        int batches = 0;
        try (var restartedProducer = new KafkaProducer<String, String>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.ACKS_CONFIG, "all"))) {
            while (published < total) {
                int count = Exp0003OutboxFixture.pollBatch(dataSource, restartedProducer,
                        topic, batchSize, () -> {});
                assertThat(count).as("batch %s", batches).isBetween(1, batchSize);
                published += count;
                batches++;
            }
            assertThat(Exp0003OutboxFixture.pollBatch(dataSource, restartedProducer,
                    topic, batchSize, () -> {})).isZero();
        }
        long deliveryMarkedAt = System.nanoTime();
        List<ConsumerRecord<String, String>> afterRestart = readRecords(total + batchSize);
        long kafkaScanCompletedAt = System.nanoTime();
        assertThat(afterRestart).hasSize(total + batchSize);
        var physicalById = new HashMap<UUID, Integer>();
        for (var record : afterRestart) {
            var body = mapper.readTree(record.value());
            String key = body.path("workKey").asText();
            UUID id = UUID.fromString(body.path("eventId").asText());
            assertThat(id).isEqualTo(expectedByKey.get(key));
            assertThat(record.key()).isEqualTo(key);
            physicalById.merge(id, 1, Integer::sum);
            Exp0003OutboxFixture.applyEffect(dataSource, id, key);
        }
        assertThat(physicalById).hasSize(total);
        for (var entry : physicalById.entrySet()) {
            assertThat(entry.getValue()).as("physical count for event %s", entry.getKey())
                    .isEqualTo(firstAckedIds.contains(entry.getKey()) ? 2 : 1);
        }
        assertThat(published).isEqualTo(total);
        assertThat(batches).isEqualTo(total / batchSize);
        assertThat(Exp0003OutboxFixture.count(dataSource, "exp0003_delivery")).isEqualTo(total);
        assertThat(Exp0003OutboxFixture.count(dataSource, "exp0003_effect")).isEqualTo(total);
        assertThat(Exp0003OutboxFixture.missingEffects(dataSource)).isZero();
        assertThat(Exp0003OutboxFixture.ghostEffects(dataSource)).isZero();

        var result = mapper.createObjectNode();
        result.put("runId", runId);
        result.put("javaVersion", System.getProperty("java.version"));
        result.put("model", "test-only synthetic Outbox; one embedded broker/partition");
        result.put("outboxAtStop", total);
        result.put("deliveryAtStop", 0);
        result.put("kafkaPhysicalAtStop", beforeRestart.size());
        result.put("childExitCode", 137);
        result.put("restartBatchSize", batchSize);
        result.put("restartBatches", batches);
        result.put("restartPublished", published);
        result.put("restartToDeliveryMarkedMs", (deliveryMarkedAt - restartedAt) / 1_000_000.0);
        result.put("restartToKafkaFullScanMs", (kafkaScanCompletedAt - restartedAt) / 1_000_000.0);
        result.put("finalOutbox", total);
        result.put("finalDelivery", total);
        result.put("finalKafkaPhysical", afterRestart.size());
        result.put("finalKafkaUnique", physicalById.size());
        result.put("finalEffects", total);
        result.put("finalMissingEffects", 0);
        result.put("finalGhostEffects", 0);
        Path output = Path.of("build", "experiment-results", "relay-backlog-" + runId + ".json");
        Files.createDirectories(output.getParent());
        Files.writeString(output, mapper.writerWithDefaultPrettyPrinter().writeValueAsString(result));
    }

    private int crashChild(String mode, String key) throws Exception {
        return crashChild(mode, key, 1);
    }

    private int crashChild(String mode, String key, int batchSize) throws Exception {
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        String classpath = System.getProperty("transaction.test.runtime-classpath");
        assertThat(classpath).isNotBlank();
        var process = new ProcessBuilder(java, "-cp", classpath,
                Exp0003PollingCrashProcess.class.getName(), mode,
                POSTGRES.getJdbcUrl("postgres", "postgres"), broker.getBrokersAsString(), topic, key,
                Integer.toString(batchSize))
                .redirectErrorStream(true)
                .redirectOutput(tempDirectory.resolve(mode + ".log").toFile())
                .start();
        if (!process.waitFor(30, TimeUnit.SECONDS)) {
            process.destroyForcibly();
            throw new IllegalStateException("Outbox crash child did not finish in 30 seconds");
        }
        return process.exitValue();
    }

    private List<ConsumerRecord<String, String>> readRecords(int expected) {
        var partition = new TopicPartition(topic, 0);
        try (var consumer = new KafkaConsumer<String, String>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString(),
                ConsumerConfig.GROUP_ID_CONFIG, "exp0003-reader-" + UUID.randomUUID(),
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"))) {
            consumer.assign(List.of(partition));
            consumer.seekToBeginning(List.of(partition));
            var found = new ArrayList<ConsumerRecord<String, String>>();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            boolean polled = false;
            while ((!polled || found.size() < expected) && System.nanoTime() < deadline) {
                consumer.poll(Duration.ofMillis(200)).forEach(found::add);
                polled = true;
            }
            return found;
        }
    }
}
