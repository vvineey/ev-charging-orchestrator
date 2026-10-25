package com.evcharging.telemetryworker.adapter.kafka;

import com.evcharging.messaging.contract.ChargerTelemetryPayload;
import com.evcharging.messaging.contract.DomainEventEnvelope;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import tools.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Opt-in rebuild of the existing latest-state model from one retained Kafka partition. */
@EnabledIfEnvironmentVariable(named = "EVC_TELEMETRY_REBUILD_TEST", matches = "true")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {"spring.kafka.listener.auto-startup=false",
                "telemetry.kafka-topic=telemetry-rebuild-bootstrap"})
@EmbeddedKafka(partitions = 1, topics = "telemetry-rebuild-bootstrap",
        bootstrapServersProperty = "spring.kafka.bootstrap-servers",
        brokerProperties = "offsets.topic.num.partitions=1")
class TelemetryReadModelRebuildTest {
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
    @Autowired TelemetryRecordListener listener;
    @Autowired @Qualifier("telemetryKafkaListenerContainerFactory")
    ConcurrentKafkaListenerContainerFactory<String, String> factory;

    @Test
    void aNewGroupRebuildsDeletedLatestRowsFromTheSameRetainedLog() throws Exception {
        String runId = UUID.randomUUID().toString();
        String topic = "telemetry-rebuild-" + runId;
        String firstGroup = "telemetry-first-" + runId;
        String rebuildGroup = "telemetry-rebuild-" + runId;
        TopicPartition partition = new TopicPartition(topic, 0);
        jdbc.sql("TRUNCATE evse_telemetry_latest").update();
        try (var admin = AdminClient.create(Map.of("bootstrap.servers", broker.getBrokersAsString()));
             var producer = new KafkaProducer<String, String>(Map.of(
                     ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString(),
                     ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                     ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                     ProducerConfig.ACKS_CONFIG, "all"))) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get(10, TimeUnit.SECONDS);
            List<DomainEventEnvelope<ChargerTelemetryPayload>> input = List.of(
                    TelemetryFixtures.first(),
                    TelemetryFixtures.latest(),
                    TelemetryFixtures.event(3, 1, Instant.parse("2026-09-28T23:59:59Z"),
                            Instant.parse("2026-09-29T00:00:03Z"), false, "777"),
                    TelemetryFixtures.latest(),
                    TelemetryFixtures.event(4, 1, Instant.parse("2026-09-29T00:00:01Z"),
                            Instant.parse("2026-09-29T00:00:02Z"), false, "888"),
                    TelemetryFixtures.event(5, 2, Instant.parse("2026-09-29T00:00:00Z"),
                            Instant.parse("2026-09-29T00:00:00Z"), false, "75"),
                    TelemetryFixtures.event(6, 2, Instant.parse("2026-09-29T00:00:04Z"),
                            Instant.parse("2026-09-29T00:00:05Z"), true, "80"),
                    TelemetryFixtures.event(7, 3, Instant.parse("2026-09-29T00:00:06Z"),
                            Instant.parse("2026-09-29T00:00:07Z"), false, "50"));
            for (var event : input) {
                producer.send(new ProducerRecord<>(topic, event.stationId(),
                        mapper.writeValueAsString(event))).get(10, TimeUnit.SECONDS);
            }
            long[] retained = logBounds(partition);
            assertThat(retained).containsExactly(0, input.size());

            var first = start(topic, firstGroup);
            List<Row> beforeDeletion;
            try {
                awaitOffset(admin, firstGroup, partition, input.size());
                beforeDeletion = snapshot();
                assertExpectedRows(beforeDeletion);
            } finally {
                first.stop();
            }
            assertThat(committedOffset(admin, firstGroup, partition)).isEqualTo(input.size());
            jdbc.sql("TRUNCATE evse_telemetry_latest").update();
            assertThat(snapshot()).isEmpty();
            assertThat(committedOffset(admin, rebuildGroup, partition)).isZero();

            long rebuildStartedAt = System.nanoTime();
            var rebuild = start(topic, rebuildGroup);
            long rebuildCompletedAt;
            List<Row> afterRebuild;
            try {
                awaitOffset(admin, rebuildGroup, partition, input.size());
                rebuildCompletedAt = System.nanoTime();
                afterRebuild = snapshot();
                assertThat(afterRebuild).containsExactlyElementsOf(beforeDeletion);
                assertExpectedRows(afterRebuild);
                assertThat(committedOffset(admin, firstGroup, partition)).isEqualTo(input.size());
            } finally {
                rebuild.stop();
            }

            var result = mapper.createObjectNode();
            result.put("runId", runId);
            result.put("javaVersion", System.getProperty("java.version"));
            result.put("scope", "one embedded broker/partition, existing latest model, test-only new group");
            result.put("inputRecords", input.size());
            result.put("uniqueEventIds", 7);
            result.put("beginningOffset", retained[0]);
            result.put("endOffset", retained[1]);
            result.put("firstGroupOffset", committedOffset(admin, firstGroup, partition));
            result.put("rebuildGroupOffset", committedOffset(admin, rebuildGroup, partition));
            result.put("rowsBeforeDeletion", beforeDeletion.size());
            result.put("rowsAfterDeletion", 0);
            result.put("rowsAfterRebuild", afterRebuild.size());
            result.put("businessSnapshotEqual", true);
            result.put("independentExpectedRowsMatched", true);
            result.put("rebuildStartToOffsetAndSnapshotMs",
                    (rebuildCompletedAt - rebuildStartedAt) / 1_000_000.0);
            result.put("updatedAtCompared", false);
            Path output = Path.of("build", "experiment-results", "telemetry-rebuild-" + runId + ".json");
            Files.createDirectories(output.getParent());
            Files.writeString(output, mapper.writerWithDefaultPrettyPrinter().writeValueAsString(result));
        }
    }

    private ConcurrentMessageListenerContainer<String, String> start(String topic, String group) {
        var container = factory.createContainer(topic);
        container.getContainerProperties().setGroupId(group);
        container.getContainerProperties().setPollTimeout(100);
        container.getContainerProperties().setMessageListener((MessageListener<String, String>) listener::consume);
        container.start();
        return container;
    }

    private void awaitOffset(AdminClient admin, String group, TopicPartition partition, long expected) {
        await().atMost(Duration.ofSeconds(25)).untilAsserted(() ->
                assertThat(committedOffset(admin, group, partition)).isEqualTo(expected));
    }

    private long committedOffset(AdminClient admin, String group, TopicPartition partition) throws Exception {
        try {
            var value = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata()
                    .get(10, TimeUnit.SECONDS).get(partition);
            return value == null ? 0 : value.offset();
        } catch (ExecutionException exception) {
            if (exception.getCause() instanceof GroupIdNotFoundException) return 0;
            throw exception;
        }
    }

    private long[] logBounds(TopicPartition partition) {
        try (var consumer = new KafkaConsumer<String, String>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString(),
                ConsumerConfig.GROUP_ID_CONFIG, "telemetry-bounds-" + UUID.randomUUID(),
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"))) {
            return new long[] {consumer.beginningOffsets(List.of(partition)).get(partition),
                    consumer.endOffsets(List.of(partition)).get(partition)};
        }
    }

    private List<Row> snapshot() {
        return jdbc.sql("""
                SELECT station_id, evse_id, charging, power, voltage, current,
                       occurred_at, received_at, last_event_id
                FROM evse_telemetry_latest ORDER BY station_id, evse_id
                """).query((rs, index) -> new Row(rs.getString("station_id"), rs.getInt("evse_id"),
                rs.getBoolean("charging"), rs.getBigDecimal("power"), rs.getBigDecimal("voltage"),
                rs.getBigDecimal("current"), rs.getObject("occurred_at", OffsetDateTime.class).toInstant(),
                rs.getObject("received_at", OffsetDateTime.class).toInstant(),
                rs.getObject("last_event_id", UUID.class))).list();
    }

    private void assertExpectedRows(List<Row> rows) {
        assertThat(rows).hasSize(3);
        assertThat(rows.stream().map(Row::evseId)).containsExactly(1, 2, 3);
        assertThat(rows.get(0).eventId()).isEqualTo(new UUID(0, 2));
        assertThat(rows.get(0).charging()).isTrue();
        assertThat(rows.get(0).power()).isEqualByComparingTo("99.875");
        assertThat(rows.get(0).occurredAt()).isEqualTo(Instant.parse("2026-09-29T00:00:01Z"));
        assertThat(rows.get(0).receivedAt()).isEqualTo(Instant.parse("2026-09-29T00:00:02Z"));
        assertThat(rows.get(1).eventId()).isEqualTo(new UUID(0, 6));
        assertThat(rows.get(1).charging()).isTrue();
        assertThat(rows.get(1).power()).isEqualByComparingTo("80");
        assertThat(rows.get(1).occurredAt()).isEqualTo(Instant.parse("2026-09-29T00:00:04Z"));
        assertThat(rows.get(1).receivedAt()).isEqualTo(Instant.parse("2026-09-29T00:00:05Z"));
        assertThat(rows.get(2).eventId()).isEqualTo(new UUID(0, 7));
        assertThat(rows.get(2).charging()).isFalse();
        assertThat(rows.get(2).power()).isEqualByComparingTo("50");
        assertThat(rows.get(2).occurredAt()).isEqualTo(Instant.parse("2026-09-29T00:00:06Z"));
        assertThat(rows.get(2).receivedAt()).isEqualTo(Instant.parse("2026-09-29T00:00:07Z"));
        for (Row row : rows) {
            assertThat(row.stationId()).isEqualTo(TelemetryFixtures.STATION);
            assertThat(row.voltage()).isEqualByComparingTo("230.125");
            assertThat(row.current()).isEqualByComparingTo("0.875");
        }
    }

    private record Row(String stationId, int evseId, boolean charging,
                       BigDecimal power, BigDecimal voltage, BigDecimal current,
                       Instant occurredAt, Instant receivedAt, UUID eventId) { }
}
