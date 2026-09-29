package com.evcharging.telemetryworker.adapter.kafka;

import com.evcharging.messaging.contract.ChargerTelemetryPayload;
import com.evcharging.messaging.contract.DomainEventEnvelope;
import com.evcharging.telemetryworker.PostgreSqlTestConfiguration;
import com.evcharging.telemetryworker.application.LatestTelemetryRepository;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import tools.jackson.databind.ObjectMapper;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {"spring.kafka.bootstrap-servers=unused:9092", "telemetry.kafka-topic=unused-topic",
        "spring.kafka.listener.auto-startup=false"})
@Import({PostgreSqlTestConfiguration.class, KafkaTelemetryConsumerIntegrationTest.KafkaConfiguration.class})
@ExtendWith(OutputCaptureExtension.class)
class KafkaTelemetryConsumerIntegrationTest {
    @TestConfiguration(proxyBeanMethods = false)
    static class KafkaConfiguration {
        @Bean @ServiceConnection
        KafkaContainer kafka() { return new KafkaContainer("apache/kafka:4.1.1"); }
    }

    @Autowired KafkaContainer kafka;
    @Autowired PostgreSQLContainer postgres;
    @Autowired JdbcClient jdbc;
    @Autowired ObjectMapper mapper;
    @Autowired TelemetryRecordListener listener;
    @Autowired @Qualifier("telemetryKafkaListenerContainerFactory")
    ConcurrentKafkaListenerContainerFactory<String, String> factory;
    @MockitoSpyBean LatestTelemetryRepository repository;
    @TempDir Path directory;

    private String topic;
    private AdminClient admin;
    private KafkaProducer<String, String> producer;
    private final List<ConcurrentMessageListenerContainer<String, String>> containers = new ArrayList<>();

    @BeforeEach
    void prepareIsolatedTopicAndRows() throws Exception {
        jdbc.sql("TRUNCATE evse_telemetry_latest").update();
        topic = "telemetry-test-" + UUID.randomUUID();
        admin = AdminClient.create(Map.of("bootstrap.servers", kafka.getBootstrapServers()));
        admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get(10, TimeUnit.SECONDS);
        producer = new KafkaProducer<>(Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.ACKS_CONFIG, "all", ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true));
    }

    @AfterEach
    void closeClientsAndConsumers() {
        containers.forEach(ConcurrentMessageListenerContainer::stop);
        if (producer != null) producer.close(Duration.ofSeconds(5));
        if (admin != null) admin.close(Duration.ofSeconds(5));
    }

    @Test
    void consumesRetainedRecordsAndPreservesTheLatestRowForDuplicatePastAndTiedEvents() throws Exception {
        send(TelemetryFixtures.first());
        send(TelemetryFixtures.latest());
        start();
        waitForOffset(2);
        Row latest = read(1);
        assertThat(latest.eventId()).isEqualTo(new UUID(0, 2));
        assertThat(latest.charging()).isTrue();
        assertThat(latest.power()).isEqualByComparingTo("99.875");
        assertThat(latest.voltage()).isEqualByComparingTo("230.125");
        assertThat(latest.current()).isEqualByComparingTo("0.875");
        assertThat(latest.observed()).isEqualTo(Instant.parse("2026-09-29T00:00:01Z"));
        assertThat(latest.received()).isEqualTo(Instant.parse("2026-09-29T00:00:02Z"));

        send(TelemetryFixtures.latest());
        send(TelemetryFixtures.event(3, 1, TelemetryFixtures.OBSERVED, TelemetryFixtures.RECEIVED, false, "777"));
        send(TelemetryFixtures.event(4, 1, Instant.parse("2026-09-29T00:00:01Z"),
                Instant.parse("2026-09-29T00:00:02Z"), false, "888"));
        waitForOffset(5);
        assertThat(read(1)).isEqualTo(latest);
        assertThat(count()).isEqualTo(1);
    }

    @Test
    void sameGroupRestartResumesFromTheCommittedOffset() throws Exception {
        send(TelemetryFixtures.first());
        var first = start();
        waitForOffset(1);
        first.stop();
        clearInvocations(repository);
        var restarted = start();
        await().atMost(Duration.ofSeconds(25)).until(() -> !restarted.getAssignedPartitions().isEmpty());
        send(TelemetryFixtures.latest());
        waitForOffset(2);
        verify(repository, times(1)).upsert(any());
        assertThat(read(1).eventId()).isEqualTo(new UUID(0, 2));
    }

    @ParameterizedTest
    @ValueSource(strings = {"malformed", "missing-charging", "type", "key"})
    void stopsAtContractFailureWithoutAdvancingPastIt(String invalid, CapturedOutput output) throws Exception {
        send(TelemetryFixtures.first());
        String json = mapper.writeValueAsString(TelemetryFixtures.latest());
        String key = TelemetryFixtures.STATION;
        switch (invalid) {
            case "malformed" -> json = "{PRIVATE-SAMPLE";
            case "missing-charging" -> json = json.replace("\"charging\":true,", "");
            case "type" -> json = json.replace("ChargerTelemetryReceived", "OtherEvent");
            case "key" -> key = "PRIVATE-SAMPLE";
        }
        send(key, json);
        send(TelemetryFixtures.event(3, 2, TelemetryFixtures.OBSERVED, TelemetryFixtures.RECEIVED, true, "777"));
        var container = start();
        await().atMost(Duration.ofSeconds(25)).until(() -> !container.isRunning());
        waitForOffset(1);
        assertThat(count()).isEqualTo(1);
        assertFirstRow(read(1));
        assertThat(output.getAll()).contains("stage=contract", "partition=0 offset=1", "failedAt=")
                .doesNotContain("PRIVATE-SAMPLE", TelemetryFixtures.STATION, "{\"eventId\"");
    }

    @Test
    void rollsBackTheFailedUpdateAndReplaysItBeforeLaterRecordsOnSameGroupRestart(CapturedOutput output) throws Exception {
        send(TelemetryFixtures.first());
        var container = start();
        waitForOffset(1);
        Row original = read(1);
        AtomicBoolean fail = new AtomicBoolean(true);
        doAnswer(invocation -> {
            Object result = invocation.callRealMethod();
            if (fail.get()) {
                assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isTrue();
                jdbc.sql("SELECT 1 / 0").query(Integer.class).single();
            }
            return result;
        }).when(repository).upsert(any());
        send(TelemetryFixtures.latest());
        send(TelemetryFixtures.event(3, 2, TelemetryFixtures.OBSERVED, TelemetryFixtures.RECEIVED, true, "777"));
        await().atMost(Duration.ofSeconds(25)).until(() -> !container.isRunning());
        assertThat(offset()).isEqualTo(1);
        assertThat(read(1)).isEqualTo(original);
        assertThat(count()).isEqualTo(1);
        assertThat(output.getAll()).contains("stage=database", "partition=0 offset=1")
                .doesNotContain(TelemetryFixtures.STATION, "SELECT 1 / 0");
        fail.set(false);
        start();
        waitForOffset(3);
        assertThat(read(1).eventId()).isEqualTo(new UUID(0, 2));
        assertThat(read(1).power()).isEqualByComparingTo("99.875");
        assertThat(read(2).eventId()).isEqualTo(new UUID(0, 3));
        assertThat(count()).isEqualTo(2);
    }

    @Test
    void actualProcessHaltAfterDbCommitLeavesOffsetUncommittedAndRedeliveryUnchanged() throws Exception {
        send(TelemetryFixtures.first());
        var builder = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", System.getProperty("telemetry.test.runtime-classpath"), CrashAfterDbCommitProcess.class.getName(),
                "--spring.kafka.listener.auto-startup=false", "--spring.profiles.active=crash-test",
                "--spring.kafka.consumer.properties[session.timeout.ms]=6000",
                "--spring.kafka.consumer.properties[heartbeat.interval.ms]=1000");
        var env = builder.environment();
        env.put("SPRING_DATASOURCE_URL", postgres.getJdbcUrl());
        env.put("SPRING_DATASOURCE_USERNAME", postgres.getUsername());
        env.put("SPRING_DATASOURCE_PASSWORD", postgres.getPassword());
        env.put("SPRING_KAFKA_BOOTSTRAPSERVERS", kafka.getBootstrapServers());
        env.put("TELEMETRY_KAFKA_TOPIC", topic);
        builder.redirectErrorStream(true).redirectOutput(directory.resolve("crash-process.log").toFile());
        Process process = builder.start();
        try {
            assertThat(process.waitFor(35, TimeUnit.SECONDS)).as("fork must reach the real DB commit and halt").isTrue();
            assertThat(process.exitValue()).isEqualTo(137);
        } finally {
            if (process.isAlive()) { process.destroyForcibly(); process.waitFor(10, TimeUnit.SECONDS); }
        }
        Row committed = read(1);
        assertFirstRow(committed);
        assertThat(offset()).isZero();
        start();
        waitForOffset(1);
        verify(repository, times(1)).upsert(any());
        assertThat(read(1)).isEqualTo(committed);
        assertThat(count()).isEqualTo(1);
    }

    private ConcurrentMessageListenerContainer<String, String> start() {
        var container = factory.createContainer(topic);
        assertThat(container.getConcurrency()).isEqualTo(1);
        assertThat(container.getContainerProperties().getAckMode()).isEqualTo(ContainerProperties.AckMode.RECORD);
        assertThat(container.getContainerProperties().isSyncCommits()).isTrue();
        assertThat(factory.getConsumerFactory().getConfigurationProperties().get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)).isEqualTo(false);
        container.getContainerProperties().setGroupId(TelemetryRecordListener.GROUP_ID);
        container.getContainerProperties().setPollTimeout(100);
        container.getContainerProperties().setMessageListener((MessageListener<String, String>) listener::consume);
        containers.add(container);
        container.start();
        return container;
    }

    private void send(DomainEventEnvelope<ChargerTelemetryPayload> event) throws Exception {
        send(event.stationId(), mapper.writeValueAsString(event));
    }

    private void send(String key, String json) throws Exception {
        producer.send(new ProducerRecord<>(topic, 0, key, json)).get(10, TimeUnit.SECONDS);
    }

    private void waitForOffset(long expected) {
        await().atMost(Duration.ofSeconds(25)).untilAsserted(() -> assertThat(offset()).isEqualTo(expected));
    }

    private long offset() throws Exception {
        try {
            var value = admin.listConsumerGroupOffsets(TelemetryRecordListener.GROUP_ID).partitionsToOffsetAndMetadata()
                    .get(10, TimeUnit.SECONDS).get(new TopicPartition(topic, 0));
            return value == null ? 0 : value.offset();
        } catch (ExecutionException exception) {
            if (exception.getCause() instanceof GroupIdNotFoundException) return 0;
            throw exception;
        }
    }

    private long count() { return jdbc.sql("SELECT count(*) FROM evse_telemetry_latest").query(Long.class).single(); }

    private Row read(int evse) {
        return jdbc.sql("SELECT * FROM evse_telemetry_latest WHERE station_id=:station AND evse_id=:evse")
                .param("station", TelemetryFixtures.STATION).param("evse", evse)
                .query((rs, i) -> new Row(rs.getBoolean("charging"), rs.getBigDecimal("power"), rs.getBigDecimal("voltage"),
                        rs.getBigDecimal("current"), rs.getObject("occurred_at", OffsetDateTime.class).toInstant(),
                        rs.getObject("received_at", OffsetDateTime.class).toInstant(), rs.getObject("last_event_id", UUID.class),
                        rs.getObject("updated_at", OffsetDateTime.class).toInstant())).single();
    }

    private void assertFirstRow(Row row) {
        assertThat(row.charging()).isFalse();
        assertThat(row.power()).isEqualByComparingTo("120.12345678901234567890123");
        assertThat(row.voltage()).isEqualByComparingTo("230.125");
        assertThat(row.current()).isEqualByComparingTo("0.875");
        assertThat(row.observed()).isEqualTo(Instant.parse("2026-09-29T00:00:00.123456Z"));
        assertThat(row.received()).isEqualTo(Instant.parse("2026-09-29T00:00:00.234567Z"));
        assertThat(row.eventId()).isEqualTo(new UUID(0, 1));
        assertThat(row.updated()).isNotNull();
    }

    private record Row(boolean charging, BigDecimal power, BigDecimal voltage, BigDecimal current,
                       Instant observed, Instant received, UUID eventId, Instant updated) { }
}
