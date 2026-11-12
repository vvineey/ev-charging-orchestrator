package com.evcharging.controlplane.transaction;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import com.evcharging.controlplane.ControlPlaneApplication;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.IllegalTransactionStateException;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {"transaction.ingress.enabled=true", "spring.kafka.listener.auto-startup=false",
                "transaction.recovery.experimental-candidate-calculation-enabled=true"})
@EmbeddedKafka(partitions = 1, topics = "charging.transaction.usage-provisional.v1",
        bootstrapServersProperty = "spring.kafka.bootstrap-servers")
class TransactionUsageOutboxPostgresTest {
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

    @Autowired JdbcClient jdbc;
    @Autowired ObjectMapper mapper;
    @Autowired StoreTransactionObservation service;
    @Autowired JdbcTransactionUsageOutbox outbox;
    @Autowired EmbeddedKafkaBroker broker;
    @Autowired ProducerFactory<Object, Object> producerFactory;

    @BeforeEach
    void clear() {
        jdbc.sql("TRUNCATE transaction_inbox_state CASCADE").update();
    }

    @Test
    void creationAndInvalidationKeepSeparateImmutableSnapshotsAndReplayCreatesNothing() {
        calculate("TX-CHANGE");
        service.store(event("TX-CHANGE", 1, "Ended", 12500));
        assertThat(count("transaction_usage_outbox")).isEqualTo(1);
        String calculated = payload(1);
        service.store(event("TX-CHANGE", 1, "Ended", 12600));
        service.store(event("TX-CHANGE", 1, "Ended", 12600));

        assertThat(count("transaction_usage_outbox")).isEqualTo(2);
        assertThat(count("transaction_usage_outbox_delivery")).isEqualTo(2);
        assertThat(payload(1)).isEqualTo(calculated);
        var created = mapper.readTree(calculated);
        var invalidated = mapper.readTree(payload(2));
        assertThat(created.get("eventType").asString()).isEqualTo("UsageProvisionalCalculated");
        assertThat(created.get("payload").get("candidateStatus").asString()).isEqualTo("PROVISIONAL");
        assertThat(invalidated.get("eventType").asString()).isEqualTo("UsageProvisionalInvalidated");
        assertThat(invalidated.get("payload").get("candidateStatus").asString()).isEqualTo("INVALIDATED");
        assertThat(invalidated.get("payload").get("energyWh").decimalValue()).isEqualByComparingTo("2500");
        assertThat(invalidated.get("payload").get("invalidationReason").asString()).isEqualTo("SOURCE_CONFLICT");
        assertThat(value("SELECT aggregate_version FROM transaction_session_candidate", Long.class)).isEqualTo(2L);
    }

    @Test
    void outboxInsertFailureRollsBackCandidateStateAndTheTriggeringInput() {
        service.store(event("TX-ROLLBACK", 0, "Started", 10000));
        rejectInserts("transaction_usage_outbox");
        try {
            assertThatThrownBy(() -> service.store(event("TX-ROLLBACK", 1, "Ended", 12500)))
                    .isInstanceOf(RuntimeException.class);
            assertThat(count("transaction_session_candidate")).isZero();
            assertThat(count("transaction_inbox_event")).isEqualTo(1);
            assertThat(count("transaction_usage_outbox")).isZero();
            assertThat(value("SELECT status FROM transaction_inbox_state", String.class)).isEqualTo("PENDING");
        } finally {
            allowInserts("transaction_usage_outbox");
        }
    }

    @Test
    void deliveryInsertFailureRollsBackTheEventAndItsCandidateVersion() {
        calculate("TX-INVALIDATION-ROLLBACK");
        rejectInserts("transaction_usage_outbox_delivery");
        try {
            assertThatThrownBy(() -> service.store(event("TX-INVALIDATION-ROLLBACK", 1, "Ended", 12600)))
                    .isInstanceOf(RuntimeException.class);
            assertThat(count("transaction_inbox_conflict")).isZero();
            assertThat(count("transaction_usage_outbox")).isEqualTo(1);
            assertThat(value("SELECT candidate_status FROM transaction_session_candidate", String.class))
                    .isEqualTo("PROVISIONAL");
            assertThat(value("SELECT aggregate_version FROM transaction_session_candidate", Long.class)).isEqualTo(1L);
            assertThat(value("SELECT status FROM transaction_inbox_state", String.class)).isEqualTo("PROVISIONAL");
        } finally {
            allowInserts("transaction_usage_outbox_delivery");
        }
    }

    @Test
    void lateNewSequenceAlsoRecordsInvalidation() {
        calculate("TX-LATE");
        service.store(event("TX-LATE", 2, "Updated", 12550));
        assertThat(count("transaction_usage_outbox")).isEqualTo(2);
        assertThat(mapper.readTree(payload(2)).get("payload").get("invalidationReason").asString())
                .isEqualTo("LATE_INVALID_EVENT");
    }

    @Test
    void expiredClaimCanBeRecoveredButItsPreviousOwnerCannotCompleteOrBlockIt() {
        calculate("TX-LEASE");
        service.store(event("TX-LEASE", 1, "Ended", 12600));
        var first = outbox.claim(Duration.ofSeconds(30), 10).getFirst();
        assertThat(outbox.claim(Duration.ofSeconds(30), 10)).isEmpty();
        expireClaims();
        var recovered = outbox.claim(Duration.ofSeconds(30), 10).getFirst();

        assertThat(recovered.eventId()).isEqualTo(first.eventId());
        assertThat(recovered.payload()).isEqualTo(first.payload());
        assertThat(recovered.claimToken()).isNotEqualTo(first.claimToken());
        assertThat(outbox.markPublished(first)).isFalse();
        assertThat(outbox.block(first, "StaleWorker")).isFalse();
        assertThat(outbox.markPublished(recovered)).isTrue();
        var second = outbox.claim(Duration.ofSeconds(30), 10).getFirst();
        assertThat(mapper.readTree(second.payload()).get("payload").get("aggregateVersion").asLong()).isEqualTo(2);
    }

    @Test
    void twoRelayClaimsCannotOwnTheSameHeadAndLaterVersionCannotPassIt() throws Exception {
        calculate("TX-RACE");
        service.store(event("TX-RACE", 1, "Ended", 12600));
        var start = new CountDownLatch(1);
        Callable<List<JdbcTransactionUsageOutbox.ClaimedEvent>> claim = () -> {
            if (!start.await(10, TimeUnit.SECONDS)) throw new IllegalStateException("claim start timed out");
            return outbox.claim(Duration.ofSeconds(30), 10);
        };
        try (var executor = Executors.newFixedThreadPool(2)) {
            var a = executor.submit(claim);
            var b = executor.submit(claim);
            start.countDown();
            var claimed = new ArrayList<>(a.get(10, TimeUnit.SECONDS));
            claimed.addAll(b.get(10, TimeUnit.SECONDS));
            assertThat(claimed).hasSize(1);
            assertThat(mapper.readTree(claimed.getFirst().payload()).get("payload").get("aggregateVersion").asLong())
                    .isEqualTo(1);
        }
    }

    @Test
    void knownSendFailureIsBlockedWithoutAutomaticRetryAndBlocksOnlyItsTransaction() {
        calculate("TX-BLOCKED");
        service.store(event("TX-BLOCKED", 1, "Ended", 12600));
        var relay = new TransactionUsageOutboxRelay(outbox, event -> {
            throw new TimeoutException("injected uncertain acknowledgement");
        }, Duration.ofSeconds(30), Duration.ofSeconds(10));
        relay.publishNext();
        assertThat(value("SELECT status FROM transaction_usage_outbox_delivery WHERE last_error_type IS NOT NULL",
                String.class)).isEqualTo("BLOCKED");
        assertThat(outbox.claim(Duration.ofSeconds(30), 10)).isEmpty();
        calculate("TX-OTHER");
        assertThat(outbox.claim(Duration.ofSeconds(30), 10)).hasSize(1);
    }

    @Test
    void failureAfterAcknowledgementCanRepublishExactlyTheSameEvent() {
        calculate("TX-ACK-WINDOW");
        var sent = new ArrayList<JdbcTransactionUsageOutbox.ClaimedEvent>();
        var relay = new TransactionUsageOutboxRelay(outbox, sent::add, Duration.ofSeconds(30), Duration.ofSeconds(10));
        jdbc.sql("""
                ALTER TABLE transaction_usage_outbox_delivery
                ADD CONSTRAINT injected_completion_failure CHECK (status <> 'PUBLISHED') NOT VALID
                """).update();
        try {
            assertThatThrownBy(relay::publishNext).isInstanceOf(RuntimeException.class);
        } finally {
            jdbc.sql("ALTER TABLE transaction_usage_outbox_delivery DROP CONSTRAINT injected_completion_failure").update();
        }
        assertThat(value("SELECT status FROM transaction_usage_outbox_delivery", String.class)).isEqualTo("PROCESSING");
        expireClaims();
        relay.publishNext();
        assertThat(sent).hasSize(2);
        assertThat(sent.get(1).eventId()).isEqualTo(sent.getFirst().eventId());
        assertThat(sent.get(1).payload()).isEqualTo(sent.getFirst().payload());
        assertThat(value("SELECT status FROM transaction_usage_outbox_delivery", String.class)).isEqualTo("PUBLISHED");
    }

    @Test
    void actualKafkaAcknowledgementPublishesTheStoredSnapshotThenMarksCompletion() throws Exception {
        calculate("TX-KAFKA");
        var properties = new HashMap<String, Object>();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "usage-outbox-" + UUID.randomUUID());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        try (var consumer = new KafkaConsumer<String, String>(properties);
                var publisher = new KafkaTransactionUsageOutboxPublisher(producerFactory, Duration.ofSeconds(10))) {
            broker.consumeFromAnEmbeddedTopic(consumer, "charging.transaction.usage-provisional.v1");
            new TransactionUsageOutboxRelay(outbox, publisher, Duration.ofSeconds(30), Duration.ofSeconds(10)).publishNext();
            var record = KafkaTestUtils.getSingleRecord(consumer, "charging.transaction.usage-provisional.v1", Duration.ofSeconds(15));
            assertThat(record.key()).isEqualTo("ST-TEST");
            assertThat(mapper.readTree(record.value())).isEqualTo(mapper.readTree(payload(1)));
            assertThat(value("SELECT status FROM transaction_usage_outbox_delivery", String.class)).isEqualTo("PUBLISHED");
        }
    }

    @Test
    void publicationIntentCannotBeWrittenOutsideTheBusinessTransaction() {
        assertThatThrownBy(() -> outbox.appendCandidate(event("TX-NO-TX", 0, "Started", 10000)))
                .isInstanceOf(IllegalTransactionStateException.class);
    }

    @Test
    void explicitRelayActivationStartsTheScheduledPublisherWithBootConnectionSettings() {
        try (var context = new SpringApplicationBuilder(ControlPlaneApplication.class).web(WebApplicationType.NONE).run(
                "--transaction.ingress.enabled=true", "--transaction.outbox.relay-enabled=true",
                "--spring.kafka.listener.auto-startup=false",
                "--spring.kafka.bootstrap-servers=" + broker.getBrokersAsString(),
                "--spring.datasource.url=" + POSTGRES.getJdbcUrl("postgres", "postgres"),
                "--spring.datasource.username=postgres", "--spring.datasource.password=postgres")) {
            assertThat(context.getBeansOfType(TransactionUsageOutboxRelay.class)).hasSize(1);
            assertThat(context.getBeansOfType(KafkaTransactionUsageOutboxPublisher.class)).hasSize(1);
            context.getBean(TransactionUsageOutboxRelay.class).publishNext();
        }
    }

    @Test
    void differentChargersCanReuseTransactionIdWithoutSharingEventVersions() {
        calculate("TX-SHARED");
        for (var event : List.of(event("TX-SHARED", 0, "Started", 10000), event("TX-SHARED", 1, "Ended", 12500))) {
            var source = (ObjectNode) mapper.readTree(event.sourceJson());
            source.put("chargingStationId", "CS-OTHER");
            service.store(new ObservedTransactionRecord(event.eventId(), event.stationId(), "CS-OTHER", event.transactionId(),
                    event.seqNo(), event.eventType(), event.occurredAt(), event.receivedAt(),
                    mapper.writeValueAsString(source), event.payload()));
        }
        assertThat(count("transaction_usage_outbox")).isEqualTo(2);
        var events = outbox.claim(Duration.ofSeconds(30), 10);
        assertThat(events).hasSize(2);
        assertThat(events.stream().map(event -> mapper.readTree(event.payload()).get("chargingStationId").asString())
                .distinct().count()).isEqualTo(2);
        assertThat(value("SELECT max(aggregate_version) FROM transaction_usage_outbox", Long.class)).isEqualTo(1L);
    }

    private void calculate(String transactionId) {
        service.store(event(transactionId, 0, "Started", 10000));
        service.store(event(transactionId, 1, "Ended", 12500));
    }

    private ObservedTransactionRecord event(String transactionId, int seqNo, String type, int wh) {
        Instant occurredAt = Instant.parse("2026-01-01T10:00:00Z").plusSeconds(seqNo * 1200L);
        ObjectNode payload = mapper.createObjectNode();
        payload.put("transactionId", transactionId).put("seqNo", seqNo).put("eventType", type).put("triggerReason", "MeterValuePeriodic");
        if (seqNo == 0) payload.putObject("evse").put("id", 1);
        var meter = payload.putArray("meterValue").addObject();
        meter.put("timestamp", occurredAt.toString());
        var sample = meter.putArray("sampledValue").addObject();
        sample.put("value", new BigDecimal(wh));
        sample.put("context", seqNo == 0 ? "Transaction.Begin" : "Transaction.End");
        sample.putObject("unitOfMeasure").put("unit", "Wh").put("multiplier", 0);
        var source = mapper.createObjectNode();
        source.put("occurredAt", occurredAt.toString()).put("stationId", "ST-TEST").put("chargingStationId", "CS-TEST");
        source.set("payload", payload);
        return new ObservedTransactionRecord(UUID.randomUUID(), "ST-TEST", "CS-TEST", transactionId, seqNo,
                type, occurredAt, occurredAt.plusSeconds(1), mapper.writeValueAsString(source), payload);
    }

    private String payload(long version) {
        return jdbc.sql("SELECT payload::text FROM transaction_usage_outbox WHERE aggregate_version = :version")
                .param("version", version).query(String.class).single();
    }

    private long count(String table) {
        return value("SELECT count(*) FROM " + table, Long.class);
    }

    private <T> T value(String sql, Class<T> type) {
        return jdbc.sql(sql).query(type).single();
    }

    private void expireClaims() {
        jdbc.sql("UPDATE transaction_usage_outbox_delivery SET claim_until = clock_timestamp() - interval '1 second' WHERE status = 'PROCESSING'")
                .update();
    }

    private void rejectInserts(String table) {
        jdbc.sql("ALTER TABLE " + table + " ADD CONSTRAINT injected_insert_failure CHECK (false) NOT VALID").update();
    }

    private void allowInserts(String table) {
        jdbc.sql("ALTER TABLE " + table + " DROP CONSTRAINT injected_insert_failure").update();
    }

}
