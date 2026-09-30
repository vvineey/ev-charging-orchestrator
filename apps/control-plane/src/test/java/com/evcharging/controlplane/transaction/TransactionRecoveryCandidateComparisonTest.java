package com.evcharging.controlplane.transaction;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
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
import tools.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Correctness-only comparison. A/B are intentionally small in-memory baselines; C is the real Kafka/PG Inbox. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {"transaction.ingress.enabled=true", "spring.kafka.listener.auto-startup=false",
                "transaction.recovery.experimental-session-confirmation-enabled=true"})
@EmbeddedKafka(partitions = 1, topics = "transaction-comparison-bootstrap",
        bootstrapServersProperty = "spring.kafka.bootstrap-servers",
        brokerProperties = "offsets.topic.num.partitions=1")
class TransactionRecoveryCandidateComparisonTest {
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
    @Autowired TransactionRecordListener listener;
    @Autowired @Qualifier("transactionKafkaListenerContainerFactory")
    ConcurrentKafkaListenerContainerFactory<String, String> factory;

    record Event(int seqNo, String stage, Integer wh, int arrivalSecond) {
        Instant occurredAt() {
            return Instant.parse("Started".equals(stage) ? "2026-01-01T10:00:00Z"
                    : "Ended".equals(stage) ? "2026-01-01T10:20:00Z" : "2026-01-01T10:10:00Z");
        }
    }

    record Result(String state, Integer energyWh, int finalizedCount) { }
    record Scenario(String name, List<Event> arrival, Result expected) { }
    record Row(String scenario, List<Integer> arrivalSeqNo, List<Integer> arrivalSeconds, Result expected,
               Result arrivalOrderA, boolean aMatches, Result guardedA, boolean guardedAMatches,
               Result windowB, boolean bMatches, Result guardedB, boolean guardedBMatches,
               Result durableInboxC, boolean cMatches, int inboxEventCount, int conflictCount) { }

    @Test
    void nineIndependentCasesCompareThreeCandidatesOnTheSameLogicalInput() throws Exception {
        String topic = "transaction-comparison-" + UUID.randomUUID();
        String groupId = "transaction-comparison-" + UUID.randomUUID();
        String bootstrap = broker.getBrokersAsString();
        try (var admin = AdminClient.create(Map.of("bootstrap.servers", bootstrap));
             var producer = new KafkaProducer<String, String>(Map.of(
                     ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                     ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                     ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                     ProducerConfig.ACKS_CONFIG, "all"))) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get(10, TimeUnit.SECONDS);
            List<Scenario> scenarios = scenarios();
            int totalRecords = 0;
            for (Scenario scenario : scenarios) {
                for (Event event : scenario.arrival()) {
                    producer.send(new ProducerRecord<>(topic, 0, "ST-COMPARE", observed(scenario.name(), event)))
                            .get(10, TimeUnit.SECONDS);
                    totalRecords++;
                }
            }
            var container = factory.createContainer(topic);
            container.getContainerProperties().setGroupId(groupId);
            container.getContainerProperties().setPollTimeout(100);
            container.getContainerProperties().setMessageListener((MessageListener<String, String>) listener::consume);
            try {
                container.start();
                int expectedOffset = totalRecords;
                await().atMost(Duration.ofSeconds(40)).untilAsserted(() ->
                        assertThat(offset(admin, groupId, topic)).isEqualTo(expectedOffset));
                List<Row> rows = new ArrayList<>();
                for (Scenario scenario : scenarios) {
                    Result a = arrivalOrder(scenario.arrival());
                    Result b = fiveMinuteWindow(scenario.arrival());
                    Result guardedA = commonFinalSafetyGate(scenario.arrival(), a);
                    Result guardedB = commonFinalSafetyGate(scenario.arrival(), b);
                    Result c = durableResult(scenario.name());
                    int events = jdbc.sql("""
                            SELECT count(*) FROM transaction_inbox_event
                            WHERE charging_station_id = 'CS-COMPARE' AND transaction_id = :id
                            """).param("id", scenario.name()).query(Long.class).single().intValue();
                    int conflicts = jdbc.sql("""
                            SELECT count(*) FROM transaction_inbox_conflict
                            WHERE charging_station_id = 'CS-COMPARE' AND transaction_id = :id
                            """).param("id", scenario.name()).query(Long.class).single().intValue();
                    rows.add(new Row(scenario.name(), scenario.arrival().stream().map(Event::seqNo).toList(),
                            scenario.arrival().stream().map(Event::arrivalSecond).toList(), scenario.expected(),
                            a, a.equals(scenario.expected()), guardedA, guardedA.equals(scenario.expected()),
                            b, b.equals(scenario.expected()), guardedB, guardedB.equals(scenario.expected()),
                            c, c.equals(scenario.expected()), events, conflicts));
                }
                writeResults(rows);
                assertThat(rows).hasSize(9);
                assertThat(rows.stream().filter(Row::aMatches).count()).isEqualTo(3);
                assertThat(rows.stream().filter(Row::bMatches).count()).isEqualTo(4);
                assertThat(rows.stream().filter(Row::guardedAMatches).count()).isEqualTo(7);
                assertThat(rows.stream().filter(Row::guardedBMatches).count()).isEqualTo(8);
                assertThat(rows.stream().filter(Row::cMatches).count()).isEqualTo(9);
                assertThat(rows.stream().filter(row -> row.scenario().equals("conflict_0_1_1_2"))
                        .findFirst().orElseThrow().conflictCount()).isEqualTo(1);
            } finally {
                container.stop();
            }
        }
    }

    private List<Scenario> scenarios() {
        Event start = new Event(0, "Started", 10000, 0);
        Event update = new Event(1, "Updated", 11500, 1);
        Event end = new Event(2, "Ended", 12500, 2);
        Result finalized = new Result("FINALIZED", 2500, 1);
        Result hold = new Result("HOLD", null, 0);
        return List.of(
                new Scenario("normal_0_1_2", List.of(start, update, end), finalized),
                new Scenario("short_0_1", List.of(start, new Event(1, "Ended", 12500, 1)), finalized),
                new Scenario("reverse_2_0_1", List.of(new Event(2, "Ended", 12500, 0),
                        new Event(0, "Started", 10000, 1), new Event(1, "Updated", 11500, 2)), finalized),
                new Scenario("duplicate_0_1_1_2", List.of(start, update,
                        new Event(1, "Updated", 11500, 2), new Event(2, "Ended", 12500, 3)), finalized),
                new Scenario("missing_0_2", List.of(start, new Event(2, "Ended", 12500, 1)), hold),
                new Scenario("conflict_0_1_1_2", List.of(start, update,
                        new Event(1, "Updated", 11700, 2), new Event(2, "Ended", 12500, 3)), hold),
                new Scenario("negative_0_1_2", List.of(start, update,
                        new Event(2, "Ended", 9500, 2)), hold),
                new Scenario("missing_start_meter", List.of(new Event(0, "Started", null, 0),
                        new Event(1, "Ended", 12500, 1)), hold),
                new Scenario("reverse_after_window", List.of(new Event(2, "Ended", 12500, 0),
                        new Event(0, "Started", 10000, 360), new Event(1, "Updated", 11500, 361)), finalized));
    }

    private Result arrivalOrder(List<Event> events) {
        String state = "EMPTY";
        Integer startWh = null;
        Integer energyWh = null;
        int finalizedCount = 0;
        for (Event event : events) {
            switch (event.stage()) {
                case "Started" -> { state = "OPEN"; startWh = event.wh(); }
                case "Updated" -> { /* The immediate row has no sequence check. */ }
                case "Ended" -> {
                    if ("OPEN".equals(state)) {
                        energyWh = event.wh() == null || startWh == null ? null : event.wh() - startWh;
                        state = "FINALIZED";
                        finalizedCount++;
                    }
                }
                default -> throw new IllegalArgumentException("unexpected stage");
            }
        }
        return new Result(state, energyWh, finalizedCount);
    }

    private Result fiveMinuteWindow(List<Event> events) {
        List<Event> ordered = new ArrayList<>();
        List<Event> batch = new ArrayList<>();
        int windowStart = events.getFirst().arrivalSecond();
        for (Event event : events) {
            if (event.arrivalSecond() - windowStart >= 300) {
                batch.sort(Comparator.comparing(Event::occurredAt));
                ordered.addAll(batch);
                batch.clear();
                windowStart = event.arrivalSecond();
            }
            batch.add(event);
        }
        batch.sort(Comparator.comparing(Event::occurredAt));
        ordered.addAll(batch);
        return arrivalOrder(ordered);
    }

    // Applied only to the final observed input set. It equalizes basic rejection rules for an
    // order-handling comparison; it cannot undo any side effect an online A/B implementation made.
    private Result commonFinalSafetyGate(List<Event> events, Result candidate) {
        Map<Integer, Event> bySequence = new HashMap<>();
        for (Event event : events) {
            Event previous = bySequence.putIfAbsent(event.seqNo(), event);
            if (previous != null && (!previous.stage().equals(event.stage())
                    || !Objects.equals(previous.wh(), event.wh())
                    || !previous.occurredAt().equals(event.occurredAt()))) {
                return new Result("HOLD", null, 0);
            }
        }
        int lastSequence = bySequence.keySet().stream().mapToInt(Integer::intValue).max().orElse(-1);
        for (int sequence = 0; sequence <= lastSequence; sequence++) {
            if (!bySequence.containsKey(sequence)) return new Result("HOLD", null, 0);
        }
        Event first = bySequence.get(0);
        Event last = bySequence.get(lastSequence);
        if (first == null || last == null || !"Started".equals(first.stage())
                || !"Ended".equals(last.stage()) || first.wh() == null || last.wh() == null
                || last.wh() < first.wh()) return new Result("HOLD", null, 0);
        return candidate;
    }

    private Result durableResult(String transactionId) {
        String status = jdbc.sql("""
                SELECT status FROM transaction_inbox_state
                WHERE charging_station_id = 'CS-COMPARE' AND transaction_id = :id
                """).param("id", transactionId).query(String.class).single();
        int count = jdbc.sql("""
                SELECT count(*) FROM recovered_transaction_session
                WHERE charging_station_id = 'CS-COMPARE' AND transaction_id = :id
                """).param("id", transactionId).query(Long.class).single().intValue();
        Integer wh = count == 0 ? null : jdbc.sql("""
                SELECT energy_wh FROM recovered_transaction_session
                WHERE charging_station_id = 'CS-COMPARE' AND transaction_id = :id
                """).param("id", transactionId).query(java.math.BigDecimal.class).single().intValueExact();
        return new Result(status.startsWith("HOLD") ? "HOLD" : status, wh, count);
    }

    private String observed(String transactionId, Event event) {
        ObjectNode record = mapper.createObjectNode();
        record.put("eventId", UUID.randomUUID().toString());
        record.put("eventType", "OcppTransactionEventObserved");
        record.put("schemaVersion", 1);
        record.put("occurredAt", event.occurredAt().toString());
        record.put("receivedAt", Instant.parse("2026-01-01T11:00:00Z").plusSeconds(event.arrivalSecond()).toString());
        record.put("stationId", "ST-COMPARE");
        record.put("chargingStationId", "CS-COMPARE");
        ObjectNode payload = record.putObject("payload");
        payload.put("transactionId", transactionId);
        payload.put("seqNo", event.seqNo());
        payload.put("eventType", event.stage());
        payload.put("triggerReason", "TEST");
        payload.putObject("transactionInfo").put("transactionId", transactionId);
        if ("Started".equals(event.stage())) payload.putObject("evse").put("id", 1);
        if (event.wh() != null) {
            ObjectNode meter = payload.putArray("meterValue").addObject();
            meter.put("timestamp", event.occurredAt().toString());
            ObjectNode sample = meter.putArray("sampledValue").addObject();
            sample.put("value", event.wh());
            sample.put("context", "Started".equals(event.stage()) ? "Transaction.Begin"
                    : "Ended".equals(event.stage()) ? "Transaction.End" : "Sample.Periodic");
            sample.put("measurand", "Energy.Active.Import.Register");
            sample.putObject("unitOfMeasure").put("unit", "Wh").put("multiplier", 0);
        }
        return mapper.writeValueAsString(record);
    }

    private long offset(AdminClient admin, String groupId, String topic) throws Exception {
        try {
            var value = admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata()
                    .get(10, TimeUnit.SECONDS).get(new TopicPartition(topic, 0));
            return value == null ? 0 : value.offset();
        } catch (ExecutionException exception) {
            if (exception.getCause() instanceof GroupIdNotFoundException) return 0;
            throw exception;
        }
    }

    private void writeResults(List<Row> rows) throws IOException {
        Path output = Path.of(System.getProperty("transaction.test.comparison-output"));
        Files.createDirectories(output.getParent());
        Files.writeString(output, mapper.writeValueAsString(rows));
    }
}
