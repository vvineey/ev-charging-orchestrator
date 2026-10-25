package com.evcharging.controlplane.transaction;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
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
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLongArray;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Opt-in, same-total-input observation of a hot station key with two real Kafka consumers. */
@EnabledIfEnvironmentVariable(named = "EVC_INBOX_STATION_SKEW_TEST", matches = "true")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {"transaction.ingress.enabled=true", "spring.kafka.listener.auto-startup=false",
                "transaction.recovery.experimental-candidate-calculation-enabled=true"})
@EmbeddedKafka(partitions = 2, topics = "transaction-skew-bootstrap",
        bootstrapServersProperty = "spring.kafka.bootstrap-servers",
        brokerProperties = "offsets.topic.num.partitions=1")
class TransactionInboxStationSkewTest {
    private static final EmbeddedPostgres POSTGRES;
    private static final int TRANSACTIONS = 120;
    private static final int RECORDS = TRANSACTIONS * 2;

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

    @Test
    void hotStationConcentratesOnePartitionWithoutChangingTheFinalInboxAnswer() throws Exception {
        String runId = UUID.randomUUID().toString();
        List<List<String>> keys = stationKeysByPartition();
        Path output = Path.of("build", "experiment-results", "inbox-station-skew-" + runId + ".json");
        ObjectNode report = mapper.createObjectNode();
        report.put("runId", runId);
        report.put("javaVersion", System.getProperty("java.version"));
        report.put("scope", "one embedded broker, two partitions and consumers, real PostgreSQL Inbox");
        report.put("transactionsPerTrial", TRANSACTIONS);
        report.put("recordsPerTrial", RECORDS);
        report.put("stationKeys", 8);
        report.put("trialOrder", "BALANCED,SKEW,SKEW,BALANCED,BALANCED,SKEW");
        var trials = report.putArray("trials");
        String[] order = {"BALANCED", "SKEW", "SKEW", "BALANCED", "BALANCED", "SKEW"};
        for (int ordinal = 0; ordinal < order.length; ordinal++) {
            ObjectNode trial = runTrial(order[ordinal], ordinal + 1, keys);
            trials.add(trial);
            Files.createDirectories(output.getParent());
            Files.writeString(output, mapper.writerWithDefaultPrettyPrinter().writeValueAsString(report));
            assertThat(trial.path("correct").asBoolean()).as("trial %s", ordinal + 1).isTrue();
        }
    }

    private ObjectNode runTrial(String mode, int ordinal, List<List<String>> keys) throws Exception {
        jdbc.sql("TRUNCATE transaction_inbox_state CASCADE").update();
        Map<String, Integer> stationTransactions = allocation(mode, keys);
        var stations = new ArrayList<String>(TRANSACTIONS);
        stationTransactions.forEach((station, count) -> {
            for (int index = 0; index < count; index++) stations.add(station);
        });
        assertThat(stations).hasSize(TRANSACTIONS);
        Collections.shuffle(stations, new Random(7251L + ordinal));
        Map<String, Integer> partitionForKey = new HashMap<>();
        for (int partition = 0; partition < 2; partition++) {
            for (String key : keys.get(partition)) partitionForKey.put(key, partition);
        }
        int[] expectedRecords = new int[2];
        var prepared = new ArrayList<ProducerRecord<String, String>>(RECORDS);
        for (int index = 0; index < TRANSACTIONS; index++) {
            String station = stations.get(index);
            String transactionId = "TX-SKEW-" + UUID.randomUUID();
            expectedRecords[partitionForKey.get(station)] += 2;
            prepared.add(new ProducerRecord<>("prepared", station, observed(transactionId, station, 0)));
            prepared.add(new ProducerRecord<>("prepared", station, observed(transactionId, station, 1)));
        }

        String topic = "transaction-skew-" + UUID.randomUUID();
        String group = "transaction-skew-" + UUID.randomUUID();
        var release = new CountDownLatch(1);
        var processed = new AtomicIntegerArray(2);
        var lastProcessedAt = new AtomicLongArray(2);
        try (var admin = AdminClient.create(Map.of("bootstrap.servers", broker.getBrokersAsString()));
             var producer = new KafkaProducer<String, String>(Map.of(
                     ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString(),
                     ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                     ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                     ProducerConfig.ACKS_CONFIG, "all"))) {
            admin.createTopics(List.of(new NewTopic(topic, 2, (short) 1))).all().get(10, TimeUnit.SECONDS);
            var container = factory.createContainer(topic);
            container.setConcurrency(2);
            container.getContainerProperties().setGroupId(group);
            container.getContainerProperties().setPollTimeout(100);
            container.getContainerProperties().setMessageListener((MessageListener<String, String>) record -> {
                try {
                    release.await();
                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Skew trial was interrupted", exception);
                }
                listener.consume(record);
                processed.incrementAndGet(record.partition());
                lastProcessedAt.set(record.partition(), System.nanoTime());
            });
            container.start();
            try {
                await().atMost(Duration.ofSeconds(25)).untilAsserted(() -> {
                    assertThat(container.getContainers()).hasSize(2);
                    container.getContainers().forEach(child ->
                            assertThat(child.getAssignedPartitions()).hasSize(1));
                });
                List<Future<RecordMetadata>> acknowledgements = new ArrayList<>(RECORDS);
                for (var source : prepared) {
                    acknowledgements.add(producer.send(
                            new ProducerRecord<>(topic, source.key(), source.value())));
                }
                for (int index = 0; index < prepared.size(); index++) {
                    int partition = acknowledgements.get(index).get(10, TimeUnit.SECONDS).partition();
                    assertThat(partition).isEqualTo(partitionForKey.get(prepared.get(index).key()));
                }
                long releasedAt = System.nanoTime();
                release.countDown();
                int[] remainingAtHalf = null;
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(45);
                while (processed.get(0) + processed.get(1) < RECORDS && System.nanoTime() < deadline) {
                    int completed = processed.get(0) + processed.get(1);
                    if (remainingAtHalf == null && completed >= RECORDS / 2) {
                        remainingAtHalf = new int[] {expectedRecords[0] - processed.get(0),
                                expectedRecords[1] - processed.get(1)};
                    }
                    Thread.sleep(2);
                }
                assertThat(processed.get(0)).isEqualTo(expectedRecords[0]);
                assertThat(processed.get(1)).isEqualTo(expectedRecords[1]);
                await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
                    long[] offsets = committedOffsets(admin, group, topic);
                    assertThat(offsets[0]).isEqualTo(expectedRecords[0]);
                    assertThat(offsets[1]).isEqualTo(expectedRecords[1]);
                });
                long[] offsets = committedOffsets(admin, group, topic);
                long sourceRows = count("transaction_inbox_event");
                long states = count("transaction_inbox_state");
                long candidates = count("transaction_session_candidate");
                long conflicts = count("transaction_inbox_conflict");
                long nonProvisionalStates = jdbc.sql("""
                        SELECT count(*) FROM transaction_inbox_state WHERE status <> 'PROVISIONAL'
                        """).query(Long.class).single();
                long invalidCandidates = jdbc.sql("""
                        SELECT count(*) FROM transaction_session_candidate
                        WHERE candidate_status <> 'PROVISIONAL' OR energy_wh <> 2500
                        """).query(Long.class).single();
                BigDecimal totalWh = jdbc.sql("SELECT sum(energy_wh) FROM transaction_session_candidate")
                        .query(BigDecimal.class).single();
                boolean correct = sourceRows == RECORDS && states == TRANSACTIONS
                        && candidates == TRANSACTIONS && conflicts == 0 && nonProvisionalStates == 0
                        && invalidCandidates == 0 && totalWh.compareTo(new BigDecimal("300000")) == 0;
                ObjectNode result = mapper.createObjectNode();
                result.put("ordinal", ordinal);
                result.put("mode", mode);
                result.put("hotKeyTransactions", mode.equals("SKEW") ? 96 : 15);
                result.put("stationsUsed", stationTransactions.size());
                result.putArray("partitionTransactions")
                        .add(expectedRecords[0] / 2).add(expectedRecords[1] / 2);
                result.putArray("partitionProducedRecords")
                        .add(expectedRecords[0]).add(expectedRecords[1]);
                result.putArray("partitionProcessedRecords")
                        .add(processed.get(0)).add(processed.get(1));
                result.putArray("partitionCommittedOffsets")
                        .add(offsets[0]).add(offsets[1]);
                if (remainingAtHalf == null) result.putNull("remainingAtHalfProcessed");
                else result.putArray("remainingAtHalfProcessed")
                        .add(remainingAtHalf[0]).add(remainingAtHalf[1]);
                result.putArray("partitionDrainMs")
                        .add((lastProcessedAt.get(0) - releasedAt) / 1_000_000.0)
                        .add((lastProcessedAt.get(1) - releasedAt) / 1_000_000.0);
                result.put("totalDrainMs", (Math.max(lastProcessedAt.get(0), lastProcessedAt.get(1))
                        - releasedAt) / 1_000_000.0);
                result.put("sourceRows", sourceRows);
                result.put("states", states);
                result.put("candidates", candidates);
                result.put("totalEnergyWh", totalWh.toPlainString());
                result.put("conflicts", conflicts);
                result.put("nonProvisionalStates", nonProvisionalStates);
                result.put("invalidCandidates", invalidCandidates);
                result.put("correct", correct);
                return result;
            } finally {
                release.countDown();
                container.stop();
            }
        }
    }

    private List<List<String>> stationKeysByPartition() {
        List<List<String>> keys = List.of(new ArrayList<>(), new ArrayList<>());
        for (int index = 0; keys.get(0).size() < 4 || keys.get(1).size() < 4; index++) {
            String key = "ST-SKEW-" + index;
            int partition = Utils.toPositive(Utils.murmur2(key.getBytes(StandardCharsets.UTF_8))) % 2;
            if (keys.get(partition).size() < 4) keys.get(partition).add(key);
        }
        return keys;
    }

    private Map<String, Integer> allocation(String mode, List<List<String>> keys) {
        var counts = new LinkedHashMap<String, Integer>();
        for (var partitionKeys : keys) {
            for (String station : partitionKeys) counts.put(station, mode.equals("BALANCED") ? 15 : 1);
        }
        if (mode.equals("SKEW")) {
            counts.put(keys.get(0).getFirst(), 96);
            for (int index = 0; index < 17; index++) {
                counts.merge(keys.get(1).get(index % 4), 1, Integer::sum);
            }
        }
        assertThat(counts.values().stream().mapToInt(Integer::intValue).sum()).isEqualTo(TRANSACTIONS);
        return counts;
    }

    private long[] committedOffsets(AdminClient admin, String group, String topic) throws Exception {
        Map<TopicPartition, OffsetAndMetadata> offsets = admin.listConsumerGroupOffsets(group)
                .partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS);
        long[] result = new long[2];
        for (int partition = 0; partition < 2; partition++) {
            OffsetAndMetadata value = offsets.get(new TopicPartition(topic, partition));
            result[partition] = value == null ? 0 : value.offset();
        }
        return result;
    }

    private long count(String table) {
        return jdbc.sql("SELECT count(*) FROM " + table).query(Long.class).single();
    }

    private String observed(String transactionId, String station, int seq) {
        String stage = seq == 0 ? "Started" : "Ended";
        String occurredAt = seq == 0 ? "2026-01-01T10:00:00Z" : "2026-01-01T10:20:00Z";
        ObjectNode root = mapper.createObjectNode();
        root.put("eventId", UUID.randomUUID().toString());
        root.put("eventType", "OcppTransactionEventObserved");
        root.put("schemaVersion", 1);
        root.put("occurredAt", occurredAt);
        root.put("receivedAt", Instant.now().toString());
        root.put("stationId", station);
        root.put("chargingStationId", "CS-" + station);
        ObjectNode payload = root.putObject("payload");
        payload.put("transactionId", transactionId);
        payload.put("seqNo", seq);
        payload.put("eventType", stage);
        payload.put("triggerReason", "TEST");
        payload.putObject("transactionInfo").put("transactionId", transactionId);
        if (seq == 0) payload.putObject("evse").put("id", 1);
        ObjectNode meter = payload.putArray("meterValue").addObject();
        meter.put("timestamp", occurredAt);
        ObjectNode sample = meter.putArray("sampledValue").addObject();
        sample.put("value", seq == 0 ? 10000 : 12500);
        sample.put("context", seq == 0 ? "Transaction.Begin" : "Transaction.End");
        sample.put("measurand", "Energy.Active.Import.Register");
        sample.putObject("unitOfMeasure").put("unit", "Wh").put("multiplier", 0);
        return mapper.writeValueAsString(root);
    }
}
