package com.evcharging.controlplane.transaction;

import com.sun.management.OperatingSystemMXBean;
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
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import static org.assertj.core.api.Assertions.assertThat;

/** Opt-in E5 exploration. Results describe this embedded one-partition testbed, not an operating SLA. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {"transaction.ingress.enabled=true", "spring.kafka.listener.auto-startup=false",
                "transaction.recovery.experimental-candidate-calculation-enabled=true"})
@EmbeddedKafka(partitions = 1, topics = "transaction-peak-bootstrap",
        bootstrapServersProperty = "spring.kafka.bootstrap-servers",
        brokerProperties = "offsets.topic.num.partitions=1")
@EnabledIfSystemProperty(named = "transaction.test.peak-enabled", matches = "true")
class TransactionInboxPeakBaselineTest {
    private static final int WARMUP_TRANSACTIONS = 60;
    private static final int WARMUP_RECORDS = WARMUP_TRANSACTIONS * 3;
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

    @Test
    void measureCurrentInboxAtFixedOfferedRate() throws Exception {
        int requestedRate = Integer.parseInt(System.getProperty("transaction.test.peak-rate", "100"));
        int seconds = Integer.parseInt(System.getProperty("transaction.test.peak-seconds", "20"));
        int partitions = Integer.parseInt(System.getProperty("transaction.test.peak-partitions", "1"));
        String ackMode = System.getProperty("transaction.test.peak-ack-mode", "RECORD");
        assertThat(requestedRate).isPositive();
        assertThat(seconds).isPositive();
        assertThat(partitions).isBetween(1, 2);
        assertThat(ackMode).isIn("RECORD", "BATCH");
        int transactions = Math.multiplyExact(requestedRate, seconds) / 3;
        int records = Math.multiplyExact(transactions, 3);
        assertThat(transactions).isPositive();

        // Generate every payload before the timed input phase so JSON creation is not part of the offered rate.
        List<ProducerRecord<String, String>> prepared = new ArrayList<>(WARMUP_RECORDS + records);
        for (int index = 0; index < WARMUP_RECORDS + records; index++) prepared.add(record(index));
        long[] sentAt = new long[records];
        long[] acknowledgedAt = new long[records];
        long[] listenerStartedAt = new long[records];
        long[] committedAt = new long[records];
        AtomicInteger acknowledged = new AtomicInteger();
        AtomicInteger acknowledgedEnds = new AtomicInteger();
        AtomicInteger processed = new AtomicInteger();
        AtomicInteger completed = new AtomicInteger();
        AtomicReference<Throwable> producerFailure = new AtomicReference<>();
        List<Sample> samples = new CopyOnWriteArrayList<>();

        String topic = "transaction-peak-" + UUID.randomUUID();
        String group = "transaction-peak-" + UUID.randomUUID();
        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", broker.getBrokersAsString()));
             KafkaProducer<String, String> producer = new KafkaProducer<>(Map.of(
                     ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString(),
                     ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                     ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                     ProducerConfig.ACKS_CONFIG, "all"))) {
            admin.createTopics(List.of(new NewTopic(topic, partitions, (short) 1))).all().get(10, TimeUnit.SECONDS);
            ConcurrentMessageListenerContainer<String, String> container = factory.createContainer(topic);
            container.setConcurrency(partitions);
            container.getContainerProperties().setGroupId(group);
            container.getContainerProperties().setPollTimeout(100);
            container.getContainerProperties().setAckMode(ContainerProperties.AckMode.valueOf(ackMode));
            container.getContainerProperties().setMessageListener((MessageListener<String, String>) record -> {
                int index = ByteBuffer.wrap(record.headers().lastHeader("peak-index").value()).getInt()
                        - WARMUP_RECORDS;
                if (index >= 0) listenerStartedAt[index] = System.nanoTime();
                listener.consume(record);
                if (index >= 0) {
                    committedAt[index] = System.nanoTime();
                    processed.incrementAndGet();
                    if (index % 3 == 2) completed.incrementAndGet();
                }
            });
            container.start();
            try {
                for (int index = 0; index < WARMUP_RECORDS; index++) {
                    ProducerRecord<String, String> source = prepared.get(index);
                    producer.send(withIndex(topic, source, index))
                            .get(10, TimeUnit.SECONDS);
                }
                waitUntil(() -> committedOffset(admin, group, topic, partitions) == WARMUP_RECORDS,
                        Duration.ofSeconds(60));
                assertThat(count("transaction_session_candidate")).isEqualTo(WARMUP_TRANSACTIONS);

                OperatingSystemMXBean os = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
                long cpuStart = os.getProcessCpuTime();
                long gcTimeStart = gcCollectionTimeMillis();
                long gcCountStart = gcCollectionCount();
                long inputStart = System.nanoTime();
                long[] inputEnd = new long[1];
                try (var sampler = Executors.newSingleThreadScheduledExecutor()) {
                    sampler.scheduleAtFixedRate(() -> {
                        try {
                            long offset = committedOffset(admin, group, topic, partitions) - WARMUP_RECORDS;
                            samples.add(new Sample((System.nanoTime() - inputStart) / 1_000_000,
                                    acknowledged.get(), processed.get(), completed.get(),
                                    Math.max(0, offset), acknowledgedEnds.get()));
                        } catch (Exception exception) {
                            producerFailure.compareAndSet(null, exception);
                        }
                    }, 0, 1, TimeUnit.SECONDS);

                    for (int index = 0; index < records; index++) {
                        long due = inputStart + (long) index * 1_000_000_000L / requestedRate;
                        while (System.nanoTime() < due) LockSupport.parkNanos(Math.min(due - System.nanoTime(), 1_000_000));
                        int measuredIndex = index;
                        ProducerRecord<String, String> source = prepared.get(WARMUP_RECORDS + index);
                        sentAt[index] = System.nanoTime();
                        producer.send(withIndex(topic, source, WARMUP_RECORDS + index), (metadata, error) -> {
                            if (error != null) producerFailure.compareAndSet(null, error);
                            else {
                                acknowledgedAt[measuredIndex] = System.nanoTime();
                                acknowledged.incrementAndGet();
                                if (measuredIndex % 3 == 2) acknowledgedEnds.incrementAndGet();
                            }
                        });
                    }
                    inputEnd[0] = System.nanoTime();
                    producer.flush();
                    long deadline = System.nanoTime() + Duration.ofSeconds(120).toNanos();
                    while (processed.get() < records && producerFailure.get() == null
                            && container.isRunning() && System.nanoTime() < deadline) Thread.sleep(100);
                    samples.add(new Sample((System.nanoTime() - inputStart) / 1_000_000,
                            acknowledged.get(), processed.get(), completed.get(),
                            Math.max(0, committedOffset(admin, group, topic, partitions) - WARMUP_RECORDS),
                            acknowledgedEnds.get()));
                }
                long cpuEnd = os.getProcessCpuTime();
                writeResult(requestedRate, seconds, partitions, ackMode,
                        transactions, records, inputStart, inputEnd[0],
                        sentAt, acknowledgedAt, listenerStartedAt, committedAt,
                        acknowledged.get(), processed.get(), completed.get(),
                        cpuEnd - cpuStart, gcCollectionTimeMillis() - gcTimeStart,
                        gcCollectionCount() - gcCountStart, samples, producerFailure.get(), topic, group);

                assertThat(producerFailure.get()).isNull();
                assertThat(acknowledged.get()).isEqualTo(records);
                assertThat(processed.get()).isEqualTo(records);
                assertThat(completed.get()).isEqualTo(transactions);
                assertThat(committedOffset(admin, group, topic, partitions)).isEqualTo(WARMUP_RECORDS + records);
                assertThat(count("transaction_inbox_event")).isEqualTo(WARMUP_RECORDS + records);
                assertThat(count("transaction_inbox_state")).isEqualTo(WARMUP_TRANSACTIONS + transactions);
                assertThat(count("transaction_session_candidate")).isEqualTo(WARMUP_TRANSACTIONS + transactions);
                assertThat(count("transaction_inbox_conflict")).isZero();
                assertThat(jdbc.sql("SELECT count(*) FROM transaction_inbox_state WHERE status <> 'PROVISIONAL'")
                        .query(Long.class).single()).isZero();
                assertThat(jdbc.sql("SELECT count(*) FROM transaction_session_candidate "
                        + "WHERE candidate_status <> 'PROVISIONAL' OR energy_wh <> 2500")
                        .query(Long.class).single()).isZero();
            } finally {
                container.stop();
            }
        }
    }

    private ProducerRecord<String, String> record(int index) {
        int transaction = index / 3;
        int seq = index % 3;
        String station = "ST-" + transaction % 50;
        String chargingStation = "CS-" + transaction % 50;
        String transactionId = "PEAK-" + transaction;
        String stage = seq == 0 ? "Started" : seq == 1 ? "Updated" : "Ended";
        String time = seq == 0 ? "2026-01-01T10:00:00Z" : seq == 1
                ? "2026-01-01T10:10:00Z" : "2026-01-01T10:20:00Z";
        ObjectNode root = mapper.createObjectNode();
        root.put("eventId", UUID.nameUUIDFromBytes(("peak-" + index).getBytes(StandardCharsets.UTF_8)).toString());
        root.put("eventType", "OcppTransactionEventObserved");
        root.put("schemaVersion", 1);
        root.put("occurredAt", time);
        root.put("receivedAt", time);
        root.put("stationId", station);
        root.put("chargingStationId", chargingStation);
        ObjectNode payload = root.putObject("payload");
        payload.put("transactionId", transactionId);
        payload.put("seqNo", seq);
        payload.put("eventType", stage);
        payload.put("triggerReason", "TEST");
        payload.putObject("transactionInfo").put("transactionId", transactionId);
        if (seq == 0) payload.putObject("evse").put("id", 1);
        var meters = payload.putArray("meterValue");
        if (seq == 0) meter(meters.addObject(), time, "Transaction.Begin", 10000);
        else if (seq == 1) meter(meters.addObject(), time, "Sample.Periodic", 11500);
        else {
            meter(meters.addObject(), "2026-01-01T10:00:00Z", "Transaction.Begin", 10000);
            meter(meters.addObject(), time, "Sample.Periodic", 12000);
            meter(meters.addObject(), time, "Transaction.End", 12500);
            meter(meters.addObject(), time, "Transaction.End", 12500);
        }
        return new ProducerRecord<>("prepared", station, mapper.writeValueAsString(root));
    }

    private void meter(ObjectNode meter, String time, String context, int wh) {
        meter.put("timestamp", time);
        ObjectNode sample = meter.putArray("sampledValue").addObject();
        sample.put("value", wh);
        sample.put("context", context);
        sample.put("measurand", "Energy.Active.Import.Register");
        sample.put("location", "Outlet");
        sample.put("format", "Raw");
        sample.putObject("unitOfMeasure").put("unit", "Wh").put("multiplier", 0);
    }

    private ProducerRecord<String, String> withIndex(String topic, ProducerRecord<String, String> source, int index) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, source.key(), source.value());
        record.headers().add("peak-index", ByteBuffer.allocate(Integer.BYTES).putInt(index).array());
        return record;
    }

    private long committedOffset(AdminClient admin, String group, String topic, int partitions) throws Exception {
        try {
            var offsets = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata()
                    .get(10, TimeUnit.SECONDS);
            long total = 0;
            for (int partition = 0; partition < partitions; partition++) {
                var value = offsets.get(new TopicPartition(topic, partition));
                if (value != null) total += value.offset();
            }
            return total;
        } catch (ExecutionException exception) {
            if (exception.getCause() instanceof GroupIdNotFoundException) return 0;
            throw exception;
        }
    }

    private void waitUntil(CheckedCondition condition, Duration timeout) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.ok() && System.nanoTime() < deadline) Thread.sleep(100);
        assertThat(condition.ok()).isTrue();
    }

    private long count(String table) {
        return jdbc.sql("SELECT count(*) FROM " + table).query(Long.class).single();
    }

    private long gcCollectionTimeMillis() {
        return ManagementFactory.getGarbageCollectorMXBeans().stream()
                .mapToLong(bean -> Math.max(0, bean.getCollectionTime())).sum();
    }

    private long gcCollectionCount() {
        return ManagementFactory.getGarbageCollectorMXBeans().stream()
                .mapToLong(bean -> Math.max(0, bean.getCollectionCount())).sum();
    }

    private void writeResult(int rate, int seconds, int partitions, String ackMode,
                             int transactions, int records,
                             long start, long inputEnd,
                             long[] sentAt, long[] acknowledgedAt, long[] listenerStartedAt, long[] committedAt,
                             int acknowledged, int processed, int completed, long cpuNanos, long gcMillis,
                             long gcCount, List<Sample> samples, Throwable failure, String topic, String group)
            throws IOException {
        long[] latencies = new long[completed];
        long[] dispatchLatencies = new long[completed];
        long[] producerAckLatencies = new long[completed];
        long[] ackToListenerLatencies = new long[completed];
        long[] listenerDurations = new long[completed];
        int cursor = 0;
        for (int index = 2; index < records; index += 3) {
            if (committedAt[index] > 0 && listenerStartedAt[index] > 0
                    && acknowledgedAt[index] > 0 && sentAt[index] > 0) {
                latencies[cursor] = committedAt[index] - sentAt[index];
                dispatchLatencies[cursor] = listenerStartedAt[index] - sentAt[index];
                producerAckLatencies[cursor] = acknowledgedAt[index] - sentAt[index];
                ackToListenerLatencies[cursor] = listenerStartedAt[index] - acknowledgedAt[index];
                listenerDurations[cursor] = committedAt[index] - listenerStartedAt[index];
                cursor++;
            }
        }
        latencies = Arrays.copyOf(latencies, cursor);
        dispatchLatencies = Arrays.copyOf(dispatchLatencies, cursor);
        producerAckLatencies = Arrays.copyOf(producerAckLatencies, cursor);
        ackToListenerLatencies = Arrays.copyOf(ackToListenerLatencies, cursor);
        listenerDurations = Arrays.copyOf(listenerDurations, cursor);
        Arrays.sort(latencies);
        Arrays.sort(dispatchLatencies);
        Arrays.sort(producerAckLatencies);
        Arrays.sort(ackToListenerLatencies);
        Arrays.sort(listenerDurations);
        long lastCommit = Arrays.stream(committedAt).max().orElse(0);
        double offeredRate = records * 1_000_000_000.0 / Math.max(1, inputEnd - start + 1_000_000_000L / rate);
        ObjectNode result = mapper.createObjectNode();
        result.put("evidenceLevel", "E0 exploratory measurement; repetitions and external validation pending");
        result.put("inputModel", "50 station keys, three ordered records per transaction");
        result.put("kafkaPartitionsAndConsumers", partitions);
        result.put("consumerAckMode", ackMode);
        result.put("timeDefinitions", "offered: send invocation interval; processed: listener return after DB commit; completed: Ended listener return; latency: Ended send invocation to Ended DB commit; Kafka lag: producer acknowledgements minus group committed offset");
        result.put("requestedRecordsPerSecond", rate);
        result.put("durationSeconds", seconds);
        result.put("warmupTransactions", WARMUP_TRANSACTIONS);
        result.put("transactions", transactions);
        result.put("records", records);
        result.put("offeredRecordsPerSecond", offeredRate);
        result.put("producerReached95PercentTarget", offeredRate >= rate * 0.95);
        result.put("acknowledgedRecords", acknowledged);
        result.put("processedRecords", processed);
        result.put("completedCandidates", completed);
        result.put("completionLatencyP50Ms", percentileMs(latencies, .50));
        result.put("completionLatencyP95Ms", percentileMs(latencies, .95));
        result.put("completionLatencyP99Ms", percentileMs(latencies, .99));
        result.put("dispatchLatencyP95Ms", percentileMs(dispatchLatencies, .95));
        result.put("dispatchLatencyP99Ms", percentileMs(dispatchLatencies, .99));
        result.put("producerAckLatencyP95Ms", percentileMs(producerAckLatencies, .95));
        result.put("producerAckLatencyP99Ms", percentileMs(producerAckLatencies, .99));
        result.put("ackToListenerLatencyP95Ms", percentileMs(ackToListenerLatencies, .95));
        result.put("ackToListenerLatencyP99Ms", percentileMs(ackToListenerLatencies, .99));
        result.put("listenerDurationP95Ms", percentileMs(listenerDurations, .95));
        result.put("listenerDurationP99Ms", percentileMs(listenerDurations, .99));
        result.put("drainAfterLastSendMs", Math.max(0, (lastCommit - inputEnd) / 1_000_000.0));
        result.put("jvmProcessCpuSeconds", cpuNanos / 1_000_000_000.0);
        result.put("jvmGcCollectionTimeMs", gcMillis);
        result.put("jvmGcCollectionCount", gcCount);
        result.put("jvmHeapUsedBytes", ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed());
        result.put("inboxEventTableBytes", jdbc.sql("SELECT pg_total_relation_size('transaction_inbox_event')")
                .query(Long.class).single());
        result.put("candidateTableBytes", jdbc.sql("SELECT pg_total_relation_size('transaction_session_candidate')")
                .query(Long.class).single());
        result.put("producerOrSamplerError", failure == null ? null : failure.getClass().getSimpleName());
        result.put("topic", topic);
        result.put("group", group);
        var timeline = result.putArray("samples");
        for (Sample sample : samples) {
            ObjectNode row = timeline.addObject();
            row.put("elapsedMs", sample.elapsedMs());
            row.put("producerAcknowledged", sample.acknowledged());
            row.put("dbProcessed", sample.processed());
            row.put("candidateCompleted", sample.completed());
            row.put("kafkaCommitted", sample.committed());
            row.put("kafkaLag", Math.max(0, sample.acknowledged() - sample.committed()));
            row.put("businessBacklog", Math.max(0, sample.acknowledgedEnds() - sample.completed()));
        }
        Path output = Path.of(System.getProperty("transaction.test.peak-output"));
        Files.createDirectories(output.getParent());
        Files.writeString(output, mapper.writerWithDefaultPrettyPrinter().writeValueAsString(result));
    }

    private double percentileMs(long[] sorted, double percentile) {
        if (sorted.length == 0) return -1;
        return sorted[Math.max(0, (int) Math.ceil(sorted.length * percentile) - 1)] / 1_000_000.0;
    }

    private record Sample(long elapsedMs, int acknowledged, int processed, int completed,
                          long committed, int acknowledgedEnds) {}

    @FunctionalInterface
    private interface CheckedCondition { boolean ok() throws Exception; }
}
