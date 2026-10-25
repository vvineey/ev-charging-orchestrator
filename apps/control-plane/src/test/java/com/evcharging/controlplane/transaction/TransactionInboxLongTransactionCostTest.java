package com.evcharging.controlplane.transaction;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Opt-in cost observation through the real decoder, transactional service and PostgreSQL Inbox. */
@EnabledIfEnvironmentVariable(named = "EVC_INBOX_LONG_TRANSACTION_TEST", matches = "true")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {"transaction.ingress.enabled=true", "spring.kafka.listener.auto-startup=false",
                "transaction.recovery.experimental-candidate-calculation-enabled=true"})
class TransactionInboxLongTransactionCostTest {
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

    @Autowired TransactionRecordListener listener;
    @Autowired JdbcClient jdbc;
    @Autowired ObjectMapper mapper;

    @Test
    void repeatedWholeTransactionReadsKeepTheIndependentResultCorrect() throws Exception {
        runWorkload(20, 1, false, null);
        String runId = UUID.randomUUID().toString();
        Path resultFile = Path.of("build", "experiment-results",
                "inbox-long-transaction-" + runId + ".json");
        ObjectNode report = mapper.createObjectNode();
        report.put("runId", runId);
        report.put("javaVersion", System.getProperty("java.version"));
        report.put("scope", "test-only synchronous listener call, real PostgreSQL Inbox, no Kafka delivery");
        report.put("warmupRecords", 20);
        report.put("expectedRowsDefinition", "N(N+1)/2 from events() on each unique insertion; code-derived, not physical DB I/O");
        report.put("trialOrder", "30,120,480,480,120,30,30,120,480");
        report.put("matchedOrder", "1x480,16x30,16x30,1x480,1x480,16x30; all have 480 records");
        var trials = report.putArray("trials");
        int[] lengths = {30, 120, 480, 480, 120, 30, 30, 120, 480};
        for (int index = 0; index < lengths.length; index++) {
            ObjectNode trial = runWorkload(lengths[index], 1, true, index + 1);
            trials.add(trial);
            Files.createDirectories(resultFile.getParent());
            Files.writeString(resultFile, mapper.writerWithDefaultPrettyPrinter().writeValueAsString(report));
            assertThat(trial.path("correct").asBoolean()).as("trial %s", index + 1).isTrue();
        }
        var matched = report.putArray("matchedWorkloads");
        int[] recordsPerTransaction = {480, 30, 30, 480, 480, 30};
        for (int index = 0; index < recordsPerTransaction.length; index++) {
            int perTransaction = recordsPerTransaction[index];
            ObjectNode trial = runWorkload(perTransaction, 480 / perTransaction, true, index + 1);
            matched.add(trial);
            Files.writeString(resultFile, mapper.writerWithDefaultPrettyPrinter().writeValueAsString(report));
            assertThat(trial.path("correct").asBoolean()).as("matched workload %s", index + 1).isTrue();
        }
    }

    private ObjectNode runWorkload(int recordsPerTransaction, int transactionCount,
                                   boolean measured, Integer ordinal) {
        jdbc.sql("TRUNCATE transaction_inbox_state CASCADE").update();
        int totalRecords = recordsPerTransaction * transactionCount;
        List<ConsumerRecord<String, String>> input = new ArrayList<>(totalRecords);
        for (int transaction = 0; transaction < transactionCount; transaction++) {
            String transactionId = "TX-LONG-" + UUID.randomUUID();
            for (int seq = 0; seq < recordsPerTransaction; seq++) {
                input.add(new ConsumerRecord<>("transaction-long-cost", 0, input.size(), "ST-TEST",
                        observed(transactionId, seq, recordsPerTransaction)));
            }
        }
        long[] durationNs = new long[totalRecords];
        long totalStart = System.nanoTime();
        for (int seq = 0; seq < totalRecords; seq++) {
            long start = System.nanoTime();
            listener.consume(input.get(seq));
            durationNs[seq] = System.nanoTime() - start;
        }
        long totalNs = System.nanoTime() - totalStart;

        long sourceRows = jdbc.sql("""
                SELECT count(*) FROM transaction_inbox_event
                """).query(Long.class).single();
        long conflicts = jdbc.sql("""
                SELECT count(*) FROM transaction_inbox_conflict
                """).query(Long.class).single();
        long candidates = jdbc.sql("""
                SELECT count(*) FROM transaction_session_candidate
                """).query(Long.class).single();
        BigDecimal energyWh = jdbc.sql("""
                SELECT sum(energy_wh) FROM transaction_session_candidate
                """).query(BigDecimal.class).optional().orElse(null);
        long provisionalCandidates = jdbc.sql("""
                SELECT count(*) FROM transaction_session_candidate WHERE candidate_status = 'PROVISIONAL'
                """).query(Long.class).single();
        long provisionalInboxStates = jdbc.sql("""
                SELECT count(*) FROM transaction_inbox_state WHERE status = 'PROVISIONAL'
                """).query(Long.class).single();
        boolean correct = sourceRows == totalRecords && conflicts == 0 && candidates == transactionCount
                && energyWh != null
                && energyWh.compareTo(new BigDecimal("2500").multiply(BigDecimal.valueOf(transactionCount))) == 0
                && provisionalCandidates == transactionCount && provisionalInboxStates == transactionCount;
        if (!measured) {
            assertThat(correct).as("warm-up correctness").isTrue();
            return null;
        }
        int quarter = Math.max(1, (recordsPerTransaction - 2) / 4);
        ObjectNode result = mapper.createObjectNode();
        result.put("ordinal", ordinal);
        result.put("transactions", transactionCount);
        result.put("recordsPerTransaction", recordsPerTransaction);
        result.put("totalRecords", totalRecords);
        result.put("expectedLogicalRowsRead", (long) transactionCount * recordsPerTransaction
                * (recordsPerTransaction + 1) / 2);
        result.put("sourceRows", sourceRows);
        result.put("conflicts", conflicts);
        result.put("candidates", candidates);
        result.put("energyWhTotal", energyWh == null ? null : energyWh.toPlainString());
        result.put("provisionalCandidates", provisionalCandidates);
        result.put("provisionalInboxStates", provisionalInboxStates);
        result.put("correct", correct);
        result.put("totalListenerMs", millis(totalNs));
        result.put("eventP50Ms", millis(percentile(durationNs, 0.50)));
        result.put("eventP95Ms", millis(percentile(durationNs, 0.95)));
        if (transactionCount == 1) {
            result.put("earlyUpdatedMeanMs", millis(mean(durationNs, 1, 1 + quarter)));
            result.put("lateUpdatedMeanMs", millis(mean(durationNs, recordsPerTransaction - 1 - quarter,
                    recordsPerTransaction - 1)));
        } else {
            result.putNull("earlyUpdatedMeanMs");
            result.putNull("lateUpdatedMeanMs");
        }
        return result;
    }

    private String observed(String transactionId, int seq, int records) {
        String stage = seq == 0 ? "Started" : seq == records - 1 ? "Ended" : "Updated";
        String occurredAt = seq == 0 ? "2026-01-01T10:00:00Z"
                : seq == records - 1 ? "2026-01-01T10:20:00Z" : "2026-01-01T10:10:00Z";
        int wh = seq == 0 ? 10000 : seq == records - 1 ? 12500 : 11500;
        ObjectNode root = mapper.createObjectNode();
        root.put("eventId", UUID.randomUUID().toString());
        root.put("eventType", "OcppTransactionEventObserved");
        root.put("schemaVersion", 1);
        root.put("occurredAt", occurredAt);
        root.put("receivedAt", Instant.now().toString());
        root.put("stationId", "ST-TEST");
        root.put("chargingStationId", "CS-TEST");
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
        sample.put("value", wh);
        sample.put("context", seq == 0 ? "Transaction.Begin"
                : seq == records - 1 ? "Transaction.End" : "Sample.Periodic");
        sample.put("measurand", "Energy.Active.Import.Register");
        sample.putObject("unitOfMeasure").put("unit", "Wh").put("multiplier", 0);
        return mapper.writeValueAsString(root);
    }

    private long percentile(long[] values, double percentile) {
        long[] sorted = values.clone();
        Arrays.sort(sorted);
        return sorted[(int) Math.ceil(percentile * sorted.length) - 1];
    }

    private long mean(long[] values, int from, int to) {
        long total = 0;
        for (int index = from; index < to; index++) total += values[index];
        return total / (to - from);
    }

    private double millis(long nanoseconds) {
        return nanoseconds / 1_000_000.0;
    }
}
