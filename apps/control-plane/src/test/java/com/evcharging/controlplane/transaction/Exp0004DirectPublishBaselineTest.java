package com.evcharging.controlplane.transaction;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
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
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** E4 baseline: an OCPP-shaped Kafka record becomes a provisional candidate before test-only closeout. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {"transaction.ingress.enabled=true", "spring.kafka.listener.auto-startup=false",
                "transaction.recovery.experimental-candidate-calculation-enabled=true"})
@EmbeddedKafka(partitions = 1, topics = "transaction-test-bootstrap",
        bootstrapServersProperty = "spring.kafka.bootstrap-servers",
        brokerProperties = "offsets.topic.num.partitions=1")
class Exp0004DirectPublishBaselineTest {
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
    @TempDir Path directory;

    @Test
    void normalDirectPublishWorksButCommitThenHaltLeavesNoKafkaRecord() throws Exception {
        String inputTopic = "exp0004-input-" + UUID.randomUUID();
        String outputTopic = "exp0004-closeout-" + UUID.randomUUID();
        try (var admin = AdminClient.create(Map.of("bootstrap.servers", broker.getBrokersAsString()));
             var input = new KafkaProducer<String, String>(Map.of(
                     ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString(),
                     ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                     ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                     ProducerConfig.ACKS_CONFIG, "all"))) {
            admin.createTopics(List.of(new NewTopic(inputTopic, 1, (short) 1),
                    new NewTopic(outputTopic, 1, (short) 1))).all().get(10, TimeUnit.SECONDS);
            jdbc.sql("""
                    CREATE TABLE exp0004_closeout (
                        charging_station_id TEXT NOT NULL,
                        transaction_id TEXT NOT NULL,
                        station_id TEXT NOT NULL,
                        energy_wh NUMERIC(24,6) NOT NULL,
                        event_id UUID NOT NULL UNIQUE,
                        PRIMARY KEY (charging_station_id, transaction_id)
                    )
                    """).update();
            send(input, inputTopic, "TX-EXP0004-NORMAL", 0, "Started", 10000);
            send(input, inputTopic, "TX-EXP0004-NORMAL", 1, "Ended", 12500);
            send(input, inputTopic, "TX-EXP0004-CRASH", 0, "Started", 10000);
            send(input, inputTopic, "TX-EXP0004-CRASH", 1, "Ended", 12500);
            send(input, inputTopic, "TX-EXP0004-MISMATCH", 0, "Started", 10000);
            send(input, inputTopic, "TX-EXP0004-MISMATCH", 1, "Ended", 12500);
            var container = factory.createContainer(inputTopic);
            container.getContainerProperties().setGroupId("exp0004-inbox-" + UUID.randomUUID());
            container.getContainerProperties().setPollTimeout(100);
            container.getContainerProperties().setMessageListener(
                    (MessageListener<String, String>) listener::consume);
            container.start();
            try {
                await().atMost(Duration.ofSeconds(25)).untilAsserted(() -> {
                    assertThat(candidate("TX-EXP0004-NORMAL")).isEqualTo("PROVISIONAL");
                    assertThat(candidate("TX-EXP0004-CRASH")).isEqualTo("PROVISIONAL");
                    assertThat(candidate("TX-EXP0004-MISMATCH")).isEqualTo("PROVISIONAL");
                });

                assertThat(runChild("normal", outputTopic, "TX-EXP0004-NORMAL")).isZero();
                assertThat(closeoutCount("TX-EXP0004-NORMAL")).isEqualTo(1);
                var normal = readOutput(outputTopic, 1);
                assertThat(normal).hasSize(1);
                assertThat(normal.getFirst()).isEqualTo("TX-EXP0004-NORMAL");

                assertThat(runChild("after-commit-before-send", outputTopic, "TX-EXP0004-CRASH"))
                        .isEqualTo(137);
                assertThat(closeoutCount("TX-EXP0004-CRASH")).isEqualTo(1);
                assertThat(Files.readString(directory.resolve("after-commit-before-send.log")))
                        .contains("COMMITTED_BEFORE_SEND");
                assertThat(readOutput(outputTopic, 1)).containsExactly("TX-EXP0004-NORMAL");

                assertThat(runChild("normal", outputTopic, "TX-EXP0004-MISMATCH", "2600"))
                        .isNotZero();
                assertThat(closeoutCount("TX-EXP0004-MISMATCH")).isZero();
                assertThat(readOutput(outputTopic, 1)).containsExactly("TX-EXP0004-NORMAL");
            } finally {
                container.stop();
            }
        }
    }

    private void send(KafkaProducer<String, String> producer, String topic, String transactionId,
                      int seq, String stage, int wh) throws Exception {
        String time = seq == 0 ? "2026-01-01T10:00:00Z" : "2026-01-01T10:20:00Z";
        ObjectNode record = mapper.createObjectNode();
        record.put("eventId", UUID.randomUUID().toString());
        record.put("eventType", "OcppTransactionEventObserved");
        record.put("schemaVersion", 1);
        record.put("occurredAt", time);
        record.put("receivedAt", Instant.now().toString());
        record.put("stationId", "ST-TEST");
        record.put("chargingStationId", "CS-TEST");
        ObjectNode payload = record.putObject("payload");
        payload.put("transactionId", transactionId);
        payload.put("seqNo", seq);
        payload.put("eventType", stage);
        payload.put("triggerReason", "TEST");
        payload.putObject("transactionInfo").put("transactionId", transactionId);
        if (seq == 0) payload.putObject("evse").put("id", 1);
        ObjectNode meter = payload.putArray("meterValue").addObject();
        meter.put("timestamp", time);
        ObjectNode sample = meter.putArray("sampledValue").addObject();
        sample.put("value", wh);
        sample.put("context", seq == 0 ? "Transaction.Begin" : "Transaction.End");
        sample.put("measurand", "Energy.Active.Import.Register");
        sample.putObject("unitOfMeasure").put("unit", "Wh").put("multiplier", 0);
        producer.send(new ProducerRecord<>(topic, "ST-TEST", mapper.writeValueAsString(record)))
                .get(10, TimeUnit.SECONDS);
    }

    private String candidate(String transactionId) {
        return jdbc.sql("""
                SELECT candidate_status FROM transaction_session_candidate
                WHERE charging_station_id = 'CS-TEST' AND transaction_id = :id
                """).param("id", transactionId).query(String.class).optional().orElse(null);
    }

    private long closeoutCount(String transactionId) {
        return jdbc.sql("""
                SELECT count(*) FROM exp0004_closeout
                WHERE charging_station_id = 'CS-TEST' AND transaction_id = :id
                """).param("id", transactionId).query(Long.class).single();
    }

    private int runChild(String mode, String outputTopic, String transactionId) throws Exception {
        return runChild(mode, outputTopic, transactionId, "2500");
    }

    private int runChild(String mode, String outputTopic, String transactionId, String expectedWh)
            throws Exception {
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        String classpath = System.getProperty("transaction.test.runtime-classpath");
        assertThat(classpath).isNotBlank();
        var process = new ProcessBuilder(java, "-cp", classpath,
                Exp0004DirectPublishProcess.class.getName(), mode,
                POSTGRES.getJdbcUrl("postgres", "postgres"), broker.getBrokersAsString(),
                outputTopic, transactionId, expectedWh)
                .redirectErrorStream(true)
                .redirectOutput(directory.resolve(mode + ".log").toFile())
                .start();
        if (!process.waitFor(30, TimeUnit.SECONDS)) {
            process.destroyForcibly();
            throw new IllegalStateException("direct publish child did not finish");
        }
        return process.exitValue();
    }

    private List<String> readOutput(String topic, int expected) {
        var partition = new TopicPartition(topic, 0);
        try (var consumer = new KafkaConsumer<String, String>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString(),
                ConsumerConfig.GROUP_ID_CONFIG, "exp0004-reader-" + UUID.randomUUID(),
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"))) {
            consumer.assign(List.of(partition));
            consumer.seekToBeginning(List.of(partition));
            var found = new ArrayList<String>();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
            while (System.nanoTime() < deadline) {
                consumer.poll(Duration.ofMillis(200)).forEach(record -> {
                    assertThat(record.key()).isEqualTo("ST-TEST");
                    try {
                        found.add(mapper.readTree(record.value()).path("payload").path("transactionId").asText());
                    } catch (Exception exception) {
                        throw new IllegalStateException(exception);
                    }
                });
                if (found.size() > expected) break;
            }
            return found;
        }
    }
}
