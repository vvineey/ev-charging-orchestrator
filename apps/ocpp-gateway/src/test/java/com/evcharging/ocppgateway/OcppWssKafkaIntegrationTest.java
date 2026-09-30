package com.evcharging.ocppgateway;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assumptions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.env.Environment;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import tools.jackson.databind.node.ObjectNode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "server.address=127.0.0.1", "server.ssl.enabled=true", "ocpp.ingress.enabled=true",
        "ocpp.station.charging-station-id=CS-KEBA-OCPP2-00001", "ocpp.station.station-id=ST-TEST",
        "ocpp.station.allowed-evse-ids=1,2", "ocpp.station.password=test-credential-only",
        "ocpp.kafka-topic=charging.transaction.observed.v1",
        "spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer",
        "spring.kafka.producer.value-serializer=org.apache.kafka.common.serialization.StringSerializer",
        "spring.kafka.producer.acks=all", "spring.kafka.producer.properties[enable.idempotence]=true"
})
@EmbeddedKafka(partitions = 1, topics = "charging.transaction.observed.v1",
        bootstrapServersProperty = "spring.kafka.bootstrap-servers")
class OcppWssKafkaIntegrationTest {
    private static final TestTlsMaterial TLS = new TestTlsMaterial();

    @Autowired Environment environment;
    @Autowired EmbeddedKafkaBroker broker;
    @Autowired ObjectMapper json;

    @DynamicPropertySource
    static void ssl(DynamicPropertyRegistry registry) {
        registry.add("server.ssl.key-store", () -> TLS.keyStore.toUri().toString());
        registry.add("server.ssl.key-store-password", () -> TLS.password);
    }

    @AfterAll
    static void cleanup() throws Exception {
        TLS.close();
        assertThat(TLS.directory).doesNotExist();
    }

    @Test
    void authenticatedWssTransactionIsAcknowledgedAfterKafkaRecordAndBadCredentialIsRejected() throws Exception {
        HttpClient client = HttpClient.newBuilder().sslContext(TLS.trustedContext).build();
        try (KafkaConsumer<String, String> consumer = consumer()) {
            consumer.subscribe(List.of("charging.transaction.observed.v1"));
            assertThatThrownBy(() -> connect(client, "wrong-password").join())
                    .isInstanceOf(CompletionException.class);
            assertThatThrownBy(() -> connect(client, "test-credential-only", new CompletableFuture<>(),
                    "localhost", "/ocpp/unregistered").join()).isInstanceOf(CompletionException.class);
            assertThatThrownBy(() -> connect(client, "test-credential-only", new CompletableFuture<>(),
                    "127.0.0.1", "/ocpp/CS-KEBA-OCPP2-00001").join())
                    .isInstanceOf(CompletionException.class);
            HttpClient untrusted = HttpClient.newHttpClient();
            try {
                assertThatThrownBy(() -> connect(untrusted, "test-credential-only").join())
                        .isInstanceOf(CompletionException.class);
            } finally {
                untrusted.shutdownNow();
            }

            var response = new CompletableFuture<String>();
            WebSocket socket = connect(client, "test-credential-only", response).get(5, TimeUnit.SECONDS);
            assertThat(socket.getSubprotocol()).isEqualTo("ocpp2.0.1");
            socket.sendText("""
                    [2,"call-1","TransactionEvent",{"eventType":"Ended",
                    "timestamp":"2026-09-30T00:00:00Z","triggerReason":"EVDisconnected","seqNo":2,
                    "transactionInfo":{"transactionId":"TX-WSS"},
                    "meterValue":[{"timestamp":"2026-09-30T00:00:00Z","sampledValue":[
                    {"value":2500,"measurand":"Energy.Active.Import.Register"},
                    {"value":2.5,"measurand":"Energy.Active.Import.Register","unitOfMeasure":{"unit":"kWh"}}]}]}]
                    """, true).get(5, TimeUnit.SECONDS);
            var reply = json.readTree(response.get(15, TimeUnit.SECONDS));
            assertThat(reply.get(0).intValue()).isEqualTo(3);
            assertThat(reply.get(1).textValue()).isEqualTo("call-1");

            var matching = new ArrayList<ConsumerRecord<String, String>>();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (matching.isEmpty() && System.nanoTime() < deadline) {
                for (var record : consumer.poll(Duration.ofSeconds(2))) {
                    if ("TX-WSS".equals(json.readTree(record.value()).path("payload")
                            .path("transactionId").textValue())) {
                        matching.add(record);
                    }
                }
            }
            assertThat(matching).hasSize(1);
            var kafkaRecord = matching.getFirst();
            assertThat(kafkaRecord.key()).isEqualTo("ST-TEST");
            var event = json.readTree(kafkaRecord.value());
            assertThat(event.path("payload").path("transactionId").textValue()).isEqualTo("TX-WSS");
            assertThat(event.path("payload").path("seqNo").intValue()).isEqualTo(2);
            assertThat(event.path("payload").has("evse")).isFalse();
            assertThat(event.path("payload").path("meterValue").get(0)
                    .path("sampledValue").size()).isEqualTo(2);
            socket.sendClose(WebSocket.NORMAL_CLOSURE, "done").get(5, TimeUnit.SECONDS);
            socket.abort();
        } finally {
            client.shutdownNow();
        }
    }

    @Test
    void pinnedSapSimulatorCanSendACompleteTransactionThroughWssIntoKafka() throws Exception {
        String directory = System.getenv("EVC_SAP_SIMULATOR_DIR");
        Assumptions.assumeTrue(directory != null && Files.isDirectory(Path.of(directory)),
                "Pinned SAP simulator is an optional, separately obtained E3 input");
        Path simulator = Path.of(directory);
        Path rootConfig = simulator.resolve("dist/assets/config.json");
        Path template = simulator.resolve("dist/assets/station-templates/boa-keba.station-template.json");
        Path savedStation = simulator.resolve("dist/assets/configurations/"
                + "e9041c294a82a2d6aa194a801c3ba39d6b24d1cb16c0a0b3db4e37c9fe4e80cbb0808843f66a320b3f58df0288c8e98a.json");
        Assumptions.assumeTrue(Files.isRegularFile(rootConfig) && Files.isRegularFile(template)
                && Files.isRegularFile(savedStation), "Expected pinned simulator configuration is absent");
        Map<Path, byte[]> originals = new HashMap<>();
        Path simulatorLog = simulator.resolve("ocpp-gateway-local-e3.log");
        Process process = null;
        try (KafkaConsumer<String, String> consumer = consumer()) {
            String url = "wss://localhost:" + environment.getRequiredProperty("local.server.port") + "/ocpp";
            for (Path path : List.of(rootConfig, template, savedStation)) {
                originals.put(path, Files.readAllBytes(path));
                ObjectNode config = (ObjectNode) json.readTree(originals.get(path));
                ObjectNode station = path.equals(savedStation)
                        ? (ObjectNode) config.path("stationInfo") : config;
                station.putArray("supervisionUrls").add(url);
                if (!path.equals(rootConfig)) {
                    station.put("supervisionUrlOcppConfiguration", false);
                    station.put("supervisionUser", "CS-KEBA-OCPP2-00001");
                    station.put("supervisionPassword", "test-credential-only");
                }
                Files.writeString(path, json.writeValueAsString(config));
            }
            consumer.subscribe(List.of("charging.transaction.observed.v1"));
            var launch = new ProcessBuilder("node", "dist/start.js").directory(simulator.toFile())
                    .redirectErrorStream(true)
                    .redirectOutput(simulatorLog.toFile());
            launch.environment().put("NODE_EXTRA_CA_CERTS", TLS.certificate.toString());
            launch.environment().put("NODE_ENV", "production");
            process = launch.start();

            var observed = new ArrayList<String>();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(70);
            while (System.nanoTime() < deadline && observed.size() < 3) {
                var records = consumer.poll(Duration.ofSeconds(2));
                for (var record : records) {
                    var event = json.readTree(record.value());
                    if (!"TX-WSS".equals(event.path("payload").path("transactionId").textValue())) {
                        assertThat(record.key()).isEqualTo("ST-TEST");
                        assertThat(event.path("chargingStationId").textValue())
                                .isEqualTo("CS-KEBA-OCPP2-00001");
                        var payload = event.path("payload");
                        assertThat(record.value()).doesNotContain("idToken");
                        switch (payload.path("eventType").textValue()) {
                            case "Started" -> {
                                assertThat(payload.path("evse").path("id").intValue()).isEqualTo(1);
                                assertThat(payload.path("meterValue").size()).isEqualTo(1);
                            }
                            case "Updated" -> {
                                assertThat(payload.has("evse")).isFalse();
                                assertThat(payload.path("meterValue").size()).isEqualTo(1);
                            }
                            case "Ended" -> {
                                assertThat(payload.has("evse")).isFalse();
                                assertThat(payload.path("meterValue").size()).isEqualTo(3);
                                assertThat(payload.path("meterValue").get(0).path("sampledValue").size())
                                        .isGreaterThan(1);
                            }
                            default -> throw new AssertionError("unexpected transaction event type");
                        }
                        observed.add(payload.path("eventType").textValue() + ":"
                                + payload.path("seqNo").intValue());
                    }
                }
                if (!process.isAlive() && observed.isEmpty()) {
                    break;
                }
            }
            assertThat(observed).as("SAP simulator WSS capture; temporary log at " + simulatorLog)
                    .contains("Started:0", "Updated:1", "Ended:2");
        } finally {
            if (process != null) {
                process.destroyForcibly();
                process.waitFor(10, TimeUnit.SECONDS);
            }
            for (var entry : originals.entrySet()) {
                Files.write(entry.getKey(), entry.getValue());
            }
        }
    }

    private CompletableFuture<WebSocket> connect(HttpClient client, String password) {
        return connect(client, password, new CompletableFuture<>());
    }

    private CompletableFuture<WebSocket> connect(HttpClient client, String password,
            CompletableFuture<String> response) {
        return connect(client, password, response, "localhost", "/ocpp/CS-KEBA-OCPP2-00001");
    }

    private CompletableFuture<WebSocket> connect(HttpClient client, String password,
            CompletableFuture<String> response, String host, String path) {
        String credentials = Base64.getEncoder().encodeToString(
                ("CS-KEBA-OCPP2-00001:" + password).getBytes(StandardCharsets.UTF_8));
        return client.newWebSocketBuilder().subprotocols("ocpp2.0.1")
                .header("Authorization", "Basic " + credentials)
                .connectTimeout(Duration.ofSeconds(5))
                .buildAsync(URI.create("wss://" + host + ":" + environment.getRequiredProperty("local.server.port")
                        + path), new WebSocket.Listener() {
                    @Override
                    public java.util.concurrent.CompletionStage<?> onText(WebSocket webSocket,
                            CharSequence data, boolean last) {
                        response.complete(data.toString());
                        webSocket.request(1);
                        return null;
                    }
                });
    }

    private KafkaConsumer<String, String> consumer() {
        return new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString(),
                ConsumerConfig.GROUP_ID_CONFIG, "ocpp-ingress-test-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class));
    }
}
