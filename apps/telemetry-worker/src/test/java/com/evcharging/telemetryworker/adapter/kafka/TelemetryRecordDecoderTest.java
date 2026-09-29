package com.evcharging.telemetryworker.adapter.kafka;

import com.evcharging.telemetryworker.PostgreSqlTestConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import tools.jackson.databind.ObjectMapper;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest
@Import(PostgreSqlTestConfiguration.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TelemetryRecordDecoderTest {
    @Autowired ObjectMapper mapper;
    @Autowired TelemetryRecordDecoder decoder;

    @Test
    void roundTripsWithTheRealAppMapperWithoutLosingDecimalOrTimePrecision() {
        var expected = TelemetryFixtures.first();
        var restored = decoder.decode(TelemetryFixtures.STATION, mapper.writeValueAsString(expected));
        assertThat(restored).isEqualTo(expected);
        assertThat(restored.payload().power()).isEqualByComparingTo("120.12345678901234567890123");
        assertThat(restored.occurredAt().toString()).isEqualTo("2026-09-29T00:00:00.123456789Z");
    }

    @Test
    void allowsTheOptionalRequestIdToBeOmitted() {
        var root = map();
        root.remove("requestId");
        assertThat(decoder.decode(TelemetryFixtures.STATION, mapper.writeValueAsString(root)).requestId()).isNull();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("missingNullAndWrongTypes")
    void rejectsRequiredFieldFailuresInsteadOfSupplyingPrimitiveDefaults(String name, String json) {
        assertThatThrownBy(() -> decoder.decode(TelemetryFixtures.STATION, json))
                .isInstanceOf(TelemetryConsumptionException.class)
                .hasFieldOrPropertyWithValue("stage", "contract").hasNoCause();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidContracts")
    void rejectsMalformedOrInconsistentContractsWithoutKeepingInputInTheException(String name, String key, String json) {
        assertThatThrownBy(() -> decoder.decode(key, json))
                .isInstanceOf(TelemetryConsumptionException.class)
                .hasNoCause().hasMessageNotContaining("PRIVATE-SAMPLE");
    }

    Stream<Arguments> missingNullAndWrongTypes() {
        String[] fields = {"eventId", "eventType", "schemaVersion", "occurredAt", "receivedAt", "stationId",
                "evseId", "correlationId", "payload", "payload.timestamp", "payload.charging",
                "payload.power", "payload.voltage", "payload.current"};
        return Stream.of(fields).flatMap(field -> Stream.of("missing", "null", "wrong-type").map(variant -> {
            var root = map();
            Map<String, Object> target = field.startsWith("payload.") ? payload(root) : root;
            String leaf = field.substring(field.lastIndexOf('.') + 1);
            switch (variant) {
                case "missing" -> target.remove(leaf);
                case "null" -> target.put(leaf, null);
                default -> {
                    Object value = target.get(leaf);
                    target.put(leaf, value instanceof Number || value instanceof Boolean ? value.toString() : 123);
                }
            }
            return Arguments.of(field + " " + variant, mapper.writeValueAsString(root));
        }));
    }

    Stream<Arguments> invalidContracts() {
        var badTime = map(); payload(badTime).put("timestamp", 1);
        var badType = map(); badType.put("eventType", "OtherEvent");
        var badVersion = map(); badVersion.put("schemaVersion", 2);
        var badUuid = map(); badUuid.put("eventId", "PRIVATE-SAMPLE");
        var badEvse = map(); badEvse.put("evseId", 0);
        var negative = map(); payload(negative).put("timestamp", -1);
        var bigInt = map(); bigInt.put("evseId", 2147483648L);
        var timeOverflow = map(); timeOverflow.put("occurredAt", "+1000000000-12-31T23:59:59.999999999Z");
        return Stream.of(
                Arguments.of("malformed", TelemetryFixtures.STATION, "{PRIVATE-SAMPLE"),
                Arguments.of("missing value", TelemetryFixtures.STATION, null),
                Arguments.of("not an object", TelemetryFixtures.STATION, "[]"),
                Arguments.of("trailing document", TelemetryFixtures.STATION, mapper.writeValueAsString(TelemetryFixtures.first()) + " {}"),
                Arguments.of("key mismatch", "PRIVATE-SAMPLE", mapper.writeValueAsString(TelemetryFixtures.first())),
                Arguments.of("null key", null, mapper.writeValueAsString(TelemetryFixtures.first())),
                Arguments.of("payload time", TelemetryFixtures.STATION, mapper.writeValueAsString(badTime)),
                Arguments.of("type", TelemetryFixtures.STATION, mapper.writeValueAsString(badType)),
                Arguments.of("version", TelemetryFixtures.STATION, mapper.writeValueAsString(badVersion)),
                Arguments.of("uuid", TelemetryFixtures.STATION, mapper.writeValueAsString(badUuid)),
                Arguments.of("evse", TelemetryFixtures.STATION, mapper.writeValueAsString(badEvse)),
                Arguments.of("negative time", TelemetryFixtures.STATION, mapper.writeValueAsString(negative)),
                Arguments.of("integer overflow", TelemetryFixtures.STATION, mapper.writeValueAsString(bigInt)),
                Arguments.of("time overflow", TelemetryFixtures.STATION, mapper.writeValueAsString(timeOverflow)));
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> map() {
        return mapper.readValue(mapper.writeValueAsString(TelemetryFixtures.first()), LinkedHashMap.class);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> payload(Map<String, Object> root) {
        return (Map<String, Object>) root.get("payload");
    }
}
