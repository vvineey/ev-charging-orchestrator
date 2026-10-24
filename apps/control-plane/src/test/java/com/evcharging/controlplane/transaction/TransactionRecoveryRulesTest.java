package com.evcharging.controlplane.transaction;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class TransactionRecoveryRulesTest {
    private final ObjectMapper mapper = new ObjectMapper();
    private final TransactionRecoveryRules rules = new TransactionRecoveryRules();

    @Test
    void reconstructsCompleteAndShortTransactionsAfterReordering() {
        var start = event(0, "Started", 10000, true, false);
        var update = event(1, "Updated", 11500, false, false);
        var end = event(2, "Ended", 12500, false, false);
        var full = rules.evaluate(List.of(end, start, update));
        assertThat(full.status()).isEqualTo("CALCULATED");
        assertThat(full.energyWh()).isEqualByComparingTo("2500");
        assertThat(full.evseId()).isEqualTo(1);
        assertThat(full.startedAt()).isEqualTo(Instant.parse("2026-01-01T10:00:00Z"));
        assertThat(full.endedAt()).isEqualTo(Instant.parse("2026-01-01T10:20:00Z"));

        var shortEnd = event(1, "Ended", 12500, false, false);
        assertThat(rules.evaluate(List.of(shortEnd, start)).energyWh()).isEqualByComparingTo("2500");
    }

    @Test
    void holdsMissingSequenceAndInvalidOrAmbiguousMeter() {
        var start = event(0, "Started", 10000, true, false);
        var end = event(2, "Ended", 12500, false, false);
        assertThat(rules.evaluate(List.of(start, end)).reason()).isEqualTo("MISSING_SEQUENCE");
        assertThat(rules.evaluate(List.of(start, event(1, "Ended", 9500, false, false))).reason())
                .isEqualTo("INVALID_OR_AMBIGUOUS_METER");
        assertThat(rules.evaluate(List.of(start, event(1, "Ended", 12500, false, true))).energyWh())
                .isEqualByComparingTo("2500");
        assertThat(rules.evaluate(List.of(event(0, "Started", null, true, false),
                event(1, "Ended", 12500, false, false))).reason())
                .isEqualTo("INVALID_OR_AMBIGUOUS_METER");
    }

    @Test
    void selectsMatchingBoundaryValuesAcrossMixedMeterGroups() {
        var start = event(0, "Started", 0, true, false);
        var update = event(1, "Updated", 141, false, false);
        var end = event(2, "Ended", null, false, false);
        var meters = ((ObjectNode) end.payload()).putArray("meterValue");
        addSample(meters.addObject(), "2026-01-01T10:00:00Z", "Transaction.Begin",
                "0", "Wh", 0).put("location", "Outlet");
        ObjectNode endSample = addSample(meters.addObject(), "2026-01-01T10:20:00Z",
                "Transaction.End", "0.15545", "kWh", 0);
        endSample.put("format", "Raw");
        addSample(meters.addObject(), "2026-01-01T10:20:00Z", "Transaction.End",
                "15545", "Wh", -2).remove("measurand");
        ((tools.jackson.databind.node.ArrayNode) meters.get(2).get("sampledValue"))
                .addObject().put("value", 230)
                .put("measurand", "Voltage").put("context", "Sample.Periodic");

        var outcome = rules.evaluate(List.of(end, update, start));
        assertThat(outcome.status()).isEqualTo("CALCULATED");
        assertThat(outcome.energyWh()).isEqualByComparingTo("155.45");
    }

    @Test
    void holdsConflictingRepeatedBoundaryAndUnsupportedSource() {
        var start = event(0, "Started", 10000, true, false);
        var end = event(1, "Ended", 12500, false, false);
        var meters = (tools.jackson.databind.node.ArrayNode) end.payload().get("meterValue");
        addSample(meters.addObject(), "2026-01-01T10:20:00Z", "Transaction.End",
                "12501", "Wh", 0);
        assertThat(rules.evaluate(List.of(start, end)).reason()).isEqualTo("INVALID_OR_AMBIGUOUS_METER");

        meters.remove(1);
        ObjectNode second = addSample(meters.addObject(), "2026-01-01T10:20:00Z",
                "Transaction.End", "12500", "Wh", 0);
        second.put("phase", "L1");
        assertThat(rules.evaluate(List.of(start, end)).reason()).isEqualTo("INVALID_OR_AMBIGUOUS_METER");

        meters.remove(1);
        second = addSample(meters.addObject(), "2026-01-01T10:20:00Z",
                "Transaction.End", "12500", "Wh", 0);
        second.put("location", "Body");
        assertThat(rules.evaluate(List.of(start, end)).reason()).isEqualTo("INVALID_OR_AMBIGUOUS_METER");

        meters.remove(1);
        addSample(meters.addObject(), "2026-01-01T10:00:00Z", "Transaction.Begin",
                "9999", "Wh", 0);
        assertThat(rules.evaluate(List.of(start, end)).reason()).isEqualTo("INVALID_OR_AMBIGUOUS_METER");
    }

    @Test
    void requiresExplicitBoundaryAndOrderedMeterTime() {
        var start = event(0, "Started", 10000, true, false);
        var end = event(1, "Ended", 12500, false, false);
        ObjectNode endSample = (ObjectNode) end.payload().get("meterValue").get(0).get("sampledValue").get(0);
        endSample.remove("context");
        assertThat(rules.evaluate(List.of(start, end)).reason()).isEqualTo("INVALID_OR_AMBIGUOUS_METER");

        endSample.put("context", "Transaction.End");
        ((ObjectNode) end.payload().get("meterValue").get(0)).put("timestamp", "2026-01-01T09:59:59Z");
        assertThat(rules.evaluate(List.of(start, end)).reason()).isEqualTo("INVALID_OR_AMBIGUOUS_METER");
    }

    @Test
    void waitsForStartedAndDoesNotInventEvse() {
        assertThat(rules.evaluate(List.of(event(2, "Ended", 12500, false, false))).status())
                .isEqualTo("PENDING");
        assertThat(rules.evaluate(List.of(event(0, "Started", 10000, false, false),
                event(1, "Ended", 12500, false, false))).reason()).isEqualTo("MISSING_EVSE");
        assertThat(rules.evaluate(List.of(event(0, "Started", 10000, true, false),
                event(1, "Ended", 12500, true, false, 2))).reason()).isEqualTo("CONFLICTING_EVSE");
    }

    @Test
    void decoderChecksStationKeyAndExcludesGatewayDeliveryMetadataFromSourceEquality() {
        var decoder = new TransactionRecordDecoder(mapper);
        String first = observed(0, "Started", 10000, "00000000-0000-0000-0000-000000000001");
        String retry = observed(0, "Started", 10000, "00000000-0000-0000-0000-000000000002");
        var decoded = decoder.decode("ST-TEST", first);
        assertThat(decoded.sourceJson()).isEqualTo(decoder.decode("ST-TEST", retry).sourceJson());
        assertThat(decoded.sourceJson()).doesNotContain("eventId", "receivedAt");
        assertThat(decoded.payload().get("evse").get("id").intValue()).isEqualTo(1);
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> decoder.decode("WRONG", first))
                .isInstanceOf(TransactionRecordDecoder.TransactionContractException.class);
    }

    private TransactionRecoveryRules.StoredEvent event(int seq, String type, Integer wh,
                                                         boolean hasEvse, boolean extraMeter) {
        return event(seq, type, wh, hasEvse, extraMeter, 1);
    }

    private TransactionRecoveryRules.StoredEvent event(int seq, String type, Integer wh,
                                                         boolean hasEvse, boolean extraMeter, int evseId) {
        ObjectNode source = source(seq, type, wh, hasEvse, extraMeter, evseId);
        return new TransactionRecoveryRules.StoredEvent(seq, type,
                Instant.parse(source.get("occurredAt").textValue()), source);
    }

    private String observed(int seq, String type, Integer wh, String id) {
        ObjectNode source = source(seq, type, wh, true, false, 1);
        ObjectNode root = mapper.createObjectNode();
        root.put("eventId", id);
        root.put("eventType", "OcppTransactionEventObserved");
        root.put("schemaVersion", 1);
        root.put("occurredAt", source.get("occurredAt").textValue());
        root.put("receivedAt", id.endsWith("1") ? "2026-01-01T10:00:01Z" : "2026-01-01T10:00:02Z");
        root.put("stationId", "ST-TEST");
        root.put("chargingStationId", "CS-TEST");
        root.set("payload", source.get("payload"));
        return mapper.writeValueAsString(root);
    }

    private ObjectNode source(int seq, String type, Integer wh, boolean hasEvse, boolean extraMeter, int evseId) {
        String time = switch (seq) {
            case 0 -> "2026-01-01T10:00:00Z";
            case 1 -> "Ended".equals(type) ? "2026-01-01T10:20:00Z" : "2026-01-01T10:10:00Z";
            default -> "2026-01-01T10:20:00Z";
        };
        ObjectNode source = mapper.createObjectNode();
        source.put("occurredAt", time);
        source.put("stationId", "ST-TEST");
        source.put("chargingStationId", "CS-TEST");
        ObjectNode payload = source.putObject("payload");
        payload.put("transactionId", "TX-TEST");
        payload.put("seqNo", seq);
        payload.put("eventType", type);
        payload.put("triggerReason", "TEST");
        payload.putObject("transactionInfo").put("transactionId", "TX-TEST");
        if (hasEvse) payload.putObject("evse").put("id", evseId);
        if (wh != null) {
            var meters = payload.putArray("meterValue");
            addMeter(meters.addObject(), time, type, wh);
            if (extraMeter) addMeter(meters.addObject(), time, type, wh);
        }
        return source;
    }

    private void addMeter(ObjectNode meter, String time, String type, int wh) {
        meter.put("timestamp", time);
        ObjectNode sample = meter.putArray("sampledValue").addObject();
        sample.put("value", wh);
        sample.put("context", switch (type) {
            case "Started" -> "Transaction.Begin";
            case "Ended" -> "Transaction.End";
            default -> "Sample.Periodic";
        });
        sample.put("measurand", "Energy.Active.Import.Register");
        sample.putObject("unitOfMeasure").put("unit", "Wh").put("multiplier", 0);
    }

    private ObjectNode addSample(ObjectNode meter, String time, String context,
                                 String value, String unit, int multiplier) {
        meter.put("timestamp", time);
        ObjectNode sample = meter.putArray("sampledValue").addObject();
        sample.put("value", new BigDecimal(value));
        sample.put("context", context);
        sample.put("measurand", "Energy.Active.Import.Register");
        sample.putObject("unitOfMeasure").put("unit", unit).put("multiplier", multiplier);
        return sample;
    }
}
