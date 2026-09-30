package com.evcharging.controlplane.transaction;

import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;

@Component
class TransactionRecoveryRules {
    record StoredEvent(int seqNo, String eventType, Instant occurredAt, JsonNode sourceDocument) {
        JsonNode payload() { return sourceDocument.get("payload"); }
    }

    record Outcome(String status, String reason, Integer evseId, Instant startedAt,
                   Instant endedAt, BigDecimal energyWh) {
        static Outcome pending() { return new Outcome("PENDING", null, null, null, null, null); }
        static Outcome hold(String reason) { return new Outcome("HOLD", reason, null, null, null, null); }
    }

    Outcome evaluate(List<StoredEvent> stored) {
        if (stored.isEmpty()) return Outcome.pending();
        List<StoredEvent> events = stored.stream().sorted(Comparator.comparingInt(StoredEvent::seqNo)).toList();
        StoredEvent first = events.getFirst();
        if (first.seqNo() != 0) return Outcome.pending();
        if (!"Started".equals(first.eventType())) return Outcome.hold("INVALID_SEQUENCE");
        long ends = events.stream().filter(event -> "Ended".equals(event.eventType())).count();
        if (ends == 0) return Outcome.pending();
        if (ends != 1 || !"Ended".equals(events.getLast().eventType())) return Outcome.hold("INVALID_SEQUENCE");
        for (int index = 0; index < events.size(); index++) {
            StoredEvent event = events.get(index);
            if (event.seqNo() != index) return Outcome.hold("MISSING_SEQUENCE");
            if (index > 0 && index < events.size() - 1 && !"Updated".equals(event.eventType())) {
                return Outcome.hold("INVALID_SEQUENCE");
            }
        }

        Integer evseId = evseId(first.payload());
        if (evseId == null) return Outcome.hold("MISSING_EVSE");
        for (StoredEvent event : events) {
            Integer reported = evseId(event.payload());
            if (reported != null && !reported.equals(evseId)) return Outcome.hold("CONFLICTING_EVSE");
        }
        StoredEvent last = events.getLast();
        if (last.occurredAt().isBefore(first.occurredAt())) return Outcome.hold("INVALID_TIME");
        BigDecimal startWh = unambiguousWh(first.payload(), "Transaction.Begin");
        BigDecimal endWh = unambiguousWh(last.payload(), "Transaction.End");
        if (startWh == null || endWh == null || endWh.compareTo(startWh) < 0) {
            return Outcome.hold("INVALID_OR_AMBIGUOUS_METER");
        }
        BigDecimal energy = endWh.subtract(startWh);
        if (energy.scale() > 6 || energy.precision() - energy.scale() > 18) {
            return Outcome.hold("INVALID_OR_AMBIGUOUS_METER");
        }
        return new Outcome("CALCULATED", null, evseId, first.occurredAt(), last.occurredAt(), energy);
    }

    private Integer evseId(JsonNode payload) {
        JsonNode evse = payload.get("evse");
        if (evse == null || !evse.isObject()) return null;
        JsonNode id = evse.get("id");
        return id == null || !id.canConvertToInt() || id.intValue() <= 0 ? null : id.intValue();
    }

    // Deliberately narrow synthetic rule. SAP's multiple meter groups remain HOLD for later policy review.
    private BigDecimal unambiguousWh(JsonNode payload, String context) {
        JsonNode meters = payload.get("meterValue");
        if (meters == null || !meters.isArray() || meters.size() != 1) return null;
        JsonNode samples = meters.get(0).get("sampledValue");
        if (samples == null || !samples.isArray() || samples.size() != 1) return null;
        JsonNode sample = samples.get(0);
        JsonNode value = sample.get("value");
        JsonNode unit = sample.get("unitOfMeasure");
        if (value == null || !value.isNumber() || unit == null || !unit.isObject()
                || !context.equals(string(sample, "context"))
                || !"Energy.Active.Import.Register".equals(string(sample, "measurand"))
                || !"Wh".equals(string(unit, "unit"))) return null;
        JsonNode multiplier = unit.get("multiplier");
        if (multiplier == null || !multiplier.isIntegralNumber() || multiplier.intValue() != 0) return null;
        return value.decimalValue();
    }

    private String string(JsonNode object, String name) {
        JsonNode value = object.get(name);
        return value == null || !value.isTextual() ? null : value.textValue();
    }
}
