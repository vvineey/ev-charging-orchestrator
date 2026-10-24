package com.evcharging.controlplane.transaction;

import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
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
        MeterReading start = boundaryWh(first.payload(), "Transaction.Begin");
        MeterReading end = boundaryWh(last.payload(), "Transaction.End");
        MeterReading repeatedStart = boundaryWh(last.payload(), "Transaction.Begin");
        if (start == null || end == null || end.wh().compareTo(start.wh()) < 0
                || start.latestAt().isAfter(end.earliestAt())
                || (hasBoundaryEnergy(last.payload(), "Transaction.Begin") && repeatedStart == null)
                || (repeatedStart != null && (repeatedStart.wh().compareTo(start.wh()) != 0
                    || repeatedStart.latestAt().isAfter(end.earliestAt())))) {
            return Outcome.hold("INVALID_OR_AMBIGUOUS_METER");
        }
        BigDecimal energy = end.wh().subtract(start.wh());
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

    private record MeterReading(BigDecimal wh, Instant earliestAt, Instant latestAt) {}

    // ADR-0024: select by explicit transaction boundary, never by array position.
    private MeterReading boundaryWh(JsonNode payload, String context) {
        JsonNode meters = payload.get("meterValue");
        if (meters == null || !meters.isArray()) return null;
        List<BigDecimal> values = new ArrayList<>();
        Instant earliest = null;
        Instant latest = null;
        for (JsonNode meter : meters) {
            JsonNode samples = meter.get("sampledValue");
            if (samples == null || !samples.isArray()) return null;
            for (JsonNode sample : samples) {
                if (!context.equals(string(sample, "context")) || !isEnergy(sample)) continue;
                BigDecimal wh = normalizedWh(sample);
                if (wh == null) return null;
                Instant at;
                try {
                    at = OffsetDateTime.parse(string(meter, "timestamp")).toInstant();
                } catch (DateTimeParseException | NullPointerException exception) {
                    return null;
                }
                values.add(wh);
                if (earliest == null || at.isBefore(earliest)) earliest = at;
                if (latest == null || at.isAfter(latest)) latest = at;
            }
        }
        if (values.isEmpty()) return null;
        BigDecimal agreed = values.getFirst();
        if (values.stream().anyMatch(value -> value.compareTo(agreed) != 0)) return null;
        return new MeterReading(agreed, earliest, latest);
    }

    private boolean isEnergy(JsonNode sample) {
        String measurand = string(sample, "measurand");
        return (!sample.has("measurand") || "Energy.Active.Import.Register".equals(measurand));
    }

    private boolean hasBoundaryEnergy(JsonNode payload, String context) {
        JsonNode meters = payload.get("meterValue");
        if (meters == null || !meters.isArray()) return false;
        for (JsonNode meter : meters) {
            JsonNode samples = meter.get("sampledValue");
            if (samples == null || !samples.isArray()) continue;
            for (JsonNode sample : samples) {
                if (context.equals(string(sample, "context")) && isEnergy(sample)) return true;
            }
        }
        return false;
    }

    private BigDecimal normalizedWh(JsonNode sample) {
        JsonNode value = sample.get("value");
        if (value == null || !value.isNumber() || value.decimalValue().signum() < 0) return null;
        String location = string(sample, "location");
        String format = string(sample, "format");
        if (sample.has("phase") || (sample.has("location") && !"Outlet".equals(location))
                || (sample.has("format") && !"Raw".equals(format))) return null;
        JsonNode unit = sample.get("unitOfMeasure");
        if (unit != null && !unit.isObject()) return null;
        String unitName = unit == null ? null : string(unit, "unit");
        if (unit != null && unit.has("unit") && unitName == null) return null;
        if (unitName != null && !"Wh".equals(unitName) && !"kWh".equals(unitName)) return null;
        JsonNode multiplierValue = unit == null ? null : unit.get("multiplier");
        if (multiplierValue != null && (!multiplierValue.isIntegralNumber()
                || !multiplierValue.canConvertToInt())) return null;
        int multiplier = multiplierValue == null ? 0 : multiplierValue.intValue();
        if (multiplier < -18 || multiplier > 18) return null;
        int exponent = multiplier + ("kWh".equals(unitName) ? 3 : 0);
        if (exponent < -18 || exponent > 18) return null;
        BigDecimal wh = value.decimalValue().scaleByPowerOfTen(exponent).stripTrailingZeros();
        return wh.scale() > 6 || wh.precision() - wh.scale() > 18 ? null : wh;
    }

    private String string(JsonNode object, String name) {
        JsonNode value = object.get(name);
        return value == null || !value.isTextual() ? null : value.textValue();
    }
}
