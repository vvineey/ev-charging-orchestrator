package com.evcharging.ocppgateway;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

import java.time.Clock;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.Set;
import java.util.UUID;

@Component
class TransactionEventMapper {
    private static final Set<String> EVENT_TYPES = Set.of("Started", "Updated", "Ended");
    private static final Set<String> SOURCE_FIELDS = Set.of("eventType", "timestamp", "triggerReason", "seqNo",
            "offline", "numberOfPhasesUsed", "cableMaxCurrent", "reservationId", "transactionInfo", "evse",
            "meterValue", "idToken");
    private static final Set<String> TRANSACTION_FIELDS = Set.of(
            "transactionId", "chargingState", "timeSpentCharging", "stoppedReason", "remoteStartId");
    private static final Set<String> SAMPLE_FIELDS = Set.of(
            "value", "context", "measurand", "phase", "location", "signedMeterValue", "unitOfMeasure");

    private final ObjectMapper mapper;
    private final Clock clock;

    @Autowired
    TransactionEventMapper(ObjectMapper mapper) {
        this(mapper, Clock.systemUTC());
    }

    TransactionEventMapper(ObjectMapper mapper, Clock clock) {
        this.mapper = mapper;
        this.clock = clock;
    }

    TransactionRecord map(JsonNode source, StationRegistration station) {
        if (source == null || !source.isObject()) {
            throw new InvalidTransactionEvent("payload must be an object");
        }
        if (source.properties().stream().anyMatch(entry -> !SOURCE_FIELDS.contains(entry.getKey()))) {
            throw new InvalidTransactionEvent("unsupported transaction extension");
        }
        String eventType = requiredText(source, "eventType");
        if (!EVENT_TYPES.contains(eventType)) {
            throw new InvalidTransactionEvent("unknown transaction event type");
        }
        String occurredAt = requiredText(source, "timestamp");
        try {
            OffsetDateTime.parse(occurredAt);
        } catch (DateTimeParseException exception) {
            throw new InvalidTransactionEvent("invalid transaction timestamp");
        }
        String triggerReason = requiredText(source, "triggerReason");
        JsonNode seqNo = source.get("seqNo");
        if (seqNo == null || !seqNo.isIntegralNumber() || !seqNo.canConvertToInt()
                || seqNo.intValue() < 0) {
            throw new InvalidTransactionEvent("invalid transaction sequence");
        }
        JsonNode transactionInfo = source.get("transactionInfo");
        if (transactionInfo == null || !transactionInfo.isObject()
                || transactionInfo.properties().stream().anyMatch(entry -> !TRANSACTION_FIELDS.contains(entry.getKey()))) {
            throw new InvalidTransactionEvent("invalid transaction info");
        }
        String transactionId = requiredText(transactionInfo, "transactionId");
        if (transactionId.isBlank() || transactionId.length() > 128) {
            throw new InvalidTransactionEvent("invalid transaction id");
        }
        for (String field : Set.of("chargingState", "stoppedReason")) {
            optionalText(transactionInfo, field);
        }
        for (String field : Set.of("timeSpentCharging", "remoteStartId")) {
            optionalNonNegativeInteger(transactionInfo, field);
        }
        JsonNode idToken = source.get("idToken");
        if (idToken != null) {
            if (!idToken.isObject() || idToken.properties().stream().anyMatch(entry ->
                    !Set.of("idToken", "type", "additionalInfo").contains(entry.getKey()))) {
                throw new InvalidTransactionEvent("invalid idToken structure");
            }
            requiredText(idToken, "idToken");
            requiredText(idToken, "type");
        }
        JsonNode evse = source.get("evse");
        if (evse != null) {
            if (!evse.isObject() || evse.properties().stream()
                    .anyMatch(entry -> !Set.of("id", "connectorId").contains(entry.getKey()))) {
                throw new InvalidTransactionEvent("invalid EVSE");
            }
            JsonNode id = evse.get("id");
            if (id == null || !id.isIntegralNumber() || !id.canConvertToInt()
                    || !station.allowedEvseIds().contains(id.intValue())) {
                throw new InvalidTransactionEvent("EVSE is not registered");
            }
            JsonNode connectorId = evse.get("connectorId");
            if (connectorId != null && (!connectorId.isIntegralNumber() || !connectorId.canConvertToInt()
                    || connectorId.intValue() <= 0)) {
                throw new InvalidTransactionEvent("invalid connector id");
            }
        }
        JsonNode meterValues = source.get("meterValue");
        if (meterValues != null) {
            validateMeterValues(meterValues);
        }
        JsonNode offline = source.get("offline");
        if (offline != null && !offline.isBoolean()) {
            throw new InvalidTransactionEvent("invalid offline flag");
        }
        for (String integerField : Set.of("numberOfPhasesUsed", "reservationId")) {
            optionalNonNegativeInteger(source, integerField);
        }
        JsonNode cableMaxCurrent = source.get("cableMaxCurrent");
        if (cableMaxCurrent != null && !cableMaxCurrent.isNumber()) {
            throw new InvalidTransactionEvent("invalid cableMaxCurrent");
        }

        String eventId = UUID.randomUUID().toString();
        ObjectNode record = mapper.createObjectNode();
        record.put("eventId", eventId);
        record.put("eventType", "OcppTransactionEventObserved");
        record.put("schemaVersion", 1);
        record.put("occurredAt", occurredAt);
        record.put("receivedAt", clock.instant().toString());
        record.put("stationId", station.stationId());
        record.put("chargingStationId", station.chargingStationId());

        ObjectNode payload = record.putObject("payload");
        payload.put("transactionId", transactionId);
        payload.put("seqNo", seqNo.intValue());
        payload.put("eventType", eventType);
        payload.put("triggerReason", triggerReason);
        copyIfPresent(source, payload, "offline");
        copyIfPresent(source, payload, "numberOfPhasesUsed");
        copyIfPresent(source, payload, "cableMaxCurrent");
        copyIfPresent(source, payload, "reservationId");
        copyIfPresent(source, payload, "evse");
        copyIfPresent(source, payload, "meterValue");
        ObjectNode copiedInfo = payload.putObject("transactionInfo");
        transactionInfo.properties().forEach(entry -> copiedInfo.set(entry.getKey(), entry.getValue().deepCopy()));
        return new TransactionRecord(eventId, station.stationId(), record);
    }

    private void validateMeterValues(JsonNode meterValues) {
        if (!meterValues.isArray()) {
            throw new InvalidTransactionEvent("meterValue must be an array");
        }
        for (JsonNode meterValue : meterValues) {
            if (!meterValue.isObject() || meterValue.properties().stream()
                    .anyMatch(entry -> !Set.of("timestamp", "sampledValue").contains(entry.getKey()))) {
                throw new InvalidTransactionEvent("invalid meterValue group");
            }
            try {
                OffsetDateTime.parse(requiredText(meterValue, "timestamp"));
            } catch (DateTimeParseException exception) {
                throw new InvalidTransactionEvent("invalid meterValue timestamp");
            }
            JsonNode samples = meterValue.get("sampledValue");
            if (samples == null || !samples.isArray() || samples.isEmpty()) {
                throw new InvalidTransactionEvent("missing sampledValue");
            }
            for (JsonNode sample : samples) {
                if (!sample.isObject() || sample.properties().stream()
                        .anyMatch(entry -> !SAMPLE_FIELDS.contains(entry.getKey()))) {
                    throw new InvalidTransactionEvent("invalid sampledValue");
                }
                JsonNode value = sample.get("value");
                if (value == null || !value.isNumber()) {
                    throw new InvalidTransactionEvent("invalid sampledValue value");
                }
                for (String field : Set.of("context", "measurand", "phase", "location")) {
                    optionalText(sample, field);
                }
                JsonNode unit = sample.get("unitOfMeasure");
                if (unit != null && (!unit.isObject() || unit.properties().stream()
                        .anyMatch(entry -> !Set.of("unit", "multiplier").contains(entry.getKey())))) {
                    throw new InvalidTransactionEvent("invalid unitOfMeasure");
                }
                if (unit != null) {
                    optionalText(unit, "unit");
                    JsonNode multiplier = unit.get("multiplier");
                    if (multiplier != null && (!multiplier.isIntegralNumber() || !multiplier.canConvertToInt())) {
                        throw new InvalidTransactionEvent("invalid unit multiplier");
                    }
                }
                JsonNode signed = sample.get("signedMeterValue");
                if (signed != null) {
                    if (!signed.isObject() || signed.properties().stream().anyMatch(entry ->
                            !Set.of("signedMeterData", "signingMethod", "encodingMethod", "publicKey")
                                    .contains(entry.getKey()))) {
                        throw new InvalidTransactionEvent("invalid signed meter value");
                    }
                    for (String field : Set.of("signedMeterData", "signingMethod", "encodingMethod", "publicKey")) {
                        optionalText(signed, field);
                    }
                }
            }
        }
    }

    private String requiredText(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || !value.isTextual() || value.textValue().isBlank()) {
            throw new InvalidTransactionEvent("missing or invalid " + field);
        }
        return value.textValue();
    }

    private void optionalText(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value != null && (!value.isTextual() || value.textValue().isBlank())) {
            throw new InvalidTransactionEvent("invalid " + field);
        }
    }

    private void optionalNonNegativeInteger(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value != null && (!value.isIntegralNumber() || !value.canConvertToInt() || value.intValue() < 0)) {
            throw new InvalidTransactionEvent("invalid " + field);
        }
    }

    private void copyIfPresent(JsonNode source, ObjectNode destination, String name) {
        JsonNode value = source.get(name);
        if (value != null) {
            destination.set(name, value.deepCopy());
        }
    }

    record TransactionRecord(String eventId, String stationId, ObjectNode json) { }

    static final class InvalidTransactionEvent extends RuntimeException {
        InvalidTransactionEvent(String message) {
            super(message);
        }
    }
}
