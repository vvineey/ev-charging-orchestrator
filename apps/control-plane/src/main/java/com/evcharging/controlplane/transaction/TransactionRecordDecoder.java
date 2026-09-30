package com.evcharging.controlplane.transaction;

import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Set;
import java.util.UUID;

@Component
class TransactionRecordDecoder {
    private static final Set<String> STAGES = Set.of("Started", "Updated", "Ended");
    private final ObjectMapper mapper;

    TransactionRecordDecoder(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    ObservedTransactionRecord decode(String key, String json) {
        try {
            if (json == null || json.isBlank()) throw invalid("empty_record");
            JsonNode root = mapper.readTree(json);
            if (root == null || !root.isObject()) throw invalid("record_object");
            if (!"OcppTransactionEventObserved".equals(text(root, "eventType"))
                    || integer(root, "schemaVersion") != 1) throw invalid("type_or_version");
            UUID eventId = UUID.fromString(text(root, "eventId"));
            String stationId = text(root, "stationId");
            if (key == null || !stationId.equals(key)) throw invalid("station_key");
            String chargingStationId = text(root, "chargingStationId");
            String occurredAt = text(root, "occurredAt");
            Instant occurred = OffsetDateTime.parse(occurredAt).toInstant();
            Instant received = OffsetDateTime.parse(text(root, "receivedAt")).toInstant();
            JsonNode payload = root.get("payload");
            if (payload == null || !payload.isObject() || payload.has("idToken")) throw invalid("payload_object");
            String transactionId = text(payload, "transactionId");
            if (transactionId.length() > 128) throw invalid("transaction_id_length");
            JsonNode info = payload.get("transactionInfo");
            if (info == null || !info.isObject() || !transactionId.equals(text(info, "transactionId"))) {
                throw invalid("transaction_info");
            }
            int seqNo = integer(payload, "seqNo");
            if (seqNo < 0) throw invalid("sequence");
            String stage = text(payload, "eventType");
            if (!STAGES.contains(stage)) throw invalid("stage");
            text(payload, "triggerReason");
            JsonNode evse = payload.get("evse");
            if (evse != null && (!evse.isObject() || integer(evse, "id") <= 0)) throw invalid("evse");
            JsonNode meters = payload.get("meterValue");
            if (meters != null) validateMeters(meters);

            ObjectNode source = mapper.createObjectNode();
            source.put("occurredAt", occurredAt);
            source.put("stationId", stationId);
            source.put("chargingStationId", chargingStationId);
            source.set("payload", payload.deepCopy());
            return new ObservedTransactionRecord(eventId, stationId, chargingStationId, transactionId,
                    seqNo, stage, occurred, received, mapper.writeValueAsString(source), payload.deepCopy());
        } catch (TransactionContractException exception) {
            throw exception;
        } catch (JacksonException | IllegalArgumentException exception) {
            // Never retain the source JSON as an exception cause or in a log message.
            throw invalid("invalid_json_or_field");
        }
    }

    private void validateMeters(JsonNode meters) {
        if (!meters.isArray()) throw invalid("meter_array");
        for (JsonNode meter : meters) {
            if (!meter.isObject()) throw invalid("meter_object");
            OffsetDateTime.parse(text(meter, "timestamp"));
            JsonNode samples = meter.get("sampledValue");
            if (samples == null || !samples.isArray() || samples.isEmpty()) throw invalid("sample_array");
            for (JsonNode sample : samples) {
                if (!sample.isObject() || sample.get("value") == null || !sample.get("value").isNumber()) {
                    throw invalid("sample_value");
                }
            }
        }
    }

    private String text(JsonNode node, String name) {
        JsonNode value = node.get(name);
        if (value == null || !value.isTextual() || value.textValue().isBlank()) throw invalid("required_" + name);
        return value.textValue();
    }

    private int integer(JsonNode node, String name) {
        JsonNode value = node.get(name);
        if (value == null || !value.isIntegralNumber() || !value.canConvertToInt()) throw invalid("required_" + name);
        return value.intValue();
    }

    private TransactionContractException invalid(String reason) {
        return new TransactionContractException(reason);
    }

    static final class TransactionContractException extends RuntimeException {
        TransactionContractException(String reason) {
            super(reason);
        }
    }
}
