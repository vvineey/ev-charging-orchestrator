package com.evcharging.telemetryworker.adapter.kafka;

import com.evcharging.messaging.contract.ChargerTelemetryPayload;
import com.evcharging.messaging.contract.ChargerTelemetryReceived;
import com.evcharging.messaging.contract.DomainEventEnvelope;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.util.UUID;

@Component
public class TelemetryRecordDecoder {
    private final ObjectMapper mapper;

    public TelemetryRecordDecoder(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public DomainEventEnvelope<ChargerTelemetryPayload> decode(String key, String json) {
        UUID eventId = null;
        try {
            if (json == null || json.isBlank()) {
                throw invalid("missing_value", null);
            }
            JsonNode root = mapper.readTree(json);
            if (root == null || !root.isObject()) {
                throw invalid("envelope_object", null);
            }
            JsonNode id = root.get("eventId");
            if (id != null && id.isString()) {
                eventId = UUID.fromString(id.asString());
            }
            for (String name : new String[]{"eventId", "eventType", "occurredAt", "receivedAt", "stationId", "correlationId"}) {
                require(root, name, "string", eventId);
            }
            require(root, "schemaVersion", "integer", eventId);
            require(root, "evseId", "integer", eventId);
            require(root, "payload", "object", eventId);
            JsonNode payload = root.get("payload");
            require(payload, "timestamp", "integer", eventId);
            require(payload, "charging", "boolean", eventId);
            for (String name : new String[]{"power", "voltage", "current"}) {
                require(payload, name, "number", eventId);
            }
            if (!root.get("schemaVersion").canConvertToInt() || !root.get("evseId").canConvertToInt()
                    || !payload.get("timestamp").canConvertToLong()) {
                throw invalid("integer_range", eventId);
            }
            var type = mapper.getTypeFactory().constructParametricType(DomainEventEnvelope.class, ChargerTelemetryPayload.class);
            DomainEventEnvelope<ChargerTelemetryPayload> event = mapper.readValue(json, type);
            if (!ChargerTelemetryReceived.EVENT_TYPE.equals(event.eventType())
                    || event.schemaVersion() != ChargerTelemetryReceived.SCHEMA_VERSION) {
                throw invalid("unsupported_type_or_version", eventId);
            }
            if (key == null || !key.equals(event.stationId())) {
                throw invalid("station_key_mismatch", eventId);
            }
            if (event.payload().timestamp() != event.occurredAt().toEpochMilli()) {
                throw invalid("payload_time_mismatch", eventId);
            }
            return event;
        } catch (TelemetryConsumptionException exception) {
            throw exception;
        } catch (JacksonException | IllegalArgumentException | ArithmeticException exception) {
            // Do not retain a cause that may embed the original JSON or identifiers.
            throw invalid("invalid_json_or_contract", eventId);
        }
    }

    private void require(JsonNode object, String name, String type, UUID eventId) {
        JsonNode node = object.get(name);
        boolean valid = node != null && switch (type) {
            case "string" -> node.isString();
            case "integer" -> node.isIntegralNumber();
            case "object" -> node.isObject();
            case "boolean" -> node.isBoolean();
            case "number" -> node.isNumber();
            default -> false;
        };
        if (!valid) {
            throw invalid("required_" + name + "_" + type, eventId);
        }
    }

    private TelemetryConsumptionException invalid(String reason, UUID eventId) {
        return new TelemetryConsumptionException("contract", reason, eventId);
    }
}
