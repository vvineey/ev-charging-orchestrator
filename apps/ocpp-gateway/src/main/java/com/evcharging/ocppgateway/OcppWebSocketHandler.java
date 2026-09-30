package com.evcharging.ocppgateway;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;
import tools.jackson.core.JacksonException;
import tools.jackson.core.StreamReadFeature;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.ObjectReader;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.time.Clock;

@Component
@ConditionalOnProperty(name = "ocpp.ingress.enabled", havingValue = "true")
class OcppWebSocketHandler extends TextWebSocketHandler {
    private static final Logger log = LoggerFactory.getLogger(OcppWebSocketHandler.class);

    private final ObjectMapper mapper;
    private final ObjectReader strictReader;
    private final TransactionEventMapper transactionMapper;
    private final TransactionKafkaPublisher publisher;
    private final Clock clock = Clock.systemUTC();

    OcppWebSocketHandler(ObjectMapper mapper, TransactionEventMapper transactionMapper,
            TransactionKafkaPublisher publisher) {
        this.mapper = mapper;
        this.strictReader = mapper.reader().with(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
                .with(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
                .with(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
        this.transactionMapper = transactionMapper;
        this.publisher = publisher;
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) throws IOException {
        StationRegistration station = (StationRegistration) session.getAttributes().get("station");
        if (station == null) {
            session.close(CloseStatus.POLICY_VIOLATION);
            return;
        }
        JsonNode frame;
        try {
            frame = strictReader.readTree(message.getPayload());
        } catch (JacksonException exception) {
            session.close(CloseStatus.BAD_DATA);
            return;
        }
        if (frame == null || !frame.isArray() || frame.size() != 4
                || !frame.get(0).isInt() || frame.get(0).intValue() != 2
                || !frame.get(1).isTextual() || frame.get(1).textValue().isBlank()
                || !frame.get(2).isTextual() || !frame.get(3).isObject()) {
            session.close(CloseStatus.BAD_DATA);
            return;
        }
        String messageId = frame.get(1).textValue();
        String action = frame.get(2).textValue();
        JsonNode payload = frame.get(3);
        try {
            ObjectNode response;
            switch (action) {
                case "BootNotification" -> {
                    response = mapper.createObjectNode();
                    response.put("status", "Accepted");
                    response.put("currentTime", clock.instant().toString());
                    response.put("interval", 30);
                }
                case "Heartbeat" -> {
                    response = mapper.createObjectNode();
                    response.put("currentTime", clock.instant().toString());
                }
                case "StatusNotification" -> response = mapper.createObjectNode();
                case "TransactionEvent" -> {
                    var record = transactionMapper.map(payload, station);
                    publisher.publish(record);
                    response = mapper.createObjectNode();
                }
                default -> {
                    sendError(session, messageId, "NotImplemented");
                    return;
                }
            }
            ArrayNode result = mapper.createArrayNode();
            result.add(3).add(messageId).add(response);
            session.sendMessage(new TextMessage(mapper.writeValueAsString(result)));
        } catch (TransactionEventMapper.InvalidTransactionEvent exception) {
            log.warn("ocpp_transaction_rejected reason={}", exception.getMessage());
            sendError(session, messageId, "FormationViolation");
        } catch (TransactionKafkaPublisher.PublicationFailure exception) {
            sendError(session, messageId, "InternalError");
        }
    }

    private void sendError(WebSocketSession session, String messageId, String code) throws IOException {
        ArrayNode error = mapper.createArrayNode();
        error.add(4).add(messageId).add(code).add(code).add(mapper.createObjectNode());
        session.sendMessage(new TextMessage(mapper.writeValueAsString(error)));
    }
}
