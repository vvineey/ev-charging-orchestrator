package com.evcharging.ocppgateway;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import tools.jackson.databind.ObjectMapper;

import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OcppWebSocketHandlerTest {
    private final ObjectMapper json = new ObjectMapper();
    private final StationRegistration station = new StationRegistration("CP-1", "ST-1", Set.of(1), "test-credential-only");
    private final TransactionKafkaPublisher publisher = mock(TransactionKafkaPublisher.class);
    private final OcppWebSocketHandler handler = new OcppWebSocketHandler(json,
            new TransactionEventMapper(json), publisher);

    @Test
    void acknowledgesTransactionOnlyAfterKafkaPublisherReturns() throws Exception {
        WebSocketSession session = mockSession();
        handler.handleTextMessage(session, call());

        InOrder order = inOrder(publisher, session);
        order.verify(publisher).publish(any());
        ArgumentCaptor<TextMessage> reply = ArgumentCaptor.forClass(TextMessage.class);
        order.verify(session).sendMessage(reply.capture());
        assertThat(json.readTree(reply.getValue().getPayload()).get(0).intValue()).isEqualTo(3);
    }

    @Test
    void publicationFailureGetsCallErrorInsteadOfSuccessfulAck() throws Exception {
        WebSocketSession session = mockSession();
        doThrow(new TransactionKafkaPublisher.PublicationFailure(new RuntimeException("secret-marker")))
                .when(publisher).publish(any());
        handler.handleTextMessage(session, call());

        ArgumentCaptor<TextMessage> reply = ArgumentCaptor.forClass(TextMessage.class);
        verify(session).sendMessage(reply.capture());
        var frame = json.readTree(reply.getValue().getPayload());
        assertThat(frame.get(0).intValue()).isEqualTo(4);
        assertThat(frame.get(2).textValue()).isEqualTo("InternalError");
        assertThat(reply.getValue().getPayload()).doesNotContain("secret-marker");
    }

    @Test
    void preservesLongDecimalSampleAndRejectsDuplicateJsonKey() throws Exception {
        WebSocketSession session = mockSession();
        handler.handleTextMessage(session, new TextMessage("""
                [2,"decimal-1","TransactionEvent",{"eventType":"Updated",
                "timestamp":"2026-09-30T00:00:00Z","triggerReason":"MeterValuePeriodic","seqNo":1,
                "transactionInfo":{"transactionId":"TX-1"},"meterValue":[{"timestamp":"2026-09-30T00:00:00Z",
                "sampledValue":[{"value":0.12345678901234567890123456789}]}]}]
                """));
        ArgumentCaptor<TransactionEventMapper.TransactionRecord> record = ArgumentCaptor.forClass(
                TransactionEventMapper.TransactionRecord.class);
        verify(publisher).publish(record.capture());
        assertThat(record.getValue().json().path("payload").path("meterValue").get(0)
                .path("sampledValue").get(0).path("value").decimalValue())
                .isEqualByComparingTo("0.12345678901234567890123456789");

        WebSocketSession duplicateSession = mockSession();
        handler.handleTextMessage(duplicateSession, new TextMessage("""
                [2,"duplicate","TransactionEvent",{"eventType":"Started","eventType":"Ended"}]
                """));
        verify(duplicateSession).close(any());
    }

    private WebSocketSession mockSession() {
        WebSocketSession session = mock(WebSocketSession.class);
        when(session.getAttributes()).thenReturn(Map.of("station", station));
        return session;
    }

    private TextMessage call() {
        return new TextMessage("""
                [2,"message-1","TransactionEvent",{"eventType":"Started",
                "timestamp":"2026-09-30T00:00:00Z","triggerReason":"CablePluggedIn","seqNo":0,
                "transactionInfo":{"transactionId":"TX-1"},"evse":{"id":1}}]
                """);
    }
}
