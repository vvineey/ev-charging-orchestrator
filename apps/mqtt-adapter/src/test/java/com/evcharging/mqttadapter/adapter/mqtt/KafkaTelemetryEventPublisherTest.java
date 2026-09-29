package com.evcharging.mqttadapter.adapter.mqtt;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.evcharging.messaging.contract.ChargerTelemetryPayload;
import com.evcharging.messaging.contract.ChargerTelemetryReceived;
import com.evcharging.messaging.contract.DomainEventEnvelope;
import com.evcharging.messaging.contract.EvseId;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.NotEnoughReplicasException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.KafkaProducerException;
import org.springframework.kafka.support.SendResult;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class KafkaTelemetryEventPublisherTest {

    private static final String TOPIC = "charger.telemetry";
    private static final String SOURCE_CONTENT = "sample-payload-not-for-logs";
    private static final String JSON = "{\"eventType\":\"ChargerTelemetryReceived\",\"payload\":\""
            + SOURCE_CONTENT + "\"}";

    private final DomainEventEnvelope<ChargerTelemetryPayload> event = telemetryEvent(
            UUID.fromString("7a75c31d-34ae-4cb3-8c80-a606ed9d2b51"));
    private final ObjectMapper objectMapper = mock(ObjectMapper.class);
    private final CompletableFuture<SendResult<String, String>> future = new CompletableFuture<>();
    private final ListAppender<ILoggingEvent> logs = new ListAppender<>();
    private final Logger logger = (Logger) LoggerFactory.getLogger(KafkaTelemetryEventPublisher.class);
    private KafkaTemplate<String, String> kafkaTemplate;
    private KafkaTelemetryEventPublisher publisher;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        logs.start();
        logger.addAppender(logs);
        kafkaTemplate = mock(KafkaTemplate.class);
        when(objectMapper.writeValueAsString(event)).thenReturn(JSON);
        when(kafkaTemplate.send(anyString(), anyString(), anyString())).thenReturn(future);
        publisher = new KafkaTelemetryEventPublisher(kafkaTemplate, objectMapper, TOPIC);
    }

    @AfterEach
    void detachLogs() {
        logger.detachAppender(logs);
        logs.stop();
    }

    @Test
    void sendsSerializedEventWithStationIdAsKafkaKeyWithoutWaitingOrLoggingSuccess() {
        assertTimeoutPreemptively(Duration.ofSeconds(2), () -> publisher.publish(event));

        verify(kafkaTemplate).send(TOPIC, event.stationId(), JSON);
        assertThat(future).isNotDone();
        assertThat(logs.list).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(longs = {42L, -1L})
    void recordsCompletionAndLocationOnlyWhenFutureCompletes(long offset) {
        publisher.publish(event);
        assertThat(logs.list).isEmpty();

        CompletableFuture.runAsync(() -> future.complete(sendResult(offset))).join();

        assertSafeSingleLog("outcome=completed", "eventId=" + event.eventId(),
                "topic=" + TOPIC, "partition=2", "offset=" + offset);
        assertThat(logs.list.getFirst().getFormattedMessage()).doesNotContain("acknowledged", "delivered");
    }

    @Test
    void observesAlreadyCompletedFuture() {
        future.complete(sendResult(42));

        publisher.publish(event);

        assertSafeSingleLog("outcome=completed", "offset=42");
    }

    @Test
    void recordsAsynchronousFailureWithSafeCauseType() {
        publisher.publish(event);
        future.completeExceptionally(new KafkaProducerException(record(), SOURCE_CONTENT,
                new NotEnoughReplicasException(SOURCE_CONTENT)));

        assertSafeSingleLog("outcome=failed", "stage=async", "eventId=" + event.eventId(),
                "errorType=KafkaProducerException", "causeType=NotEnoughReplicasException");
        verify(kafkaTemplate).send(TOPIC, event.stationId(), JSON);
    }

    @ParameterizedTest
    @MethodSource("timeoutFailures")
    void recordsTimeoutAsUnknownIncludingWrappedCauses(Throwable failure) {
        publisher.publish(event);
        future.completeExceptionally(failure);

        assertSafeSingleLog("outcome=unknown", "stage=async", "causeType=TimeoutException");
        assertThat(logs.list.getFirst().getFormattedMessage()).doesNotContain("outcome=failed");
        verify(kafkaTemplate).send(TOPIC, event.stationId(), JSON);
    }

    @Test
    void preservesSerializationExceptionWrappingAndDoesNotSend() {
        JacksonException failure = JacksonException.wrapWithPath(
                new IllegalArgumentException(SOURCE_CONTENT), event, "payload");
        when(objectMapper.writeValueAsString(event)).thenThrow(failure);

        assertThatThrownBy(() -> publisher.publish(event))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("failed to serialize telemetry event")
                .hasCause(failure);

        verifyNoInteractions(kafkaTemplate);
        assertSafeSingleLog("outcome=failed", "stage=json_serialization", "eventId=" + event.eventId());
    }

    @Test
    void recordsAndRethrowsSameSynchronousSendException() {
        IllegalStateException failure = new IllegalStateException(SOURCE_CONTENT);
        when(kafkaTemplate.send(TOPIC, event.stationId(), JSON)).thenThrow(failure);

        assertThatThrownBy(() -> publisher.publish(event)).isSameAs(failure);

        assertSafeSingleLog("outcome=failed", "stage=send", "causeType=IllegalStateException");
    }

    @Test
    void recordsKafkaSerializerFailureAtSendStageAndPreservesException() {
        RuntimeException failure = new org.apache.kafka.common.errors.SerializationException(SOURCE_CONTENT);
        when(kafkaTemplate.send(TOPIC, event.stationId(), JSON)).thenThrow(failure);

        assertThatThrownBy(() -> publisher.publish(event)).isSameAs(failure);

        assertSafeSingleLog("outcome=failed", "stage=send", "causeType=SerializationException");
    }

    @Test
    void preservesSynchronousTimeoutAndMarksDeliveryUnknown() {
        RuntimeException failure = new org.springframework.kafka.KafkaException(SOURCE_CONTENT,
                new org.apache.kafka.common.errors.TimeoutException(SOURCE_CONTENT));
        when(kafkaTemplate.send(TOPIC, event.stationId(), JSON)).thenThrow(failure);

        assertThatThrownBy(() -> publisher.publish(event)).isSameAs(failure);

        assertSafeSingleLog("outcome=unknown", "stage=send", "causeType=TimeoutException");
    }

    @Test
    void correlatesEventsWhenFuturesCompleteInReverseOrder() {
        var secondEvent = telemetryEvent(UUID.fromString("d4ca2d0e-3e92-4085-9d24-984a119a3c34"));
        CompletableFuture<SendResult<String, String>> secondFuture = new CompletableFuture<>();
        when(objectMapper.writeValueAsString(secondEvent)).thenReturn(JSON);
        when(kafkaTemplate.send(TOPIC, event.stationId(), JSON)).thenReturn(future).thenReturn(secondFuture);

        publisher.publish(event);
        publisher.publish(secondEvent);
        secondFuture.complete(sendResult(43));
        future.complete(sendResult(42));

        assertThat(logs.list).hasSize(2);
        assertThat(logs.list.get(0).getFormattedMessage()).contains("eventId=" + secondEvent.eventId(), "offset=43");
        assertThat(logs.list.get(1).getFormattedMessage()).contains("eventId=" + event.eventId(), "offset=42");
    }

    private void assertSafeSingleLog(String... fields) {
        assertThat(logs.list).hasSize(1);
        var entry = logs.list.getFirst();
        assertThat(entry.getFormattedMessage()).contains(fields)
                .doesNotContain(SOURCE_CONTENT, JSON, event.stationId());
        assertThat(entry.getThrowableProxy()).isNull();
    }

    private static Stream<Throwable> timeoutFailures() {
        return Stream.of(
                new org.apache.kafka.common.errors.TimeoutException(SOURCE_CONTENT),
                new java.util.concurrent.TimeoutException(SOURCE_CONTENT),
                new CompletionException(new KafkaProducerException(record(), SOURCE_CONTENT,
                        new org.apache.kafka.common.errors.TimeoutException(SOURCE_CONTENT)))
        );
    }

    private static ProducerRecord<String, String> record() {
        return new ProducerRecord<>(TOPIC, "EV001", JSON);
    }

    private static SendResult<String, String> sendResult(long offset) {
        return new SendResult<>(record(), new RecordMetadata(new TopicPartition(TOPIC, 2),
                offset, 0, 1704067200000L, 5, JSON.length()));
    }

    private static DomainEventEnvelope<ChargerTelemetryPayload> telemetryEvent(UUID eventId) {
        Instant occurredAt = Instant.parse("2024-01-01T00:00:00Z");
        return ChargerTelemetryReceived.create(eventId, occurredAt, occurredAt.plusMillis(123),
                "EV001", new EvseId(1), null, new ChargerTelemetryPayload(occurredAt.toEpochMilli(),
                        true, new BigDecimal("120.0"), new BigDecimal("24.0"), new BigDecimal("5.0")));
    }
}
