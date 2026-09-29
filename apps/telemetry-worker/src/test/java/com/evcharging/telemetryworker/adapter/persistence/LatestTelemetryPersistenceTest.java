package com.evcharging.telemetryworker.adapter.persistence;

import com.evcharging.messaging.contract.ChargerTelemetryPayload;
import com.evcharging.messaging.contract.ChargerTelemetryReceived;
import com.evcharging.messaging.contract.DomainEventEnvelope;
import com.evcharging.messaging.contract.EvseId;
import com.evcharging.telemetryworker.PostgreSqlTestConfiguration;
import com.evcharging.telemetryworker.application.StoreLatestTelemetryService;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;

@SpringBootTest
@Import(PostgreSqlTestConfiguration.class)
class LatestTelemetryPersistenceTest {

    private static final String STATION = "station-fixture";
    private static final Instant OBSERVED = Instant.parse("2026-09-29T00:00:00Z");
    private static final Instant RECEIVED = Instant.parse("2026-09-29T00:00:01Z");

    @Autowired
    private StoreLatestTelemetryService service;

    @Autowired
    private JdbcClient jdbc;

    @Autowired
    private Flyway flyway;

    @MockitoSpyBean
    private JdbcLatestTelemetryRepository repository;

    @BeforeEach
    void clearLatestState() {
        jdbc.sql("DELETE FROM evse_telemetry_latest").update();
    }

    @Test
    void migratesAndCommitsFirstObservationWithExactDecimalValues() {
        var event = event(1, OBSERVED, RECEIVED, "120.12345678901234567890123");

        assertThat(service.store(event)).isTrue();

        var row = read(STATION, 1);
        assertThat(row.eventId()).isEqualTo(new UUID(0, 1));
        assertThat(row.charging()).isFalse();
        assertThat(row.power()).isEqualByComparingTo("120.12345678901234567890123");
        assertThat(row.voltage()).isEqualByComparingTo("220.75");
        assertThat(row.current()).isEqualByComparingTo("0.54567890123456789");
        assertThat(row.occurredAt()).isEqualTo(OBSERVED);
        assertThat(row.receivedAt()).isEqualTo(RECEIVED);
        assertThat(row.updatedAt()).isNotNull();
        assertThat(count()).isEqualTo(1);
        assertThat(flyway.info().current().getVersion().getVersion()).isEqualTo("1");
    }

    @Test
    void newerObservationReplacesAllValuesAndUpdatesProcessingTime() {
        service.store(event(1, OBSERVED, RECEIVED, "120.25"));
        jdbc.sql("UPDATE evse_telemetry_latest SET updated_at = '2020-01-01T00:00:00Z'").update();

        var newer = ChargerTelemetryReceived.create(new UUID(0, 2), OBSERVED.plusSeconds(1),
                RECEIVED.plusSeconds(1), STATION, new EvseId(1), null,
                new ChargerTelemetryPayload(OBSERVED.plusSeconds(1).toEpochMilli(), true,
                        new BigDecimal("99.875"), new BigDecimal("230.125"), new BigDecimal("0.875")));
        assertThat(service.store(newer)).isTrue();

        var row = read(STATION, 1);
        assertThat(row.eventId()).isEqualTo(new UUID(0, 2));
        assertThat(row.charging()).isTrue();
        assertThat(row.power()).isEqualByComparingTo("99.875");
        assertThat(row.voltage()).isEqualByComparingTo("230.125");
        assertThat(row.current()).isEqualByComparingTo("0.875");
        assertThat(row.occurredAt()).isEqualTo(Instant.parse("2026-09-29T00:00:01Z"));
        assertThat(row.receivedAt()).isEqualTo(Instant.parse("2026-09-29T00:00:02Z"));
        assertThat(row.updatedAt()).isAfter(Instant.parse("2020-01-01T00:00:00Z"));
        assertThat(count()).isEqualTo(1);
    }

    @Test
    void observedTimeTakesPriorityOverAnOlderReceivedTime() {
        service.store(event(1, OBSERVED, RECEIVED, "120.25"));

        assertThat(service.store(event(2, OBSERVED.plusSeconds(1), RECEIVED.minusSeconds(1), "99.875")))
                .isTrue();
        assertThat(read(STATION, 1).eventId()).isEqualTo(new UUID(0, 2));
    }

    @Test
    void ignoresPastObservationEvenWhenReceivedLater() {
        service.store(event(1, OBSERVED, RECEIVED, "120.25"));
        var before = read(STATION, 1);

        assertThat(service.store(event(2, OBSERVED.minusSeconds(1), RECEIVED.plusSeconds(1), "999.99")))
                .isFalse();
        assertThat(read(STATION, 1)).isEqualTo(before);
    }

    @Test
    void redeliveredEventDoesNotChangeAnyStoredField() {
        var event = event(1, OBSERVED, RECEIVED, "120.25");
        service.store(event);
        var before = read(STATION, 1);

        assertThat(service.store(event)).isFalse();
        assertThat(read(STATION, 1)).isEqualTo(before);
    }

    @Test
    void equalObservedTimeUsesNewerReceivedTime() {
        service.store(event(1, OBSERVED, RECEIVED, "120.25"));

        assertThat(service.store(event(2, OBSERVED, RECEIVED.plusSeconds(1), "99.875"))).isTrue();
        var row = read(STATION, 1);
        assertThat(row.eventId()).isEqualTo(new UUID(0, 2));
        assertThat(row.power()).isEqualByComparingTo("99.875");
        assertThat(row.receivedAt()).isEqualTo(Instant.parse("2026-09-29T00:00:02Z"));
    }

    @Test
    void equalObservedTimeWithOlderReceivedTimeKeepsExistingRow() {
        service.store(event(1, OBSERVED, RECEIVED, "120.25"));
        var before = read(STATION, 1);

        assertThat(service.store(event(2, OBSERVED, RECEIVED.minusSeconds(1), "999.99"))).isFalse();
        assertThat(read(STATION, 1)).isEqualTo(before);
    }

    @ParameterizedTest
    @CsvSource({"1, 2", "2, 1"})
    void equalTimesKeepFirstRowRegardlessOfUuidOrder(int firstId, int nextId) {
        service.store(event(firstId, OBSERVED, RECEIVED, "120.25"));
        var before = read(STATION, 1);

        assertThat(service.store(event(nextId, OBSERVED, RECEIVED, "999.99"))).isFalse();
        assertThat(read(STATION, 1)).isEqualTo(before);
    }

    @Test
    void timesInSameMicrosecondKeepFirstRowAfterTruncation() {
        service.store(event(1, Instant.parse("2026-09-29T00:00:00.123456101Z"),
                Instant.parse("2026-09-29T00:00:01.234567101Z"), "120.25"));
        var before = read(STATION, 1);

        assertThat(service.store(event(2, Instant.parse("2026-09-29T00:00:00.123456999Z"),
                Instant.parse("2026-09-29T00:00:01.234567999Z"), "999.99"))).isFalse();
        assertThat(read(STATION, 1)).isEqualTo(before);
        assertThat(before.occurredAt()).isEqualTo(Instant.parse("2026-09-29T00:00:00.123456Z"));
        assertThat(before.receivedAt()).isEqualTo(Instant.parse("2026-09-29T00:00:01.234567Z"));
    }

    @Test
    void databaseRoundTripTruncatesWithoutMutatingOriginalEventOrRedelivery() {
        var observed = Instant.parse("2026-09-29T00:00:00.123456789Z");
        var received = Instant.parse("2026-09-29T00:00:01.987654999Z");
        var event = event(1, observed, received, "120.25");
        service.store(event);
        var before = read(STATION, 1);

        assertThat(before.occurredAt()).isEqualTo(Instant.parse("2026-09-29T00:00:00.123456Z"));
        assertThat(before.receivedAt()).isEqualTo(Instant.parse("2026-09-29T00:00:01.987654Z"));
        assertThat(event.occurredAt()).isEqualTo(observed);
        assertThat(event.receivedAt()).isEqualTo(received);
        assertThat(service.store(event)).isFalse();
        assertThat(read(STATION, 1)).isEqualTo(before);
    }

    @Test
    void stationAndEvseTogetherIdentifyIndependentRows() {
        service.store(event(1, OBSERVED, RECEIVED, "120.25"));
        service.store(event(2, "other-station-fixture", 1, OBSERVED, RECEIVED, "99.875"));
        service.store(event(3, STATION, 6, OBSERVED, RECEIVED, "10.5"));

        assertThat(count()).isEqualTo(3);
        assertThat(read(STATION, 1).eventId()).isEqualTo(new UUID(0, 1));
        assertThat(read("other-station-fixture", 1).eventId()).isEqualTo(new UUID(0, 2));
        assertThat(read(STATION, 6).eventId()).isEqualTo(new UUID(0, 3));
    }

    @Test
    void databaseFailureAfterInsertRollsBackServiceTransaction() {
        failAfterRealUpsert();

        assertThatThrownBy(() -> service.store(event(1, OBSERVED, RECEIVED, "120.25")))
                .isInstanceOf(DataAccessException.class);
        assertThat(count()).isZero();
    }

    @Test
    void databaseFailureAfterUpdateRestoresPreviouslyCommittedRow() {
        service.store(event(1, OBSERVED, RECEIVED, "120.25"));
        var before = read(STATION, 1);
        failAfterRealUpsert();

        assertThatThrownBy(() -> service.store(event(2, OBSERVED.plusSeconds(1), RECEIVED, "99.875")))
                .isInstanceOf(DataAccessException.class);
        assertThat(read(STATION, 1)).isEqualTo(before);
    }

    @Test
    void concurrentDifferentTimesLeaveOneRowWithNewestObservation() throws Exception {
        var start = new CountDownLatch(1);
        try (var executor = Executors.newFixedThreadPool(2)) {
            var older = executor.submit(() -> {
                start.await();
                return service.store(event(1, OBSERVED, RECEIVED.plusSeconds(10), "120.25"));
            });
            var newer = executor.submit(() -> {
                start.await();
                return service.store(event(2, OBSERVED.plusSeconds(1), RECEIVED, "99.875"));
            });
            start.countDown();
            older.get(10, TimeUnit.SECONDS);
            assertThat(newer.get(10, TimeUnit.SECONDS)).isTrue();
        }
        assertThat(count()).isEqualTo(1);
        assertThat(read(STATION, 1).eventId()).isEqualTo(new UUID(0, 2));
        assertThat(read(STATION, 1).power()).isEqualByComparingTo("99.875");
    }

    @ParameterizedTest
    @CsvSource({"OtherEvent, 1, 0", "ChargerTelemetryReceived, 2, 0", "ChargerTelemetryReceived, 1, 1"})
    void unsupportedContractDoesNotWriteState(String type, int version, long timestampShift) {
        var valid = event(1, OBSERVED, RECEIVED, "120.25");
        var payload = valid.payload();
        var invalid = new DomainEventEnvelope<>(valid.eventId(), type, version, OBSERVED, RECEIVED,
                STATION, 1, null, valid.correlationId(), new ChargerTelemetryPayload(
                payload.timestamp() + timestampShift, payload.charging(),
                payload.power(), payload.voltage(), payload.current()));

        assertThatThrownBy(() -> service.store(invalid)).isInstanceOf(IllegalArgumentException.class);
        assertThat(count()).isZero();
    }

    private void failAfterRealUpsert() {
        doAnswer(invocation -> {
            assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isTrue();
            invocation.callRealMethod();
            // The real SQL has run; a second SQL error must undo it in the service transaction.
            return jdbc.sql("SELECT 1 / 0").query(Integer.class).single();
        }).when(repository).upsert(any());
    }

    private long count() {
        return jdbc.sql("SELECT count(*) FROM evse_telemetry_latest").query(Long.class).single();
    }

    private StoredRow read(String station, int evse) {
        // No test transaction: this reads after the service has committed or rolled back.
        return jdbc.sql("SELECT * FROM evse_telemetry_latest WHERE station_id = :station AND evse_id = :evse")
                .param("station", station).param("evse", evse)
                .query((rs, rowNum) -> new StoredRow(
                        rs.getBoolean("charging"), rs.getBigDecimal("power"),
                        rs.getBigDecimal("voltage"), rs.getBigDecimal("current"),
                        rs.getObject("occurred_at", OffsetDateTime.class).toInstant(),
                        rs.getObject("received_at", OffsetDateTime.class).toInstant(),
                        rs.getObject("last_event_id", UUID.class),
                        rs.getObject("updated_at", OffsetDateTime.class).toInstant()
                )).single();
    }

    private static DomainEventEnvelope<ChargerTelemetryPayload> event(
            int id, Instant occurred, Instant received, String power) {
        return event(id, STATION, 1, occurred, received, power);
    }

    private static DomainEventEnvelope<ChargerTelemetryPayload> event(
            int id, String station, int evse, Instant occurred, Instant received, String power) {
        return ChargerTelemetryReceived.create(new UUID(0, id), occurred, received, station,
                new EvseId(evse), null, new ChargerTelemetryPayload(occurred.toEpochMilli(), id != 1,
                new BigDecimal(power), new BigDecimal("220.75"), new BigDecimal("0.54567890123456789")));
    }

    private record StoredRow(boolean charging, BigDecimal power, BigDecimal voltage, BigDecimal current,
                             Instant occurredAt, Instant receivedAt, UUID eventId, Instant updatedAt) {
    }
}
