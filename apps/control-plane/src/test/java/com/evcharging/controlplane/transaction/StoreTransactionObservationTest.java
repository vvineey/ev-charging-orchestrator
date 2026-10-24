package com.evcharging.controlplane.transaction;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class StoreTransactionObservationTest {
    private final JdbcTransactionInbox inbox = mock(JdbcTransactionInbox.class);
    private final TransactionRecoveryRules rules = mock(TransactionRecoveryRules.class);
    private final ObservedTransactionRecord event = new ObservedTransactionRecord(UUID.randomUUID(), "ST-TEST",
            "CS-TEST", "TX-TEST", 2, "Ended", Instant.parse("2026-01-01T10:20:00Z"),
            Instant.parse("2026-01-01T10:20:01Z"), "{}", null);

    @Test
    void liveIngressDoesNotConfirmEnergyBeforeMeterPolicyApproval() {
        var result = new TransactionRecoveryRules.Outcome("CALCULATED", null, 1,
                Instant.parse("2026-01-01T10:00:00Z"), Instant.parse("2026-01-01T10:20:00Z"),
                new BigDecimal("2500"));
        when(inbox.ensureAndLock(event)).thenReturn(new JdbcTransactionInbox.State("ST-TEST", "PENDING"));
        when(inbox.insertEvent(event)).thenReturn(true);
        when(inbox.events(event)).thenReturn(List.of());
        when(rules.evaluate(List.of())).thenReturn(result);

        new StoreTransactionObservation(inbox, rules, false).store(event);
        verify(inbox).updateState(event, "HOLD", "METER_POLICY_PENDING", 1);
        verify(inbox, never()).insertCandidate(event, result);
    }

    @Test
    void experimentalFixtureCanStoreOneProvisionalCandidate() {
        var result = new TransactionRecoveryRules.Outcome("CALCULATED", null, 1,
                Instant.parse("2026-01-01T10:00:00Z"), Instant.parse("2026-01-01T10:20:00Z"),
                new BigDecimal("2500"));
        when(inbox.ensureAndLock(event)).thenReturn(new JdbcTransactionInbox.State("ST-TEST", "PENDING"));
        when(inbox.insertEvent(event)).thenReturn(true);
        when(inbox.events(event)).thenReturn(List.of());
        when(rules.evaluate(List.of())).thenReturn(result);

        new StoreTransactionObservation(inbox, rules, true).store(event);
        verify(inbox).insertCandidate(event, result);
        verify(inbox).updateState(event, "PROVISIONAL", null, 1);
    }

    @Test
    void conflictAfterProvisionalCalculationInvalidatesCandidate() {
        when(inbox.ensureAndLock(event)).thenReturn(new JdbcTransactionInbox.State("ST-TEST", "PROVISIONAL"));
        when(inbox.insertEvent(event)).thenReturn(false);
        when(inbox.sameSource(event)).thenReturn(false);

        new StoreTransactionObservation(inbox, rules, true).store(event);
        verify(inbox).preserveConflict(event);
        verify(inbox).invalidateCandidate(event, "SOURCE_CONFLICT");
        verify(inbox).updateState(event, "HOLD_AFTER_PROVISIONAL", "SOURCE_CONFLICT", null);
        verify(inbox, never()).insertCandidate(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any());
    }

    @Test
    void lateNewEventAfterProvisionalCalculationIsHeldForReview() {
        when(inbox.ensureAndLock(event)).thenReturn(new JdbcTransactionInbox.State("ST-TEST", "PROVISIONAL"));
        when(inbox.insertEvent(event)).thenReturn(true);
        when(inbox.events(event)).thenReturn(List.of());
        when(rules.evaluate(List.of())).thenReturn(TransactionRecoveryRules.Outcome.hold("INVALID_SEQUENCE"));

        new StoreTransactionObservation(inbox, rules, true).store(event);
        verify(inbox).invalidateCandidate(event, "LATE_INVALID_EVENT");
        verify(inbox).updateState(event, "HOLD_AFTER_PROVISIONAL", "LATE_INVALID_EVENT", null);
    }

    @Test
    void exactReplayCanReevaluateHeldFixtureAfterPolicyChange() {
        var result = new TransactionRecoveryRules.Outcome("CALCULATED", null, 1,
                Instant.parse("2026-01-01T10:00:00Z"), Instant.parse("2026-01-01T10:20:00Z"),
                new BigDecimal("2500"));
        when(inbox.ensureAndLock(event)).thenReturn(new JdbcTransactionInbox.State("ST-TEST", "HOLD"));
        when(inbox.insertEvent(event)).thenReturn(false);
        when(inbox.sameSource(event)).thenReturn(true);
        when(inbox.events(event)).thenReturn(List.of());
        when(rules.evaluate(List.of())).thenReturn(result);

        new StoreTransactionObservation(inbox, rules, true).store(event);
        verify(inbox).insertCandidate(event, result);
    }
}
