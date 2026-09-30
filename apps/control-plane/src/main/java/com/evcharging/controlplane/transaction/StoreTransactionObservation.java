package com.evcharging.controlplane.transaction;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
class StoreTransactionObservation {
    private final JdbcTransactionInbox inbox;
    private final TransactionRecoveryRules rules;
    private final boolean experimentalSessionConfirmation;

    StoreTransactionObservation(JdbcTransactionInbox inbox, TransactionRecoveryRules rules,
            @Value("${transaction.recovery.experimental-session-confirmation-enabled:false}")
            boolean experimentalSessionConfirmation) {
        this.inbox = inbox;
        this.rules = rules;
        this.experimentalSessionConfirmation = experimentalSessionConfirmation;
    }

    @Transactional
    public void store(ObservedTransactionRecord event) {
        JdbcTransactionInbox.State state = inbox.ensureAndLock(event);
        if (!state.stationId().equals(event.stationId())) {
            inbox.preserveConflict(event);
            holdConflict(event, state);
            return;
        }
        boolean inserted = inbox.insertEvent(event);
        if (!inserted) {
            if (!inbox.sameSource(event)) {
                inbox.preserveConflict(event);
                holdConflict(event, state);
                return;
            }
            if ("FINALIZED".equals(state.status())) return;
        }
        if ("HOLD_CONFLICT".equals(state.status()) || "HOLD_AFTER_FINALIZATION".equals(state.status())) {
            return;
        }
        TransactionRecoveryRules.Outcome outcome = rules.evaluate(inbox.events(event));
        if ("FINALIZED".equals(state.status()) && !"FINALIZED".equals(outcome.status())) {
            inbox.updateState(event, "HOLD_AFTER_FINALIZATION", "LATE_INVALID_EVENT", null);
            return;
        }
        if ("FINALIZED".equals(outcome.status())) {
            if (!experimentalSessionConfirmation) {
                inbox.updateState(event, "HOLD", "METER_POLICY_PENDING", outcome.evseId());
                return;
            }
            inbox.insertSession(event, outcome);
        }
        inbox.updateState(event, outcome.status(), outcome.reason(), outcome.evseId());
    }

    private void holdConflict(ObservedTransactionRecord event, JdbcTransactionInbox.State previous) {
        String status = "FINALIZED".equals(previous.status())
                ? "HOLD_AFTER_FINALIZATION" : "HOLD_CONFLICT";
        inbox.updateState(event, status, "SOURCE_CONFLICT", null);
    }
}
