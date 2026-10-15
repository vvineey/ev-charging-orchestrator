package com.evcharging.controlplane.transaction;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
class StoreTransactionObservation {
    private final JdbcTransactionInbox inbox;
    private final TransactionRecoveryRules rules;
    private final boolean experimentalCandidateCalculation;

    StoreTransactionObservation(JdbcTransactionInbox inbox, TransactionRecoveryRules rules,
            @Value("${transaction.recovery.experimental-candidate-calculation-enabled:false}")
            boolean experimentalCandidateCalculation) {
        this.inbox = inbox;
        this.rules = rules;
        this.experimentalCandidateCalculation = experimentalCandidateCalculation;
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
            if ("PROVISIONAL".equals(state.status())) return;
        }
        if ("HOLD_CONFLICT".equals(state.status()) || "HOLD_AFTER_PROVISIONAL".equals(state.status())) {
            return;
        }
        TransactionRecoveryRules.Outcome outcome = rules.evaluate(inbox.events(event));
        if ("PROVISIONAL".equals(state.status()) && !"CALCULATED".equals(outcome.status())) {
            inbox.invalidateCandidate(event, "LATE_INVALID_EVENT");
            inbox.updateState(event, "HOLD_AFTER_PROVISIONAL", "LATE_INVALID_EVENT", null);
            return;
        }
        if ("CALCULATED".equals(outcome.status())) {
            if (!experimentalCandidateCalculation) {
                inbox.updateState(event, "HOLD", "METER_POLICY_PENDING", outcome.evseId());
                return;
            }
            inbox.insertCandidate(event, outcome);
            inbox.updateState(event, "PROVISIONAL", null, outcome.evseId());
            return;
        }
        inbox.updateState(event, outcome.status(), outcome.reason(), outcome.evseId());
    }

    private void holdConflict(ObservedTransactionRecord event, JdbcTransactionInbox.State previous) {
        if ("HOLD_AFTER_PROVISIONAL".equals(previous.status())) return;
        boolean provisional = "PROVISIONAL".equals(previous.status());
        if (provisional) inbox.invalidateCandidate(event, "SOURCE_CONFLICT");
        String status = provisional ? "HOLD_AFTER_PROVISIONAL" : "HOLD_CONFLICT";
        inbox.updateState(event, status, "SOURCE_CONFLICT", null);
    }
}
