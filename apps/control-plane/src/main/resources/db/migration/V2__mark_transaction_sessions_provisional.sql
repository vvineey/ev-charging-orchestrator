ALTER TABLE recovered_transaction_session RENAME TO transaction_session_candidate;
ALTER TABLE transaction_session_candidate RENAME COLUMN confirmed_at TO calculated_at;

ALTER TABLE transaction_session_candidate
    ADD COLUMN candidate_status TEXT NOT NULL DEFAULT 'PROVISIONAL'
        CHECK (candidate_status IN ('PROVISIONAL', 'INVALIDATED')),
    ADD COLUMN invalidated_at TIMESTAMPTZ(6),
    ADD COLUMN invalidation_reason TEXT;

UPDATE transaction_session_candidate AS candidate
SET candidate_status = 'INVALIDATED',
    invalidated_at = clock_timestamp(),
    invalidation_reason = COALESCE(state.hold_reason, 'MIGRATED_HOLD')
FROM transaction_inbox_state AS state
WHERE candidate.charging_station_id = state.charging_station_id
  AND candidate.transaction_id = state.transaction_id
  AND state.status <> 'FINALIZED';

UPDATE transaction_inbox_state SET status = 'PROVISIONAL' WHERE status = 'FINALIZED';
UPDATE transaction_inbox_state SET status = 'HOLD_AFTER_PROVISIONAL'
WHERE status = 'HOLD_AFTER_FINALIZATION';

ALTER TABLE transaction_session_candidate ADD CONSTRAINT transaction_candidate_invalidation_consistent
    CHECK ((candidate_status = 'PROVISIONAL' AND invalidated_at IS NULL AND invalidation_reason IS NULL)
        OR (candidate_status = 'INVALIDATED' AND invalidated_at IS NOT NULL
            AND invalidation_reason IS NOT NULL));
