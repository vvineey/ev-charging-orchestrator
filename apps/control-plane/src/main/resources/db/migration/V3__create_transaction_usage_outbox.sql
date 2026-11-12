ALTER TABLE transaction_session_candidate
    ADD COLUMN aggregate_version BIGINT NOT NULL DEFAULT 0 CHECK (aggregate_version >= 0);

CREATE TABLE transaction_usage_outbox (
    event_id UUID PRIMARY KEY,
    charging_station_id TEXT NOT NULL,
    transaction_id TEXT NOT NULL,
    aggregate_version BIGINT NOT NULL CHECK (aggregate_version > 0),
    event_type TEXT NOT NULL CHECK (event_type IN ('UsageProvisionalCalculated', 'UsageProvisionalInvalidated')),
    topic TEXT NOT NULL CHECK (btrim(topic) <> ''),
    message_key TEXT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ(6) NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (charging_station_id, transaction_id, aggregate_version),
    FOREIGN KEY (charging_station_id, transaction_id)
        REFERENCES transaction_session_candidate (charging_station_id, transaction_id)
);

CREATE TABLE transaction_usage_outbox_delivery (
    event_id UUID PRIMARY KEY REFERENCES transaction_usage_outbox (event_id),
    status TEXT NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'PROCESSING', 'PUBLISHED', 'BLOCKED')),
    claim_token UUID,
    claimed_at TIMESTAMPTZ(6),
    claim_until TIMESTAMPTZ(6),
    published_at TIMESTAMPTZ(6),
    last_error_type TEXT,
    CHECK ((status = 'PROCESSING' AND claim_token IS NOT NULL AND claimed_at IS NOT NULL AND claim_until IS NOT NULL)
        OR (status <> 'PROCESSING' AND claim_token IS NULL AND claimed_at IS NULL AND claim_until IS NULL)),
    CHECK ((status = 'PUBLISHED') = (published_at IS NOT NULL)),
    CHECK ((status = 'BLOCKED') = (last_error_type IS NOT NULL))
);

CREATE INDEX transaction_usage_outbox_delivery_claim_idx
    ON transaction_usage_outbox_delivery (status, claim_until) WHERE published_at IS NULL;
