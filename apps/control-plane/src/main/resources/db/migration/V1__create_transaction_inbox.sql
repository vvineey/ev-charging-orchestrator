CREATE TABLE transaction_inbox_state (
    charging_station_id TEXT NOT NULL CHECK (btrim(charging_station_id) <> ''),
    transaction_id TEXT NOT NULL CHECK (btrim(transaction_id) <> ''),
    station_id TEXT NOT NULL CHECK (btrim(station_id) <> ''),
    status TEXT NOT NULL DEFAULT 'PENDING',
    hold_reason TEXT,
    evse_id INTEGER CHECK (evse_id > 0),
    updated_at TIMESTAMPTZ(6) NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (charging_station_id, transaction_id)
);

CREATE TABLE transaction_inbox_event (
    charging_station_id TEXT NOT NULL,
    transaction_id TEXT NOT NULL,
    seq_no INTEGER NOT NULL CHECK (seq_no >= 0),
    event_type TEXT NOT NULL CHECK (event_type IN ('Started', 'Updated', 'Ended')),
    occurred_at TIMESTAMPTZ(6) NOT NULL,
    first_received_at TIMESTAMPTZ(6) NOT NULL,
    first_event_id UUID NOT NULL,
    source_document JSONB NOT NULL,
    PRIMARY KEY (charging_station_id, transaction_id, seq_no),
    FOREIGN KEY (charging_station_id, transaction_id)
        REFERENCES transaction_inbox_state (charging_station_id, transaction_id)
);

CREATE TABLE transaction_inbox_conflict (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    charging_station_id TEXT NOT NULL,
    transaction_id TEXT NOT NULL,
    seq_no INTEGER NOT NULL CHECK (seq_no >= 0),
    event_id UUID NOT NULL,
    received_at TIMESTAMPTZ(6) NOT NULL,
    source_document JSONB NOT NULL,
    FOREIGN KEY (charging_station_id, transaction_id)
        REFERENCES transaction_inbox_state (charging_station_id, transaction_id)
);

CREATE INDEX transaction_inbox_conflict_source_idx
    ON transaction_inbox_conflict (charging_station_id, transaction_id, seq_no);

CREATE TABLE recovered_transaction_session (
    charging_station_id TEXT NOT NULL,
    transaction_id TEXT NOT NULL,
    station_id TEXT NOT NULL,
    evse_id INTEGER NOT NULL CHECK (evse_id > 0),
    started_at TIMESTAMPTZ(6) NOT NULL,
    ended_at TIMESTAMPTZ(6) NOT NULL,
    energy_wh NUMERIC(24, 6) NOT NULL CHECK (energy_wh >= 0),
    confirmed_at TIMESTAMPTZ(6) NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (charging_station_id, transaction_id),
    FOREIGN KEY (charging_station_id, transaction_id)
        REFERENCES transaction_inbox_state (charging_station_id, transaction_id)
);
