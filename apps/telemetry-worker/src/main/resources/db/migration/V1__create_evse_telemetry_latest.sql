CREATE TABLE evse_telemetry_latest (
    station_id TEXT NOT NULL CHECK (btrim(station_id) <> ''),
    evse_id INTEGER NOT NULL CHECK (evse_id > 0),
    charging BOOLEAN NOT NULL,
    power NUMERIC NOT NULL,
    voltage NUMERIC NOT NULL,
    current NUMERIC NOT NULL,
    occurred_at TIMESTAMPTZ(6) NOT NULL,
    received_at TIMESTAMPTZ(6) NOT NULL,
    last_event_id UUID NOT NULL,
    updated_at TIMESTAMPTZ(6) NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (station_id, evse_id)
);
