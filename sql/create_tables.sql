CREATE TABLE IF NOT EXISTS kraken_ticker (
    id BIGSERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    event_type TEXT NOT NULL,
    last_price NUMERIC NOT NULL,
    volume NUMERIC,
    vwap NUMERIC,
    low_price NUMERIC,
    high_price NUMERIC,
    change NUMERIC,
    change_pct NUMERIC,
    event_timestamp TIMESTAMPTZ NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
