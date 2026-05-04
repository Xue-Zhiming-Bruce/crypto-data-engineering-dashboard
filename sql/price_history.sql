SELECT
    symbol,
    last_price,
    volume,
    event_timestamp
FROM kraken_ticker
ORDER BY event_timestamp, symbol;
