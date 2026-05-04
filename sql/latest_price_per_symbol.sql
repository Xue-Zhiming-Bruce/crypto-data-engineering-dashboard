SELECT DISTINCT ON (symbol)
    symbol,
    last_price,
    volume,
    change,
    change_pct,
    event_timestamp
FROM kraken_ticker
ORDER BY symbol, event_timestamp DESC;
