select
    symbol,
    event_type,
    last_price,
    volume,
    vwap,
    low_price,
    high_price,
    change,
    change_pct,
    event_timestamp,
    event_date
from {{ ref('stg_kraken_ticker') }}
qualify row_number() over (
    partition by symbol
    order by event_timestamp desc
) = 1
