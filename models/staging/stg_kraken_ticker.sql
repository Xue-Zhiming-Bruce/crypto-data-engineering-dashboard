select
    symbol,
    event_type,
    cast(last_price as numeric) as last_price,
    cast(volume as numeric) as volume,
    cast(vwap as numeric) as vwap,
    cast(low_price as numeric) as low_price,
    cast(high_price as numeric) as high_price,
    cast(change as numeric) as change,
    cast(change_pct as numeric) as change_pct,
    cast(event_timestamp as timestamp) as event_timestamp,
    date(event_timestamp) as event_date
from {{ source('crypto_raw', 'kraken_ticker') }}
where symbol is not null
  and event_timestamp is not null
