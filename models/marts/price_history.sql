select
    symbol,
    last_price,
    volume,
    event_timestamp,
    event_date
from {{ ref('stg_kraken_ticker') }}
