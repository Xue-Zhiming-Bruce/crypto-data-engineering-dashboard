select
    symbol,
    count(*) as record_count,
    min(event_timestamp) as first_event_timestamp,
    max(event_timestamp) as latest_event_timestamp,
    max(event_date) as event_date
from {{ ref('stg_kraken_ticker') }}
group by symbol
