select
    symbol,
    timestamp_trunc(event_timestamp, hour) as event_hour,
    date(timestamp_trunc(event_timestamp, hour)) as event_date,
    count(*) as message_count,
    avg(last_price) as avg_last_price,
    min(last_price) as min_last_price,
    max(last_price) as max_last_price,
    max(event_timestamp) as latest_event_timestamp
from {{ ref('stg_kraken_ticker') }}
group by symbol, event_hour, event_date
