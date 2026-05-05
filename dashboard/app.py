import os
import sys
import time
from pathlib import Path

from google.cloud import bigquery
import pandas as pd
import psycopg
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from src.ingestion.list_kraken_usd_pairs import fetch_asset_pairs, usd_pairs


DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://crypto:crypto@localhost:5432/crypto",
)
DASHBOARD_SOURCE = os.environ.get("DASHBOARD_SOURCE", "postgres").lower()
BIGQUERY_PROJECT_ID = os.environ.get("BIGQUERY_PROJECT_ID", "dido-486313")
BIGQUERY_DATASET_ID = os.environ.get("BIGQUERY_DATASET_ID", "crypto_analytics")
DEFAULT_SYMBOLS = ["BTC/USD", "ETH/USD", "SOL/USD"]
REFRESH_INTERVAL_SECONDS = 1


@st.cache_data(ttl=5)
def read_postgres(query: str) -> pd.DataFrame:
    with psycopg.connect(DATABASE_URL) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
            columns = [column.name for column in cursor.description]

    return pd.DataFrame(rows, columns=columns)


@st.cache_data(ttl=5)
def read_bigquery(query: str, project_id: str) -> pd.DataFrame:
    client = bigquery.Client(project=project_id)
    result = client.query(query).result()
    rows = [dict(row) for row in result]
    columns = [field.name for field in result.schema]

    return pd.DataFrame(rows, columns=columns)


def read_sql(query: str, source: str) -> pd.DataFrame:
    if source == "bigquery":
        return read_bigquery(query, BIGQUERY_PROJECT_ID)
    return read_postgres(query)


@st.cache_data(ttl=3600)
def kraken_usd_pairs() -> list[str]:
    return usd_pairs(fetch_asset_pairs())


def bigquery_table(table_id: str) -> str:
    return f"`{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{table_id}`"


def postgres_ticker_table() -> str:
    return "kraken_ticker"


def latest_prices_query(source: str) -> str:
    if source == "bigquery":
        table = bigquery_table("latest_price_per_symbol")
        return f"""
SELECT
    symbol,
    last_price,
    volume,
    change,
    change_pct,
    event_timestamp
FROM {table}
ORDER BY symbol;
"""

    table = postgres_ticker_table()
    return f"""
SELECT
    symbol,
    last_price,
    volume,
    change,
    change_pct,
    event_timestamp
FROM (
    SELECT
        symbol,
        last_price,
        volume,
        change,
        change_pct,
        event_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY symbol
            ORDER BY event_timestamp DESC
        ) AS row_number
    FROM {table}
)
WHERE row_number = 1
ORDER BY symbol;
"""


def price_history_query(source: str) -> str:
    if source == "bigquery":
        table = bigquery_table("price_history")
    else:
        table = postgres_ticker_table()

    return f"""
SELECT
    symbol,
    last_price,
    volume,
    event_timestamp
FROM {table}
ORDER BY event_timestamp, symbol;
"""


def recent_ticks_query(source: str) -> str:
    if source == "bigquery":
        table = bigquery_table("price_history")
    else:
        table = postgres_ticker_table()

    return f"""
SELECT
    symbol,
    last_price,
    volume,
    event_timestamp
FROM {table}
ORDER BY event_timestamp DESC, symbol
LIMIT 200;
"""


def records_by_symbol_query(source: str) -> str:
    if source == "bigquery":
        table = bigquery_table("message_count_per_symbol")
        return f"""
SELECT
    symbol,
    record_count
FROM {table}
ORDER BY symbol;
"""

    table = postgres_ticker_table()
    return f"""
SELECT
    symbol,
    COUNT(*) AS record_count
FROM {table}
GROUP BY symbol
ORDER BY symbol;
"""


st.set_page_config(page_title="Crypto Market Dashboard", layout="wide")
st.title("Crypto Market Dashboard")

with st.sidebar:
    st.header("Mode")
    default_mode = "Batch" if DASHBOARD_SOURCE == "bigquery" else "Streaming"
    dashboard_mode = st.radio(
        "Data mode",
        options=["Batch", "Streaming"],
        index=["Batch", "Streaming"].index(default_mode),
    )
    data_source = "bigquery" if dashboard_mode == "Batch" else "postgres"
    st.header("Refresh")
    if st.button("Refresh data"):
        st.cache_data.clear()
        st.rerun()
    auto_refresh = dashboard_mode == "Streaming" and st.checkbox(
        "Auto refresh",
        value=True,
    )

latest_prices = read_sql(latest_prices_query(data_source), data_source)
price_history = read_sql(price_history_query(data_source), data_source)
recent_ticks = read_sql(recent_ticks_query(data_source), data_source)
records_by_symbol = read_sql(records_by_symbol_query(data_source), data_source)

if not price_history.empty:
    price_history["event_timestamp"] = pd.to_datetime(price_history["event_timestamp"])
    price_history["last_price"] = pd.to_numeric(price_history["last_price"])

if not latest_prices.empty:
    latest_prices["event_timestamp"] = pd.to_datetime(latest_prices["event_timestamp"])
    latest_prices["last_price"] = pd.to_numeric(latest_prices["last_price"])

if not recent_ticks.empty:
    recent_ticks["event_timestamp"] = pd.to_datetime(recent_ticks["event_timestamp"])
    recent_ticks["last_price"] = pd.to_numeric(recent_ticks["last_price"])

if not records_by_symbol.empty:
    records_by_symbol["record_count"] = pd.to_numeric(
        records_by_symbol["record_count"]
    )

try:
    available_symbols = kraken_usd_pairs()
except Exception as error:
    st.sidebar.warning(f"Could not load Kraken symbols: {error}")
    available_symbols = sorted(price_history["symbol"].dropna().unique())

default_symbols = [
    symbol for symbol in DEFAULT_SYMBOLS if symbol in available_symbols
] or available_symbols[:3]

with st.sidebar:
    st.header("Filters")
    selected_symbols = st.multiselect(
        "Symbols",
        options=available_symbols,
        default=default_symbols,
    )

latest_prices = latest_prices[latest_prices["symbol"].isin(selected_symbols)]
price_history = price_history[price_history["symbol"].isin(selected_symbols)]
recent_ticks = recent_ticks[recent_ticks["symbol"].isin(selected_symbols)].head(20)
records_by_symbol = records_by_symbol[
    records_by_symbol["symbol"].isin(selected_symbols)
]

latest_event_time = None
if not price_history.empty:
    latest_event_time = price_history["event_timestamp"].max()

metric_columns = st.columns(2)
metric_columns[0].metric("Selected Symbols", len(selected_symbols))
metric_columns[1].metric(
    "Latest Event Time",
    latest_event_time.strftime("%Y-%m-%d %H:%M:%S UTC")
    if latest_event_time is not None
    else "No data",
)

st.subheader("Latest Prices")
if latest_prices.empty:
    st.info("No latest price data loaded yet.")
else:
    price_columns = st.columns(len(latest_prices))
    for column, row in zip(price_columns, latest_prices.itertuples(index=False)):
        change_pct = pd.to_numeric(row.change_pct, errors="coerce")
        delta = f"{change_pct:.2f}%" if pd.notna(change_pct) else None
        column.metric(
            row.symbol,
            f"{row.last_price:,.2f}",
            delta=delta,
        )

st.dataframe(latest_prices, width="stretch")

st.subheader("Recent Ticks")
st.dataframe(recent_ticks, width="stretch")

st.subheader("Records by Symbol")
if records_by_symbol.empty:
    st.info("No record count data loaded yet.")
else:
    st.bar_chart(
        records_by_symbol.set_index("symbol")["record_count"],
        x_label="Symbol",
        y_label="Records",
    )

st.subheader("Price History")
if price_history.empty:
    st.info("No price history loaded yet.")
else:
    chart_data = price_history.pivot_table(
        index="event_timestamp",
        columns="symbol",
        values="last_price",
        aggfunc="last",
    ).sort_index()
    st.line_chart(chart_data)

if auto_refresh:
    time.sleep(REFRESH_INTERVAL_SECONDS)
    st.rerun()
