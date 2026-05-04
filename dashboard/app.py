import os
import sys
import time
from pathlib import Path

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
DEFAULT_SYMBOLS = ["BTC/USD", "ETH/USD", "SOL/USD"]
REFRESH_INTERVAL_SECONDS = 5


@st.cache_data(ttl=5)
def read_sql(query: str) -> pd.DataFrame:
    with psycopg.connect(DATABASE_URL) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
            columns = [column.name for column in cursor.description]

    return pd.DataFrame(rows, columns=columns)


@st.cache_data(ttl=3600)
def kraken_usd_pairs() -> list[str]:
    return usd_pairs(fetch_asset_pairs())


latest_prices_query = """
SELECT DISTINCT ON (symbol)
    symbol,
    last_price,
    volume,
    change,
    change_pct,
    event_timestamp
FROM kraken_ticker
ORDER BY symbol, event_timestamp DESC;
"""

price_history_query = """
SELECT
    symbol,
    last_price,
    volume,
    event_timestamp
FROM kraken_ticker
ORDER BY event_timestamp, symbol;
"""

recent_ticks_query = """
SELECT
    symbol,
    last_price,
    volume,
    change,
    change_pct,
    event_timestamp
FROM kraken_ticker
ORDER BY event_timestamp DESC, symbol
LIMIT 200;
"""


st.set_page_config(page_title="Crypto Market Dashboard", layout="wide")
st.title("Crypto Market Dashboard")

with st.sidebar:
    st.header("Mode")
    dashboard_mode = st.radio(
        "Data mode",
        options=["Batch", "Streaming"],
    )
    st.header("Refresh")
    if st.button("Refresh data"):
        st.cache_data.clear()
        st.rerun()
    auto_refresh = dashboard_mode == "Streaming" and st.checkbox(
        "Auto refresh",
        value=True,
    )

latest_prices = read_sql(latest_prices_query)
price_history = read_sql(price_history_query)
recent_ticks = read_sql(recent_ticks_query)

if not price_history.empty:
    price_history["event_timestamp"] = pd.to_datetime(price_history["event_timestamp"])
    price_history["last_price"] = pd.to_numeric(price_history["last_price"])

if not latest_prices.empty:
    latest_prices["event_timestamp"] = pd.to_datetime(latest_prices["event_timestamp"])
    latest_prices["last_price"] = pd.to_numeric(latest_prices["last_price"])

if not recent_ticks.empty:
    recent_ticks["event_timestamp"] = pd.to_datetime(recent_ticks["event_timestamp"])
    recent_ticks["last_price"] = pd.to_numeric(recent_ticks["last_price"])

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
