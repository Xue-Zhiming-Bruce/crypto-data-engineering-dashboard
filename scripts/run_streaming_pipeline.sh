#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

if [ "$#" -gt 0 ]; then
  SYMBOLS=("$@")
else
  SYMBOLS=("BTC/USD" "ETH/USD" "SOL/USD")
fi
PIDS=()

cleanup() {
  if [ "${#PIDS[@]}" -gt 0 ]; then
    echo "Stopping streaming worker processes..."
    kill "${PIDS[@]}" 2>/dev/null || true
    wait "${PIDS[@]}" 2>/dev/null || true
  fi
}

trap cleanup EXIT INT TERM

if [ ! -x ".venv/bin/python" ]; then
  echo "Project virtual environment not found. Run:"
  echo "python3 -m venv .venv"
  echo ".venv/bin/python -m pip install -r requirements.txt"
  exit 1
fi

echo "Starting PostgreSQL and Kafka..."
docker compose up -d postgres kafka

echo "Starting Kafka to PostgreSQL consumer..."
.venv/bin/python src/streaming/consume_kraken_ticker_to_postgres.py &
PIDS+=("$!")

echo "Starting Kraken to Kafka producer for: ${SYMBOLS[*]}"
.venv/bin/python src/ingestion/produce_kraken_ticker_to_kafka.py \
  --symbols "${SYMBOLS[@]}" &
PIDS+=("$!")

echo "Starting Streamlit dashboard in streaming mode..."
DASHBOARD_SOURCE=postgres .venv/bin/streamlit run dashboard/app.py
