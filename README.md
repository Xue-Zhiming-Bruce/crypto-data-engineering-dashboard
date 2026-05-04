# Crypto Data Engineering Final Project

This project builds a small batch data pipeline using public Kraken crypto
ticker data. The goal is to practice the main steps of a data engineering
workflow: extracting data from an API, saving raw data, transforming it into a
clean format, checking data quality, loading it into a database, and visualizing
the result in a simple dashboard.

## Success Criteria

The local MVP is complete when:

- I can collect sample ticker data from Kraken.
- I can save the raw data locally.
- I can transform the raw data into a clean table.
- I can run basic data quality checks.
- I can load the clean data into PostgreSQL.
- I can view the data in a Streamlit dashboard.
- The README explains how to run the project from scratch.

## MVP Completion Checklist

- [x] API ingestion from Kraken.
- [x] Raw JSONL storage.
- [x] CSV transformation.
- [x] Basic data quality checks.
- [x] PostgreSQL table creation and loading.
- [x] SQL queries for latest prices and price history.
- [x] Streamlit dashboard.
- [x] Local run instructions.

Current MVP scope:

- Exchange: Kraken
- Symbols: `BTC/USD`, `ETH/USD`, `SOL/USD`
- Source: Kraken WebSocket ticker channel
- Raw format: JSONL
- Processed format: CSV
- Storage: PostgreSQL

## Project Status

This repository currently contains a working local MVP. It is designed as a
learning project, not a production crypto trading system.

Current limitations:

- Data is stored locally instead of in a cloud data lake.
- PostgreSQL is used as the local analytical database.
- The pipeline is run manually from the command line.
- Transformations are simple Python and SQL scripts instead of dbt models.

Possible future upgrades:

- Store raw and processed files in GCS.
- Load clean data into BigQuery.
- Add dbt models and tests for analytics transformations.
- Schedule the pipeline with an orchestrator such as Kestra.
- Extend the Kafka streaming path with more production-like topic and consumer
  design.

## Setup

Create and use the project virtual environment:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
```

Start PostgreSQL:

```bash
docker compose up -d
```

## Run The Full Local MVP

After setting up the virtual environment and starting PostgreSQL, run the full
local pipeline:

```bash
.venv/bin/python src/pipeline/run_local_pipeline.py \
  --symbols BTC/USD ETH/USD SOL/USD \
  --limit 10
```

Then start the dashboard:

```bash
.venv/bin/streamlit run dashboard/app.py
```

## Run Kafka Streaming Mode

Start PostgreSQL and Kafka:

```bash
docker compose up -d
```

Produce raw Kraken ticker messages to Kafka:

```bash
.venv/bin/python src/ingestion/produce_kraken_ticker_to_kafka.py \
  --symbols BTC/USD ETH/USD SOL/USD
```

Preview messages from Kafka:

```bash
.venv/bin/python src/streaming/consume_kraken_ticker_from_kafka.py \
  --limit 10
```

Load Kafka messages into PostgreSQL:

```bash
.venv/bin/python src/streaming/consume_kraken_ticker_to_postgres.py
```

Then start the dashboard:

```bash
.venv/bin/streamlit run dashboard/app.py
```

For a short test run, add `--limit 10` to the producer and consumer commands.

## Dashboard

The Streamlit dashboard reads clean ticker records from PostgreSQL and shows:

- Selected symbol count.
- Latest event timestamp.
- Latest price cards by symbol.
- Recent ticker records.
- Price history line chart.

## Explore The API

Print a few Kraken WebSocket messages:

```bash
.venv/bin/python src/ingestion/explore_kraken_ws.py
```

## Ingest Raw Data

Collect a small sample of ticker messages and save them as JSONL:

```bash
.venv/bin/python src/ingestion/ingest_kraken_ticker_raw.py \
  --symbols BTC/USD ETH/USD SOL/USD \
  --limit 10
```

Output folder:

```text
data/raw/kraken_ticker/
```

## Transform Data

Convert the latest raw JSONL file into a clean CSV:

```bash
.venv/bin/python src/transform/kraken_ticker_to_csv.py
```

Or transform a specific raw file:

```bash
.venv/bin/python src/transform/kraken_ticker_to_csv.py \
  --input data/raw/kraken_ticker/<raw-file>.jsonl
```

Output folder:

```text
data/processed/kraken_ticker/
```

## Check Data Quality

Run simple checks on a processed CSV:

```bash
.venv/bin/python src/quality/check_kraken_ticker_csv.py \
  --input data/processed/kraken_ticker/<processed-file>.csv
```

## Run Local Pipeline

Run ingestion, transformation, quality checks, table creation, and PostgreSQL loading in one command:

```bash
.venv/bin/python src/pipeline/run_local_pipeline.py \
  --symbols BTC/USD ETH/USD SOL/USD \
  --limit 10
```

## Query Latest Prices

Run the first analytics query:

```bash
docker exec -i crypto-postgres psql -U crypto -d crypto -f - \
  < sql/latest_price_per_symbol.sql
```

Run the price history query:

```bash
docker exec -i crypto-postgres psql -U crypto -d crypto -f - \
  < sql/price_history.sql
```

## Run Dashboard

Start the Streamlit dashboard:

```bash
.venv/bin/streamlit run dashboard/app.py
```

## Run Streaming Mode

Run the streaming worker in one terminal:

```bash
.venv/bin/python src/ingestion/stream_kraken_ticker_to_postgres.py \
  --symbols BTC/USD ETH/USD SOL/USD
```

Then run the dashboard in another terminal:

```bash
.venv/bin/streamlit run dashboard/app.py
```
