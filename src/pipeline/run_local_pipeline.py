import argparse
import subprocess
import sys
from pathlib import Path

import psycopg

RAW_DATA_DIR = Path("data/raw/kraken_ticker")
PROCESSED_DATA_DIR = Path("data/processed/kraken_ticker")
SCHEMA_PATH = Path("sql/create_tables.sql")
DEFAULT_DATABASE_URL = "postgresql://crypto:crypto@localhost:5432/crypto"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the local Kraken ticker pipeline: ingest, transform, check, load."
    )
    parser.add_argument(
        "--symbols",
        nargs="+",
        default=["BTC/USD", "ETH/USD", "SOL/USD"],
        help="Kraken symbols to subscribe to.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Number of ticker messages to ingest.",
    )
    parser.add_argument(
        "--database-url",
        default=DEFAULT_DATABASE_URL,
        help="PostgreSQL connection URL.",
    )
    return parser.parse_args()


def latest_file(directory: Path, pattern: str) -> Path:
    files = sorted(directory.glob(pattern))
    if not files:
        raise FileNotFoundError(f"No files matching {pattern} found in {directory}")
    return files[-1]


def run_command(command: list[str]) -> None:
    print(f"Running: {' '.join(command)}", flush=True)
    subprocess.run(command, check=True)


def ensure_database_schema(database_url: str) -> None:
    schema_sql = SCHEMA_PATH.read_text(encoding="utf-8")

    with psycopg.connect(database_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute(schema_sql)
        connection.commit()

    print(f"Ensured database schema from {SCHEMA_PATH}", flush=True)


def main() -> None:
    args = parse_args()

    run_command(
        [
            sys.executable,
            "src/ingestion/ingest_kraken_ticker_raw.py",
            "--symbols",
            *args.symbols,
            "--limit",
            str(args.limit),
        ]
    )
    raw_path = latest_file(RAW_DATA_DIR, "*.jsonl")

    run_command(
        [
            sys.executable,
            "src/transform/kraken_ticker_to_csv.py",
            "--input",
            str(raw_path),
        ]
    )
    processed_path = PROCESSED_DATA_DIR / raw_path.with_suffix(".csv").name

    run_command(
        [
            sys.executable,
            "src/quality/check_kraken_ticker_csv.py",
            "--input",
            str(processed_path),
        ]
    )

    ensure_database_schema(args.database_url)

    run_command(
        [
            sys.executable,
            "src/load/load_kraken_ticker_csv.py",
            "--input",
            str(processed_path),
            "--database-url",
            args.database_url,
        ]
    )

    print(f"Pipeline completed successfully: {processed_path}")


if __name__ == "__main__":
    main()
