import argparse
import csv
from pathlib import Path

from google.cloud import bigquery


DEFAULT_PROCESSED_DATA_DIR = Path("data/processed/kraken_ticker")
DEFAULT_PROJECT_ID = "dido-486313"
DEFAULT_DATASET_ID = "crypto_analytics"
DEFAULT_TABLE_ID = "kraken_ticker"

def latest_processed_file() -> Path:
    processed_files = sorted(DEFAULT_PROCESSED_DATA_DIR.glob("*.csv"))
    if not processed_files:
        raise FileNotFoundError(f"No processed CSV files found in {DEFAULT_PROCESSED_DATA_DIR}")
    return processed_files[-1]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load processed Kraken ticker CSV rows into BigQuery."
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=None,
        help="Processed Kraken ticker CSV file to load. Defaults to the latest file.",
    )
    parser.add_argument(
        "--project-id",
        default=DEFAULT_PROJECT_ID,
        help="GCP project ID containing the BigQuery dataset.",
    )
    parser.add_argument(
        "--dataset-id",
        default=DEFAULT_DATASET_ID,
        help="BigQuery dataset ID.",
    )
    parser.add_argument(
        "--table-id",
        default=DEFAULT_TABLE_ID,
        help="BigQuery table ID.",
    )
    parser.add_argument(
        "--replace",
        action="store_true",
        help="Replace existing rows in the BigQuery table instead of appending.",
    )
    return parser.parse_args()


def clean_numeric(value: str) -> str | None:
    return value if value else None


def bigquery_rows(csv_path: Path) -> list[dict]:
    rows = []

    with csv_path.open("r", encoding="utf-8", newline="") as input_file:
        reader = csv.DictReader(input_file)

        for row in reader:
            rows.append(
                {
                    "symbol": row["symbol"],
                    "event_type": row["event_type"],
                    "last_price": clean_numeric(row["last"]),
                    "volume": clean_numeric(row["volume"]),
                    "vwap": clean_numeric(row["vwap"]),
                    "low_price": clean_numeric(row["low"]),
                    "high_price": clean_numeric(row["high"]),
                    "change": clean_numeric(row["change"]),
                    "change_pct": clean_numeric(row["change_pct"]),
                    "event_timestamp": row["timestamp"],
                }
            )

    return rows


def load_rows(
    csv_path: Path,
    project_id: str,
    dataset_id: str,
    table_id: str,
    replace: bool,
) -> int:
    rows = bigquery_rows(csv_path)
    if not rows:
        raise ValueError(f"No rows found in {csv_path}")

    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    write_disposition = (
        bigquery.WriteDisposition.WRITE_TRUNCATE
        if replace
        else bigquery.WriteDisposition.WRITE_APPEND
    )

    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disposition,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )

    load_job = client.load_table_from_json(
        rows,
        table_ref,
        job_config=job_config,
    )
    load_job.result()

    return len(rows)


def main() -> None:
    args = parse_args()
    csv_path = args.input or latest_processed_file()
    rows_loaded = load_rows(
        csv_path=csv_path,
        project_id=args.project_id,
        dataset_id=args.dataset_id,
        table_id=args.table_id,
        replace=args.replace,
    )

    table_ref = f"{args.project_id}.{args.dataset_id}.{args.table_id}"
    print(f"Loaded {rows_loaded} rows from {csv_path} into {table_ref}")


if __name__ == "__main__":
    main()
