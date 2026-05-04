import argparse
import csv
import json
from pathlib import Path


RAW_DATA_DIR = Path("data/raw/kraken_ticker")
PROCESSED_DATA_DIR = Path("data/processed/kraken_ticker")
COLUMNS = [
    "symbol",
    "event_type",
    "last",
    "volume",
    "vwap",
    "low",
    "high",
    "change",
    "change_pct",
    "timestamp",
]


def latest_raw_file() -> Path:
    raw_files = sorted(RAW_DATA_DIR.glob("*.jsonl"))
    if not raw_files:
        raise FileNotFoundError(f"No raw JSONL files found in {RAW_DATA_DIR}")
    return raw_files[-1]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Transform raw Kraken ticker JSONL into CSV."
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=None,
        help="Raw JSONL file to transform. Defaults to the latest Kraken raw file.",
    )
    return parser.parse_args()


def ticker_rows(path: Path) -> list[dict]:
    rows = []

    with path.open("r", encoding="utf-8") as input_file:
        for line in input_file:
            message = json.loads(line)

            for item in message.get("data", []):
                rows.append(
                    {
                        "symbol": item.get("symbol"),
                        "event_type": message.get("type"),
                        "last": item.get("last"),
                        "volume": item.get("volume"),
                        "vwap": item.get("vwap"),
                        "low": item.get("low"),
                        "high": item.get("high"),
                        "change": item.get("change"),
                        "change_pct": item.get("change_pct"),
                        "timestamp": item.get("timestamp"),
                    }
                )

    return rows


def main(input_path: Path | None) -> None:
    raw_path = input_path or latest_raw_file()
    rows = ticker_rows(raw_path)

    PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)
    output_path = PROCESSED_DATA_DIR / raw_path.with_suffix(".csv").name

    with output_path.open("w", encoding="utf-8", newline="") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote {len(rows)} rows to {output_path}")


if __name__ == "__main__":
    args = parse_args()
    main(input_path=args.input)
