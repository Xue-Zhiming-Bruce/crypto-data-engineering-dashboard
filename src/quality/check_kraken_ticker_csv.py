import argparse
import csv
from pathlib import Path


REQUIRED_COLUMNS = {
    "symbol",
    "event_type",
    "last",
    "volume",
    "timestamp",
}
REQUIRED_NON_EMPTY_COLUMNS = {
    "symbol",
    "last",
    "timestamp",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run simple quality checks on processed Kraken ticker CSV data."
    )
    parser.add_argument(
        "--input",
        type=Path,
        required=True,
        help="Processed Kraken ticker CSV file to check.",
    )
    return parser.parse_args()


def check_file(path: Path) -> list[str]:
    errors = []

    with path.open("r", encoding="utf-8", newline="") as input_file:
        reader = csv.DictReader(input_file)
        columns = set(reader.fieldnames or [])

        missing_columns = REQUIRED_COLUMNS - columns
        if missing_columns:
            errors.append(f"Missing required columns: {sorted(missing_columns)}")

        row_count = 0
        for row_number, row in enumerate(reader, start=2):
            row_count += 1

            for column in REQUIRED_NON_EMPTY_COLUMNS:
                if not row.get(column):
                    errors.append(f"Row {row_number} has empty {column}")

            try:
                last_price = float(row.get("last", ""))
            except ValueError:
                errors.append(f"Row {row_number} has non-numeric last price")
                continue

            if last_price <= 0:
                errors.append(f"Row {row_number} has non-positive last price")

        if row_count == 0:
            errors.append("CSV has no data rows")

    return errors


def main() -> None:
    args = parse_args()
    errors = check_file(args.input)

    if errors:
        print("Quality check failed:")
        for error in errors:
            print(f"- {error}")
        raise SystemExit(1)

    print(f"Quality check passed: {args.input}")


if __name__ == "__main__":
    main()
