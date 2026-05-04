import argparse
import csv
from pathlib import Path

import psycopg


DEFAULT_DATABASE_URL = "postgresql://crypto:crypto@localhost:5432/crypto"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load processed Kraken ticker CSV rows into PostgreSQL."
    )
    parser.add_argument(
        "--input",
        type=Path,
        required=True,
        help="Processed Kraken ticker CSV file to load.",
    )
    parser.add_argument(
        "--database-url",
        default=DEFAULT_DATABASE_URL,
        help="PostgreSQL connection URL.",
    )
    return parser.parse_args()


def load_rows(csv_path: Path, database_url: str) -> int:
    rows_loaded = 0

    with csv_path.open("r", encoding="utf-8", newline="") as input_file:
        reader = csv.DictReader(input_file)

        with psycopg.connect(database_url) as connection:
            with connection.cursor() as cursor:
                for row in reader:
                    cursor.execute(
                        """
                        INSERT INTO kraken_ticker (
                            symbol,
                            event_type,
                            last_price,
                            volume,
                            vwap,
                            low_price,
                            high_price,
                            change,
                            change_pct,
                            event_timestamp
                        )
                        VALUES (
                            %(symbol)s,
                            %(event_type)s,
                            %(last_price)s,
                            %(volume)s,
                            %(vwap)s,
                            %(low_price)s,
                            %(high_price)s,
                            %(change)s,
                            %(change_pct)s,
                            %(event_timestamp)s
                        )
                        """,
                        {
                            "symbol": row["symbol"],
                            "event_type": row["event_type"],
                            "last_price": row["last"],
                            "volume": row["volume"],
                            "vwap": row["vwap"],
                            "low_price": row["low"],
                            "high_price": row["high"],
                            "change": row["change"],
                            "change_pct": row["change_pct"],
                            "event_timestamp": row["timestamp"],
                        },
                    )
                    rows_loaded += 1

            connection.commit()

    return rows_loaded


def main() -> None:
    args = parse_args()
    rows_loaded = load_rows(args.input, args.database_url)
    print(f"Loaded {rows_loaded} rows into kraken_ticker")


if __name__ == "__main__":
    main()
