import argparse
import json
from pathlib import Path

from confluent_kafka import Consumer, KafkaException
import psycopg


DEFAULT_BOOTSTRAP_SERVER = "localhost:9092"
DEFAULT_TOPIC = "kraken_ticker_raw"
DEFAULT_GROUP_ID = "kraken-ticker-postgres-consumer"
DEFAULT_DATABASE_URL = "postgresql://crypto:crypto@localhost:5432/crypto"
SCHEMA_PATH = Path("sql/create_tables.sql")


INSERT_TICKER_SQL = """
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
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read Kraken ticker messages from Kafka and load PostgreSQL."
    )
    parser.add_argument(
        "--bootstrap-server",
        default=DEFAULT_BOOTSTRAP_SERVER,
        help="Kafka bootstrap server.",
    )
    parser.add_argument(
        "--topic",
        default=DEFAULT_TOPIC,
        help="Kafka topic to read from.",
    )
    parser.add_argument(
        "--group-id",
        default=DEFAULT_GROUP_ID,
        help="Kafka consumer group ID.",
    )
    parser.add_argument(
        "--database-url",
        default=DEFAULT_DATABASE_URL,
        help="PostgreSQL connection URL.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional number of ticker rows to load before exiting.",
    )
    return parser.parse_args()


def ensure_database_schema(connection: psycopg.Connection) -> None:
    schema_sql = SCHEMA_PATH.read_text(encoding="utf-8")

    with connection.cursor() as cursor:
        cursor.execute(schema_sql)
    connection.commit()


def ticker_rows(message_value: bytes) -> list[dict]:
    ticker_message = json.loads(message_value.decode("utf-8"))
    rows = []

    for item in ticker_message.get("data", []):
        rows.append(
            {
                "symbol": item.get("symbol"),
                "event_type": ticker_message.get("type"),
                "last_price": item.get("last"),
                "volume": item.get("volume"),
                "vwap": item.get("vwap"),
                "low_price": item.get("low"),
                "high_price": item.get("high"),
                "change": item.get("change"),
                "change_pct": item.get("change_pct"),
                "event_timestamp": item.get("timestamp"),
            }
        )

    return rows


def consume_to_postgres(
    bootstrap_server: str,
    topic: str,
    group_id: str,
    database_url: str,
    limit: int | None,
) -> None:
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_server,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    rows_loaded = 0

    consumer.subscribe([topic])

    with psycopg.connect(database_url) as connection:
        ensure_database_schema(connection)

        try:
            while limit is None or rows_loaded < limit:
                message = consumer.poll(timeout=1.0)

                if message is None:
                    continue

                if message.error():
                    raise KafkaException(message.error())

                rows = ticker_rows(message.value())
                if not rows:
                    consumer.commit(message=message)
                    continue

                with connection.cursor() as cursor:
                    cursor.executemany(INSERT_TICKER_SQL, rows)
                connection.commit()
                consumer.commit(message=message)

                rows_loaded += len(rows)
                print(f"Loaded {rows_loaded} ticker rows into PostgreSQL")
        finally:
            consumer.close()

    print(f"Loaded {rows_loaded} rows from Kafka topic {topic}")


def main() -> None:
    args = parse_args()
    consume_to_postgres(
        bootstrap_server=args.bootstrap_server,
        topic=args.topic,
        group_id=args.group_id,
        database_url=args.database_url,
        limit=args.limit,
    )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Kafka PostgreSQL consumer stopped by user.")
