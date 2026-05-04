import argparse
import asyncio
import json
import logging
import socket
from pathlib import Path

import psycopg
import websockets
from websockets.exceptions import ConnectionClosed


WS_URL = "wss://ws.kraken.com/v2"
CHANNEL = "ticker"
CONNECT_TIMEOUT_SECONDS = 10
RECONNECT_DELAY_SECONDS = 5
DEFAULT_SYMBOLS = ["BTC/USD", "ETH/USD", "SOL/USD"]
DEFAULT_DATABASE_URL = "postgresql://crypto:crypto@localhost:5432/crypto"
SCHEMA_PATH = Path("sql/create_tables.sql")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)


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
        description="Stream Kraken ticker WebSocket messages into PostgreSQL."
    )
    parser.add_argument(
        "--symbols",
        nargs="+",
        default=DEFAULT_SYMBOLS,
        help="Kraken symbols to subscribe to, like BTC/USD ETH/USD SOL/USD.",
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
        help="Optional number of ticker rows to insert before exiting.",
    )
    return parser.parse_args()


def ensure_database_schema(connection: psycopg.Connection) -> None:
    schema_sql = SCHEMA_PATH.read_text(encoding="utf-8")

    with connection.cursor() as cursor:
        cursor.execute(schema_sql)
    connection.commit()


def ticker_rows(message: dict) -> list[dict]:
    rows = []

    for item in message.get("data", []):
        rows.append(
            {
                "symbol": item.get("symbol"),
                "event_type": message.get("type"),
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


async def stream_to_postgres(
    symbols: list[str],
    database_url: str,
    limit: int | None,
) -> None:
    subscribe_message = {
        "method": "subscribe",
        "params": {
            "channel": CHANNEL,
            "symbol": symbols,
        },
    }
    rows_inserted = 0

    with psycopg.connect(database_url) as connection:
        ensure_database_schema(connection)

        while limit is None or rows_inserted < limit:
            try:
                async with websockets.connect(
                    WS_URL,
                    open_timeout=CONNECT_TIMEOUT_SECONDS,
                ) as websocket:
                    await websocket.send(json.dumps(subscribe_message))
                    logger.info(
                        "Streaming Kraken ticker rows for: %s",
                        ", ".join(symbols),
                    )

                    while limit is None or rows_inserted < limit:
                        raw_message = await websocket.recv()
                        message = json.loads(raw_message)

                        if message.get("channel") != CHANNEL:
                            continue

                        rows = ticker_rows(message)
                        with connection.cursor() as cursor:
                            cursor.executemany(INSERT_TICKER_SQL, rows)
                        connection.commit()

                        rows_inserted += len(rows)
                        logger.info("Inserted %s ticker rows", rows_inserted)
            except ConnectionClosed as error:
                logger.warning(
                    "Kraken WebSocket disconnected "
                    "(%s). Reconnecting in %s seconds...",
                    error,
                    RECONNECT_DELAY_SECONDS,
                )
                await asyncio.sleep(RECONNECT_DELAY_SECONDS)


def main() -> None:
    args = parse_args()
    asyncio.run(
        stream_to_postgres(
            symbols=args.symbols,
            database_url=args.database_url,
            limit=args.limit,
        )
    )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Streaming stopped by user.")
    except TimeoutError:
        logger.error(
            "Connection to Kraken timed out after %s seconds.",
            CONNECT_TIMEOUT_SECONDS,
        )
        raise SystemExit(1)
    except socket.gaierror:
        logger.error("Could not resolve the Kraken WebSocket hostname.")
        raise SystemExit(1)
    except OSError as error:
        logger.error("Network error while streaming Kraken data: %s", error)
        raise SystemExit(1)
