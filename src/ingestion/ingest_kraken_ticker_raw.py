import argparse
import asyncio
import json
import socket
from datetime import UTC, datetime
from pathlib import Path

import websockets


WS_URL = "wss://ws.kraken.com/v2"
DEFAULT_SYMBOLS = ["BTC/USD", "ETH/USD", "SOL/USD"]
CHANNEL = "ticker"
CONNECT_TIMEOUT_SECONDS = 10
DEFAULT_TICKER_MESSAGE_LIMIT = 10
RAW_DATA_DIR = Path("data/raw/kraken_ticker")


def output_path() -> Path:
    run_timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    return RAW_DATA_DIR / f"ticker_{run_timestamp}.jsonl"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest raw Kraken ticker WebSocket messages as JSONL."
    )
    parser.add_argument(
        "--symbols",
        nargs="+",
        default=DEFAULT_SYMBOLS,
        help="Kraken symbols to subscribe to, like BTC/USD ETH/USD SOL/USD.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_TICKER_MESSAGE_LIMIT,
        help="Number of ticker messages to write before exiting.",
    )
    return parser.parse_args()


async def main(symbols: list[str], limit: int) -> None:
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
    path = output_path()

    subscribe_message = {
        "method": "subscribe",
        "params": {
            "channel": CHANNEL,
            "symbol": symbols,
        },
    }

    ticker_messages_written = 0

    async with websockets.connect(
        WS_URL,
        open_timeout=CONNECT_TIMEOUT_SECONDS,
    ) as websocket:
        await websocket.send(json.dumps(subscribe_message))

        with path.open("w", encoding="utf-8") as output_file:
            while ticker_messages_written < limit:
                raw_message = await websocket.recv()
                message = json.loads(raw_message)

                if message.get("channel") != CHANNEL:
                    continue

                output_file.write(json.dumps(message) + "\n")
                ticker_messages_written += 1

    print(f"Wrote {ticker_messages_written} ticker messages to {path}")


if __name__ == "__main__":
    args = parse_args()

    try:
        asyncio.run(main(symbols=args.symbols, limit=args.limit))
    except TimeoutError:
        print(f"Connection to Kraken timed out after {CONNECT_TIMEOUT_SECONDS} seconds.")
        raise SystemExit(1)
    except socket.gaierror:
        print("Could not resolve the Kraken WebSocket hostname.")
        raise SystemExit(1)
    except OSError as error:
        print(f"Network error while connecting to Kraken: {error}")
        raise SystemExit(1)
