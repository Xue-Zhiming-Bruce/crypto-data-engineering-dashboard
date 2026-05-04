import asyncio
import json
import socket
from datetime import datetime, timezone
from pathlib import Path

import websockets


WS_URL = "wss://ws-feed.exchange.coinbase.com"
PRODUCT_IDS = ["BTC-USD"]
CHANNEL = "ticker"
CONNECT_TIMEOUT_SECONDS = 10
MESSAGE_LIMIT = 20
OUTPUT_PATH = Path("data/raw/coinbase_btc_usd_ticker.jsonl")


async def main() -> None:
    subscribe_message = {
        "type": "subscribe",
        "product_ids": PRODUCT_IDS,
        "channels": [CHANNEL],
    }

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    async with websockets.connect(
        WS_URL,
        open_timeout=CONNECT_TIMEOUT_SECONDS,
    ) as websocket:
        await websocket.send(json.dumps(subscribe_message))

        with OUTPUT_PATH.open("a", encoding="utf-8") as output_file:
            for _ in range(MESSAGE_LIMIT):
                raw_message = await websocket.recv()
                message = json.loads(raw_message)
                record = {
                    "ingested_at": datetime.now(timezone.utc).isoformat(),
                    "source": "coinbase_exchange_ws",
                    "payload": message,
                }
                output_file.write(json.dumps(record) + "\n")

    print(f"Saved {MESSAGE_LIMIT} messages to {OUTPUT_PATH}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except TimeoutError:
        print(f"Connection to Coinbase timed out after {CONNECT_TIMEOUT_SECONDS} seconds.")
    except socket.gaierror:
        print("Could not resolve the Coinbase WebSocket hostname.")
    except OSError as error:
        print(f"Network error while connecting to Coinbase: {error}")
