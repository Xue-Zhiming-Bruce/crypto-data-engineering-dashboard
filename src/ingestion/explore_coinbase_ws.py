import asyncio
import json
import socket

import websockets


WS_URL = "wss://ws-feed.exchange.coinbase.com"
PRODUCT_IDS = ["BTC-USD"]
CHANNEL = "ticker"
CONNECT_TIMEOUT_SECONDS = 10
MESSAGE_LIMIT = 5


async def main() -> None:
    subscribe_message = {
        "type": "subscribe",
        "product_ids": PRODUCT_IDS,
        "channels": [CHANNEL],
    }

    async with websockets.connect(
        WS_URL,
        open_timeout=CONNECT_TIMEOUT_SECONDS,
    ) as websocket:
        await websocket.send(json.dumps(subscribe_message))

        for _ in range(MESSAGE_LIMIT):
            raw_message = await websocket.recv()
            message = json.loads(raw_message)
            print(json.dumps(message, indent=2))


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except TimeoutError:
        print(f"Connection to Coinbase timed out after {CONNECT_TIMEOUT_SECONDS} seconds.")
    except socket.gaierror:
        print("Could not resolve the Coinbase WebSocket hostname.")
    except OSError as error:
        print(f"Network error while connecting to Coinbase: {error}")
