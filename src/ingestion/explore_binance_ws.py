import asyncio
import json
import socket

import websockets


WS_URL = "wss://stream.binance.com:443/ws/btcusdt@trade"
CONNECT_TIMEOUT_SECONDS = 10
MESSAGE_LIMIT = 5


async def main() -> None:
    async with websockets.connect(
        WS_URL,
        open_timeout=CONNECT_TIMEOUT_SECONDS,
    ) as websocket:
        for _ in range(MESSAGE_LIMIT):
            raw_message = await websocket.recv()
            message = json.loads(raw_message)
            print(json.dumps(message, indent=2))


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except TimeoutError:
        print(f"Connection to Binance timed out after {CONNECT_TIMEOUT_SECONDS} seconds.")
    except socket.gaierror:
        print("Could not resolve the Binance WebSocket hostname.")
    except OSError as error:
        print(f"Network error while connecting to Binance: {error}")
