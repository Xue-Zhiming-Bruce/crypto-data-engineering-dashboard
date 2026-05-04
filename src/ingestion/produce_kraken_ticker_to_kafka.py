import argparse
import asyncio
import json
import socket

from confluent_kafka import Producer
import websockets


WS_URL = "wss://ws.kraken.com/v2"
DEFAULT_SYMBOLS = ["BTC/USD", "ETH/USD", "SOL/USD"]
CHANNEL = "ticker"
CONNECT_TIMEOUT_SECONDS = 10
DEFAULT_TICKER_MESSAGE_LIMIT = 10
DEFAULT_BOOTSTRAP_SERVER = "localhost:9092"
DEFAULT_TOPIC = "kraken_ticker_raw"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Produce raw Kraken ticker WebSocket messages to Kafka."
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
        help="Number of ticker messages to produce before exiting.",
    )
    parser.add_argument(
        "--bootstrap-server",
        default=DEFAULT_BOOTSTRAP_SERVER,
        help="Kafka bootstrap server.",
    )
    parser.add_argument(
        "--topic",
        default=DEFAULT_TOPIC,
        help="Kafka topic for raw ticker messages.",
    )
    return parser.parse_args()


def delivery_report(error, message) -> None:
    if error is not None:
        print(f"Kafka delivery failed: {error}")
        return

    print(
        "Produced message to "
        f"{message.topic()} [{message.partition()}] at offset {message.offset()}"
    )


async def produce_ticker_messages(
    symbols: list[str],
    limit: int,
    bootstrap_server: str,
    topic: str,
) -> None:
    producer = Producer({"bootstrap.servers": bootstrap_server})
    subscribe_message = {
        "method": "subscribe",
        "params": {
            "channel": CHANNEL,
            "symbol": symbols,
        },
    }
    ticker_messages_produced = 0

    async with websockets.connect(
        WS_URL,
        open_timeout=CONNECT_TIMEOUT_SECONDS,
    ) as websocket:
        await websocket.send(json.dumps(subscribe_message))

        while ticker_messages_produced < limit:
            raw_message = await websocket.recv()
            message = json.loads(raw_message)

            if message.get("channel") != CHANNEL:
                continue

            symbol = message.get("data", [{}])[0].get("symbol")
            producer.produce(
                topic,
                key=symbol,
                value=json.dumps(message),
                callback=delivery_report,
            )
            producer.poll(0)
            ticker_messages_produced += 1

    producer.flush()
    print(
        f"Produced {ticker_messages_produced} ticker messages "
        f"to Kafka topic {topic}"
    )


def main() -> None:
    args = parse_args()
    asyncio.run(
        produce_ticker_messages(
            symbols=args.symbols,
            limit=args.limit,
            bootstrap_server=args.bootstrap_server,
            topic=args.topic,
        )
    )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Kafka producer stopped by user.")
    except TimeoutError:
        print(f"Connection to Kraken timed out after {CONNECT_TIMEOUT_SECONDS} seconds.")
        raise SystemExit(1)
    except socket.gaierror:
        print("Could not resolve the Kraken WebSocket hostname.")
        raise SystemExit(1)
    except OSError as error:
        print(f"Network error while connecting to Kraken: {error}")
        raise SystemExit(1)
