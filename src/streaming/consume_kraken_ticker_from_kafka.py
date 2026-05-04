import argparse
import json

from confluent_kafka import Consumer, KafkaException


DEFAULT_BOOTSTRAP_SERVER = "localhost:9092"
DEFAULT_TOPIC = "kraken_ticker_raw"
DEFAULT_GROUP_ID = "kraken-ticker-print-consumer"
DEFAULT_MESSAGE_LIMIT = 10


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read raw Kraken ticker messages from Kafka and print them."
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
        "--limit",
        type=int,
        default=DEFAULT_MESSAGE_LIMIT,
        help="Number of messages to read before exiting.",
    )
    return parser.parse_args()


def print_ticker_message(message_value: bytes) -> None:
    ticker_message = json.loads(message_value.decode("utf-8"))

    for item in ticker_message.get("data", []):
        print(
            "symbol="
            f"{item.get('symbol')} "
            "timestamp="
            f"{item.get('timestamp')} "
            "last_price="
            f"{item.get('last')}"
        )


def consume_messages(
    bootstrap_server: str,
    topic: str,
    group_id: str,
    limit: int,
) -> None:
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_server,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
        }
    )
    messages_read = 0

    consumer.subscribe([topic])

    try:
        while messages_read < limit:
            message = consumer.poll(timeout=1.0)

            if message is None:
                continue

            if message.error():
                raise KafkaException(message.error())

            print_ticker_message(message.value())
            messages_read += 1
    finally:
        consumer.close()

    print(f"Read {messages_read} messages from Kafka topic {topic}")


def main() -> None:
    args = parse_args()
    consume_messages(
        bootstrap_server=args.bootstrap_server,
        topic=args.topic,
        group_id=args.group_id,
        limit=args.limit,
    )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Kafka consumer stopped by user.")
