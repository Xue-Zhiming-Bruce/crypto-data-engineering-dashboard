import argparse
from pathlib import Path

from google.cloud import storage


DEFAULT_RAW_DATA_DIR = Path("data/raw/kraken_ticker")
DEFAULT_GCS_PREFIX = "raw/kraken_ticker"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Upload raw Kraken ticker JSONL files to GCS."
    )
    parser.add_argument(
        "--bucket",
        required=True,
        help="GCS bucket name for raw Kraken JSONL files.",
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=DEFAULT_RAW_DATA_DIR,
        help="Local directory containing raw Kraken JSONL files.",
    )
    parser.add_argument(
        "--prefix",
        default=DEFAULT_GCS_PREFIX,
        help="GCS object prefix for uploaded JSONL files.",
    )
    return parser.parse_args()


def upload_jsonl_files(bucket_name: str, input_dir: Path, prefix: str) -> int:
    if not input_dir.exists():
        raise FileNotFoundError(f"Input directory does not exist: {input_dir}")

    jsonl_files = sorted(input_dir.glob("*.jsonl"))
    if not jsonl_files:
        raise FileNotFoundError(f"No JSONL files found in {input_dir}")

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    normalized_prefix = prefix.strip("/")

    uploaded_count = 0
    for path in jsonl_files:
        object_name = f"{normalized_prefix}/{path.name}"
        blob = bucket.blob(object_name)
        blob.upload_from_filename(path)
        print(f"Uploaded {path} to gs://{bucket_name}/{object_name}")
        uploaded_count += 1

    return uploaded_count


def main() -> None:
    args = parse_args()
    uploaded_count = upload_jsonl_files(
        bucket_name=args.bucket,
        input_dir=args.input_dir,
        prefix=args.prefix,
    )
    print(f"Uploaded {uploaded_count} raw JSONL file(s) to GCS")


if __name__ == "__main__":
    main()
