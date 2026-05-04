import json
import socket
import urllib.error
import urllib.request


ASSET_PAIRS_URL = "https://api.kraken.com/0/public/AssetPairs"
REQUEST_TIMEOUT_SECONDS = 10


def fetch_asset_pairs() -> dict:
    with urllib.request.urlopen(
        ASSET_PAIRS_URL,
        timeout=REQUEST_TIMEOUT_SECONDS,
    ) as response:
        return json.loads(response.read().decode("utf-8"))


def usd_pairs(asset_pairs_response: dict) -> list[str]:
    pairs = []

    for pair_info in asset_pairs_response.get("result", {}).values():
        ws_name = pair_info.get("wsname")
        if ws_name and ws_name.endswith("/USD"):
            pairs.append(ws_name)

    return sorted(set(pairs))


def main() -> None:
    response = fetch_asset_pairs()
    pairs = usd_pairs(response)

    for pair in pairs:
        print(pair)

    print(f"\nFound {len(pairs)} Kraken USD pairs.")


if __name__ == "__main__":
    try:
        main()
    except TimeoutError:
        print(f"Connection to Kraken timed out after {REQUEST_TIMEOUT_SECONDS} seconds.")
        raise SystemExit(1)
    except socket.gaierror:
        print("Could not resolve the Kraken REST API hostname.")
        raise SystemExit(1)
    except urllib.error.URLError as error:
        print(f"Network error while connecting to Kraken: {error}")
        raise SystemExit(1)
