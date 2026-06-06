#!/usr/bin/env python3
"""
Runs hourly to keep data/crypto.json current.

Fetches the Crypto Fear & Greed Index from alternative.me (no API key needed).
Skips the commit step when the value hasn't changed since the last fetch.
"""

import json
import os
import sys
import time
from datetime import datetime, timezone

import requests

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}
ALTERNATIVE_ME_FNG = "https://api.alternative.me/fng/"
CRYPTO_PATH        = "data/crypto.json"


def display_date(ts_ms: int) -> str:
    return datetime.utcfromtimestamp(ts_ms / 1000).strftime("%b %Y")


def load_existing_value() -> int | None:
    try:
        with open(CRYPTO_PATH) as f:
            return json.load(f).get("crypto_fg", {}).get("value")
    except Exception:
        return None


def save(data: object):
    os.makedirs(os.path.dirname(CRYPTO_PATH) or ".", exist_ok=True)
    with open(CRYPTO_PATH, "w") as f:
        json.dump(data, f, separators=(",", ":"))
    size_kb = os.path.getsize(CRYPTO_PATH) // 1024
    print(f"  Saved {CRYPTO_PATH} ({size_kb} KB)")


def fetch_crypto_fg(retries: int = 3, retry_delay: int = 10) -> dict | None:
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(
                ALTERNATIVE_ME_FNG,
                params={"limit": 0, "format": "json"},
                headers=HEADERS,
                timeout=20,
            )
            r.raise_for_status()
            raw  = r.json()
            data = raw.get("data", [])
            if not data:
                last_err = "empty data array"
                print(f"  WARN: empty response (attempt {attempt})")
                time.sleep(retry_delay)
                continue

            points = sorted(
                [
                    {"x": int(o["timestamp"]) * 1000, "y": int(o["value"])}
                    for o in data
                    if o.get("timestamp") and o.get("value")
                ],
                key=lambda p: p["x"],
            )

            latest = data[0]
            time_until = int(latest.get("time_until_update") or 0)
            print(
                f"  Crypto F&G: {latest['value']} ({latest['value_classification']}) "
                f"— next update in {time_until // 3600}h {(time_until % 3600) // 60}m"
            )

            return {
                "fetched_at":      int(time.time() * 1000),
                "source":          "alternative.me",
                "source_url":      "https://alternative.me/crypto/fear-and-greed-index/",
                "crypto_fg": {
                    "label":          "Crypto Fear & Greed",
                    "value":          int(latest["value"]),
                    "classification": latest.get("value_classification", ""),
                    "unit":           "",
                    "date":           display_date(points[-1]["x"]),
                    "history":        points,
                },
            }

        except Exception as e:
            last_err = e
            print(f"  ERROR (attempt {attempt}): {e}")
            if attempt < retries:
                time.sleep(retry_delay)

    print(f"  FAILED after {retries} attempts: {last_err}")
    return None


def main():
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print(f"=== fetch_crypto.py  {ts} ===")

    prev_value = load_existing_value()
    result     = fetch_crypto_fg()

    if result is None:
        print("  No data — skipping write.")
        sys.exit(1)

    new_value = result["crypto_fg"]["value"]
    if new_value == prev_value:
        print(f"  Value unchanged ({new_value}) — skipping write.")
        # Signal to the workflow that nothing needs committing
        sys.exit(2)

    save(result)
    print("=== Done ===")


if __name__ == "__main__":
    main()
