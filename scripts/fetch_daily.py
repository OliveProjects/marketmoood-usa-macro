#!/usr/bin/env python3
"""
Runs once daily after US market close.
Fetches all FRED macro indicators: Fed funds rate, CPI, unemployment,
10Y Treasury yield, yield curve (10Y-2Y), consumer sentiment.
"""

import json
import os
import time
from datetime import datetime, timezone, timedelta

import requests

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}
FRED_API_BASE = "https://api.stlouisfed.org/fred/series/observations"
FRED_CSV_BASE = "https://fred.stlouisfed.org/graph/fredgraph.csv"
FRED_API_KEY  = os.environ.get("FRED_API_KEY", "")
MACRO_PATH    = "data/macro.json"


def save(path: str, data: object):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, separators=(",", ":"))
    size_kb = os.path.getsize(path) // 1024
    print(f"  Saved {path} ({size_kb} KB)")


def load_existing() -> dict:
    """Load the previous macro.json so we can preserve fields that fail to fetch."""
    try:
        with open(MACRO_PATH) as f:
            return json.load(f)
    except Exception:
        return {}


def parse_fred_csv(text: str) -> list:
    lines = text.strip().splitlines()
    result = []
    for line in lines[1:]:
        parts = line.split(",")
        if len(parts) < 2:
            continue
        val_str = parts[1].strip()
        if not val_str or val_str == ".":
            continue
        try:
            dt = datetime.strptime(parts[0].strip(), "%Y-%m-%d").replace(tzinfo=timezone.utc)
            result.append({"x": int(dt.timestamp() * 1000), "y": float(val_str)})
        except (ValueError, IndexError):
            continue
    return sorted(result, key=lambda p: p["x"])


def fetch_fred(series: str, start_date: str) -> list:
    if FRED_API_KEY:
        r = requests.get(
            FRED_API_BASE,
            params={
                "series_id": series, "observation_start": start_date,
                "file_type": "json", "api_key": FRED_API_KEY,
            },
            headers=HEADERS, timeout=20,
        )
        r.raise_for_status()
        obs = r.json().get("observations", [])
        result = []
        for o in obs:
            val_str = o.get("value", ".")
            if not val_str or val_str == ".":
                continue
            try:
                dt = datetime.strptime(o["date"], "%Y-%m-%d").replace(tzinfo=timezone.utc)
                result.append({"x": int(dt.timestamp() * 1000), "y": float(val_str)})
            except (ValueError, KeyError):
                continue
        return sorted(result, key=lambda p: p["x"])

    r = requests.get(
        FRED_CSV_BASE,
        params={"id": series, "observation_start": start_date},
        headers=HEADERS, timeout=20,
    )
    r.raise_for_status()
    if not r.text.startswith('"observation_date"') and not r.text.startswith("observation_date"):
        raise ValueError(f"FRED CSV looks like HTML: {r.text[:80]!r}")
    return parse_fred_csv(r.text)


def calc_cpi_yoy(points: list) -> list:
    result = []
    for i in range(12, len(points)):
        cur, prev = points[i], points[i - 12]
        result.append({"x": cur["x"], "y": (cur["y"] - prev["y"]) / prev["y"] * 100.0})
    return result


def display_date(ts_ms: int) -> str:
    return datetime.utcfromtimestamp(ts_ms / 1000).strftime("%b %Y")


def main():
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print(f"=== fetch_daily.py (macro)  {ts} ===")
    now = datetime.now(timezone.utc)

    # Load previous data so nulls fall back to last known good value
    previous = load_existing()

    def fetch_indicator(series: str, label: str, unit: str, years: int,
                        retries: int = 3, retry_delay: int = 10) -> dict | None:
        start = (now - timedelta(days=years * 365)).strftime("%Y-%m-%d")
        last_err = None
        for attempt in range(1, retries + 1):
            try:
                points = fetch_fred(series, start)
                if not points:
                    print(f"  WARN {series}: empty (attempt {attempt})")
                    last_err = "empty response"
                    time.sleep(retry_delay)
                    continue
                last = points[-1]
                return {
                    "label":   label,
                    "value":   last["y"],
                    "unit":    unit,
                    "date":    display_date(last["x"]),
                    "history": points,
                }
            except Exception as e:
                last_err = e
                print(f"  ERROR {series} (attempt {attempt}): {e}")
                if attempt < retries:
                    time.sleep(retry_delay)
        print(f"  FAILED {series} after {retries} attempts: {last_err}")
        return None

    def fetch_cpi(years: int, retries: int = 3, retry_delay: int = 10) -> dict | None:
        start = (now - timedelta(days=(years + 1) * 365)).strftime("%Y-%m-%d")
        last_err = None
        for attempt in range(1, retries + 1):
            try:
                points = fetch_fred("CPIAUCSL", start)
                if len(points) < 13:
                    last_err = f"only {len(points)} points"
                    print(f"  WARN CPIAUCSL: too few points (attempt {attempt})")
                    time.sleep(retry_delay)
                    continue
                yoy = calc_cpi_yoy(points)
                last = yoy[-1]
                return {
                    "label":   "CPI Inflation (YoY)",
                    "value":   last["y"],
                    "unit":    "%",
                    "date":    display_date(points[-1]["x"]),
                    "history": yoy,
                }
            except Exception as e:
                last_err = e
                print(f"  ERROR CPI (attempt {attempt}): {e}")
                if attempt < retries:
                    time.sleep(retry_delay)
        print(f"  FAILED CPI after {retries} attempts: {last_err}")
        return None

    print("Fetching FRED indicators...")
    fresh = {
        "fetched_at":         int(time.time() * 1000),
        "fed_rate":           fetch_indicator("FEDFUNDS", "Fed Funds Rate",       "%",   years=10),
        "cpi":                fetch_cpi(years=12),
        "unemployment":       fetch_indicator("UNRATE",   "Unemployment Rate",    "%",   years=10),
        "yield10y":           fetch_indicator("DGS10",    "10Y Treasury Yield",   "%",   years=10),
        "yield_curve":        fetch_indicator("T10Y2Y",   "Yield Curve (10Y-2Y)", "%",   years=10),
        "consumer_sentiment": fetch_indicator("UMCSENT",  "Consumer Sentiment",   "pts", years=10),
    }

    # For any field that failed to fetch, keep the last known good value
    for key in ("fed_rate", "cpi", "unemployment", "yield10y", "yield_curve", "consumer_sentiment"):
        if fresh[key] is None and previous.get(key) is not None:
            print(f"  FALLBACK {key}: using previous value ({previous[key].get('value')})")
            fresh[key] = previous[key]

    save(MACRO_PATH, fresh)
    print("=== Done ===")


if __name__ == "__main__":
    main()
