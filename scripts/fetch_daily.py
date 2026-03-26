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


def save(path: str, data: object):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, separators=(",", ":"))
    size_kb = os.path.getsize(path) // 1024
    print(f"  Saved {path} ({size_kb} KB)")


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

    def fetch_indicator(series: str, label: str, unit: str, years: int) -> dict | None:
        try:
            start = (now - timedelta(days=years * 365)).strftime("%Y-%m-%d")
            points = fetch_fred(series, start)
            if not points:
                print(f"  WARN {series}: empty")
                return None
            last = points[-1]
            return {
                "label":   label,
                "value":   last["y"],
                "unit":    unit,
                "date":    display_date(last["x"]),
                "history": points,
            }
        except Exception as e:
            print(f"  ERROR {series}: {e}")
            return None

    def fetch_cpi(years: int) -> dict | None:
        try:
            start = (now - timedelta(days=(years + 1) * 365)).strftime("%Y-%m-%d")
            points = fetch_fred("CPIAUCSL", start)
            if len(points) < 13:
                return None
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
            print(f"  ERROR CPI: {e}")
            return None

    print("Fetching FRED indicators...")
    macro = {
        "fetched_at":         int(time.time() * 1000),
        "fed_rate":           fetch_indicator("FEDFUNDS", "Fed Funds Rate",       "%",   years=10),
        "cpi":                fetch_cpi(years=12),
        "unemployment":       fetch_indicator("UNRATE",   "Unemployment Rate",    "%",   years=10),
        "yield10y":           fetch_indicator("DGS10",    "10Y Treasury Yield",   "%",   years=10),
        "yield_curve":        fetch_indicator("T10Y2Y",   "Yield Curve (10Y-2Y)", "%",   years=10),
        "consumer_sentiment": fetch_indicator("UMCSENT",  "Consumer Sentiment",   "pts", years=10),
    }
    save("data/macro.json", macro)

    print("=== Done ===")


if __name__ == "__main__":
    main()
