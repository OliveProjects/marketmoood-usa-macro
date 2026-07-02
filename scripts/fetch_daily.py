#!/usr/bin/env python3
"""
Runs once daily after US market close.

Outputs:
  data/macro.json   – FRED macro indicators (Fed rate, CPI, unemployment, yields, sentiment)
  data/events.json  – Upcoming high-impact US economic events (Finnhub)
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
FRED_API_BASE        = "https://api.stlouisfed.org/fred/series/observations"
FRED_CSV_BASE        = "https://fred.stlouisfed.org/graph/fredgraph.csv"
FRED_API_KEY         = os.environ.get("FRED_API_KEY", "")
FINNHUB_API_KEY      = os.environ.get("FINNHUB_API_KEY", "")
ALTERNATIVE_ME_FNG   = "https://api.alternative.me/fng/"
MACRO_PATH           = "data/macro.json"
CRYPTO_PATH          = "data/crypto.json"


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


# ── FRED helpers ──────────────────────────────────────────────────────────────

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


# ── Finnhub events ────────────────────────────────────────────────────────────

_RELEVANT = [
    "interest rate", "fomc", "federal reserve", "fed rate",
    "cpi", "core cpi", "pce", "core pce", "inflation",
    "gdp", "gross domestic",
    "nonfarm payroll", "non farm payroll", "payrolls", "unemployment rate",
]

_CATEGORY_KEYWORDS = {
    "fed":       ["interest rate", "fomc", "federal reserve", "fed rate"],
    "inflation": ["cpi", "pce", "inflation"],
    "gdp":       ["gdp", "gross domestic"],
    "jobs":      ["nonfarm payroll", "non farm payroll", "payrolls", "unemployment rate"],
}


def _categorize(name: str) -> str:
    lower = name.lower()
    for cat, keywords in _CATEGORY_KEYWORDS.items():
        if any(k in lower for k in keywords):
            return cat
    return "macro"


def _is_relevant(item: dict) -> bool:
    country = item.get("country", "").upper()
    if country not in ("US", "USA"):
        return False
    if item.get("impact", "").lower() != "high":
        return False
    name = item.get("event", "").lower().replace("-", " ")  # normalise "Non-Farm" → "Non Farm"
    return any(k in name for k in _RELEVANT)


def fetch_events(now: datetime) -> dict | None:
    if not FINNHUB_API_KEY:
        print("  WARN: FINNHUB_API_KEY not set — skipping events.json")
        return None

    from_date = now.replace(day=1).strftime("%Y-%m-%d")  # start of current month to capture past events
    to_date   = (now + timedelta(days=90)).strftime("%Y-%m-%d")

    try:
        r = requests.get(
            "https://finnhub.io/api/v1/calendar/economic",
            params={"from": from_date, "to": to_date, "token": FINNHUB_API_KEY},
            headers=HEADERS,
            timeout=20,
        )
        if not r.ok:
            print(f"  ERROR events: HTTP {r.status_code} — {r.text[:300]}")
            return None
        raw   = r.json()
        items = raw.get("economicCalendar", raw if isinstance(raw, list) else [])
        print(f"  Finnhub raw items: {len(items)} total (before filter, {from_date} → {to_date})")
        if items:
            sample = items[:3]
            for s in sample:
                print(f"    sample: {s.get('date') or s.get('time','')[:10]} | {s.get('event','')} | impact={s.get('impact','')} | country={s.get('country','')}")

        events = []
        for item in items:
            if not _is_relevant(item):
                continue

            estimate = item.get("estimate")
            previous = item.get("prev")
            actual   = item.get("actual")
            unit     = item.get("unit", "")

            def fmt(val) -> str | None:
                if val is None or val == "":
                    return None
                try:
                    return f"{float(val):.2f}{unit}".rstrip("0").rstrip(".")
                except (ValueError, TypeError):
                    return str(val)

            time_raw = item.get("time", "") or ""
            date_raw = item.get("date", "") or ""
            # Finnhub returns full datetime in 'time' ("2026-06-10 12:30:00"), 'date' is often empty
            if not date_raw and len(time_raw) >= 10:
                date_raw = time_raw[:10]
            time_only = time_raw[11:] if len(time_raw) > 10 else time_raw

            events.append({
                "event":    item.get("event", ""),
                "date":     date_raw,
                "time":     time_only,
                "category": _categorize(item.get("event", "")),
                "estimate": fmt(estimate),
                "previous": fmt(previous),
                "actual":   fmt(actual),
            })

        events.sort(key=lambda e: (e["date"], e["time"] or ""))
        future = [e for e in events if e["date"] >= now.strftime("%Y-%m-%d")]
        print(f"  Events: {len(events)} matched filter, {len(future)} upcoming")

        return {
            "fetched_at": int(time.time() * 1000),
            "from":       from_date,
            "to":         to_date,
            "events":     events,
        }

    except Exception as e:
        print(f"  ERROR events: {type(e).__name__}: {e}")
        return None


# ── Alternative.me Crypto Fear & Greed ───────────────────────────────────────

def fetch_crypto_fg(retries: int = 3, retry_delay: int = 10) -> dict | None:
    """
    Fetches the full Crypto Fear & Greed history from alternative.me and
    returns a payload ready to be written to data/crypto.json.

    API docs: https://alternative.me/crypto/fear-and-greed-index/
    Attribution is required next to any display of this data.
    No API key needed.
    """
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
                print(f"  WARN crypto F&G: empty (attempt {attempt})")
                time.sleep(retry_delay)
                continue

            # API returns newest-first; sort ascending for charting
            points = sorted(
                [
                    {"x": int(o["timestamp"]) * 1000, "y": int(o["value"])}
                    for o in data
                    if o.get("timestamp") and o.get("value")
                ],
                key=lambda p: p["x"],
            )

            latest = data[0]  # still newest-first in original list
            print(f"  Crypto F&G: {latest['value']} ({latest['value_classification']})  — {len(points)} history points")

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
            print(f"  ERROR crypto F&G (attempt {attempt}): {e}")
            if attempt < retries:
                time.sleep(retry_delay)

    print(f"  FAILED crypto F&G after {retries} attempts: {last_err}")
    return None


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    ts  = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    now = datetime.now(timezone.utc)
    print(f"=== fetch_daily.py  {ts} ===")

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

    # ── macro.json ──
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

    # ── events.json ──
    print("Fetching upcoming events (Finnhub)...")
    events = fetch_events(now)
    if events is not None:
        today_str = now.strftime("%Y-%m-%d")
        new_future = [e for e in events["events"] if e["date"] >= today_str]
        # Load existing to compare
        try:
            with open("data/events.json") as f:
                old = json.load(f)
            old_future = [e for e in old.get("events", []) if e["date"] >= today_str]
        except Exception:
            old_future = []
        if new_future or not old_future:
            save("data/events.json", events)
        else:
            print(f"  WARN events: new fetch has 0 upcoming events but old has {len(old_future)} — keeping existing events.json")

    # ── crypto.json ──
    print("Fetching Crypto Fear & Greed (alternative.me)...")
    crypto = fetch_crypto_fg()
    if crypto is not None:
        save(CRYPTO_PATH, crypto)

    print("=== Done ===")


if __name__ == "__main__":
    main()
