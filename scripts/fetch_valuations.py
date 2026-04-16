#!/usr/bin/env python3
"""
Runs once daily. Fetches valuation indicators:
  - CAPE & Shiller P/E         (multpl.com shiller-pe, monthly)
  - Price-to-Book              (multpl.com s-p-500-price-to-book, quarterly)
  - Tobin's Q                  (FRED: MVEONWMVBSNNCB / TNWBSNNCB, quarterly)
  - Fed Balance Sheet (WALCL)  (FRED)
  - M2 Money Supply  (M2SL)    (FRED)

Output: data/valuations.json
"""

import json
import os
import re
import time
from datetime import datetime, timezone, timedelta
from html.parser import HTMLParser

import requests

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}
FRED_CSV_BASE  = "https://fred.stlouisfed.org/graph/fredgraph.csv"
FRED_API_BASE  = "https://api.stlouisfed.org/fred/series/observations"
FRED_API_KEY   = os.environ.get("FRED_API_KEY", "")
OUTPUT_PATH    = "data/valuations.json"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def save(path: str, data: object):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, separators=(",", ":"))
    size_kb = os.path.getsize(path) // 1024
    print(f"  Saved {path} ({size_kb} KB)")


def load_existing() -> dict:
    try:
        with open(OUTPUT_PATH) as f:
            return json.load(f)
    except Exception:
        return {}


def display_date_monthly(ts_ms: int) -> str:
    return datetime.utcfromtimestamp(ts_ms / 1000).strftime("%b %Y")


def display_date_weekly(ts_ms: int) -> str:
    return datetime.utcfromtimestamp(ts_ms / 1000).strftime("%d %b %Y")


# ---------------------------------------------------------------------------
# FRED fetcher
# ---------------------------------------------------------------------------

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
            params={"series_id": series, "observation_start": start_date,
                    "file_type": "json", "api_key": FRED_API_KEY},
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


def fetch_fred_indicator(series: str, label: str, unit: str, years: int,
                         date_fn=display_date_monthly, retries: int = 3) -> dict | None:
    now = datetime.now(timezone.utc)
    start = (now - timedelta(days=years * 365)).strftime("%Y-%m-%d")
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            points = fetch_fred(series, start)
            if not points:
                last_err = "empty response"
                time.sleep(10)
                continue
            last = points[-1]
            return {
                "label":   label,
                "value":   last["y"],
                "unit":    unit,
                "date":    date_fn(last["x"]),
                "history": points,
            }
        except Exception as e:
            last_err = e
            print(f"  ERROR {series} (attempt {attempt}): {e}")
            if attempt < retries:
                time.sleep(10)
    print(f"  FAILED {series} after {retries} attempts: {last_err}")
    return None


# ---------------------------------------------------------------------------
# multpl.com scraper
# ---------------------------------------------------------------------------

class _TableParser(HTMLParser):
    """Extracts rows from multpl.com data tables."""

    def __init__(self):
        super().__init__()
        self._in_td = False
        self._cell = ""
        self._row: list[str] = []
        self.rows: list[list[str]] = []

    def handle_starttag(self, tag, attrs):
        if tag == "tr":
            self._row = []
        elif tag == "td":
            self._in_td = True
            self._cell = ""

    def handle_endtag(self, tag):
        if tag == "td":
            self._in_td = False
            self._row.append(self._cell.strip())
        elif tag == "tr" and len(self._row) >= 2:
            self.rows.append(self._row[:2])

    def handle_data(self, data):
        if self._in_td:
            self._cell += data

    def handle_entityref(self, name):
        pass

    def handle_charref(self, name):
        pass


def _parse_multpl_date(s: str) -> datetime | None:
    s = s.strip()
    # Remove ordinal suffixes: 1st, 2nd, 3rd, 4th …
    s = re.sub(r"(\d)(st|nd|rd|th)", r"\1", s)
    for fmt in ("%b %d, %Y", "%b %d %Y", "%b %Y"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def scrape_multpl(slug: str, label: str, unit: str, years: int = 20,
                  table: str = "by-month", retries: int = 3) -> dict | None:
    """Scrape monthly or quarterly table from multpl.com."""
    url = f"https://www.multpl.com/{slug}/table/{table}"
    cutoff = datetime.now(timezone.utc) - timedelta(days=years * 365)
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=25)
            r.raise_for_status()
            parser = _TableParser()
            parser.feed(r.text)
            points = []
            for row in parser.rows:
                dt = _parse_multpl_date(row[0])
                if dt is None or dt < cutoff:
                    continue
                val_str = row[1].strip().replace(",", "")
                try:
                    points.append({"x": int(dt.timestamp() * 1000), "y": float(val_str)})
                except ValueError:
                    continue
            if not points:
                last_err = "no data points parsed"
                time.sleep(15)
                continue
            points.sort(key=lambda p: p["x"])
            last = points[-1]
            return {
                "label":   label,
                "value":   last["y"],
                "unit":    unit,
                "date":    display_date_monthly(last["x"]),
                "history": points,
            }
        except Exception as e:
            last_err = e
            print(f"  ERROR multpl {slug} (attempt {attempt}): {e}")
            if attempt < retries:
                time.sleep(15)
    print(f"  FAILED multpl {slug}: {last_err}")
    return None


# ---------------------------------------------------------------------------
# Tobin's Q: MVEONWMVBSNNCB (market value equities, $B) / TNWBSNNCB (net worth, $B)
# Both series are quarterly; we align by exact date match.
# ---------------------------------------------------------------------------

def fetch_tobins_q(years: int = 20) -> dict | None:
    now = datetime.now(timezone.utc)
    start = (now - timedelta(days=years * 365)).strftime("%Y-%m-%d")
    try:
        mve = fetch_fred("MVEONWMVBSNNCB", start)
        tnw = fetch_fred("TNWBSNNCB", start)
    except Exception as e:
        print(f"  FAILED Tobin's Q fetch: {e}")
        return None

    if not mve or not tnw:
        print("  FAILED Tobin's Q: empty series")
        return None

    # Build lookup by timestamp for TNW
    tnw_map = {p["x"]: p["y"] for p in tnw}
    points = []
    for p in mve:
        denom = tnw_map.get(p["x"])
        if denom and denom != 0.0:
            points.append({"x": p["x"], "y": round(p["y"] / denom, 4)})

    if not points:
        print("  FAILED Tobin's Q: no overlapping dates")
        return None

    points.sort(key=lambda p: p["x"])
    last = points[-1]
    return {
        "label":   "Tobin's Q",
        "value":   last["y"],
        "unit":    "x",
        "date":    display_date_monthly(last["x"]),
        "history": points,
    }


# ---------------------------------------------------------------------------
# Fed balance sheet conversion: WALCL is in millions → convert to trillions
# ---------------------------------------------------------------------------

def fetch_fed_balance_sheet(years: int = 20) -> dict | None:
    raw = fetch_fred_indicator(
        series="WALCL", label="Fed Balance Sheet",
        unit="T", years=years, date_fn=display_date_weekly
    )
    if raw is None:
        return None
    # WALCL is in millions USD — convert to trillions
    raw["value"] = raw["value"] / 1_000_000
    raw["history"] = [{"x": p["x"], "y": p["y"] / 1_000_000} for p in raw["history"]]
    return raw


# ---------------------------------------------------------------------------
# M2: M2SL is in billions USD → convert to trillions
# ---------------------------------------------------------------------------

def fetch_m2(years: int = 20) -> dict | None:
    raw = fetch_fred_indicator(
        series="M2SL", label="M2 Money Supply",
        unit="T", years=years, date_fn=display_date_monthly
    )
    if raw is None:
        return None
    # M2SL is in billions USD — convert to trillions
    raw["value"] = raw["value"] / 1_000
    raw["history"] = [{"x": p["x"], "y": p["y"] / 1_000} for p in raw["history"]]
    return raw


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print(f"=== fetch_valuations.py  {ts} ===")

    previous = load_existing()

    print("Fetching CAPE (multpl.com)...")
    cape = scrape_multpl("shiller-pe", "CAPE & Shiller P/E", "x", years=20)
    time.sleep(3)

    print("Fetching Price-to-Book (multpl.com)...")
    ptb = scrape_multpl("s-p-500-price-to-book", "Price-to-Book", "x", years=20, table="by-quarter")
    time.sleep(3)

    print("Fetching Fed Balance Sheet (FRED WALCL)...")
    fed_bs = fetch_fed_balance_sheet(years=20)

    print("Fetching M2 Money Supply (FRED M2SL)...")
    m2 = fetch_m2(years=20)

    print("Fetching Tobin's Q (FRED MVEONWMVBSNNCB / TNWBSNNCB)...")
    tobins_q = fetch_tobins_q(years=20)

    fresh = {
        "fetched_at":        int(time.time() * 1000),
        "cape":              cape,
        "price_to_book":     ptb,
        "tobins_q":          tobins_q,
        "fed_balance_sheet": fed_bs,
        "m2":                m2,
    }

    # Fall back to previous for any field that failed
    for key in ("cape", "price_to_book", "tobins_q", "fed_balance_sheet", "m2"):
        if fresh[key] is None and previous.get(key) is not None:
            print(f"  FALLBACK {key}: using previous value ({previous[key].get('value')})")
            fresh[key] = previous[key]

    save(OUTPUT_PATH, fresh)
    print("=== Done ===")


if __name__ == "__main__":
    main()
