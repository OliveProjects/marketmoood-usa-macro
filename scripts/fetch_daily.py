#!/usr/bin/env python3
"""
Runs once daily after US market close.

Outputs:
  data/macro.json   – FRED macro indicators (Fed rate, CPI, unemployment, yields, sentiment)
  data/events.json  – Upcoming high-impact US economic events (BLS / Fed / BEA)
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
FRED_API_BASE        = "https://api.stlouisfed.org/fred/series/observations"
FRED_CSV_BASE        = "https://fred.stlouisfed.org/graph/fredgraph.csv"
FRED_API_KEY         = os.environ.get("FRED_API_KEY", "")
ALTERNATIVE_ME_FNG   = "https://api.alternative.me/fng/"
CNN_FNG_HISTORY_URL  = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata/2021-01-01"
MACRO_PATH           = "data/macro.json"
CRYPTO_PATH          = "data/crypto.json"
FNG_STATS_PATH       = "data/fng_stats.json"
FNG_DEBOUNCE_DAYS    = 30


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


# ── Official US economic calendar (BLS / Fed / BEA) ──────────────────────────

class _TableParser(HTMLParser):
    """Collects plain text from HTML table cells."""
    def __init__(self):
        super().__init__()
        self.rows: list[list[str]] = []
        self._row:  list[str] | None = None
        self._cell: str | None = None

    def handle_starttag(self, tag, attrs):
        if tag == "tr":
            self._row = []
        elif tag in ("td", "th") and self._row is not None:
            self._cell = ""

    def handle_endtag(self, tag):
        if tag in ("td", "th") and self._cell is not None:
            self._row.append(self._cell.strip())
            self._cell = None
        elif tag == "tr" and self._row is not None:
            if any(c.strip() for c in self._row):
                self.rows.append(self._row)
            self._row = None

    def handle_data(self, data):
        if self._cell is not None:
            self._cell += data


def _to_iso(text: str) -> str | None:
    """Parse 'July 15, 2026' or 'Jul 15, 2026' → '2026-07-15'."""
    text = text.strip()
    for fmt in ("%B %d, %Y", "%b %d, %Y", "%B %d,%Y"):
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def _event(label: str, date_iso: str, release_time: str, category: str) -> dict:
    return {
        "event": label, "date": date_iso, "time": release_time,
        "category": category, "estimate": None, "previous": None, "actual": None,
    }


def _fomc_events(now: datetime, cutoff: datetime) -> list[dict]:
    """Fetch FOMC meeting end-dates (= rate decision day) from federalreserve.gov."""
    try:
        r = requests.get(
            "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm",
            headers=HEADERS, timeout=20,
        )
        r.raise_for_status()
        months_pat = (
            "January|February|March|April|May|June|July|"
            "August|September|October|November|December"
        )
        today_str  = now.strftime("%Y-%m-%d")
        cutoff_str = cutoff.strftime("%Y-%m-%d")
        events: list[dict] = []
        seen: set[str] = set()
        # Matches "January 28-29, 2026" or "July 29-30* 2026" anywhere in page
        for m in re.finditer(
            rf'\b({months_pat})\s+(\d{{1,2}})\s*[-–]\s*(\d{{1,2}})\*?,?\s*(20\d\d)',
            r.text,
        ):
            month_name, _day1, day2, year = m.groups()
            try:
                date_iso = datetime.strptime(
                    f"{month_name} {day2} {year}", "%B %d %Y"
                ).strftime("%Y-%m-%d")
            except ValueError:
                continue
            if date_iso < today_str or date_iso > cutoff_str or date_iso in seen:
                continue
            seen.add(date_iso)
            events.append(_event("Fed Interest Rate Decision", date_iso, "18:00:00", "fed"))
        print(f"  FOMC: {len(events)} upcoming")
        return events
    except Exception as e:
        print(f"  WARN FOMC: {type(e).__name__}: {e}")
        return []


def _bls_events(now: datetime, cutoff: datetime) -> list[dict]:
    """Fetch CPI and NFP release dates from the BLS news release schedule."""
    TARGETS = {
        "consumer price index": ("Inflation Rate (CPI)", "inflation", "12:30:00"),
        "employment situation": ("Non Farm Payrolls",    "jobs",      "12:30:00"),
    }
    today_str  = now.strftime("%Y-%m-%d")
    cutoff_str = cutoff.strftime("%Y-%m-%d")
    events: list[dict] = []
    try:
        r = requests.get(
            "https://www.bls.gov/schedule/news_release/",
            headers=HEADERS, timeout=20,
        )
        r.raise_for_status()
        parser = _TableParser()
        parser.feed(r.text)
        seen: set[tuple] = set()
        for row in parser.rows:
            if len(row) < 2:
                continue
            name_lower = row[0].lower()
            match = next(
                ((k, v) for k, v in TARGETS.items() if k in name_lower), None
            )
            if not match:
                continue
            _, (label, category, release_time) = match
            date_iso = None
            for cell in row[1:]:
                date_iso = _to_iso(cell)
                if date_iso:
                    break
            if not date_iso or date_iso < today_str or date_iso > cutoff_str:
                continue
            key = (date_iso, label)
            if key in seen:
                continue
            seen.add(key)
            events.append(_event(label, date_iso, release_time, category))
        print(f"  BLS: {len(events)} upcoming")
    except Exception as e:
        print(f"  WARN BLS: {type(e).__name__}: {e}")
    return events


def _bea_events(now: datetime, cutoff: datetime) -> list[dict]:
    """Fetch GDP and PCE release dates from BEA news release schedule."""
    TARGETS = {
        "gross domestic product":           ("GDP Growth Rate",      "gdp",       "12:30:00"),
        "personal income and outlays":      ("Core PCE Price Index", "inflation", "12:30:00"),
        "personal consumption expenditure": ("Core PCE Price Index", "inflation", "12:30:00"),
    }
    today_str  = now.strftime("%Y-%m-%d")
    cutoff_str = cutoff.strftime("%Y-%m-%d")
    events: list[dict] = []
    try:
        r = requests.get(
            "https://www.bea.gov/news/schedule",
            headers=HEADERS, timeout=20,
        )
        r.raise_for_status()
        parser = _TableParser()
        parser.feed(r.text)
        seen: set[tuple] = set()
        for row in parser.rows:
            if len(row) < 2:
                continue
            name_lower = row[0].lower()
            match = next(
                ((k, v) for k, v in TARGETS.items() if k in name_lower), None
            )
            if not match:
                continue
            _, (label, category, release_time) = match
            date_iso = None
            for cell in row[1:]:
                date_iso = _to_iso(cell)
                if date_iso:
                    break
            if not date_iso or date_iso < today_str or date_iso > cutoff_str:
                continue
            key = (date_iso, label)
            if key in seen:
                continue
            seen.add(key)
            events.append(_event(label, date_iso, release_time, category))
        print(f"  BEA: {len(events)} upcoming")
    except Exception as e:
        print(f"  WARN BEA: {type(e).__name__}: {e}")
    return events


def fetch_events(now: datetime) -> dict | None:
    from_date = now.replace(day=1).strftime("%Y-%m-%d")
    to_date   = (now + timedelta(days=90)).strftime("%Y-%m-%d")
    cutoff    = now + timedelta(days=90)

    all_events = (
        _fomc_events(now, cutoff)
        + _bls_events(now, cutoff)
        + _bea_events(now, cutoff)
    )

    seen: set[tuple] = set()
    events: list[dict] = []
    for e in all_events:
        key = (e["date"], e["event"])
        if key not in seen:
            seen.add(key)
            events.append(e)
    events.sort(key=lambda e: (e["date"], e.get("time") or ""))

    future = [e for e in events if e["date"] >= now.strftime("%Y-%m-%d")]
    print(f"  Events total: {len(events)} ({len(future)} upcoming)")

    return {
        "fetched_at": int(time.time() * 1000),
        "from":       from_date,
        "to":         to_date,
        "events":     events,
    }


# ── F&G zone forward-return statistics ───────────────────────────────────────

def _fng_classify(score: float) -> str:
    if score < 25: return "Extreme Fear"
    if score < 45: return "Fear"
    if score < 56: return "Neutral"
    if score < 75: return "Greed"
    return "Extreme Greed"


def _fng_debounced_entries(fng_points: list[dict], min_gap: int) -> list[dict]:
    """One entry per zone per min_gap calendar days (avoids re-counting boundary churn)."""
    last_entry: dict[str, str] = {}
    entries: list[dict] = []
    prev_zone: str | None = None
    for p in fng_points:
        z = _fng_classify(p["score"])
        if z == prev_zone:
            continue
        prev_zone = z
        last = last_entry.get(z)
        if last is None or (datetime.strptime(p["date"], "%Y-%m-%d") -
                            datetime.strptime(last, "%Y-%m-%d")).days >= min_gap:
            entries.append({"date": p["date"], "score": p["score"], "zone": z})
            last_entry[z] = p["date"]
    return entries


def compute_fng_stats() -> dict | None:
    """
    Fetches CNN F&G history + SPX from FRED, computes SPX forward-return
    statistics per zone (debounced) plus an unconditional baseline,
    and returns a JSON-serializable dict for fng_stats.json.

    Design choices documented here so future maintainers know why:
    - Debounce (30 days) avoids counting boundary churn as independent episodes.
    - Baseline row lets users compare zone returns vs. "just hold the market".
    - N is stored per horizon (not per zone) because the most recent entries
      lack a complete forward window and are excluded from that horizon only.
    - Returns are stored as % (e.g. 1.6 means +1.6%) for display convenience.
    """
    # ── CNN F&G history ───────────────────────────────────────────────────────
    try:
        r = requests.get(
            CNN_FNG_HISTORY_URL,
            headers={**HEADERS, "Referer": "https://edition.cnn.com/markets/fear-and-greed"},
            timeout=20,
        )
        r.raise_for_status()
        raw = r.json()["fear_and_greed_historical"]["data"]
        fng = sorted(
            [{"date": datetime.utcfromtimestamp(p["x"] / 1000).strftime("%Y-%m-%d"),
              "score": float(p["y"])} for p in raw],
            key=lambda p: p["date"],
        )
        print(f"  F&G stats: {len(fng)} days ({fng[0]['date']} to {fng[-1]['date']})")
    except Exception as e:
        print(f"  WARN F&G stats history: {type(e).__name__}: {e}")
        return None

    # ── SPX from Yahoo Finance (no API key, faster than FRED CSV) ────────────
    try:
        r = requests.get(
            "https://query1.finance.yahoo.com/v8/finance/chart/%5EGSPC",
            params={"interval": "1d", "period1": "1609459200", "period2": "9999999999"},
            headers={**HEADERS, "Accept": "application/json"},
            timeout=15,
        )
        r.raise_for_status()
        result     = r.json()["chart"]["result"][0]
        timestamps = result["timestamp"]
        closes     = result["indicators"]["quote"][0]["close"]
        spx = {
            datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d"): price
            for ts, price in zip(timestamps, closes)
            if price is not None
        }
        print(f"  F&G stats SPX: {len(spx)} trading days")
    except Exception as e:
        print(f"  WARN F&G stats SPX: {type(e).__name__}: {e}")
        return None

    # ── Helpers ───────────────────────────────────────────────────────────────
    HORIZONS = [(30, "1M"), (91, "3M"), (182, "6M"), (365, "12M")]
    ZONES    = ["Extreme Fear", "Fear", "Neutral", "Greed", "Extreme Greed"]

    def nearest_spx(date_str: str) -> float | None:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        for offset in range(8):
            d = (dt + timedelta(days=offset)).strftime("%Y-%m-%d")
            if d in spx:
                return spx[d]
        return None

    def fwd_date(date_str: str, cal_days: int) -> str:
        return (datetime.strptime(date_str, "%Y-%m-%d") + timedelta(days=cal_days)).strftime("%Y-%m-%d")

    def horizon_stats(returns: list[float]) -> dict | None:
        # avg and pct_positive are stored in PERCENT form, not fractions:
        #   avg = 1.6 means +1.6%, NOT 0.016
        #   pct_positive = 56.0 means 56%, NOT 0.56
        # The Android app reads these values and formats them directly — do not change the unit.
        if not returns:
            return None
        n   = len(returns)
        avg = round(sum(returns) / n, 2)
        pct = round(sum(1 for v in returns if v > 0) / n * 100, 1)
        return {"n": n, "avg": avg, "pct_positive": pct}

    # ── Zone entries (debounced) ──────────────────────────────────────────────
    entries = _fng_debounced_entries(fng, FNG_DEBOUNCE_DAYS)

    zone_returns: dict[str, dict[str, list[float]]] = {z: {h[1]: [] for h in HORIZONS} for z in ZONES}

    for entry in entries:
        base = nearest_spx(entry["date"])
        if base is None:
            continue
        for cal_days, label in HORIZONS:
            future = nearest_spx(fwd_date(entry["date"], cal_days))
            if future is not None:
                zone_returns[entry["zone"]][label].append(
                    round((future / base - 1) * 100, 4)
                )

    # ── Baseline (all days in F&G window) ────────────────────────────────────
    baseline_ret: dict[str, list[float]] = {h[1]: [] for h in HORIZONS}
    for p in fng:
        base = nearest_spx(p["date"])
        if base is None:
            continue
        for cal_days, label in HORIZONS:
            future = nearest_spx(fwd_date(p["date"], cal_days))
            if future is not None:
                baseline_ret[label].append(round((future / base - 1) * 100, 4))

    # ── Build output ──────────────────────────────────────────────────────────
    zones_out = []
    for z in ZONES:
        r_map = zone_returns[z]
        n_zone = len(r_map["1M"])
        zones_out.append({
            "zone": z,
            "n": n_zone,
            "returns": {label: horizon_stats(r_map[label]) for _, label in HORIZONS},
        })

    return {
        "fetched_at":    int(time.time() * 1000),
        "from_date":     fng[0]["date"],
        "to_date":       fng[-1]["date"],
        "debounce_days": FNG_DEBOUNCE_DAYS,
        "baseline": {
            "zone":    "All days",
            "n":       len(baseline_ret["1M"]),
            "returns": {label: horizon_stats(baseline_ret[label]) for _, label in HORIZONS},
        },
        "zones": zones_out,
    }


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
    print("Fetching upcoming events (BLS / Fed / BEA)...")
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

    # ── fng_stats.json ──
    print("Computing F&G zone return statistics...")
    fng_stats = compute_fng_stats()
    if fng_stats is not None:
        # Keep old file if new one has no zone data (e.g. FRED was down)
        has_data = any(z["n"] > 0 for z in fng_stats.get("zones", []))
        if has_data:
            save(FNG_STATS_PATH, fng_stats)
        else:
            print("  WARN fng_stats: all zones have N=0 — keeping existing file")
    else:
        print("  WARN fng_stats: computation failed — keeping existing file")

    # ── crypto.json ──
    print("Fetching Crypto Fear & Greed (alternative.me)...")
    crypto = fetch_crypto_fg()
    if crypto is not None:
        save(CRYPTO_PATH, crypto)

    print("=== Done ===")


if __name__ == "__main__":
    main()
