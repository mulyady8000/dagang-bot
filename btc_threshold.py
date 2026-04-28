"""
BTC threshold mispricing detector.

Hypothesis (from Day 1 analysis): Polymarket has many "Will Bitcoin reach $X
by date Y" markets that should re-price in real time as spot BTC moves. Some
do not — they lag the spot by minutes during volatile periods. That lag is
the most algorithm-friendly edge we can find without sports/news knowledge.

This bot:
  1. Fetches live BTC spot from a working exchange (OKX).
  2. Fetches all active Polymarket markets whose slug looks like a BTC
     threshold question ("bitcoin-above-Xk-on-...", "will-bitcoin-reach-X...").
  3. For each, parses the threshold and end date from the slug.
  4. Computes a *naive* expected probability:
        - if spot already past threshold AND end date has not passed -> >= 0.5
          (specifically: 1 - small decay reflecting time to potentially fall back)
        - if spot below threshold and far away -> small (depends on distance)
     This is NOT a real options-pricing model. It's a sanity check to flag
     obvious dislocations.
  5. Emits a row per market each poll: spot, threshold, days_left, market_yes,
     naive_expected, gap_pct. Big gap = candidate to investigate manually.

Outputs JSONL — same pattern as cloud_collect.py — so analyze.py can read it.
"""

from __future__ import annotations

import json
import math
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

ROOT = Path(__file__).parent
DATA_DIR = ROOT / "data"

GAMMA_MARKETS_URL = "https://gamma-api.polymarket.com/markets"
CLOB_PRICE_URL = "https://clob.polymarket.com/price"
OKX_TICKER_URL = "https://www.okx.com/api/v5/market/ticker"

HTTP_TIMEOUT = 15

# Threshold-style slug:  "...above-79k...", "...dip-to-70000...", "...reach-150k..."
# Captures (number, optional 'k' suffix).
THRESHOLD_RE = re.compile(
    r"(?:bitcoin|btc).*?(above|reach|hit|dip[- ]?to|below|under)[- ]?\$?(\d+)(k?)",
    re.IGNORECASE,
)

# Range-style slug:  "...between-66000-68000...", "...between-78000-and-80000..."
# Captures (low, low_k_suffix, high, high_k_suffix).
BETWEEN_RE = re.compile(
    r"(?:bitcoin|btc).*?between[- ]?\$?(\d+)(k?)[- ]?(?:and[- ]?)?\$?(\d+)(k?)",
    re.IGNORECASE,
)


def _to_dollars(n: int, has_k: bool) -> int:
    if has_k:
        return n * 1000
    if n < 1000:
        return n * 1000
    return n


# BTC realized vol on the dataset is ~3-4% daily. Implied is usually higher.
# Tuneable via env if you want to backtest different assumptions.
DAILY_VOL = float(__import__("os").environ.get("BTC_DAILY_VOL", "0.04"))
DAILY_DRIFT = float(__import__("os").environ.get("BTC_DAILY_DRIFT", "0.0005"))
MIN_DAYS_LEFT = 0.01  # filter out markets with under ~15 minutes to expiry


def fetch_btc_spot() -> float | None:
    try:
        r = requests.get(OKX_TICKER_URL, params={"instId": "BTC-USDT"}, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        items = r.json().get("data", [])
        if not items:
            return None
        return (float(items[0]["bidPx"]) + float(items[0]["askPx"])) / 2
    except (requests.RequestException, KeyError, ValueError, IndexError):
        return None


def fetch_btc_threshold_markets() -> list[dict]:
    """Return active Polymarket markets that look like BTC threshold questions."""
    params = {
        "active": "true",
        "closed": "false",
        "limit": 500,
        "order": "volume24hr",
        "ascending": "false",
    }
    r = requests.get(GAMMA_MARKETS_URL, params=params, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    raw = r.json()

    out = []
    for m in raw:
        slug = (m.get("slug") or "").lower()
        question = m.get("question", "")
        if "bitcoin" not in slug and "btc" not in slug:
            continue
        # Skip markets we can't model: "X or Y first" races, conditionals.
        if "first" in slug or " or " in question.lower():
            continue

        kind: str | None = None
        threshold: int | None = None
        low: int | None = None
        high: int | None = None

        # Try "between X and Y" first — they otherwise match THRESHOLD_RE wrongly.
        bm = BETWEEN_RE.search(slug)
        if bm:
            lo = _to_dollars(int(bm.group(1)), bool(bm.group(2)))
            hi = _to_dollars(int(bm.group(3)), bool(bm.group(4)))
            if lo > hi:
                lo, hi = hi, lo
            if 10_000 <= lo < hi <= 1_000_000:
                kind = "between"
                low, high = lo, hi
        else:
            tm = THRESHOLD_RE.search(slug)
            if tm:
                verb = tm.group(1).lower()
                n = int(tm.group(2))
                t = _to_dollars(n, bool(tm.group(3)))
                if 10_000 <= t <= 1_000_000:
                    threshold = t
                    kind = "below" if any(w in verb for w in ("below", "under", "dip")) else "above"

        if kind is None:
            continue

        token_ids_raw = m.get("clobTokenIds")
        if not token_ids_raw:
            continue
        try:
            token_ids = json.loads(token_ids_raw) if isinstance(token_ids_raw, str) else token_ids_raw
        except json.JSONDecodeError:
            continue
        if len(token_ids) != 2:
            continue

        out.append({
            "slug": slug,
            "question": question,
            "kind": kind,
            "threshold": threshold,
            "low": low,
            "high": high,
            "yes_token_id": str(token_ids[0]),
            "no_token_id": str(token_ids[1]),
            "end_date": m.get("endDate"),
            "volume_usd": float(m.get("volume") or 0),
        })
    return out


def fetch_yes_prices(token_id: str) -> tuple[float | None, float | None]:
    """Return (best_bid, best_ask) for one token."""
    try:
        bid_resp = requests.get(CLOB_PRICE_URL, params={"token_id": token_id, "side": "BUY"}, timeout=HTTP_TIMEOUT)
        ask_resp = requests.get(CLOB_PRICE_URL, params={"token_id": token_id, "side": "SELL"}, timeout=HTTP_TIMEOUT)
        bid = bid_resp.json().get("price")
        ask = ask_resp.json().get("price")
        return (float(bid) if bid else None, float(ask) if ask else None)
    except (requests.RequestException, ValueError):
        return None, None


def days_until(end_date: str | None) -> float | None:
    if not end_date:
        return None
    try:
        end = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
        delta = end - datetime.now(timezone.utc)
        return delta.total_seconds() / 86400.0
    except ValueError:
        return None


def _norm_cdf(x: float) -> float:
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


def _prob_above(spot: float, threshold: float, days_left: float) -> float:
    """P(S_T > threshold) under lognormal w/ daily drift + vol."""
    sigma = DAILY_VOL * math.sqrt(days_left)
    drift = (DAILY_DRIFT - 0.5 * DAILY_VOL ** 2) * days_left  # risk-neutral-ish
    if sigma <= 0 or spot <= 0:
        return 1.0 if spot >= threshold else 0.0
    z = (math.log(threshold / spot) - drift) / sigma
    return 1 - _norm_cdf(z)


def naive_expected_prob(market: dict, spot: float, days_left: float | None) -> float:
    """Probability the YES outcome resolves true, under our toy lognormal model.

    Used only to flag obvious dislocations (gap > ~30c). NOT a real pricing model.
    Model assumes ~4% daily vol with mild positive drift — tunable via env.
    """
    kind = market["kind"]

    # Already expired markets — bot shouldn't see them, but if it does, settle binary.
    if days_left is None or days_left <= MIN_DAYS_LEFT:
        if kind == "above":
            return 1.0 if spot >= market["threshold"] else 0.0
        if kind == "below":
            return 1.0 if spot <= market["threshold"] else 0.0
        if kind == "between":
            return 1.0 if market["low"] <= spot <= market["high"] else 0.0
        return 0.5

    if kind == "above":
        return _prob_above(spot, market["threshold"], days_left)
    if kind == "below":
        return 1 - _prob_above(spot, market["threshold"], days_left)
    if kind == "between":
        return _prob_above(spot, market["low"], days_left) - _prob_above(spot, market["high"], days_left)
    return 0.5


def append_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, separators=(",", ":")) + "\n")


def collect(ts: str) -> list[dict]:
    spot = fetch_btc_spot()
    if spot is None:
        print("  could not fetch BTC spot — skipping", flush=True)
        return []
    print(f"  BTC spot: ${spot:,.2f}", flush=True)

    markets = fetch_btc_threshold_markets()
    print(f"  candidate threshold markets: {len(markets)}", flush=True)
    if not markets:
        return []

    out: list[dict] = []
    with ThreadPoolExecutor(max_workers=8) as pool:
        future_to_m = {pool.submit(fetch_yes_prices, m["yes_token_id"]): m for m in markets}
        for fut in as_completed(future_to_m):
            m = future_to_m[fut]
            try:
                yes_bid, yes_ask = fut.result()
            except Exception:
                continue
            if yes_bid is None or yes_ask is None:
                continue
            yes_mid = (yes_bid + yes_ask) / 2
            d_left = days_until(m["end_date"])

            # Skip markets that already settled — they're not mispriced, just done.
            if d_left is not None and d_left <= MIN_DAYS_LEFT:
                continue

            expected = naive_expected_prob(m, spot, d_left)
            gap = yes_mid - expected  # positive = market overpriced YES, negative = underpriced

            out.append({
                "ts": ts,
                "slug": m["slug"],
                "question": m["question"],
                "kind": m["kind"],
                "threshold": m.get("threshold"),
                "low": m.get("low"),
                "high": m.get("high"),
                "btc_spot": spot,
                "days_left": d_left,
                "yes_bid": yes_bid,
                "yes_ask": yes_ask,
                "yes_mid": yes_mid,
                "naive_expected": expected,
                "gap": gap,
                "volume_usd": m["volume_usd"],
                "end_date": m["end_date"],
            })

    # log the most extreme dislocations to stdout
    out.sort(key=lambda r: -abs(r["gap"]))
    for r in out[:5]:
        if r["kind"] == "between":
            spec = f"{r['kind']} ${r['low']:,}-${r['high']:,}"
        else:
            spec = f"{r['kind']} ${r['threshold']:,}"
        print(
            f"  gap={r['gap']:+.2f}  yes_mid={r['yes_mid']:.3f}  "
            f"expected={r['naive_expected']:.3f}  {spec:<22}  "
            f"{r['slug'][:55]}",
            flush=True,
        )
    return out


def main() -> int:
    ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
    print(f"[{ts}] BTC threshold scan", flush=True)
    rows = collect(ts)
    if rows:
        append_jsonl(DATA_DIR / ts[:10] / "btc_threshold.jsonl", rows)
        print(f"  wrote {len(rows)} rows -> data/{ts[:10]}/btc_threshold.jsonl", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
