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

# Slug patterns that look like BTC threshold markets.
# Captures (number, optional 'k' suffix). e.g.:
#   "...reach-79k..."     -> ("79", "k")  -> $79,000
#   "...dip-to-70000..."  -> ("70000", "") -> $70,000
#   "...above-150k..."    -> ("150", "k") -> $150,000
SLUG_RE = re.compile(
    r"(?:bitcoin|btc).*?(?:above|reach|hit|dip[- ]?to|below|under|between)[- ]?\$?(\d+)(k?)",
    re.IGNORECASE,
)


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
        match = SLUG_RE.search(slug)
        if not match:
            continue
        n = int(match.group(1))
        has_k_suffix = bool(match.group(2))
        if has_k_suffix:
            threshold = n * 1000      # "79k" -> 79000
        elif n < 1000:
            threshold = n * 1000      # bare "79" assumed thousands (rare)
        else:
            threshold = n             # "70000" stays 70000

        # Sanity: BTC thresholds we care about are between $10k and $1M.
        if threshold < 10_000 or threshold > 1_000_000:
            continue
        # detect direction
        if any(w in slug for w in ["below", "under", "dip"]):
            direction = "below"
        else:
            direction = "above"

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
            "threshold": threshold,
            "direction": direction,
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


def naive_expected_prob(spot: float, threshold: float, direction: str, days_left: float | None) -> float:
    """A toy heuristic — not a real pricing model.

    Gives a rough probability the threshold is hit by expiry. Used only to flag
    obvious dislocations: when actual market price is wildly different from this
    estimate, something is interesting.

    Assumes ~3% daily BTC volatility, lognormal-ish. Calibrate later from data.
    """
    if days_left is None or days_left <= 0:
        # event has expired or unknown timing -> binary check
        if direction == "above":
            return 1.0 if spot >= threshold else 0.0
        else:
            return 1.0 if spot <= threshold else 0.0

    # Distance in log-vol units
    daily_vol = 0.03
    sigma = daily_vol * math.sqrt(days_left)
    if spot <= 0 or threshold <= 0:
        return 0.5
    log_dist = math.log(threshold / spot)
    z = log_dist / sigma if sigma > 0 else 0

    # P(spot at expiry > threshold)  for a geometric Brownian motion w/ zero drift
    if direction == "above":
        return 1 - 0.5 * (1 + math.erf(z / math.sqrt(2)))
    else:
        return 0.5 * (1 + math.erf(z / math.sqrt(2)))


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
            expected = naive_expected_prob(spot, m["threshold"], m["direction"], d_left)
            gap = yes_mid - expected  # positive = market overpriced YES, negative = underpriced

            out.append({
                "ts": ts,
                "slug": m["slug"],
                "question": m["question"],
                "threshold": m["threshold"],
                "direction": m["direction"],
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
        sign = "+" if r["gap"] > 0 else ""
        print(
            f"  gap={sign}{r['gap']:+.2f}  yes_mid={r['yes_mid']:.3f}  "
            f"expected={r['naive_expected']:.3f}  thresh=${r['threshold']:,}  "
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
