"""
Resolution tracker — records the final outcome of Polymarket markets.

Goal: answer "did our bot's signals predict reality?". Each cron cycle we
fetch the most recently closed markets from Polymarket and append their
outcome (YES or NO) to data/<date>/resolutions.jsonl.

At analysis time we join this against btc_threshold.jsonl / polymarket.jsonl /
small_market.jsonl on `slug`, then check:
  - For markets the bot saw, what was the final outcome?
  - For threshold markets where our model said gap = +0.45 (sell YES),
    did YES actually settle to 0?
  - Calibration: of all markets the bot observed at "60% YES probability",
    what fraction actually resolved YES? (Should be ~60% if market is wise.)

This is the only honest test of bot skill. Without it, we just have noise.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

ROOT = Path(__file__).parent
DATA_DIR = ROOT / "data"

GAMMA_MARKETS_URL = "https://gamma-api.polymarket.com/markets"
HTTP_TIMEOUT = 20

# How many closed markets to fetch per run. The API returns most-recently-closed
# first, so 500 covers far more than 1 day of resolutions on Polymarket.
FETCH_LIMIT = 500


def parse_outcome(outcome_prices_raw: object) -> str | None:
    """Polymarket gives outcomePrices as a JSON-encoded string of two strings.
    ["1", "0"] => YES won, ["0", "1"] => NO won, else unknown.
    """
    if outcome_prices_raw is None:
        return None
    try:
        arr = json.loads(outcome_prices_raw) if isinstance(outcome_prices_raw, str) else outcome_prices_raw
    except json.JSONDecodeError:
        return None
    if not isinstance(arr, list) or len(arr) != 2:
        return None
    try:
        yes_p = float(arr[0])
        no_p = float(arr[1])
    except (TypeError, ValueError):
        return None
    if yes_p == 1.0 and no_p == 0.0:
        return "YES"
    if yes_p == 0.0 and no_p == 1.0:
        return "NO"
    return None  # ambiguous (e.g., not yet finalized)


def collect(ts: str) -> list[dict]:
    """Return rows for all currently-closed Polymarket markets with a clear outcome."""
    try:
        r = requests.get(
            GAMMA_MARKETS_URL,
            params={
                "closed": "true",
                "limit": FETCH_LIMIT,
                "order": "endDate",
                "ascending": "false",
            },
            timeout=HTTP_TIMEOUT,
        )
        r.raise_for_status()
        raw = r.json()
    except requests.RequestException as e:
        print(f"  resolution fetch failed: {e}", flush=True)
        return []

    out: list[dict] = []
    yes_count = 0
    no_count = 0
    skipped = 0
    for m in raw:
        slug = m.get("slug")
        if not slug:
            continue
        outcome = parse_outcome(m.get("outcomePrices"))
        if outcome is None:
            skipped += 1
            continue
        if outcome == "YES":
            yes_count += 1
        else:
            no_count += 1
        out.append({
            "ts": ts,
            "slug": slug,
            "question": (m.get("question") or "")[:200],
            "outcome": outcome,
            "end_date": m.get("endDate"),
            "volume_usd": float(m.get("volume") or 0),
        })

    print(
        f"  resolutions: {len(out)} clear ({yes_count} YES / {no_count} NO), {skipped} ambiguous",
        flush=True,
    )
    return out


def main() -> int:
    ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
    print(f"[{ts}] resolution tracker", flush=True)
    rows = collect(ts)
    if rows:
        out_path = DATA_DIR / ts[:10] / "resolutions.jsonl"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with out_path.open("a", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row, separators=(",", ":")) + "\n")
        print(f"  wrote {len(rows)} rows -> {out_path.relative_to(ROOT)}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
