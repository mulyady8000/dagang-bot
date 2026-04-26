"""
Single-shot data collector for GitHub Actions cron mode.

Runs once: pulls one snapshot from all 3 sources (crypto exchanges, top
Polymarket crypto markets, small Polymarket markets with orderbook depth),
appends each as a JSON line to per-day, per-source files under data/.

Designed to be invoked every 5 minutes by .github/workflows/collect.yml.
Total runtime per invocation: ~10-30 seconds. Total monthly cost: $0.
"""

from __future__ import annotations

import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# Reuse the fetch logic from the interactive bots.
import crypto_monitor as cm
import monitor as pm
import small_market_monitor as sm

ROOT = Path(__file__).parent
DATA_DIR = ROOT / "data"


def append_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, separators=(",", ":")) + "\n")


def collect_crypto(ts: str) -> list[dict]:
    cfg = cm.load_config()
    quotes = cm.fetch_all_parallel(cfg["symbols"], cfg["exchanges"])
    return [
        {
            "ts": ts,
            "exchange": q.exchange,
            "symbol": q.symbol,
            "bid": q.bid,
            "ask": q.ask,
            "mid": q.mid,
        }
        for q in quotes
    ]


def _fetch_pm_market(m, ts: str) -> dict | None:
    yes_mid = pm.fetch_midpoint(m.yes_token_id)
    no_mid = pm.fetch_midpoint(m.no_token_id)
    yes_ask = pm.fetch_ask(m.yes_token_id)
    no_ask = pm.fetch_ask(m.no_token_id)
    if yes_mid is None or no_mid is None:
        return None
    return {
        "ts": ts,
        "slug": m.slug,
        "question": m.question,
        "yes_mid": yes_mid,
        "no_mid": no_mid,
        "yes_ask": yes_ask,
        "no_ask": no_ask,
        "ask_sum": (yes_ask + no_ask) if (yes_ask and no_ask) else None,
        "volume_usd": m.volume_usd,
    }


def collect_polymarket_top(ts: str) -> list[dict]:
    cfg = pm.load_config()
    markets = pm.fetch_active_markets(
        min_volume=cfg["min_volume_usd"],
        limit=cfg["max_markets_to_watch"],
        watch_slugs=cfg["watch_slugs"],
        keywords=cfg.get("keyword_filter") or None,
    )
    out: list[dict] = []
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = [pool.submit(_fetch_pm_market, m, ts) for m in markets]
        for f in as_completed(futures):
            row = f.result()
            if row:
                out.append(row)
    return out


def _fetch_sm_market(m, ts: str) -> dict:
    book = sm.fetch_full_book(m)
    ya, yb, na, nb = book.yes_ask, book.yes_bid, book.no_ask, book.no_bid
    return {
        "ts": ts,
        "slug": m.slug,
        "question": m.question,
        "volume_usd": m.volume_usd,
        "yes_ask": ya.best_price if ya else None,
        "yes_ask_size_usd": ya.best_size if ya else None,
        "yes_bid": yb.best_price if yb else None,
        "no_ask": na.best_price if na else None,
        "no_ask_size_usd": na.best_size if na else None,
        "no_bid": nb.best_price if nb else None,
        "ask_sum": (ya.best_price + na.best_price) if (ya and na) else None,
    }


def collect_small_markets(ts: str) -> list[dict]:
    cfg = sm.load_config()
    markets = sm.fetch_candidate_markets(cfg)
    out: list[dict] = []
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = [pool.submit(_fetch_sm_market, m, ts) for m in markets]
        for f in as_completed(futures):
            try:
                out.append(f.result())
            except Exception:
                continue
    return out


def main() -> int:
    started = time.time()
    ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
    date_str = ts[:10]  # YYYY-MM-DD

    print(f"[{ts}] starting single-shot collect")

    sources = [
        ("crypto",       lambda: collect_crypto(ts)),
        ("polymarket",   lambda: collect_polymarket_top(ts)),
        ("small_market", lambda: collect_small_markets(ts)),
    ]

    total_rows = 0
    for name, fetch in sources:
        try:
            rows = fetch()
            append_jsonl(DATA_DIR / date_str / f"{name}.jsonl", rows)
            print(f"  {name}: {len(rows)} rows -> data/{date_str}/{name}.jsonl")
            total_rows += len(rows)
        except Exception as e:
            print(f"  {name}: FAILED ({type(e).__name__}: {e})")

    elapsed = time.time() - started
    print(f"done in {elapsed:.1f}s, {total_rows} total rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
