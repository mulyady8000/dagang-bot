"""
Polymarket read-only price monitor.

Polls active markets, logs price snapshots to SQLite, and alerts when:
  - YES + NO prices drift from $1.00 by more than the configured threshold
    (a true mispricing — buy both for < $1, get $1 at resolution)
  - A market's price moves more than X% between consecutive polls
    (catches news lag — when the market hasn't reacted yet)

This bot does NOT trade. It is for Month 1 of the learning plan: observe,
log, validate that you can actually spot edges before risking money.
"""

from __future__ import annotations

import json
import sqlite3
import sys
import time

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import requests

ROOT = Path(__file__).parent
CONFIG_PATH = ROOT / "config.json"
DB_PATH = ROOT / "prices.db"

GAMMA_MARKETS_URL = "https://gamma-api.polymarket.com/markets"
CLOB_PRICE_URL = "https://clob.polymarket.com/price"
CLOB_MIDPOINT_URL = "https://clob.polymarket.com/midpoint"

HTTP_TIMEOUT = 15


@dataclass
class Market:
    slug: str
    question: str
    yes_token_id: str
    no_token_id: str
    volume_usd: float


def load_config() -> dict:
    with CONFIG_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)


def init_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS price_snapshots (
            ts TEXT NOT NULL,
            slug TEXT NOT NULL,
            question TEXT NOT NULL,
            yes_mid REAL,
            no_mid REAL,
            yes_ask REAL,
            no_ask REAL,
            ask_sum REAL,
            volume_usd REAL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS alerts (
            ts TEXT NOT NULL,
            slug TEXT NOT NULL,
            alert_type TEXT NOT NULL,
            message TEXT NOT NULL
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_snap_slug_ts ON price_snapshots(slug, ts)")
    conn.commit()
    return conn


def fetch_active_markets(min_volume: float, limit: int, watch_slugs: list[str], keywords: list[str] | None = None) -> list[Market]:
    """Pull active binary markets from the Gamma API.

    Filtering precedence: explicit watch_slugs > keyword match > top-by-volume.
    """
    params = {
        "active": "true",
        "closed": "false",
        "limit": 200,
        "order": "volume24hr",
        "ascending": "false",
    }
    resp = requests.get(GAMMA_MARKETS_URL, params=params, timeout=HTTP_TIMEOUT)
    resp.raise_for_status()
    raw = resp.json()

    markets: list[Market] = []
    for m in raw:
        slug = m.get("slug")
        if not slug:
            continue
        if watch_slugs and slug not in watch_slugs:
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

        volume = float(m.get("volume") or 0)
        if not watch_slugs and volume < min_volume:
            continue

        if not watch_slugs and keywords:
            haystack = (slug + " " + m.get("question", "")).lower()
            if not any(kw.lower() in haystack for kw in keywords):
                continue

        markets.append(
            Market(
                slug=slug,
                question=m.get("question", "")[:120],
                yes_token_id=str(token_ids[0]),
                no_token_id=str(token_ids[1]),
                volume_usd=volume,
            )
        )

    if not watch_slugs:
        markets = markets[:limit]
    return markets


def fetch_midpoint(token_id: str) -> float | None:
    try:
        resp = requests.get(CLOB_MIDPOINT_URL, params={"token_id": token_id}, timeout=HTTP_TIMEOUT)
        resp.raise_for_status()
        mid = resp.json().get("mid")
        return float(mid) if mid is not None else None
    except (requests.RequestException, ValueError):
        return None


def fetch_ask(token_id: str) -> float | None:
    """Best ask = lowest price someone will sell at = what you'd pay to buy."""
    try:
        resp = requests.get(
            CLOB_PRICE_URL,
            params={"token_id": token_id, "side": "SELL"},
            timeout=HTTP_TIMEOUT,
        )
        resp.raise_for_status()
        price = resp.json().get("price")
        return float(price) if price is not None else None
    except (requests.RequestException, ValueError):
        return None


def post_webhook(url: str, kind: str, content: str) -> None:
    if not url:
        return
    try:
        if kind == "discord":
            requests.post(url, json={"content": content[:1900]}, timeout=10)
        elif kind == "telegram":
            requests.post(url, json={"text": content[:4000]}, timeout=10)
    except requests.RequestException:
        pass


def record_alert(conn: sqlite3.Connection, slug: str, alert_type: str, message: str, webhook_url: str, webhook_type: str) -> None:
    ts = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "INSERT INTO alerts (ts, slug, alert_type, message) VALUES (?, ?, ?, ?)",
        (ts, slug, alert_type, message),
    )
    conn.commit()
    print(f"  ALERT [{alert_type}] {message}")
    post_webhook(webhook_url, webhook_type, f"**[{alert_type}]** {message}")


def check_alerts(
    conn: sqlite3.Connection,
    market: Market,
    yes_mid: float,
    no_mid: float,
    yes_ask: float | None,
    no_ask: float | None,
    prev_yes: float | None,
    cfg: dict,
) -> None:
    if yes_ask is not None and no_ask is not None and yes_ask > 0 and no_ask > 0:
        ask_sum = yes_ask + no_ask
        if ask_sum < 1.0 - cfg["alert_thresholds"]["yes_no_sum_deviation"]:
            profit = (1.0 - ask_sum) * 100
            msg = (
                f"ARB? {market.slug}: YES_ask={yes_ask:.3f} + NO_ask={no_ask:.3f} = "
                f"{ask_sum:.3f} (theoretical edge {profit:.1f}c per $1) | {market.question}"
            )
            record_alert(conn, market.slug, "ARB_CANDIDATE", msg, cfg.get("webhook_url", ""), cfg.get("webhook_type", "discord"))

    if prev_yes is not None and prev_yes > 0.01:
        change_pct = abs(yes_mid - prev_yes) / prev_yes * 100
        if change_pct >= cfg["alert_thresholds"]["price_change_pct"]:
            direction = "+" if yes_mid > prev_yes else "-"
            msg = (
                f"{market.slug}: YES moved {prev_yes:.3f} -> {yes_mid:.3f} "
                f"({direction}{change_pct:.2f}%) | {market.question}"
            )
            record_alert(conn, market.slug, "PRICE_MOVE", msg, cfg.get("webhook_url", ""), cfg.get("webhook_type", "discord"))


def poll_once(conn: sqlite3.Connection, markets: list[Market], last_yes: dict[str, float], cfg: dict) -> None:
    ts = datetime.now(timezone.utc).isoformat()
    print(f"\n[{ts}] polling {len(markets)} markets")

    for m in markets:
        yes_mid = fetch_midpoint(m.yes_token_id)
        no_mid = fetch_midpoint(m.no_token_id)
        if yes_mid is None or no_mid is None:
            print(f"  - {m.slug}: midpoint fetch failed")
            continue

        yes_ask = fetch_ask(m.yes_token_id)
        no_ask = fetch_ask(m.no_token_id)
        ask_sum = (yes_ask + no_ask) if (yes_ask and no_ask) else None
        ask_str = f" ask_sum={ask_sum:.3f}" if ask_sum is not None else ""
        print(f"  - {m.slug}: YES={yes_mid:.3f} NO={no_mid:.3f} mid_sum={yes_mid + no_mid:.3f}{ask_str} vol=${m.volume_usd:,.0f}")

        conn.execute(
            "INSERT INTO price_snapshots (ts, slug, question, yes_mid, no_mid, yes_ask, no_ask, ask_sum, volume_usd) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (ts, m.slug, m.question, yes_mid, no_mid, yes_ask, no_ask, ask_sum, m.volume_usd),
        )

        check_alerts(conn, m, yes_mid, no_mid, yes_ask, no_ask, last_yes.get(m.slug), cfg)
        last_yes[m.slug] = yes_mid

    conn.commit()


def main() -> int:
    cfg = load_config()
    conn = init_db()

    print("Polymarket read-only monitor")
    print(f"  poll interval: {cfg['poll_interval_seconds']}s")
    print(f"  min volume:    ${cfg['min_volume_usd']:,.0f}")
    print(f"  max markets:   {cfg['max_markets_to_watch']}")
    print(f"  thresholds:    sum_dev>={cfg['alert_thresholds']['yes_no_sum_deviation']}  move>={cfg['alert_thresholds']['price_change_pct']}%")
    print(f"  watch slugs:   {cfg['watch_slugs'] or '(auto-discover top markets)'}")
    print(f"  db:            {DB_PATH}")

    markets = fetch_active_markets(
        min_volume=cfg["min_volume_usd"],
        limit=cfg["max_markets_to_watch"],
        watch_slugs=cfg["watch_slugs"],
        keywords=cfg.get("keyword_filter") or None,
    )
    if not markets:
        print("No markets matched. Lower min_volume_usd or check watch_slugs.")
        return 1
    print(f"Tracking {len(markets)} markets.")

    last_yes: dict[str, float] = {}
    try:
        while True:
            try:
                poll_once(conn, markets, last_yes, cfg)
            except requests.RequestException as e:
                print(f"  network error: {e}")
            time.sleep(cfg["poll_interval_seconds"])
    except KeyboardInterrupt:
        print("\nStopped.")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
