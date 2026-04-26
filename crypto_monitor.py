"""
Crypto exchange price monitor — Binance + Bybit + OKX (read-only, no API keys).

Polls best bid/ask for the same trading pairs across multiple exchanges, logs
each snapshot to SQLite, and alerts when the cross-exchange spread exceeds the
configured threshold (a potential arbitrage signal).

Reality check on cross-exchange arb for retail:
  - Spreads >0.3% are rare on majors (BTC, ETH) — pros take them in <100ms.
  - Even when you spot one, you need balances pre-funded on BOTH exchanges
    (you don't have time to deposit/withdraw during the spread).
  - Fees: ~0.1% per side = 0.2% round-trip just in fees, before slippage.
  - Threshold for "actually profitable": 0.5%+ on a liquid pair, more on alts.
  - This bot is for OBSERVATION ONLY. Watch how often real edges appear, then
    you'll know if it's worth pursuing.
"""

from __future__ import annotations

import json
import sqlite3
import sys
import time

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import requests

ROOT = Path(__file__).parent
CONFIG_PATH = ROOT / "crypto_config.json"
DB_PATH = ROOT / "crypto_prices.db"

HTTP_TIMEOUT = 10


@dataclass
class Quote:
    exchange: str
    symbol: str
    bid: float
    ask: float

    @property
    def mid(self) -> float:
        return (self.bid + self.ask) / 2

    @property
    def spread_pct(self) -> float:
        return (self.ask - self.bid) / self.mid * 100 if self.mid > 0 else 0.0


def load_config() -> dict:
    if not CONFIG_PATH.exists():
        default = {
            "poll_interval_seconds": 10,
            "symbols": ["BTCUSDT", "ETHUSDT", "SOLUSDT", "DOGEUSDT", "XRPUSDT"],
            "exchanges": ["binance", "bybit", "okx"],
            "alert_spread_pct": 0.3,
            "webhook_url": "",
            "webhook_type": "discord"
        }
        CONFIG_PATH.write_text(json.dumps(default, indent=2), encoding="utf-8")
    with CONFIG_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)


def init_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS quotes (
            ts TEXT NOT NULL,
            exchange TEXT NOT NULL,
            symbol TEXT NOT NULL,
            bid REAL,
            ask REAL,
            mid REAL,
            spread_pct REAL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS arb_signals (
            ts TEXT NOT NULL,
            symbol TEXT NOT NULL,
            buy_exchange TEXT NOT NULL,
            sell_exchange TEXT NOT NULL,
            buy_ask REAL,
            sell_bid REAL,
            spread_pct REAL,
            est_profit_after_fees_pct REAL
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_quotes_sym_ts ON quotes(symbol, ts)")
    conn.commit()
    return conn


def fetch_binance(symbol: str) -> Quote | None:
    try:
        r = requests.get(
            "https://api.binance.com/api/v3/ticker/bookTicker",
            params={"symbol": symbol},
            timeout=HTTP_TIMEOUT,
        )
        r.raise_for_status()
        d = r.json()
        return Quote("binance", symbol, float(d["bidPrice"]), float(d["askPrice"]))
    except (requests.RequestException, KeyError, ValueError):
        return None


def fetch_bybit(symbol: str) -> Quote | None:
    try:
        r = requests.get(
            "https://api.bybit.com/v5/market/tickers",
            params={"category": "spot", "symbol": symbol},
            timeout=HTTP_TIMEOUT,
        )
        r.raise_for_status()
        items = r.json().get("result", {}).get("list", [])
        if not items:
            return None
        d = items[0]
        return Quote("bybit", symbol, float(d["bid1Price"]), float(d["ask1Price"]))
    except (requests.RequestException, KeyError, ValueError, IndexError):
        return None


def fetch_okx(symbol: str) -> Quote | None:
    """OKX uses 'BTC-USDT' format instead of 'BTCUSDT'."""
    okx_symbol = symbol.replace("USDT", "-USDT")
    try:
        r = requests.get(
            "https://www.okx.com/api/v5/market/ticker",
            params={"instId": okx_symbol},
            timeout=HTTP_TIMEOUT,
        )
        r.raise_for_status()
        items = r.json().get("data", [])
        if not items:
            return None
        d = items[0]
        return Quote("okx", symbol, float(d["bidPx"]), float(d["askPx"]))
    except (requests.RequestException, KeyError, ValueError, IndexError):
        return None


FETCHERS = {"binance": fetch_binance, "bybit": fetch_bybit, "okx": fetch_okx}


def fetch_all_parallel(symbols: list[str], exchanges: list[str]) -> list[Quote]:
    """Fetch every (exchange, symbol) pair in parallel for a tight snapshot."""
    quotes: list[Quote] = []
    with ThreadPoolExecutor(max_workers=min(len(symbols) * len(exchanges), 16)) as pool:
        futures = []
        for sym in symbols:
            for ex in exchanges:
                fetcher = FETCHERS.get(ex)
                if fetcher:
                    futures.append(pool.submit(fetcher, sym))
        for f in as_completed(futures):
            q = f.result()
            if q is not None:
                quotes.append(q)
    return quotes


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


def detect_arb(conn: sqlite3.Connection, ts: str, quotes: list[Quote], threshold_pct: float, webhook_url: str, webhook_type: str) -> int:
    """For each symbol, find the lowest ask (where to buy) vs highest bid (where to sell).

    If sell_bid > buy_ask, that's a raw cross-exchange spread. Subtract 0.2%
    estimated round-trip fees to get realistic profit.
    """
    by_symbol: dict[str, list[Quote]] = {}
    for q in quotes:
        by_symbol.setdefault(q.symbol, []).append(q)

    fired = 0
    for symbol, qs in by_symbol.items():
        if len(qs) < 2:
            continue
        buy = min(qs, key=lambda x: x.ask)
        sell = max(qs, key=lambda x: x.bid)
        if buy.exchange == sell.exchange:
            continue
        if buy.ask <= 0:
            continue
        raw_spread_pct = (sell.bid - buy.ask) / buy.ask * 100
        net_pct = raw_spread_pct - 0.2  # rough round-trip taker fee estimate

        if raw_spread_pct >= threshold_pct:
            conn.execute(
                "INSERT INTO arb_signals (ts, symbol, buy_exchange, sell_exchange, buy_ask, sell_bid, spread_pct, est_profit_after_fees_pct) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (ts, symbol, buy.exchange, sell.exchange, buy.ask, sell.bid, raw_spread_pct, net_pct),
            )
            tag = "PROFITABLE" if net_pct > 0 else "fee-eaten"
            msg = (
                f"[{tag}] {symbol}: buy {buy.exchange}@{buy.ask:.4f} -> sell {sell.exchange}@{sell.bid:.4f} "
                f"raw={raw_spread_pct:.3f}% net={net_pct:+.3f}%"
            )
            print(f"  ARB {msg}")
            if net_pct > 0:
                post_webhook(webhook_url, webhook_type, f"**ARB** {msg}")
            fired += 1
    return fired


def poll_once(conn: sqlite3.Connection, cfg: dict) -> None:
    ts = datetime.now(timezone.utc).isoformat()
    quotes = fetch_all_parallel(cfg["symbols"], cfg["exchanges"])

    rows_by_symbol: dict[str, list[str]] = {}
    for q in quotes:
        conn.execute(
            "INSERT INTO quotes (ts, exchange, symbol, bid, ask, mid, spread_pct) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (ts, q.exchange, q.symbol, q.bid, q.ask, q.mid, q.spread_pct),
        )
        rows_by_symbol.setdefault(q.symbol, []).append(f"{q.exchange}={q.mid:.2f}")

    print(f"\n[{ts[11:19]} UTC] {len(quotes)} quotes")
    for sym in cfg["symbols"]:
        parts = rows_by_symbol.get(sym, [])
        if parts:
            print(f"  {sym:10} {' | '.join(parts)}")

    fired = detect_arb(
        conn, ts, quotes,
        cfg["alert_spread_pct"],
        cfg.get("webhook_url", ""),
        cfg.get("webhook_type", "discord"),
    )
    if fired == 0:
        print("  (no cross-exchange arb above threshold)")
    conn.commit()


def main() -> int:
    cfg = load_config()
    conn = init_db()

    print("Crypto exchange monitor")
    print(f"  exchanges: {cfg['exchanges']}")
    print(f"  symbols:   {cfg['symbols']}")
    print(f"  interval:  {cfg['poll_interval_seconds']}s")
    print(f"  threshold: {cfg['alert_spread_pct']}% raw spread")
    print(f"  db:        {DB_PATH}")

    try:
        while True:
            try:
                poll_once(conn, cfg)
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
