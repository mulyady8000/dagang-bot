"""
Polymarket SMALL-market monitor — the long tail where retail edges actually live.

The other bot watches the top-20 by volume. Those are efficiently priced because
every quant scrapes them. This bot does the opposite: watches markets in the
$10k - $500k volume band where:

  - Spreads are naturally wider (less competition)
  - Arbitrage windows last seconds-to-minutes, not microseconds
  - But: liquidity is THIN, so apparent edges may not be tradeable

To handle the thin-liquidity trap, this bot also fetches the orderbook depth
and only fires "TRADEABLE" alerts when there's at least $50 of size at the
quoted ask price. Anything smaller is logged as "ghost" — looks good on screen,
can't actually be traded.
"""

from __future__ import annotations

import json
import sqlite3
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import requests

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

ROOT = Path(__file__).parent
CONFIG_PATH = ROOT / "small_market_config.json"
DB_PATH = ROOT / "small_market.db"

GAMMA_MARKETS_URL = "https://gamma-api.polymarket.com/markets"
CLOB_BOOK_URL = "https://clob.polymarket.com/book"

HTTP_TIMEOUT = 15


@dataclass
class Market:
    slug: str
    question: str
    yes_token_id: str
    no_token_id: str
    volume_usd: float
    end_date: str | None


@dataclass
class BookSide:
    """One side of an orderbook with cumulative depth."""
    best_price: float
    best_size: float
    depth_at_best_5pct: float  # total size within 5% of best price


@dataclass
class Book:
    yes_ask: BookSide | None
    yes_bid: BookSide | None
    no_ask: BookSide | None
    no_bid: BookSide | None


def load_config() -> dict:
    if not CONFIG_PATH.exists():
        default = {
            "poll_interval_seconds": 60,
            "min_volume_usd": 10000,
            "max_volume_usd": 500000,
            "max_markets_to_watch": 30,
            "min_tradeable_size_usd": 50.0,
            "alert_arb_pct": 1.5,
            "alert_price_move_pct": 8.0,
            "exclude_keywords": ["nba", "nhl", "mlb", "nfl", "soccer", "tennis", "golf"],
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
        CREATE TABLE IF NOT EXISTS book_snapshots (
            ts TEXT NOT NULL,
            slug TEXT NOT NULL,
            question TEXT,
            yes_ask REAL, yes_ask_size REAL, yes_ask_depth REAL,
            yes_bid REAL, yes_bid_size REAL,
            no_ask REAL,  no_ask_size REAL,  no_ask_depth REAL,
            no_bid REAL,  no_bid_size REAL,
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
            tradeable INTEGER NOT NULL,
            message TEXT NOT NULL
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_book_slug_ts ON book_snapshots(slug, ts)")
    conn.commit()
    return conn


def fetch_candidate_markets(cfg: dict) -> list[Market]:
    """Pull active binary markets in the small-volume band."""
    params = {
        "active": "true",
        "closed": "false",
        "limit": 500,
        "order": "volume24hr",
        "ascending": "false",
    }
    resp = requests.get(GAMMA_MARKETS_URL, params=params, timeout=HTTP_TIMEOUT)
    resp.raise_for_status()
    raw = resp.json()

    excludes = [kw.lower() for kw in cfg.get("exclude_keywords", [])]
    out: list[Market] = []
    for m in raw:
        slug = m.get("slug")
        if not slug:
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
        if not m.get("acceptingOrders", True):
            continue

        vol = float(m.get("volume") or 0)
        if vol < cfg["min_volume_usd"] or vol > cfg["max_volume_usd"]:
            continue

        haystack = (slug + " " + m.get("question", "")).lower()
        if excludes and any(kw in haystack for kw in excludes):
            continue

        out.append(
            Market(
                slug=slug,
                question=m.get("question", "")[:140],
                yes_token_id=str(token_ids[0]),
                no_token_id=str(token_ids[1]),
                volume_usd=vol,
                end_date=m.get("endDate"),
            )
        )
        if len(out) >= cfg["max_markets_to_watch"]:
            break
    return out


def parse_book_side(levels: list[dict], is_ask: bool) -> BookSide | None:
    """Return best price, best size, and cumulative size within 5% of best.

    Polymarket returns asks ascending and bids descending — we just use index 0
    as best regardless and iterate while within 5% of it.
    """
    if not levels:
        return None
    try:
        parsed = [(float(lvl["price"]), float(lvl["size"])) for lvl in levels]
    except (KeyError, ValueError):
        return None
    if not parsed:
        return None

    if is_ask:
        parsed.sort(key=lambda x: x[0])
    else:
        parsed.sort(key=lambda x: -x[0])

    best_price, best_size = parsed[0]
    if best_price <= 0:
        return None

    depth = 0.0
    for price, size in parsed:
        if is_ask and price > best_price * 1.05:
            break
        if (not is_ask) and price < best_price * 0.95:
            break
        depth += price * size  # USD value (price × shares)
    return BookSide(best_price=best_price, best_size=best_size * best_price, depth_at_best_5pct=depth)


def fetch_book(token_id: str) -> tuple[BookSide | None, BookSide | None]:
    """Returns (best_ask_side, best_bid_side) for one token."""
    try:
        r = requests.get(CLOB_BOOK_URL, params={"token_id": token_id}, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        ask = parse_book_side(data.get("asks") or [], is_ask=True)
        bid = parse_book_side(data.get("bids") or [], is_ask=False)
        return ask, bid
    except (requests.RequestException, ValueError):
        return None, None


def fetch_full_book(market: Market) -> Book:
    """Fetch YES and NO orderbooks in parallel."""
    with ThreadPoolExecutor(max_workers=2) as pool:
        yes_fut = pool.submit(fetch_book, market.yes_token_id)
        no_fut = pool.submit(fetch_book, market.no_token_id)
        yes_ask, yes_bid = yes_fut.result()
        no_ask, no_bid = no_fut.result()
    return Book(yes_ask, yes_bid, no_ask, no_bid)


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


def record_alert(conn: sqlite3.Connection, slug: str, alert_type: str, tradeable: bool, message: str, cfg: dict) -> None:
    ts = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "INSERT INTO alerts (ts, slug, alert_type, tradeable, message) VALUES (?, ?, ?, ?, ?)",
        (ts, slug, alert_type, 1 if tradeable else 0, message),
    )
    tag = "TRADEABLE" if tradeable else "ghost"
    print(f"  ALERT [{alert_type} {tag}] {message}")
    if tradeable:
        post_webhook(cfg.get("webhook_url", ""), cfg.get("webhook_type", "discord"), f"**[{alert_type}]** {message}")


def check_arb(conn: sqlite3.Connection, market: Market, book: Book, cfg: dict) -> None:
    """True arb: buy YES at ask + buy NO at ask < $1, get $1 at resolution."""
    if not book.yes_ask or not book.no_ask:
        return
    ask_sum = book.yes_ask.best_price + book.no_ask.best_price
    edge_pct = (1.0 - ask_sum) * 100
    if edge_pct < cfg["alert_arb_pct"]:
        return

    min_size = cfg["min_tradeable_size_usd"]
    tradeable_size = min(book.yes_ask.best_size, book.no_ask.best_size)
    tradeable = tradeable_size >= min_size

    msg = (
        f"{market.slug}: YES_ask={book.yes_ask.best_price:.3f}({book.yes_ask.best_size:.0f}$) + "
        f"NO_ask={book.no_ask.best_price:.3f}({book.no_ask.best_size:.0f}$) = {ask_sum:.3f} "
        f"edge={edge_pct:.2f}% min_size=${tradeable_size:.0f} | {market.question[:60]}"
    )
    record_alert(conn, market.slug, "ARB", tradeable, msg, cfg)


def check_price_move(conn: sqlite3.Connection, market: Market, book: Book, prev_yes_mid: float | None, cfg: dict) -> float | None:
    if not book.yes_ask or not book.yes_bid:
        return None
    yes_mid = (book.yes_ask.best_price + book.yes_bid.best_price) / 2
    if prev_yes_mid is None or prev_yes_mid <= 0.01:
        return yes_mid
    change_pct = (yes_mid - prev_yes_mid) / prev_yes_mid * 100
    if abs(change_pct) >= cfg["alert_price_move_pct"]:
        depth = book.yes_ask.depth_at_best_5pct if change_pct > 0 else book.yes_bid.depth_at_best_5pct
        tradeable = depth >= cfg["min_tradeable_size_usd"]
        sign = "+" if change_pct > 0 else ""
        msg = (
            f"{market.slug}: YES {prev_yes_mid:.3f} -> {yes_mid:.3f} ({sign}{change_pct:.2f}%) "
            f"depth_within_5%=${depth:.0f} | {market.question[:60]}"
        )
        record_alert(conn, market.slug, "MOVE", tradeable, msg, cfg)
    return yes_mid


def poll_market(market: Market) -> tuple[Market, Book]:
    return market, fetch_full_book(market)


def poll_once(conn: sqlite3.Connection, markets: list[Market], last_yes_mid: dict[str, float], cfg: dict) -> None:
    ts = datetime.now(timezone.utc).isoformat()
    print(f"\n[{ts[11:19]} UTC] polling {len(markets)} small markets")

    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = [pool.submit(poll_market, m) for m in markets]
        for f in as_completed(futures):
            try:
                m, book = f.result()
            except Exception as e:
                print(f"  fetch failed: {e}")
                continue

            yes_a = book.yes_ask
            no_a = book.no_ask
            ask_sum = (yes_a.best_price + no_a.best_price) if (yes_a and no_a) else None

            conn.execute(
                """INSERT INTO book_snapshots
                (ts, slug, question, yes_ask, yes_ask_size, yes_ask_depth, yes_bid, yes_bid_size,
                 no_ask, no_ask_size, no_ask_depth, no_bid, no_bid_size, ask_sum, volume_usd)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    ts, m.slug, m.question,
                    yes_a.best_price if yes_a else None, yes_a.best_size if yes_a else None, yes_a.depth_at_best_5pct if yes_a else None,
                    book.yes_bid.best_price if book.yes_bid else None, book.yes_bid.best_size if book.yes_bid else None,
                    no_a.best_price if no_a else None, no_a.best_size if no_a else None, no_a.depth_at_best_5pct if no_a else None,
                    book.no_bid.best_price if book.no_bid else None, book.no_bid.best_size if book.no_bid else None,
                    ask_sum, m.volume_usd,
                ),
            )

            if yes_a and no_a:
                ask_str = f"ask_sum={ask_sum:.3f}"
                size_str = f"yes_size=${yes_a.best_size:.0f} no_size=${no_a.best_size:.0f}"
                print(f"  {m.slug[:55]:55} {ask_str} {size_str} vol=${m.volume_usd:,.0f}")

            check_arb(conn, m, book, cfg)
            new_mid = check_price_move(conn, m, book, last_yes_mid.get(m.slug), cfg)
            if new_mid is not None:
                last_yes_mid[m.slug] = new_mid

    conn.commit()


def main() -> int:
    cfg = load_config()
    conn = init_db()

    print("Polymarket SMALL-market monitor")
    print(f"  volume band:    ${cfg['min_volume_usd']:,} - ${cfg['max_volume_usd']:,}")
    print(f"  max markets:    {cfg['max_markets_to_watch']}")
    print(f"  poll interval:  {cfg['poll_interval_seconds']}s")
    print(f"  min tradeable:  ${cfg['min_tradeable_size_usd']}")
    print(f"  arb threshold:  {cfg['alert_arb_pct']}%")
    print(f"  move threshold: {cfg['alert_price_move_pct']}%")
    print(f"  excluding:      {cfg['exclude_keywords']}")
    print(f"  db:             {DB_PATH}")

    markets = fetch_candidate_markets(cfg)
    if not markets:
        print("No markets matched the volume band. Loosen min/max_volume_usd.")
        return 1
    print(f"Tracking {len(markets)} small markets.")
    for m in markets[:5]:
        print(f"  - ${m.volume_usd:>10,.0f}  {m.slug[:60]} ({m.question[:60]})")
    if len(markets) > 5:
        print(f"  ... and {len(markets)-5} more")

    last_yes_mid: dict[str, float] = {}
    refresh_every = 20  # re-discover candidate markets every N polls
    poll_count = 0
    try:
        while True:
            try:
                poll_once(conn, markets, last_yes_mid, cfg)
                poll_count += 1
                if poll_count % refresh_every == 0:
                    fresh = fetch_candidate_markets(cfg)
                    if fresh:
                        markets = fresh
                        print(f"\n[refresh] now tracking {len(markets)} markets")
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
