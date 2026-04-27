"""Analyze JSONL data collected by cloud_collect.py."""
from __future__ import annotations
import json
import sys
from collections import defaultdict
from pathlib import Path
from statistics import mean, median, stdev

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

DATA = Path(__file__).parent / "data_pull"
FEE_RT_PCT = 0.2  # 0.1% per side, rough


def load(path: Path) -> list[dict]:
    rows = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def hr(c: str = "=") -> None:
    print(c * 70)


def analyze_crypto(rows: list[dict]) -> None:
    hr()
    print("CRYPTO EXCHANGE DATA")
    hr()
    if not rows:
        print("(empty)")
        return

    timestamps = sorted({r["ts"] for r in rows})
    print(f"polls: {len(timestamps)}   first: {timestamps[0]}   last: {timestamps[-1]}")

    # Per (symbol, exchange) coverage
    cov = defaultdict(int)
    for r in rows:
        cov[(r["symbol"], r["exchange"])] += 1
    print("\nCoverage (rows per symbol × exchange):")
    symbols = sorted({s for s, _ in cov})
    exchanges = sorted({e for _, e in cov})
    print(f"  {'symbol':10}", *[f"{e:>10}" for e in exchanges])
    for s in symbols:
        print(f"  {s:10}", *[f"{cov.get((s, e), 0):>10}" for e in exchanges])

    # Cross-exchange spread per timestamp per symbol
    print("\nCross-exchange spreads (raw best_bid - best_ask across exchanges):")
    by_ts_sym: dict[tuple, list[dict]] = defaultdict(list)
    for r in rows:
        by_ts_sym[(r["ts"], r["symbol"])].append(r)

    sym_spreads: dict[str, list[float]] = defaultdict(list)
    sym_best_arb: dict[str, dict] = {}
    for (ts, sym), quotes in by_ts_sym.items():
        if len(quotes) < 2:
            continue
        best_bid = max(quotes, key=lambda q: q["bid"])
        best_ask = min(quotes, key=lambda q: q["ask"])
        if best_bid["exchange"] == best_ask["exchange"]:
            continue
        spread_pct = (best_bid["bid"] - best_ask["ask"]) / best_ask["ask"] * 100
        sym_spreads[sym].append(spread_pct)
        if sym not in sym_best_arb or spread_pct > sym_best_arb[sym]["spread_pct"]:
            sym_best_arb[sym] = {
                "ts": ts, "spread_pct": spread_pct,
                "buy_at": best_ask["exchange"], "buy_ask": best_ask["ask"],
                "sell_at": best_bid["exchange"], "sell_bid": best_bid["bid"],
            }

    print(f"  {'symbol':10} {'samples':>8} {'avg%':>8} {'max%':>8} {'>0.3% count':>14} {'profitable*':>11}")
    for sym in sorted(sym_spreads):
        sp = sym_spreads[sym]
        above = sum(1 for x in sp if x > 0.3)
        profitable = sum(1 for x in sp if x > FEE_RT_PCT)
        print(f"  {sym:10} {len(sp):>8} {mean(sp):>8.4f} {max(sp):>8.4f} {above:>14} {profitable:>11}")
    print(f"  *profitable = spread above estimated round-trip fees of {FEE_RT_PCT}%")

    print("\nBest single arb opportunity per symbol (raw spread, before fees):")
    for sym, arb in sorted(sym_best_arb.items(), key=lambda x: -x[1]["spread_pct"]):
        net = arb["spread_pct"] - FEE_RT_PCT
        verdict = "PROFITABLE" if net > 0 else "fee-eaten"
        print(
            f"  {sym:10} {arb['spread_pct']:+.4f}% raw / {net:+.4f}% net [{verdict}]"
            f"  buy {arb['buy_at']}@{arb['buy_ask']:.4f} sell {arb['sell_at']}@{arb['sell_bid']:.4f}  ({arb['ts'][11:19]})"
        )


def analyze_polymarket(rows: list[dict], label: str, has_size: bool = False) -> None:
    hr()
    print(f"POLYMARKET — {label}")
    hr()
    if not rows:
        print("(empty)")
        return

    timestamps = sorted({r["ts"] for r in rows})
    print(f"polls: {len(timestamps)}   first: {timestamps[0]}   last: {timestamps[-1]}")

    # Group by slug
    by_slug: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        by_slug[r["slug"]].append(r)
    slugs = sorted(by_slug, key=lambda s: -len(by_slug[s]))
    print(f"unique markets observed: {len(slugs)}")
    print(f"avg observations/market: {mean(len(v) for v in by_slug.values()):.1f}")

    # Arb candidates (ask_sum < 1.00)
    print(f"\nTrue arbitrage check (yes_ask + no_ask < $1.00):")
    arb_hits: list[dict] = []
    for r in rows:
        asum = r.get("ask_sum")
        if asum is not None and asum < 1.0:
            arb_hits.append(r)
    print(f"  rows where ask_sum < 1.00: {len(arb_hits)} of {len(rows)}")
    if arb_hits:
        # Sort by edge size
        arb_hits.sort(key=lambda r: r["ask_sum"])
        print(f"  top 10 raw edges (most negative sum = biggest theoretical profit):")
        seen_slugs = set()
        for r in arb_hits[:30]:
            if r["slug"] in seen_slugs:
                continue
            seen_slugs.add(r["slug"])
            edge_c = (1.0 - r["ask_sum"]) * 100
            extra = ""
            if has_size and r.get("yes_ask_size_usd") and r.get("no_ask_size_usd"):
                tradeable = min(r["yes_ask_size_usd"], r["no_ask_size_usd"])
                extra = f"  min_side_size=${tradeable:.0f}"
            print(f"    {r['ts'][11:19]}  edge={edge_c:.2f}c  sum={r['ask_sum']:.4f}  {r['slug'][:55]}{extra}")
            if len(seen_slugs) >= 10:
                break

    # Biggest price movements over the day (per slug)
    print(f"\nBiggest YES-price movements (across all polls today):")
    moves = []
    price_field = "yes_mid" if not has_size else "yes_ask"
    for slug, obs in by_slug.items():
        prices = [r[price_field] for r in obs if r.get(price_field) is not None]
        if len(prices) < 2:
            continue
        lo, hi = min(prices), max(prices)
        if lo < 0.01:
            continue
        range_pct = (hi - lo) / lo * 100
        first = prices[0]
        last = prices[-1]
        net_pct = (last - first) / first * 100 if first > 0 else 0
        moves.append({"slug": slug, "lo": lo, "hi": hi, "range_pct": range_pct, "net_pct": net_pct, "n": len(prices), "q": obs[0].get("question", "")})
    moves.sort(key=lambda m: -m["range_pct"])
    print(f"  {'range%':>8} {'net%':>8} {'lo':>6} {'hi':>6} {'pts':>4}  market")
    for m in moves[:10]:
        print(f"  {m['range_pct']:>8.1f} {m['net_pct']:>+8.2f} {m['lo']:>6.3f} {m['hi']:>6.3f} {m['n']:>4}  {m['slug'][:60]}")


def analyze_liquidity(rows: list[dict]) -> None:
    hr()
    print("SMALL-MARKET LIQUIDITY PROFILE")
    hr()
    if not rows:
        print("(empty)")
        return
    sizes = []
    real_liq_markets: dict[str, dict] = {}
    for r in rows:
        ya = r.get("yes_ask_size_usd")
        na = r.get("no_ask_size_usd")
        if ya is None or na is None:
            continue
        ms = min(ya, na)
        sizes.append(ms)
        if ms >= 1000:
            cur = real_liq_markets.get(r["slug"])
            if not cur or ms > cur["min_size"]:
                real_liq_markets[r["slug"]] = {
                    "min_size": ms, "yes_size": ya, "no_size": na,
                    "yes_ask": r.get("yes_ask"), "no_ask": r.get("no_ask"),
                    "vol": r.get("volume_usd"), "q": r.get("question", "")
                }

    if sizes:
        sizes_sorted = sorted(sizes)
        print(f"min-side ask size at best price (USD), n={len(sizes)}:")
        print(f"  median: ${median(sizes_sorted):.0f}")
        print(f"  p25:    ${sizes_sorted[len(sizes_sorted)//4]:.0f}")
        print(f"  p75:    ${sizes_sorted[3*len(sizes_sorted)//4]:.0f}")
        print(f"  ghosts (<$50):   {sum(1 for x in sizes if x < 50)} ({100*sum(1 for x in sizes if x < 50)/len(sizes):.0f}%)")
        print(f"  thin ($50-500):  {sum(1 for x in sizes if 50 <= x < 500)} ({100*sum(1 for x in sizes if 50 <= x < 500)/len(sizes):.0f}%)")
        print(f"  real (>$500):    {sum(1 for x in sizes if x >= 500)} ({100*sum(1 for x in sizes if x >= 500)/len(sizes):.0f}%)")

    print(f"\nMarkets with REAL liquidity (>=$1000 on both sides at some point):")
    if not real_liq_markets:
        print("  (none)")
    for slug, info in sorted(real_liq_markets.items(), key=lambda x: -x[1]["min_size"])[:15]:
        print(f"  ${info['min_size']:>8.0f}  {slug[:55]}")
        print(f"      yes_ask={info['yes_ask']:.3f}(${info['yes_size']:.0f}) no_ask={info['no_ask']:.3f}(${info['no_size']:.0f})  vol=${info['vol']:,.0f}")
        print(f"      '{info['q'][:90]}'")


def main() -> int:
    folders = sorted([p for p in DATA.iterdir() if p.is_dir()])
    if not folders:
        print(f"no data found under {DATA}")
        return 1
    for folder in folders:
        print(f"\n{'#'*70}\n# {folder.name}\n{'#'*70}")
        for fname, label in [
            ("crypto.jsonl", "crypto"),
            ("polymarket.jsonl", "top crypto markets"),
            ("small_market.jsonl", "small markets ($10k-$500k)"),
        ]:
            path = folder / fname
            if not path.exists():
                continue
            rows = load(path)
            if fname == "crypto.jsonl":
                analyze_crypto(rows)
            elif fname == "polymarket.jsonl":
                analyze_polymarket(rows, label, has_size=False)
            else:
                analyze_polymarket(rows, label, has_size=True)
                analyze_liquidity(rows)
    return 0


if __name__ == "__main__":
    sys.exit(main())
