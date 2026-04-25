"""PD signal performance by value book x sport (and market).

Usage:
    python /app/scripts/analyze_pd_by_book_sport.py            # all-time
    python /app/scripts/analyze_pd_by_book_sport.py 30         # last 30 days

Purpose: figure out whether DraftKings (or any book) is structurally bad as a
PD value book in MLB only, or across the board. Decides between:
  - global addition to PD_EXCLUDED_BOOKS, or
  - building per-sport book exclusions (pd_sport_excluded_books).
"""

import json
import sqlite3
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone

DB = "/app/data/sharp_seeker.db"

SPORT_SHORT = {
    "basketball_nba": "NBA",
    "basketball_ncaab": "NCAAB",
    "icehockey_nhl": "NHL",
    "baseball_mlb": "MLB",
}

MARKET_SHORT = {"h2h": "ML", "spreads": "Spread", "totals": "Total"}


def connect():
    for attempt in range(10):
        try:
            conn = sqlite3.connect(DB, timeout=10)
            conn.row_factory = sqlite3.Row
            conn.execute("SELECT 1 FROM signal_results LIMIT 1")
            return conn
        except sqlite3.OperationalError:
            print(f"  DB locked, retrying ({attempt + 1}/10)...")
            time.sleep(3)
    raise SystemExit("ERROR: Could not acquire DB lock after 10 attempts.")


def compute_units(price, result):
    if result == "push" or price is None:
        return 0.0
    if price < 0:
        risk = abs(price) / 100.0
    else:
        risk = 100.0 / price if price > 0 else 1.0
    if result == "won":
        return 1.0
    if result == "lost":
        return -risk
    return 0.0


def fmt(d):
    w, l, p, u = d["won"], d["lost"], d["push"], d["units"]
    n = w + l + p
    decided = w + l
    rate_str = f"{w / decided:.0%}" if decided else "--"
    sign = "+" if u >= 0 else ""
    return f"(n={n:5d})  {w:4d}W-{l:4d}L-{p:3d}P  ({rate_str})  [{sign}{u:7.1f}u]"


def section(title):
    print()
    print("=" * 82)
    print(f"  {title}")
    print("=" * 82)


def run():
    days = int(sys.argv[1]) if len(sys.argv) > 1 else None

    where = "WHERE signal_type = 'pinnacle_divergence' AND result IS NOT NULL"
    params: tuple = ()
    if days is not None:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        where += " AND signal_at >= ?"
        params = (cutoff,)

    conn = connect()
    cur = conn.execute(
        f"""
        SELECT sport_key, market_key, result, details_json
        FROM signal_results
        {where}
        """,
        params,
    )
    rows = cur.fetchall()
    conn.close()

    if not rows:
        print("No PD signals found.")
        return

    # Bucket: (sport, market, book) -> w/l/p/units
    by_sport_book = defaultdict(lambda: {"won": 0, "lost": 0, "push": 0, "units": 0.0})
    by_sport_market_book = defaultdict(lambda: {"won": 0, "lost": 0, "push": 0, "units": 0.0})
    by_book = defaultdict(lambda: {"won": 0, "lost": 0, "push": 0, "units": 0.0})

    for r in rows:
        sport = r["sport_key"] or "unknown"
        market = r["market_key"]
        result = r["result"]
        details_raw = r["details_json"]
        if not details_raw:
            continue
        try:
            details = json.loads(details_raw) if isinstance(details_raw, str) else details_raw
        except (json.JSONDecodeError, TypeError):
            continue
        vb = details.get("value_books", [])
        if not vb:
            continue
        book = vb[0].get("bookmaker") or "unknown"
        price = vb[0].get("price")
        u = compute_units(price, result)

        for bucket in (
            by_sport_book[(sport, book)],
            by_sport_market_book[(sport, market, book)],
            by_book[book],
        ):
            bucket[result] += 1
            bucket["units"] += u

    label = f"last {days} days" if days else "all time"
    print(f"PD performance by value book x sport ({label})")
    print(f"Total graded PD signals: {len(rows)}")

    # ── 1. Per-sport: book ranked by units ────────────────────────────
    section("1. PD BY SPORT x VALUE BOOK (ranked by units within sport)")
    sports_seen = sorted({k[0] for k in by_sport_book})
    for sport in sports_seen:
        rows_for_sport = [(book, by_sport_book[(sport, book)]) for (sp, book) in by_sport_book if sp == sport]
        rows_for_sport.sort(key=lambda x: x[1]["units"])
        print(f"\n  {SPORT_SHORT.get(sport, sport)}")
        for book, d in rows_for_sport:
            print(f"    {book:18s} {fmt(d)}")

    # ── 2. Same data pivoted: book across sports ──────────────────────
    section("2. PD BY VALUE BOOK x SPORT (is this book bad everywhere or just MLB?)")
    books_seen = sorted({k[1] for k in by_sport_book})
    for book in books_seen:
        print(f"\n  {book}")
        for sport in sports_seen:
            d = by_sport_book.get((sport, book))
            if d is None:
                continue
            print(f"    {SPORT_SHORT.get(sport, sport):8s} {fmt(d)}")

    # ── 3. Three-dimensional: sport x market x book (top bleeders) ────
    section("3. WORST 12 (sport x market x book) BY UNITS")
    flat = [
        (sport, market, book, d)
        for (sport, market, book), d in by_sport_market_book.items()
        if (d["won"] + d["lost"]) >= 10  # min sample
    ]
    flat.sort(key=lambda x: x[3]["units"])
    for sport, market, book, d in flat[:12]:
        label_str = f"{SPORT_SHORT.get(sport, sport):6s} {MARKET_SHORT.get(market, market):6s} {book}"
        print(f"  {label_str:40s} {fmt(d)}")

    section("4. BEST 12 (sport x market x book) BY UNITS")
    flat.sort(key=lambda x: x[3]["units"], reverse=True)
    for sport, market, book, d in flat[:12]:
        label_str = f"{SPORT_SHORT.get(sport, sport):6s} {MARKET_SHORT.get(market, market):6s} {book}"
        print(f"  {label_str:40s} {fmt(d)}")

    # ── 5. DraftKings deep dive (the suspect) ─────────────────────────
    section("5. DRAFTKINGS — DETAIL ACROSS ALL SPORTS x MARKETS")
    for sport in sports_seen:
        sport_label = SPORT_SHORT.get(sport, sport)
        sport_total = {"won": 0, "lost": 0, "push": 0, "units": 0.0}
        markets_for_sport = []
        for market in ("h2h", "spreads", "totals"):
            d = by_sport_market_book.get((sport, market, "draftkings"))
            if d is None:
                continue
            markets_for_sport.append((market, d))
            for k in ("won", "lost", "push", "units"):
                sport_total[k] += d[k]
        if not markets_for_sport:
            continue
        print(f"\n  {sport_label}")
        for market, d in markets_for_sport:
            print(f"    {MARKET_SHORT.get(market, market):8s} {fmt(d)}")
        if len(markets_for_sport) > 1:
            print(f"    {'TOTAL':8s} {fmt(sport_total)}")

    # ── 6. Caesars (already excluded — sanity check) ──────────────────
    section("6. CAESARS / williamhill_us — SANITY CHECK")
    for sport in sports_seen:
        sport_label = SPORT_SHORT.get(sport, sport)
        markets_for_sport = []
        for market in ("h2h", "spreads", "totals"):
            d = by_sport_market_book.get((sport, market, "williamhill_us"))
            if d is None:
                continue
            markets_for_sport.append((market, d))
        if not markets_for_sport:
            continue
        print(f"\n  {sport_label}")
        for market, d in markets_for_sport:
            print(f"    {MARKET_SHORT.get(market, market):8s} {fmt(d)}")

    print()
    print("Done.")


if __name__ == "__main__":
    run()
