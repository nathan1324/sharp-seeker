"""Diagnose h2h Pinnacle-Divergence volume vs the (effective) firing bar.

READ-ONLY. Scans stored h2h snapshots and, per game-side, finds the BEST
favorable US-vs-Pinnacle implied-prob divergence ever observed, then buckets
those game-sides against:
  - the NOMINAL detector threshold (pinnacle_ml_prob_threshold, default 3%), and
  - the EFFECTIVE bar after the pipeline's 0.5 min-strength floor.

Why two bars: PD strength = min(1, delta / (3 * threshold)), and the pipeline
keeps a signal only if strength > 0.5, i.e. delta > 1.5 * threshold. So with
the default 3% h2h threshold, a signal must actually clear ~4.5% to survive.
This script quantifies how much h2h volume that floor eats, and how much more
we'd surface by lowering the bar.

Usage (server):
    docker compose exec sharp-seeker python /app/scripts/pd_h2h_delta_distribution.py
Optional args: [db_path] [since_iso]
    e.g. ... pd_h2h_delta_distribution.py "" 2026-05-18
"""

import sqlite3
import sys
from collections import defaultdict

try:
    from sharp_seeker.engine.pinnacle_divergence import PINNACLE_KEY, US_BOOKS
    from sharp_seeker.engine.exchange_monitor import american_to_implied_prob
except Exception:  # fallback so the script is runnable standalone
    PINNACLE_KEY = "pinnacle"
    US_BOOKS = {"draftkings", "fanduel", "betmgm", "williamhill_us", "betrivers"}

    def american_to_implied_prob(price):
        price = float(price)
        if price > 0:
            return 100.0 / (price + 100.0)
        return abs(price) / (abs(price) + 100.0)


DB_PATH = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] else "/app/data/sharp_seeker.db"
SINCE = sys.argv[2] if len(sys.argv) > 2 and sys.argv[2] else None

NOMINAL = 0.03          # default h2h threshold
EFFECTIVE = 0.045       # 1.5 * NOMINAL — real bar after the 0.5 min-strength floor
BUCKETS = [0.045, 0.03, 0.025, 0.02, 0.015]  # report counts at/above each


def main():
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    sql = (
        "SELECT event_id, sport_key, fetched_at, bookmaker_key, outcome_name, price "
        "FROM odds_snapshots WHERE market_key = 'h2h'"
    )
    params = []
    if SINCE:
        sql += " AND fetched_at >= ?"
        params.append(SINCE)
    # Stream ordered by the cell key so rows for one detection opportunity arrive
    # contiguously. We never materialize all rows (the box has ~1GB RAM): we hold
    # only the current cell's books plus the bounded per-game-side accumulators.
    sql += " ORDER BY event_id, fetched_at, outcome_name"

    best_delta = {}  # (event, outcome) -> max favorable delta
    cycle_opps = 0   # per-cycle sides where ANY favorable divergence existed
    sport_of = {}
    fetched_min = fetched_max = None
    total_rows = 0

    def flush(key, books):
        """Fold one completed (event, fetched_at, outcome) cell into best_delta."""
        nonlocal cycle_opps
        if key is None or PINNACLE_KEY not in books:
            return
        us_prices = [p for bk, p in books.items() if bk in US_BOOKS]
        if not us_prices:
            return
        pin_prob = american_to_implied_prob(books[PINNACLE_KEY])
        # US more generous = lower implied prob on this side = best (min) implied
        best_us_prob = min(american_to_implied_prob(p) for p in us_prices)
        favorable = pin_prob - best_us_prob  # >0 means US beats Pinnacle here
        if favorable <= 0:
            return
        cycle_opps += 1
        gk = (key[0], key[2])  # (event_id, outcome)
        if favorable > best_delta.get(gk, 0.0):
            best_delta[gk] = favorable

    cur_key = None
    cur_books = {}
    for r in db.execute(sql, params):
        total_rows += 1
        key = (r["event_id"], r["fetched_at"], r["outcome_name"])
        if key != cur_key:
            flush(cur_key, cur_books)
            cur_key = key
            cur_books = {}
        cur_books[r["bookmaker_key"]] = r["price"]
        sport_of[r["event_id"]] = r["sport_key"]
        fa = r["fetched_at"]
        fetched_min = fa if fetched_min is None or fa < fetched_min else fetched_min
        fetched_max = fa if fetched_max is None or fa > fetched_max else fetched_max
    flush(cur_key, cur_books)  # final cell
    db.close()

    if total_rows == 0:
        print("No h2h snapshots found" + (f" since {SINCE}" if SINCE else "") + ".")
        return

    total_sides = len(best_delta)
    span = f" (since {SINCE})" if SINCE else " (all stored data)"
    print(f"h2h PD delta distribution{span} - DB: {DB_PATH}")
    print(f"Snapshot window: {fetched_min}  ->  {fetched_max}")
    print(f"Distinct game-sides with ANY favorable US>Pinnacle h2h divergence: {total_sides}")
    print(f"(Per-cycle favorable opportunities, incl. repeats: {cycle_opps})\n")

    if total_sides == 0:
        print("No favorable h2h divergences in range.")
        return

    print("  Game-sides clearing each implied-prob bar (best delta ever seen):")
    print("  {:<8} {:>7} {:>7}   {}".format("bar", "count", "% sides", "note"))
    print("  " + "-" * 46)
    for b in BUCKETS:
        n = sum(1 for d in best_delta.values() if d >= b)
        pct = 100.0 * n / total_sides
        note = ""
        if abs(b - EFFECTIVE) < 1e-9:
            note = "<- EFFECTIVE bar today (min-strength floor)"
        elif abs(b - NOMINAL) < 1e-9:
            note = "<- NOMINAL detector threshold"
        print("  {:<8} {:>7} {:>6.1f}%   {}".format(f"{b*100:.1f}%", n, pct, note))

    eaten = sum(1 for d in best_delta.values() if NOMINAL <= d < EFFECTIVE)
    survive = sum(1 for d in best_delta.values() if d >= EFFECTIVE)
    print(
        f"\n  Floor impact: {eaten} game-sides cleared the 3% detector threshold but"
        f"\n  were killed by the 0.5 min-strength floor (needed 4.5%)."
        f"\n  {survive} survived. Lowering the effective bar to 3% would ~{_mult(survive, survive + eaten)} h2h volume."
    )

    # By sport, at the effective bar vs nominal
    by_sport = defaultdict(lambda: {"eff": 0, "nom": 0, "total": 0})
    for (event_id, _outcome), d in best_delta.items():
        s = by_sport[sport_of.get(event_id, "?")]
        s["total"] += 1
        if d >= EFFECTIVE:
            s["eff"] += 1
        if d >= NOMINAL:
            s["nom"] += 1
    print("\n  By sport (game-sides >= 4.5% effective / >= 3% nominal / total):")
    for sport in sorted(by_sport, key=lambda k: -by_sport[k]["nom"]):
        s = by_sport[sport]
        print("    {:<18} {:>4} / {:>4} / {:>4}".format(sport, s["eff"], s["nom"], s["total"]))


def _mult(survive, nominal_total):
    if survive == 0:
        return "add"
    return f"{nominal_total / survive:.1f}x"


if __name__ == "__main__":
    main()
