"""Retrofit analysis: replay after-period signals with today's changes applied.

Simulates what performance would have looked like if these changes
were in place from Mar 20:
  1. Caesars (williamhill_us) excluded from PD
  2. MLB PD totals threshold raised to 1.0 (filters delta < 1.0)
  3. MLB PD quiet hours: UTC 15, 21

Shows before/after for each filter and cumulative impact.
"""

import json
import sqlite3
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone

DB = "/app/data/sharp_seeker.db"
CUTOFF = "2026-03-20T00:00:00"

SPORT_SHORT = {
    "basketball_nba": "NBA",
    "basketball_ncaab": "NCAAB",
    "icehockey_nhl": "NHL",
    "baseball_mlb": "MLB",
}

SIGNAL_LABELS = {
    "steam_move": "Steam",
    "rapid_change": "Rapid",
    "pinnacle_divergence": "PinDiv",
    "reverse_line": "RevLine",
    "exchange_shift": "ExchShift",
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


def compute_units(price, result, multiplier=1):
    if result == "push" or price is None:
        return 0.0
    if price < 0:
        risk = abs(price) / 100.0
    else:
        risk = 100.0 / price if price > 0 else 1.0
    if result == "won":
        return 1.0 * multiplier
    elif result == "lost":
        return -risk * multiplier
    return 0.0


def fmt(wins, losses, pushes, units=None):
    n = wins + losses + pushes
    decided = wins + losses
    if decided == 0:
        base = f"(n={n:4d})  {wins}W-{losses}L-{pushes}P  (--)"
    else:
        r = wins / decided
        base = f"(n={n:4d})  {wins}W-{losses}L-{pushes}P  ({r:.0%})"
    if units is not None:
        sign = "+" if units >= 0 else ""
        base += f"  [{sign}{units:.1f}u]"
    return base


def tally_rows(rows):
    w = sum(1 for r in rows if r["result"] == "won")
    l = sum(1 for r in rows if r["result"] == "lost")
    p = sum(1 for r in rows if r["result"] == "push")
    u = sum(compute_units(r.get("best_price"), r["result"], r.get("multiplier", 1)) for r in rows)
    return w, l, p, u


def tally_group(rows, key_fn):
    buckets = defaultdict(list)
    for r in rows:
        k = key_fn(r)
        if k is not None:
            buckets[k].append(r)
    return buckets


def section(title):
    print()
    print("=" * 70)
    print(f"  {title}")
    print("=" * 70)


def run():
    conn = connect()

    # Get ALL resolved signals
    cur = conn.execute("""
        SELECT event_id, sport_key, signal_type, market_key, outcome_name,
               signal_strength, signal_at, result, details_json
        FROM signal_results
        WHERE result IS NOT NULL
        ORDER BY signal_at
    """)
    all_rows = [dict(r) for r in cur.fetchall()]
    conn.close()

    # Enrich
    for row in all_rows:
        row["dt"] = datetime.fromisoformat(row["signal_at"])
        row["best_price"] = None
        row["multiplier"] = 1
        row["value_book"] = "unknown"
        row["delta"] = None
        details_raw = row.get("details_json")
        if details_raw:
            try:
                details = json.loads(details_raw) if isinstance(details_raw, str) else details_raw
                row["delta"] = details.get("delta")
                vb = details.get("value_books", [])
                if vb:
                    row["best_price"] = vb[0].get("price")
                    row["value_book"] = vb[0].get("bookmaker", "unknown")
                if details.get("qualifier_count", 0) >= 2:
                    row["multiplier"] = 2
            except (json.JSONDecodeError, TypeError):
                pass

    before = [r for r in all_rows if r["signal_at"] < CUTOFF]
    after = [r for r in all_rows if r["signal_at"] >= CUTOFF]

    print(f"Retrofit Analysis")
    print(f"Before period: {len(before)} signals")
    print(f"After period:  {len(after)} signals (pre-filter)")

    # ── Define filters ─────────────────────────────────────
    def is_caesars_pd(r):
        return (r["signal_type"] == "pinnacle_divergence"
                and r["value_book"] == "williamhill_us")

    def is_mlb_pd_totals_low_delta(r):
        return (r["signal_type"] == "pinnacle_divergence"
                and r["sport_key"] == "baseball_mlb"
                and r["market_key"] == "totals"
                and r["delta"] is not None
                and abs(r["delta"]) < 1.0)

    def is_mlb_pd_quiet_hour(r):
        return (r["signal_type"] == "pinnacle_divergence"
                and r["sport_key"] == "baseball_mlb"
                and r["dt"].hour in [15, 21])

    # ── Apply filters incrementally ────────────────────────
    section("1. INCREMENTAL FILTER IMPACT (after period)")

    current = list(after)
    w, l, p, u = tally_rows(current)
    print(f"  Baseline (no changes):       {fmt(w, l, p, u)}")

    # Filter 1: Caesars PD
    removed_caesars = [r for r in current if is_caesars_pd(r)]
    current = [r for r in current if not is_caesars_pd(r)]
    w, l, p, u = tally_rows(current)
    rw, rl, rp, ru = tally_rows(removed_caesars)
    print(f"  - Caesars PD removed:        {fmt(rw, rl, rp, ru)}")
    print(f"  After Caesars filter:         {fmt(w, l, p, u)}")

    # Filter 2: MLB PD totals delta < 1.0
    removed_mlb = [r for r in current if is_mlb_pd_totals_low_delta(r)]
    current = [r for r in current if not is_mlb_pd_totals_low_delta(r)]
    w, l, p, u = tally_rows(current)
    rw, rl, rp, ru = tally_rows(removed_mlb)
    print(f"  - MLB PD totals <1.0 removed: {fmt(rw, rl, rp, ru)}")
    print(f"  After MLB threshold filter:   {fmt(w, l, p, u)}")

    # Filter 3: MLB PD quiet hours
    removed_quiet = [r for r in current if is_mlb_pd_quiet_hour(r)]
    current = [r for r in current if not is_mlb_pd_quiet_hour(r)]
    w, l, p, u = tally_rows(current)
    rw, rl, rp, ru = tally_rows(removed_quiet)
    print(f"  - MLB quiet hours removed:   {fmt(rw, rl, rp, ru)}")
    print(f"  After all filters:            {fmt(w, l, p, u)}")

    total_removed = len(after) - len(current)
    print(f"\n  Total signals removed: {total_removed} ({total_removed/len(after):.0%} of after period)")

    # ── Before period for comparison ───────────────────────
    section("2. BEFORE PERIOD (reference)")
    w, l, p, u = tally_rows(before)
    print(f"  Before period:               {fmt(w, l, p, u)}")
    b_days = max(1, (datetime.fromisoformat(before[-1]["signal_at"]) - datetime.fromisoformat(before[0]["signal_at"])).days)
    a_days = max(1, (datetime.fromisoformat(after[-1]["signal_at"]) - datetime.fromisoformat(after[0]["signal_at"])).days)
    bw, bl, bp, bu = tally_rows(before)
    aw, al, ap, au = tally_rows(current)
    print(f"  Before: {bw/(bw+bl):.1%} WR, {bu:+.1f}u, {len(before)/b_days:.0f} signals/day, {bu/b_days:+.1f}u/day")
    print(f"  After (filtered): {aw/(aw+al):.1%} WR, {au:+.1f}u, {len(current)/a_days:.0f} signals/day, {au/a_days:+.1f}u/day")

    # ── Filtered after by signal type ──────────────────────
    section("3. FILTERED AFTER — BY SIGNAL TYPE")
    by_type = tally_group(current, lambda r: r["signal_type"])
    for st in sorted(by_type.keys()):
        w, l, p, u = tally_rows(by_type[st])
        print(f"    {SIGNAL_LABELS.get(st, st):12s} {fmt(w, l, p, u)}")

    # ── Filtered after by sport ────────────────────────────
    section("4. FILTERED AFTER — BY SPORT")
    by_sport = tally_group(current, lambda r: r["sport_key"])
    for sp in sorted(by_sport.keys()):
        w, l, p, u = tally_rows(by_sport[sp])
        print(f"    {SPORT_SHORT.get(sp, sp):12s} {fmt(w, l, p, u)}")

    # ── Filtered after by signal type x market ─────────────
    section("5. FILTERED AFTER — BY SIGNAL TYPE x MARKET")
    by_tm = tally_group(current, lambda r: (r["signal_type"], r["market_key"]))
    for k in sorted(by_tm.keys()):
        w, l, p, u = tally_rows(by_tm[k])
        print(f"    {SIGNAL_LABELS.get(k[0], k[0]):10s} {MARKET_SHORT.get(k[1], k[1]):8s} {fmt(w, l, p, u)}")

    # ── Filtered after daily trend ─────────────────────────
    section("6. FILTERED AFTER — DAILY TREND")
    by_day = defaultdict(list)
    for r in current:
        by_day[r["signal_at"][:10]].append(r)
    cumulative = 0.0
    for day in sorted(by_day.keys()):
        w, l, p, u = tally_rows(by_day[day])
        cumulative += u
        decided = w + l
        wr = w / decided if decided > 0 else 0
        sign = "+" if u >= 0 else ""
        cum_sign = "+" if cumulative >= 0 else ""
        print(f"    {day}  {w}W-{l}L-{p}P  ({wr:.0%})  [{sign}{u:.1f}u]  cum: {cum_sign}{cumulative:.1f}u")

    # ── Free plays impact ──────────────────────────────────
    section("7. FREE PLAY IMPACT")
    # Check which removed signals were free plays (had qualifier_count >= 2)
    all_removed = removed_caesars + removed_mlb + removed_quiet
    removed_with_qualifiers = []
    for r in all_removed:
        details_raw = r.get("details_json")
        if details_raw:
            try:
                details = json.loads(details_raw) if isinstance(details_raw, str) else details_raw
                qc = details.get("qualifier_count", 0)
                if qc >= 1:
                    removed_with_qualifiers.append((r, qc))
            except (json.JSONDecodeError, TypeError):
                pass
    if removed_with_qualifiers:
        print(f"  Removed signals that had qualifiers: {len(removed_with_qualifiers)}")
        for r, qc in removed_with_qualifiers:
            tier = "2U" if qc >= 3 else "Elite" if qc == 2 else "TopPerf"
            res = r["result"]
            sport = SPORT_SHORT.get(r["sport_key"], r["sport_key"])
            mkt = MARKET_SHORT.get(r["market_key"], r["market_key"])
            print(f"    {res:5s} {tier:8s} {sport:6s} {mkt:6s} {r['outcome_name']:20s} book={r['value_book']}")
    else:
        print("  No removed signals had qualifiers.")

    print("\nDone.")


if __name__ == "__main__":
    run()
