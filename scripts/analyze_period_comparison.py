"""Period comparison: pre vs post stabilization (March 20, 2026).

Compares overall performance, by signal type, by sport, by tier,
and free plays across both periods to confirm improvement.
"""

import json
import sqlite3
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone

DB = "/app/data/sharp_seeker.db"
MST = timezone(timedelta(hours=-7))

# Default cutoff: March 20, 2026 UTC (when stabilization changes landed)
DEFAULT_CUTOFF = "2026-03-20T00:00:00"

SIGNAL_LABELS = {
    "steam_move": "Steam",
    "rapid_change": "Rapid",
    "pinnacle_divergence": "PinDiv",
    "reverse_line": "RevLine",
    "exchange_shift": "ExchShift",
}

SPORT_SHORT = {
    "basketball_nba": "NBA",
    "basketball_ncaab": "NCAAB",
    "icehockey_nhl": "NHL",
    "baseball_mlb": "MLB",
}

MARKET_SHORT = {
    "h2h": "ML",
    "spreads": "Spread",
    "totals": "Total",
}


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


def rate(wins, losses):
    decided = wins + losses
    return wins / decided if decided > 0 else 0.0


def section(title):
    print()
    print("=" * 70)
    print(f"  {title}")
    print("=" * 70)


def tally(rows, key_fn):
    buckets = defaultdict(lambda: {"won": 0, "lost": 0, "push": 0, "units": 0.0})
    for row in rows:
        k = key_fn(row)
        if k is not None:
            buckets[k][row["result"]] += 1
            buckets[k]["units"] += compute_units(
                row.get("best_price"), row["result"], row.get("multiplier", 1),
            )
    return buckets


def print_comparison(label, before, after, indent=4):
    """Print before/after for a single dimension."""
    pad = " " * indent
    bw, bl, bp, bu = before["won"], before["lost"], before["push"], before["units"]
    aw, al, ap, au = after["won"], after["lost"], after["push"], after["units"]
    b_decided = bw + bl
    a_decided = aw + al
    b_rate = bw / b_decided if b_decided > 0 else 0
    a_rate = aw / a_decided if a_decided > 0 else 0
    delta_rate = a_rate - b_rate
    delta_units = au - bu

    print(f"{pad}{label}")
    print(f"{pad}  Before: {fmt(bw, bl, bp, bu)}")
    print(f"{pad}  After:  {fmt(aw, al, ap, au)}")

    if b_decided > 0 and a_decided > 0:
        arrow_r = "^" if delta_rate > 0 else "v" if delta_rate < 0 else "="
        arrow_u = "^" if delta_units > 0 else "v" if delta_units < 0 else "="
        print(f"{pad}  Delta:  {arrow_r} {delta_rate:+.1%} WR   {arrow_u} {delta_units:+.1f}u")
    print()


def tier_label(row):
    details_raw = row.get("details_json")
    if not details_raw:
        return "No qualifiers"
    try:
        details = json.loads(details_raw) if isinstance(details_raw, str) else details_raw
    except (json.JSONDecodeError, TypeError):
        return "No qualifiers"
    q = details.get("qualifier_count", 0)
    if q >= 3:
        return "2U (3+)"
    if q == 2:
        return "Elite (2)"
    if q == 1:
        return "Top Perf (1)"
    return "No qualifiers"


TIER_ORDER = ["2U (3+)", "Elite (2)", "Top Perf (1)", "No qualifiers"]


def compare_dimension(before_rows, after_rows, key_fn, keys, label_fn):
    """Compare before/after tallies for a dimension."""
    before_tally = tally(before_rows, key_fn)
    after_tally = tally(after_rows, key_fn)
    empty = {"won": 0, "lost": 0, "push": 0, "units": 0.0}
    for k in keys:
        b = before_tally.get(k, empty)
        a = after_tally.get(k, empty)
        if (b["won"] + b["lost"] + a["won"] + a["lost"]) > 0:
            print_comparison(label_fn(k), b, a)


def run():
    cutoff = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_CUTOFF
    print(f"Period cutoff: {cutoff}")
    print(f"Before = signals before {cutoff}")
    print(f"After  = signals on/after {cutoff}")

    conn = connect()

    cur = conn.execute("""
        SELECT event_id, sport_key, signal_type, market_key, outcome_name,
               signal_strength, signal_at, result, details_json
        FROM signal_results
        WHERE result IS NOT NULL
        ORDER BY signal_at
    """)
    rows = [dict(r) for r in cur.fetchall()]

    fp_cur = conn.execute("""
        SELECT event_id, market_key, outcome_name
        FROM sent_alerts
        WHERE is_free_play = 1
    """)
    free_play_keys = set()
    for fp in fp_cur.fetchall():
        fp_dict = dict(fp)
        free_play_keys.add(
            (fp_dict["event_id"], fp_dict["market_key"], fp_dict["outcome_name"])
        )
    conn.close()

    if not rows:
        print("No graded signals found.")
        return

    # Enrich
    for row in rows:
        row["dt"] = datetime.fromisoformat(row["signal_at"])
        row["tier"] = tier_label(row)
        row["is_free_play"] = (
            row["event_id"], row["market_key"], row["outcome_name"]
        ) in free_play_keys
        row["best_price"] = None
        row["multiplier"] = 1
        details_raw = row.get("details_json")
        if details_raw:
            try:
                details = json.loads(details_raw) if isinstance(details_raw, str) else details_raw
                vb = details.get("value_books", [])
                if vb:
                    row["best_price"] = vb[0].get("price")
                if details.get("qualifier_count", 0) >= 2:
                    row["multiplier"] = 2
            except (json.JSONDecodeError, TypeError):
                pass

    before = [r for r in rows if r["signal_at"] < cutoff]
    after = [r for r in rows if r["signal_at"] >= cutoff]

    print(f"\nBefore: {len(before)} graded signals")
    print(f"After:  {len(after)} graded signals")
    if before:
        b_first = before[0]["signal_at"][:10]
        b_last = before[-1]["signal_at"][:10]
        print(f"  Before range: {b_first} to {b_last}")
    if after:
        a_first = after[0]["signal_at"][:10]
        a_last = after[-1]["signal_at"][:10]
        print(f"  After range:  {a_first} to {a_last}")

    # ── 1. Overall ─────────────────────────────────────────
    section("1. OVERALL")
    b_all = {"won": 0, "lost": 0, "push": 0, "units": 0.0}
    a_all = {"won": 0, "lost": 0, "push": 0, "units": 0.0}
    for r in before:
        b_all[r["result"]] += 1
        b_all["units"] += compute_units(r.get("best_price"), r["result"], r.get("multiplier", 1))
    for r in after:
        a_all[r["result"]] += 1
        a_all["units"] += compute_units(r.get("best_price"), r["result"], r.get("multiplier", 1))
    print_comparison("All Signals", b_all, a_all)

    # ── 2. By signal type ──────────────────────────────────
    section("2. BY SIGNAL TYPE")
    all_types = sorted(set(r["signal_type"] for r in rows))
    compare_dimension(
        before, after,
        lambda r: r["signal_type"],
        all_types,
        lambda k: SIGNAL_LABELS.get(k, k),
    )

    # ── 3. By sport ────────────────────────────────────────
    section("3. BY SPORT")
    all_sports = sorted(set(r["sport_key"] for r in rows))
    compare_dimension(
        before, after,
        lambda r: r["sport_key"],
        all_sports,
        lambda k: SPORT_SHORT.get(k, k),
    )

    # ── 4. By tier ─────────────────────────────────────────
    section("4. BY TIER")
    compare_dimension(
        before, after,
        lambda r: r["tier"],
        TIER_ORDER,
        lambda k: k,
    )

    # ── 5. By signal type x sport ──────────────────────────
    section("5. BY SIGNAL TYPE x SPORT")
    all_type_sport = sorted(set((r["signal_type"], r["sport_key"]) for r in rows))
    compare_dimension(
        before, after,
        lambda r: (r["signal_type"], r["sport_key"]),
        all_type_sport,
        lambda k: f"{SIGNAL_LABELS.get(k[0], k[0]):10s} {SPORT_SHORT.get(k[1], k[1])}",
    )

    # ── 6. By signal type x market ─────────────────────────
    section("6. BY SIGNAL TYPE x MARKET")
    all_type_market = sorted(set((r["signal_type"], r["market_key"]) for r in rows))
    compare_dimension(
        before, after,
        lambda r: (r["signal_type"], r["market_key"]),
        all_type_market,
        lambda k: f"{SIGNAL_LABELS.get(k[0], k[0]):10s} {MARKET_SHORT.get(k[1], k[1])}",
    )

    # ── 7. Free plays ──────────────────────────────────────
    section("7. FREE PLAYS")
    fp_before = [r for r in before if r["is_free_play"]]
    fp_after = [r for r in after if r["is_free_play"]]
    b_fp = {"won": 0, "lost": 0, "push": 0, "units": 0.0}
    a_fp = {"won": 0, "lost": 0, "push": 0, "units": 0.0}
    for r in fp_before:
        b_fp[r["result"]] += 1
        b_fp["units"] += compute_units(r.get("best_price"), r["result"], r.get("multiplier", 1))
    for r in fp_after:
        a_fp[r["result"]] += 1
        a_fp["units"] += compute_units(r.get("best_price"), r["result"], r.get("multiplier", 1))
    print_comparison("Free Plays", b_fp, a_fp)

    # Free plays by sport
    if fp_before or fp_after:
        fp_sports = sorted(set(r["sport_key"] for r in fp_before + fp_after))
        compare_dimension(
            fp_before, fp_after,
            lambda r: r["sport_key"],
            fp_sports,
            lambda k: f"FP {SPORT_SHORT.get(k, k)}",
        )

    # ── 8. Daily trend (after period only) ─────────────────
    section("8. DAILY TREND (after period)")
    if after:
        by_day = defaultdict(lambda: {"won": 0, "lost": 0, "push": 0, "units": 0.0})
        for r in after:
            day = r["signal_at"][:10]
            by_day[day][r["result"]] += 1
            by_day[day]["units"] += compute_units(
                r.get("best_price"), r["result"], r.get("multiplier", 1)
            )
        cumulative_units = 0.0
        for day in sorted(by_day.keys()):
            d = by_day[day]
            cumulative_units += d["units"]
            decided = d["won"] + d["lost"]
            wr = d["won"] / decided if decided > 0 else 0
            sign = "+" if d["units"] >= 0 else ""
            cum_sign = "+" if cumulative_units >= 0 else ""
            print(f"    {day}  {d['won']}W-{d['lost']}L-{d['push']}P  ({wr:.0%})  [{sign}{d['units']:.1f}u]  cum: {cum_sign}{cumulative_units:.1f}u")

    # ── 9. Summary ─────────────────────────────────────────
    section("9. SUMMARY")
    b_decided = b_all["won"] + b_all["lost"]
    a_decided = a_all["won"] + a_all["lost"]
    if b_decided > 0 and a_decided > 0:
        b_wr = b_all["won"] / b_decided
        a_wr = a_all["won"] / a_decided
        print(f"  Before: {b_wr:.1%} WR, {b_all['units']:+.1f}u  ({b_decided} decided)")
        print(f"  After:  {a_wr:.1%} WR, {a_all['units']:+.1f}u  ({a_decided} decided)")
        print(f"  Change: {a_wr - b_wr:+.1%} WR, {a_all['units'] - b_all['units']:+.1f}u")
        print()

        # Volume comparison
        if before:
            b_days = (datetime.fromisoformat(before[-1]["signal_at"]) - datetime.fromisoformat(before[0]["signal_at"])).days or 1
        else:
            b_days = 1
        if after:
            a_days = (datetime.fromisoformat(after[-1]["signal_at"]) - datetime.fromisoformat(after[0]["signal_at"])).days or 1
        else:
            a_days = 1
        print(f"  Before: {len(before)} signals over {b_days}d = {len(before)/b_days:.0f}/day")
        print(f"  After:  {len(after)} signals over {a_days}d = {len(after)/a_days:.0f}/day")
        print(f"  Units/day: before {b_all['units']/b_days:+.1f}, after {a_all['units']/a_days:+.1f}")

    print("\nDone.")


if __name__ == "__main__":
    run()
