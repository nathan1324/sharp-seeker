"""PD Totals deep dive — investigate the regression from +28.4u to -32.3u.

Compares before (pre-Mar 20) vs after across every dimension to find
what shifted and what's actionable.
"""

import json
import sqlite3
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone

DB = "/app/data/sharp_seeker.db"
MST = timezone(timedelta(hours=-7))
CUTOFF = "2026-03-20T00:00:00"

SPORT_SHORT = {
    "basketball_nba": "NBA",
    "basketball_ncaab": "NCAAB",
    "icehockey_nhl": "NHL",
    "baseball_mlb": "MLB",
}

STRENGTH_BUCKETS = [
    (0.0, 0.25, "<25%"),
    (0.25, 0.35, "25-34%"),
    (0.35, 0.50, "35-49%"),
    (0.50, 0.67, "50-66%"),
    (0.67, 0.80, "67-79%"),
    (0.80, 1.01, "80%+"),
]

HOLD_BUCKETS = [
    (-999, -2.0, "Arb (<-2%)"),
    (-2.0, 0.0, "Neg (-2 to 0%)"),
    (0.0, 2.0, "Tight (0-2%)"),
    (2.0, 3.0, "Edge (2-3%)"),
    (3.0, 5.0, "Wide (3-5%)"),
    (5.0, 999, "Very Wide (5%+)"),
]

DELTA_BUCKETS = [
    (0.0, 0.5, "<0.5"),
    (0.5, 0.75, "0.5-0.74"),
    (0.75, 1.0, "0.75-0.99"),
    (1.0, 1.5, "1.0-1.49"),
    (1.5, 2.0, "1.5-1.99"),
    (2.0, 999, "2.0+"),
]


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


def rate(w, l):
    return w / (w + l) if (w + l) > 0 else 0.0


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
    pad = " " * indent
    bw, bl, bp, bu = before["won"], before["lost"], before["push"], before["units"]
    aw, al, ap, au = after["won"], after["lost"], after["push"], after["units"]
    b_decided = bw + bl
    a_decided = aw + al
    b_rate = bw / b_decided if b_decided > 0 else 0
    a_rate = aw / a_decided if a_decided > 0 else 0

    print(f"{pad}{label}")
    print(f"{pad}  Before: {fmt(bw, bl, bp, bu)}")
    print(f"{pad}  After:  {fmt(aw, al, ap, au)}")
    if b_decided > 0 and a_decided > 0:
        delta_r = a_rate - b_rate
        delta_u = au - bu
        arrow_r = "^" if delta_r > 0 else "v" if delta_r < 0 else "="
        arrow_u = "^" if delta_u > 0 else "v" if delta_u < 0 else "="
        print(f"{pad}  Delta:  {arrow_r} {delta_r:+.1%} WR   {arrow_u} {delta_u:+.1f}u")
    print()


def compare_dim(before, after, key_fn, keys, label_fn):
    bt = tally(before, key_fn)
    at = tally(after, key_fn)
    empty = {"won": 0, "lost": 0, "push": 0, "units": 0.0}
    for k in keys:
        b = bt.get(k, empty)
        a = at.get(k, empty)
        if (b["won"] + b["lost"] + a["won"] + a["lost"]) > 0:
            print_comparison(label_fn(k), b, a)


def bucket_label(val, buckets):
    if val is None:
        return None
    for lo, hi, label in buckets:
        if lo <= val < hi:
            return label
    return "?"


def mst_label(utc_hour):
    mst_hour = (utc_hour - 7) % 24
    ampm = "AM" if mst_hour < 12 else "PM"
    display = mst_hour % 12 or 12
    return f"{display:2d} {ampm} MST (UTC {utc_hour:02d})"


def run():
    conn = connect()
    cur = conn.execute("""
        SELECT event_id, sport_key, signal_type, market_key, outcome_name,
               signal_strength, signal_at, result, details_json
        FROM signal_results
        WHERE result IS NOT NULL
          AND signal_type = 'pinnacle_divergence'
          AND market_key = 'totals'
        ORDER BY signal_at
    """)
    rows = [dict(r) for r in cur.fetchall()]

    fp_cur = conn.execute("""
        SELECT event_id, market_key, outcome_name
        FROM sent_alerts WHERE is_free_play = 1
    """)
    free_play_keys = set()
    for fp in fp_cur.fetchall():
        d = dict(fp)
        free_play_keys.add((d["event_id"], d["market_key"], d["outcome_name"]))
    conn.close()

    if not rows:
        print("No PD totals signals found.")
        return

    # Enrich
    for row in rows:
        row["dt"] = datetime.fromisoformat(row["signal_at"])
        row["is_free_play"] = (
            row["event_id"], row["market_key"], row["outcome_name"]
        ) in free_play_keys
        row["best_price"] = None
        row["multiplier"] = 1
        row["cross_hold"] = None
        row["us_hold"] = None
        row["delta"] = None
        row["value_book"] = "unknown"
        row["direction"] = row["outcome_name"]  # Over/Under
        details_raw = row.get("details_json")
        if details_raw:
            try:
                details = json.loads(details_raw) if isinstance(details_raw, str) else details_raw
                row["cross_hold"] = details.get("cross_book_hold")
                row["us_hold"] = details.get("us_hold")
                row["delta"] = details.get("delta")
                row["hold_boost"] = details.get("hold_boost", 0)
                vb = details.get("value_books", [])
                if vb:
                    row["best_price"] = vb[0].get("price")
                    row["value_book"] = vb[0].get("bookmaker", "unknown")
                if details.get("qualifier_count", 0) >= 2:
                    row["multiplier"] = 2
            except (json.JSONDecodeError, TypeError):
                pass

    before = [r for r in rows if r["signal_at"] < CUTOFF]
    after = [r for r in rows if r["signal_at"] >= CUTOFF]

    print(f"PD Totals Deep Dive")
    print(f"Cutoff: {CUTOFF}")
    print(f"Before: {len(before)} signals ({before[0]['signal_at'][:10]} to {before[-1]['signal_at'][:10]})" if before else "Before: 0")
    print(f"After:  {len(after)} signals ({after[0]['signal_at'][:10]} to {after[-1]['signal_at'][:10]})" if after else "After: 0")

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
    print_comparison("PD Totals (all sports)", b_all, a_all)

    # ── 2. By sport ────────────────────────────────────────
    section("2. BY SPORT")
    all_sports = sorted(set(r["sport_key"] for r in rows))
    compare_dim(before, after, lambda r: r["sport_key"], all_sports,
                lambda k: SPORT_SHORT.get(k, k))

    # ── 3. By direction (Over vs Under) ────────────────────
    section("3. OVER vs UNDER")
    compare_dim(before, after, lambda r: r["direction"],
                ["Over", "Under"], lambda k: k)

    # By sport x direction
    print("  By Sport x Direction:")
    combos = sorted(set((r["sport_key"], r["direction"]) for r in rows))
    compare_dim(before, after, lambda r: (r["sport_key"], r["direction"]),
                combos, lambda k: f"{SPORT_SHORT.get(k[0], k[0])} {k[1]}")

    # ── 4. By strength ─────────────────────────────────────
    section("4. BY STRENGTH")
    str_keys = [b[2] for b in STRENGTH_BUCKETS]
    compare_dim(before, after,
                lambda r: bucket_label(r["signal_strength"], STRENGTH_BUCKETS),
                str_keys, lambda k: k)

    # ── 5. By cross-book hold ──────────────────────────────
    section("5. BY CROSS-BOOK HOLD")
    hold_keys = [b[2] for b in HOLD_BUCKETS]
    compare_dim(
        [r for r in before if r["cross_hold"] is not None],
        [r for r in after if r["cross_hold"] is not None],
        lambda r: bucket_label(r["cross_hold"], HOLD_BUCKETS),
        hold_keys, lambda k: k,
    )

    # ── 6. By delta ────────────────────────────────────────
    section("6. BY DELTA (divergence size)")
    delta_keys = [b[2] for b in DELTA_BUCKETS]
    compare_dim(
        [r for r in before if r["delta"] is not None],
        [r for r in after if r["delta"] is not None],
        lambda r: bucket_label(abs(r["delta"]), DELTA_BUCKETS),
        delta_keys, lambda k: k,
    )

    # ── 7. By hour ─────────────────────────────────────────
    section("7. BY HOUR (MST)")
    all_hours = sorted(set(r["dt"].hour for r in rows))
    compare_dim(before, after, lambda r: r["dt"].hour,
                all_hours, lambda k: mst_label(k))

    # After-only hour breakdown (actionable)
    if after:
        print("  After period hours ranked by units:")
        at = tally(after, lambda r: r["dt"].hour)
        ranked = []
        for h, d in at.items():
            decided = d["won"] + d["lost"]
            if decided >= 2:
                ranked.append((h, d, d["units"]))
        ranked.sort(key=lambda x: x[2])
        for h, d, u in ranked:
            print(f"    {mst_label(h):28s} {fmt(d['won'], d['lost'], d['push'], u)}")

    # ── 8. By value book ───────────────────────────────────
    section("8. BY VALUE BOOK")
    all_books = sorted(set(r["value_book"] for r in rows))
    compare_dim(before, after, lambda r: r["value_book"],
                all_books, lambda k: k)

    # After-only book breakdown
    if after:
        print("  After period books ranked by units:")
        at = tally(after, lambda r: r["value_book"])
        ranked = sorted(at.items(), key=lambda x: x[1]["units"])
        for book, d in ranked:
            print(f"    {book:20s} {fmt(d['won'], d['lost'], d['push'], d['units'])}")

    # ── 9. By sport x hour (after only) ───────────────────
    section("9. SPORT x HOUR — after period")
    if after:
        by_sport_hour = tally(after, lambda r: (r["sport_key"], r["dt"].hour))
        for sport in all_sports:
            sport_hours = [(sport, h) for h in range(24) if (sport, h) in by_sport_hour]
            if sport_hours:
                print(f"\n    {SPORT_SHORT.get(sport, sport)}:")
                for k in sport_hours:
                    d = by_sport_hour[k]
                    print(f"      {mst_label(k[1]):28s} {fmt(d['won'], d['lost'], d['push'], d['units'])}")

    # ── 10. By sport x book (after only) ──────────────────
    section("10. SPORT x BOOK — after period")
    if after:
        by_sport_book = tally(after, lambda r: (r["sport_key"], r["value_book"]))
        for sport in all_sports:
            sport_books = sorted(
                [(sport, b) for b in all_books if (sport, b) in by_sport_book],
                key=lambda k: by_sport_book[k]["units"]
            )
            if sport_books:
                print(f"\n    {SPORT_SHORT.get(sport, sport)}:")
                for k in sport_books:
                    d = by_sport_book[k]
                    print(f"      {k[1]:20s} {fmt(d['won'], d['lost'], d['push'], d['units'])}")

    # ── 11. Daily trend (after) ────────────────────────────
    section("11. DAILY TREND — after period")
    if after:
        by_day = defaultdict(lambda: {"won": 0, "lost": 0, "push": 0, "units": 0.0})
        for r in after:
            day = r["signal_at"][:10]
            by_day[day][r["result"]] += 1
            by_day[day]["units"] += compute_units(
                r.get("best_price"), r["result"], r.get("multiplier", 1)
            )
        cumulative = 0.0
        for day in sorted(by_day.keys()):
            d = by_day[day]
            cumulative += d["units"]
            decided = d["won"] + d["lost"]
            wr = d["won"] / decided if decided > 0 else 0
            sign = "+" if d["units"] >= 0 else ""
            cum_sign = "+" if cumulative >= 0 else ""
            print(f"    {day}  {d['won']}W-{d['lost']}L-{d['push']}P  ({wr:.0%})  [{sign}{d['units']:.1f}u]  cum: {cum_sign}{cumulative:.1f}u")

    # ── 12. Summary ────────────────────────────────────────
    section("12. SUMMARY")
    b_d = b_all["won"] + b_all["lost"]
    a_d = a_all["won"] + a_all["lost"]
    if b_d > 0:
        print(f"  Before: {rate(b_all['won'], b_all['lost']):.1%} WR, {b_all['units']:+.1f}u  ({b_d} decided, {len(before)}/{(datetime.fromisoformat(before[-1]['signal_at']) - datetime.fromisoformat(before[0]['signal_at'])).days or 1}d)")
    if a_d > 0:
        print(f"  After:  {rate(a_all['won'], a_all['lost']):.1%} WR, {a_all['units']:+.1f}u  ({a_d} decided, {len(after)}/{(datetime.fromisoformat(after[-1]['signal_at']) - datetime.fromisoformat(after[0]['signal_at'])).days or 1}d)")

    # Flag biggest drags in after period
    if after:
        print()
        print("  Biggest drags (after period):")
        drags = []

        # Sport
        st = tally(after, lambda r: r["sport_key"])
        for k, d in st.items():
            if d["units"] < -3:
                drags.append((f"Sport: {SPORT_SHORT.get(k, k)}", d["units"], d))

        # Book
        bt = tally(after, lambda r: r["value_book"])
        for k, d in bt.items():
            if d["units"] < -3:
                drags.append((f"Book: {k}", d["units"], d))

        # Hour
        ht = tally(after, lambda r: r["dt"].hour)
        for k, d in ht.items():
            if d["units"] < -3:
                drags.append((f"Hour: {mst_label(k)}", d["units"], d))

        drags.sort(key=lambda x: x[1])
        for label, u, d in drags[:10]:
            print(f"    {label:40s} {fmt(d['won'], d['lost'], d['push'], u)}")

    print("\nDone.")


if __name__ == "__main__":
    run()
