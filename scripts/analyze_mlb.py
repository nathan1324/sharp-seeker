"""MLB deep dive analysis.

Breaks down all MLB signals across every dimension to understand
early-season performance and identify what to tune.

Sections:
  1. Overall + by signal type
  2. Signal type x market
  3. Strength buckets
  4. Hour analysis (MST)
  5. Cross-book hold analysis
  6. Individual signal breakdown (every signal with result)
  7. By book (value book that was recommended)
  8. By team
  9. Qualifier / tier analysis
  10. Actionable summary
"""

import json
import sqlite3
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone

DB = "/app/data/sharp_seeker.db"
MST = timezone(timedelta(hours=-7))

SIGNAL_LABELS = {
    "steam_move": "Steam",
    "rapid_change": "Rapid",
    "pinnacle_divergence": "PinDiv",
    "reverse_line": "RevLine",
    "exchange_shift": "ExchShift",
}

MARKET_SHORT = {
    "h2h": "ML",
    "spreads": "Spread",
    "totals": "Total",
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
    (-2.0, 0.0, "Negative (-2 to 0%)"),
    (0.0, 2.0, "Tight (0-2%)"),
    (2.0, 3.0, "Edge (2-3%)"),
    (3.0, 5.0, "Wide (3-5%)"),
    (5.0, 999, "Very Wide (5%+)"),
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


def print_buckets(buckets, key_order=None, label_fn=None, indent=4):
    if key_order is None:
        key_order = sorted(buckets.keys())
    pad = " " * indent
    for k in key_order:
        if k not in buckets:
            continue
        d = buckets[k]
        label = label_fn(k) if label_fn else str(k)
        print(f"{pad}{label:42s} {fmt(d['won'], d['lost'], d['push'], d['units'])}")


def strength_label(s):
    for lo, hi, label in STRENGTH_BUCKETS:
        if lo <= s < hi:
            return label
    return "?"


def hold_label(h):
    for lo, hi, label in HOLD_BUCKETS:
        if lo <= h < hi:
            return label
    return "?"


def mst_label(utc_hour):
    mst_hour = (utc_hour - 7) % 24
    ampm = "AM" if mst_hour < 12 else "PM"
    display = mst_hour % 12 or 12
    return f"{display:2d} {ampm} MST (UTC {utc_hour:02d})"


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


def parse_details(row):
    """Extract useful fields from details_json."""
    details_raw = row.get("details_json")
    if not details_raw:
        return {}
    try:
        return json.loads(details_raw) if isinstance(details_raw, str) else details_raw
    except (json.JSONDecodeError, TypeError):
        return {}


def run():
    conn = connect()

    cur = conn.execute("""
        SELECT event_id, sport_key, signal_type, market_key, outcome_name,
               signal_direction, signal_strength, signal_at, result, details_json
        FROM signal_results
        WHERE sport_key = 'baseball_mlb' AND result IS NOT NULL
        ORDER BY signal_at
    """)
    rows = [dict(r) for r in cur.fetchall()]

    # Also get unresolved signals
    unresolved_cur = conn.execute("""
        SELECT event_id, sport_key, signal_type, market_key, outcome_name,
               signal_direction, signal_strength, signal_at, result, details_json
        FROM signal_results
        WHERE sport_key = 'baseball_mlb' AND result IS NULL
        ORDER BY signal_at
    """)
    unresolved = [dict(r) for r in unresolved_cur.fetchall()]

    # Free play keys
    fp_cur = conn.execute("""
        SELECT event_id, market_key, outcome_name
        FROM sent_alerts WHERE is_free_play = 1
    """)
    free_play_keys = set()
    for fp in fp_cur.fetchall():
        fp_dict = dict(fp)
        free_play_keys.add(
            (fp_dict["event_id"], fp_dict["market_key"], fp_dict["outcome_name"])
        )

    # Suppressed (0-qualifier) signals — in signal_results but NOT in sent_alerts
    sent_cur = conn.execute("""
        SELECT event_id, alert_type, market_key, outcome_name
        FROM sent_alerts
    """)
    sent_keys = set()
    for s in sent_cur.fetchall():
        s_dict = dict(s)
        sent_keys.add(
            (s_dict["event_id"], s_dict["alert_type"], s_dict["market_key"], s_dict["outcome_name"])
        )

    conn.close()

    if not rows:
        print("No graded MLB signals found.")
        if unresolved:
            print(f"({len(unresolved)} unresolved MLB signals pending grading)")
        return

    # Enrich rows
    for row in rows:
        row["dt"] = datetime.fromisoformat(row["signal_at"])
        row["tier"] = tier_label(row)
        row["is_free_play"] = (
            row["event_id"], row["market_key"], row["outcome_name"]
        ) in free_play_keys
        row["was_sent"] = (
            row["event_id"], row["signal_type"], row["market_key"], row["outcome_name"]
        ) in sent_keys
        details = parse_details(row)
        row["details"] = details
        row["best_price"] = None
        row["multiplier"] = 1
        row["cross_hold"] = details.get("cross_book_hold")
        row["us_hold"] = details.get("us_hold")
        row["delta"] = details.get("delta")
        row["qualifier_tags"] = details.get("qualifier_tags", [])
        vb = details.get("value_books", [])
        if vb:
            row["best_price"] = vb[0].get("price")
            row["best_book"] = vb[0].get("bookmaker", vb[0].get("book", "unknown"))
        else:
            row["best_book"] = "unknown"
        if details.get("qualifier_count", 0) >= 2:
            row["multiplier"] = 2

    total = len(rows)
    first = rows[0]["signal_at"][:10]
    last = rows[-1]["signal_at"][:10]
    print(f"MLB Deep Dive: {total} graded signals")
    print(f"Range: {first} to {last}")
    if unresolved:
        print(f"({len(unresolved)} unresolved signals pending)")

    # ── 1. Overall + by signal type ────────────────────────
    section("1. OVERALL + BY SIGNAL TYPE")
    all_w = sum(1 for r in rows if r["result"] == "won")
    all_l = sum(1 for r in rows if r["result"] == "lost")
    all_p = sum(1 for r in rows if r["result"] == "push")
    all_u = sum(compute_units(r.get("best_price"), r["result"], r.get("multiplier", 1)) for r in rows)
    print(f"    Overall: {fmt(all_w, all_l, all_p, all_u)}")
    print()

    by_type = tally(rows, lambda r: r["signal_type"])
    sig_types = sorted(by_type.keys())
    print_buckets(by_type, sig_types, lambda k: SIGNAL_LABELS.get(k, k))

    # Sent vs suppressed
    sent_rows = [r for r in rows if r["was_sent"]]
    supp_rows = [r for r in rows if not r["was_sent"]]
    if sent_rows or supp_rows:
        print()
        sw = sum(1 for r in sent_rows if r["result"] == "won")
        sl = sum(1 for r in sent_rows if r["result"] == "lost")
        sp = sum(1 for r in sent_rows if r["result"] == "push")
        su = sum(compute_units(r.get("best_price"), r["result"], r.get("multiplier", 1)) for r in sent_rows)
        print(f"    Sent to Discord:  {fmt(sw, sl, sp, su)}")
        uw = sum(1 for r in supp_rows if r["result"] == "won")
        ul = sum(1 for r in supp_rows if r["result"] == "lost")
        up = sum(1 for r in supp_rows if r["result"] == "push")
        uu = sum(compute_units(r.get("best_price"), r["result"], r.get("multiplier", 1)) for r in supp_rows)
        print(f"    Suppressed:       {fmt(uw, ul, up, uu)}")

    # ── 2. Signal type x market ────────────────────────────
    section("2. SIGNAL TYPE x MARKET")
    by_type_mkt = tally(rows, lambda r: (r["signal_type"], r["market_key"]))
    keys = sorted(by_type_mkt.keys())
    print_buckets(
        by_type_mkt, keys,
        lambda k: f"{SIGNAL_LABELS.get(k[0], k[0]):10s} {MARKET_SHORT.get(k[1], k[1])}"
    )

    # ── 3. Strength buckets ────────────────────────────────
    section("3. STRENGTH ANALYSIS")
    by_str = tally(rows, lambda r: strength_label(r["signal_strength"]))
    str_order = [b[2] for b in STRENGTH_BUCKETS]
    print_buckets(by_str, str_order, lambda k: k)

    # By signal type x strength
    print()
    by_type_str = tally(rows, lambda r: (r["signal_type"], strength_label(r["signal_strength"])))
    for st in sig_types:
        print(f"\n    {SIGNAL_LABELS.get(st, st)}:")
        for _, _, slabel in STRENGTH_BUCKETS:
            k = (st, slabel)
            if k in by_type_str:
                d = by_type_str[k]
                print(f"      {slabel:12s} {fmt(d['won'], d['lost'], d['push'], d['units'])}")

    # ── 4. Hour analysis ───────────────────────────────────
    section("4. HOUR ANALYSIS (MST)")
    by_hour = tally(rows, lambda r: r["dt"].hour)
    for utc_h in range(24):
        if utc_h in by_hour:
            d = by_hour[utc_h]
            print(f"    {mst_label(utc_h):28s} {fmt(d['won'], d['lost'], d['push'], d['units'])}")

    # By signal type x hour
    by_type_hour = tally(rows, lambda r: (r["signal_type"], r["dt"].hour))
    for st in sig_types:
        type_hours = [(st, h) for h in range(24) if (st, h) in by_type_hour]
        if type_hours:
            print(f"\n    {SIGNAL_LABELS.get(st, st)}:")
            for k in type_hours:
                d = by_type_hour[k]
                print(f"      {mst_label(k[1]):28s} {fmt(d['won'], d['lost'], d['push'], d['units'])}")

    # ── 5. Cross-book hold analysis ────────────────────────
    section("5. CROSS-BOOK HOLD ANALYSIS")
    hold_rows = [r for r in rows if r["cross_hold"] is not None]
    if hold_rows:
        by_hold = tally(hold_rows, lambda r: hold_label(r["cross_hold"]))
        hold_order = [b[2] for b in HOLD_BUCKETS]
        print_buckets(by_hold, hold_order, lambda k: k)

        # By market x hold
        for mkt in ["h2h", "spreads", "totals"]:
            mkt_hold = [r for r in hold_rows if r["market_key"] == mkt]
            if mkt_hold:
                print(f"\n    {MARKET_SHORT[mkt]}:")
                by_mkt_hold = tally(mkt_hold, lambda r: hold_label(r["cross_hold"]))
                for label in hold_order:
                    if label in by_mkt_hold:
                        d = by_mkt_hold[label]
                        print(f"      {label:28s} {fmt(d['won'], d['lost'], d['push'], d['units'])}")
    else:
        print("    No cross-book hold data available.")

    # ── 5b. Delta analysis (PD signals) ────────────────────
    pd_rows = [r for r in rows if r["signal_type"] == "pinnacle_divergence" and r["delta"] is not None]
    if pd_rows:
        print()
        print("    PD Delta (divergence size):")
        delta_buckets = [
            (0.0, 0.5, "<0.5"),
            (0.5, 1.0, "0.5-0.9"),
            (1.0, 1.5, "1.0-1.4"),
            (1.5, 2.0, "1.5-1.9"),
            (2.0, 999, "2.0+"),
        ]
        by_delta = tally(pd_rows, lambda r: next(
            (label for lo, hi, label in delta_buckets if lo <= abs(r["delta"]) < hi), "?"
        ))
        delta_order = [b[2] for b in delta_buckets]
        print_buckets(by_delta, delta_order, lambda k: f"  delta {k}")

    # ── 6. Individual signal log ───────────────────────────
    section("6. INDIVIDUAL SIGNAL LOG (all MLB signals)")
    for r in rows:
        dt_mst = r["dt"].astimezone(MST)
        ts = dt_mst.strftime("%m/%d %I:%M%p")
        sig = SIGNAL_LABELS.get(r["signal_type"], r["signal_type"])
        mkt = MARKET_SHORT.get(r["market_key"], r["market_key"])
        strength = f"{r['signal_strength']:.0%}"
        price_str = ""
        if r["best_price"]:
            p = r["best_price"]
            price_str = f" ({'+' if p > 0 else ''}{p:.0f})"
        tier_str = ""
        if r["tier"] != "No qualifiers":
            tier_str = f" [{r['tier']}]"
        fp_str = " *FP*" if r["is_free_play"] else ""
        sent_str = "" if r["was_sent"] else " (suppressed)"
        hold_str = ""
        if r["cross_hold"] is not None:
            hold_str = f" hold:{r['cross_hold']:.1f}%"
        result_icon = {"won": "W", "lost": "L", "push": "P"}.get(r["result"], "?")

        print(f"    {result_icon} {ts}  {sig:8s} {mkt:6s} {r['outcome_name']:25s} str={strength:4s}{price_str:10s}{hold_str}{tier_str}{fp_str}{sent_str}")

    # ── 7. By book ─────────────────────────────────────────
    section("7. BY VALUE BOOK")
    by_book = tally(rows, lambda r: r["best_book"])
    print_buckets(by_book, label_fn=lambda k: k)

    # ── 8. By team ─────────────────────────────────────────
    section("8. BY TEAM (outcome bet on)")
    by_team = tally(rows, lambda r: r["outcome_name"])
    # Sort by volume
    team_order = sorted(by_team.keys(), key=lambda k: -(by_team[k]["won"] + by_team[k]["lost"] + by_team[k]["push"]))
    print_buckets(by_team, team_order, lambda k: k)

    # ── 9. Tier analysis ──────────────────────────────────
    section("9. TIER / QUALIFIER ANALYSIS")
    by_tier = tally(rows, lambda r: r["tier"])
    print_buckets(by_tier, TIER_ORDER, lambda k: k)

    # Qualifier tag breakdown
    print()
    print("    Qualifier tags seen:")
    tag_counts = defaultdict(lambda: {"won": 0, "lost": 0, "push": 0, "units": 0.0})
    for r in rows:
        for tag in r["qualifier_tags"]:
            tag_counts[tag][r["result"]] += 1
            tag_counts[tag]["units"] += compute_units(
                r.get("best_price"), r["result"], r.get("multiplier", 1)
            )
    for tag in sorted(tag_counts.keys()):
        d = tag_counts[tag]
        print(f"      {tag:35s} {fmt(d['won'], d['lost'], d['push'], d['units'])}")
    if not tag_counts:
        print("      (none)")

    # ── 10. Daily breakdown ────────────────────────────────
    section("10. DAILY BREAKDOWN")
    by_day = defaultdict(lambda: {"won": 0, "lost": 0, "push": 0, "units": 0.0})
    for r in rows:
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

    # ── 11. Summary ────────────────────────────────────────
    section("11. ACTIONABLE SUMMARY")
    print()
    print(f"  Total: {total} graded MLB signals, {all_w}W-{all_l}L-{all_p}P ({rate(all_w, all_l):.0%})")
    sign = "+" if all_u >= 0 else ""
    print(f"  Units: {sign}{all_u:.1f}u")
    print()

    # Flag worst combos
    print("  Worst type x market combos:")
    combos = []
    for k, d in by_type_mkt.items():
        decided = d["won"] + d["lost"]
        if decided >= 3:
            combos.append((k, d, rate(d["won"], d["lost"]), d["units"]))
    combos.sort(key=lambda x: x[3])
    for k, d, wr, u in combos[:5]:
        label = f"{SIGNAL_LABELS.get(k[0], k[0])} {MARKET_SHORT.get(k[1], k[1])}"
        print(f"    {label:20s} {fmt(d['won'], d['lost'], d['push'], u)}")

    # Flag worst hours
    print()
    print("  Worst hours:")
    hour_list = []
    for h, d in by_hour.items():
        decided = d["won"] + d["lost"]
        if decided >= 2:
            hour_list.append((h, d, rate(d["won"], d["lost"]), d["units"]))
    hour_list.sort(key=lambda x: x[3])
    for h, d, wr, u in hour_list[:5]:
        print(f"    {mst_label(h):28s} {fmt(d['won'], d['lost'], d['push'], u)}")

    print()
    print("Done.")


if __name__ == "__main__":
    run()
