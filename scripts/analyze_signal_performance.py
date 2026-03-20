"""Comprehensive signal performance analysis.

Breaks down ALL signal types across every useful dimension to find
what's working, what to ignore, and where to lean in.

Sections:
  1. Overall by signal type
  2. Signal type × sport
  3. Signal type × market
  4. Signal type × sport × market (combo matrix)
  5. Signal type × strength bucket
  6. Signal type × hour (MST)
  7. Top/bottom combos ranked by hit rate (min sample)
  8. Actionable summary
"""

import json
import sqlite3
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone

DB = "/app/data/sharp_seeker.db"
MST = timezone(timedelta(hours=-7))

STRENGTH_BUCKETS = [
    (0.0, 0.33, "<33%"),
    (0.33, 0.50, "33-49%"),
    (0.50, 0.67, "50-66%"),
    (0.67, 0.80, "67-79%"),
    (0.80, 1.01, "80%+"),
]

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
}

MARKET_SHORT = {
    "h2h": "ML",
    "spreads": "Spread",
    "totals": "Total",
}

MIN_SAMPLE = 10  # minimum signals for a combo to appear in rankings


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


def fmt(wins, losses, pushes):
    n = wins + losses + pushes
    decided = wins + losses
    if decided == 0:
        return f"(n={n:4d})  {wins}W-{losses}L-{pushes}P  (--)"
    rate = wins / decided
    return f"(n={n:4d})  {wins}W-{losses}L-{pushes}P  ({rate:.0%})"


def rate(wins, losses):
    decided = wins + losses
    if decided == 0:
        return 0.0
    return wins / decided


def section(title):
    print()
    print("=" * 70)
    print(f"  {title}")
    print("=" * 70)


def strength_label(s):
    for lo, hi, label in STRENGTH_BUCKETS:
        if lo <= s < hi:
            return label
    return "?"


def mst_label(utc_hour):
    mst_hour = (utc_hour - 7) % 24
    ampm = "AM" if mst_hour < 12 else "PM"
    display = mst_hour % 12 or 12
    return f"{display:2d} {ampm} MST"


def tally(rows, key_fn):
    """Group rows by key_fn and tally W/L/P."""
    buckets = defaultdict(lambda: {"won": 0, "lost": 0, "push": 0})
    for row in rows:
        k = key_fn(row)
        if k is not None:
            buckets[k][row["result"]] += 1
    return buckets


def print_buckets(buckets, key_order=None, label_fn=None, indent=2):
    """Print a tally dict with optional ordering and label formatting."""
    if key_order is None:
        key_order = sorted(buckets.keys())
    pad = " " * indent
    for k in key_order:
        if k not in buckets:
            continue
        d = buckets[k]
        label = label_fn(k) if label_fn else str(k)
        print(f"{pad}{label:42s} {fmt(d['won'], d['lost'], d['push'])}")


def tier_label(row):
    """Return tier label based on qualifier_count in details_json."""
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


def odds_bucket(price):
    """Bucket a moneyline price into ranges."""
    if price is None:
        return None
    if price <= -301:
        return "Heavy fav (<= -301)"
    if price <= -200:
        return "Big fav (-200 to -300)"
    if price <= -110:
        return "Mod fav (-110 to -199)"
    if price <= 100:
        return "Pick'em (-109 to +100)"
    if price <= 200:
        return "Sm dog (+101 to +200)"
    if price <= 400:
        return "Med dog (+201 to +400)"
    return "Long shot (+401+)"


ODDS_ORDER = [
    "Heavy fav (<= -301)",
    "Big fav (-200 to -300)",
    "Mod fav (-110 to -199)",
    "Pick'em (-109 to +100)",
    "Sm dog (+101 to +200)",
    "Med dog (+201 to +400)",
    "Long shot (+401+)",
]


def run():
    conn = connect()

    since = sys.argv[1] if len(sys.argv) > 1 else None
    if since:
        cur = conn.execute("""
            SELECT event_id, sport_key, signal_type, market_key, outcome_name,
                   signal_strength, signal_at, result, details_json
            FROM signal_results
            WHERE result IS NOT NULL AND signal_at >= ?
            ORDER BY signal_at
        """, (since,))
    else:
        cur = conn.execute("""
            SELECT event_id, sport_key, signal_type, market_key, outcome_name,
                   signal_strength, signal_at, result, details_json
            FROM signal_results
            WHERE result IS NOT NULL
            ORDER BY signal_at
        """)
    rows = [dict(r) for r in cur.fetchall()]

    # Also fetch free play event+market+outcome combos for cross-reference
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

    # Parse datetimes and enrich rows
    for row in rows:
        row["dt"] = datetime.fromisoformat(row["signal_at"])
        row["dt_mst"] = row["dt"].astimezone(MST)
        row["tier"] = tier_label(row)
        row["is_free_play"] = (
            row["event_id"], row["market_key"], row["outcome_name"]
        ) in free_play_keys
        # Parse best price for odds analysis
        details_raw = row.get("details_json")
        row["best_price"] = None
        if details_raw:
            try:
                details = json.loads(details_raw) if isinstance(details_raw, str) else details_raw
                vb = details.get("value_books", [])
                if vb:
                    row["best_price"] = vb[0].get("price")
            except (json.JSONDecodeError, TypeError):
                pass

    total = len(rows)
    first = rows[0]["dt_mst"]
    last = rows[-1]["dt_mst"]
    print(f"Analyzing {total} graded signals (all types)")
    print(f"Range: {first.strftime('%m/%d %I:%M %p')} — {last.strftime('%m/%d %I:%M %p')} MST")

    # ── 1. Overall by signal type ───────────────────────────
    section("1. OVERALL BY SIGNAL TYPE")
    by_type = tally(rows, lambda r: r["signal_type"])
    sig_types = sorted(by_type.keys())
    print_buckets(by_type, sig_types, lambda k: SIGNAL_LABELS.get(k, k))

    # ── 2. Signal type × sport ──────────────────────────────
    section("2. SIGNAL TYPE x SPORT")
    by_type_sport = tally(rows, lambda r: (r["signal_type"], r["sport_key"]))
    keys = sorted(by_type_sport.keys())
    print_buckets(
        by_type_sport, keys,
        lambda k: f"{SIGNAL_LABELS.get(k[0], k[0]):10s} {SPORT_SHORT.get(k[1], k[1])}"
    )

    # ── 3. Signal type × market ─────────────────────────────
    section("3. SIGNAL TYPE x MARKET")
    by_type_market = tally(rows, lambda r: (r["signal_type"], r["market_key"]))
    keys = sorted(by_type_market.keys())
    print_buckets(
        by_type_market, keys,
        lambda k: f"{SIGNAL_LABELS.get(k[0], k[0]):10s} {MARKET_SHORT.get(k[1], k[1])}"
    )

    # ── 4. Signal type × sport × market ─────────────────────
    section("4. SIGNAL TYPE x SPORT x MARKET (full combo)")
    by_combo = tally(rows, lambda r: (r["signal_type"], r["sport_key"], r["market_key"]))
    keys = sorted(by_combo.keys())
    print_buckets(
        by_combo, keys,
        lambda k: f"{SIGNAL_LABELS.get(k[0], k[0]):10s} {SPORT_SHORT.get(k[1], k[1]):6s} {MARKET_SHORT.get(k[2], k[2])}"
    )

    # ── 5. Signal type × strength bucket ────────────────────
    section("5. SIGNAL TYPE x STRENGTH")
    by_type_str = tally(
        rows,
        lambda r: (r["signal_type"], strength_label(r["signal_strength"]))
    )
    for st in sig_types:
        print(f"\n  {SIGNAL_LABELS.get(st, st)}:")
        for _, _, slabel in STRENGTH_BUCKETS:
            k = (st, slabel)
            if k in by_type_str:
                d = by_type_str[k]
                print(f"    {slabel:10s}  {fmt(d['won'], d['lost'], d['push'])}")

    # ── 6. Signal type × hour (MST) ────────────────────────
    section("6. SIGNAL TYPE x HOUR (MST)")
    by_type_hour = tally(
        rows,
        lambda r: (r["signal_type"], r["dt"].hour)
    )
    for st in sig_types:
        print(f"\n  {SIGNAL_LABELS.get(st, st)}:")
        for utc_h in range(24):
            k = (st, utc_h)
            if k in by_type_hour:
                d = by_type_hour[k]
                n = d["won"] + d["lost"] + d["push"]
                decided = d["won"] + d["lost"]
                pct = f"{d['won']/decided:.0%}" if decided else "--"
                print(f"    {mst_label(utc_h):10s}  (n={n:3d})  {d['won']}W-{d['lost']}L  ({pct})")

    # ── 7. Ranked combos (min N sample) ─────────────────────
    section(f"7. BEST & WORST COMBOS (n >= {MIN_SAMPLE})")

    # Build combo list: signal_type × sport × market
    combos = []
    for k, d in by_combo.items():
        decided = d["won"] + d["lost"]
        if decided >= MIN_SAMPLE:
            combos.append({
                "key": k,
                "label": f"{SIGNAL_LABELS.get(k[0], k[0]):10s} {SPORT_SHORT.get(k[1], k[1]):6s} {MARKET_SHORT.get(k[2], k[2])}",
                "won": d["won"],
                "lost": d["lost"],
                "push": d["push"],
                "n": decided + d["push"],
                "rate": rate(d["won"], d["lost"]),
            })

    if combos:
        combos.sort(key=lambda c: c["rate"], reverse=True)
        print("\n  BEST:")
        for c in combos[:8]:
            print(f"    {c['label']:35s} {fmt(c['won'], c['lost'], c['push'])}  {'***' if c['rate'] >= 0.55 else ''}")

        print("\n  WORST:")
        for c in combos[-5:]:
            print(f"    {c['label']:35s} {fmt(c['won'], c['lost'], c['push'])}  {'!!!' if c['rate'] < 0.45 else ''}")
    else:
        print(f"  No combos with n >= {MIN_SAMPLE}")

    # Also rank signal_type × strength
    section(f"7b. BEST & WORST: SIGNAL TYPE x STRENGTH (n >= {MIN_SAMPLE})")
    str_combos = []
    for k, d in by_type_str.items():
        decided = d["won"] + d["lost"]
        if decided >= MIN_SAMPLE:
            str_combos.append({
                "label": f"{SIGNAL_LABELS.get(k[0], k[0]):10s} {k[1]}",
                "won": d["won"],
                "lost": d["lost"],
                "push": d["push"],
                "n": decided + d["push"],
                "rate": rate(d["won"], d["lost"]),
            })

    if str_combos:
        str_combos.sort(key=lambda c: c["rate"], reverse=True)
        print("\n  BEST:")
        for c in str_combos[:6]:
            print(f"    {c['label']:30s} {fmt(c['won'], c['lost'], c['push'])}  {'***' if c['rate'] >= 0.55 else ''}")
        print("\n  WORST:")
        for c in str_combos[-4:]:
            print(f"    {c['label']:30s} {fmt(c['won'], c['lost'], c['push'])}  {'!!!' if c['rate'] < 0.45 else ''}")
    else:
        print(f"  No combos with n >= {MIN_SAMPLE}")

    # Also rank signal_type × hour
    section(f"7c. BEST & WORST: SIGNAL TYPE x HOUR (n >= {MIN_SAMPLE})")
    hour_combos = []
    for k, d in by_type_hour.items():
        decided = d["won"] + d["lost"]
        if decided >= MIN_SAMPLE:
            hour_combos.append({
                "label": f"{SIGNAL_LABELS.get(k[0], k[0]):10s} {mst_label(k[1])}",
                "won": d["won"],
                "lost": d["lost"],
                "push": d["push"],
                "n": decided + d["push"],
                "rate": rate(d["won"], d["lost"]),
            })

    if hour_combos:
        hour_combos.sort(key=lambda c: c["rate"], reverse=True)
        print("\n  BEST:")
        for c in hour_combos[:8]:
            print(f"    {c['label']:30s} {fmt(c['won'], c['lost'], c['push'])}  {'***' if c['rate'] >= 0.55 else ''}")
        print("\n  WORST:")
        for c in hour_combos[-5:]:
            print(f"    {c['label']:30s} {fmt(c['won'], c['lost'], c['push'])}  {'!!!' if c['rate'] < 0.45 else ''}")
    else:
        print(f"  No combos with n >= {MIN_SAMPLE}")

    # ── 8. Performance by qualifier tier ──────────────────────
    section("8. PERFORMANCE BY TIER")
    by_tier = tally(rows, lambda r: r["tier"])
    print_buckets(by_tier, TIER_ORDER, lambda k: k)

    # Tier × sport
    print("\n  By Tier x Sport:")
    by_tier_sport = tally(rows, lambda r: (r["tier"], r["sport_key"]))
    keys = [(t, s) for t in TIER_ORDER for s in sorted(SPORT_SHORT.keys()) if (t, s) in by_tier_sport]
    print_buckets(
        by_tier_sport, keys,
        lambda k: f"  {k[0]:18s} {SPORT_SHORT.get(k[1], k[1])}", indent=2,
    )

    # Tier × market
    print("\n  By Tier x Market:")
    by_tier_market = tally(rows, lambda r: (r["tier"], r["market_key"]))
    keys = [(t, m) for t in TIER_ORDER for m in sorted(MARKET_SHORT.keys()) if (t, m) in by_tier_market]
    print_buckets(
        by_tier_market, keys,
        lambda k: f"  {k[0]:18s} {MARKET_SHORT.get(k[1], k[1])}", indent=2,
    )

    # ── 9. Free play vs non-free-play ─────────────────────────
    section("9. FREE PLAY vs NON-FREE-PLAY")
    by_fp = tally(rows, lambda r: "Free Play" if r["is_free_play"] else "Regular")
    print_buckets(by_fp, ["Free Play", "Regular"], lambda k: k)

    # Free play by sport
    fp_rows = [r for r in rows if r["is_free_play"]]
    if fp_rows:
        print("\n  Free Plays by Sport:")
        by_fp_sport = tally(fp_rows, lambda r: r["sport_key"])
        print_buckets(by_fp_sport, label_fn=lambda k: f"  {SPORT_SHORT.get(k, k)}")

        print("\n  Free Plays by Market:")
        by_fp_market = tally(fp_rows, lambda r: r["market_key"])
        print_buckets(by_fp_market, label_fn=lambda k: f"  {MARKET_SHORT.get(k, k)}")

    # ── 10. Moneyline odds range analysis ─────────────────────
    section("10. MONEYLINE ODDS RANGE ANALYSIS")
    ml_rows = [r for r in rows if r["market_key"] == "h2h" and r["best_price"] is not None]
    if ml_rows:
        by_odds = tally(ml_rows, lambda r: odds_bucket(r["best_price"]))
        print("  All signals (h2h only):")
        print_buckets(by_odds, ODDS_ORDER, lambda k: f"  {k}")

        ml_fp = [r for r in ml_rows if r["is_free_play"]]
        if ml_fp:
            print("\n  Free plays (h2h only):")
            by_odds_fp = tally(ml_fp, lambda r: odds_bucket(r["best_price"]))
            print_buckets(by_odds_fp, ODDS_ORDER, lambda k: f"  {k}")
    else:
        print("  No moneyline signals with price data.")

    # ── 11. Recent trend (last 7 days vs prior) ──────────────
    section("11. RECENT TREND (last 7d vs prior)")
    now_utc = datetime.now(timezone.utc)
    recent_cutoff = now_utc - timedelta(days=7)
    recent = [r for r in rows if r["dt"].replace(tzinfo=timezone.utc) if r["dt"] >= recent_cutoff]
    prior = [r for r in rows if r["dt"] < recent_cutoff]
    if recent and prior:
        rw = sum(1 for r in recent if r["result"] == "won")
        rl = sum(1 for r in recent if r["result"] == "lost")
        rp = sum(1 for r in recent if r["result"] == "push")
        pw = sum(1 for r in prior if r["result"] == "won")
        pl = sum(1 for r in prior if r["result"] == "lost")
        pp = sum(1 for r in prior if r["result"] == "push")
        print(f"  Last 7 days:  {fmt(rw, rl, rp)}")
        print(f"  Prior:        {fmt(pw, pl, pp)}")

        # Recent by sport
        print("\n  Last 7d by Sport:")
        by_recent_sport = tally(recent, lambda r: r["sport_key"])
        print_buckets(by_recent_sport, label_fn=lambda k: f"  {SPORT_SHORT.get(k, k)}")

        # Recent free plays
        recent_fp = [r for r in recent if r["is_free_play"]]
        if recent_fp:
            rfw = sum(1 for r in recent_fp if r["result"] == "won")
            rfl = sum(1 for r in recent_fp if r["result"] == "lost")
            rfp = sum(1 for r in recent_fp if r["result"] == "push")
            print(f"\n  Last 7d Free Plays: {fmt(rfw, rfl, rfp)}")
    else:
        print("  Not enough data for trend comparison.")

    # ── 12. Summary ──────────────────────────────────────────
    section("12. ACTIONABLE SUMMARY")
    print()
    print("  Signal types ranked by hit rate:")
    type_ranked = []
    for st in sig_types:
        d = by_type[st]
        decided = d["won"] + d["lost"]
        if decided > 0:
            type_ranked.append((st, rate(d["won"], d["lost"]), decided))
    type_ranked.sort(key=lambda x: x[1], reverse=True)
    for st, r, n in type_ranked:
        flag = " <-- strong" if r >= 0.55 else " <-- weak" if r < 0.47 else ""
        print(f"    {SIGNAL_LABELS.get(st, st):12s} {r:.0%}  (n={n}){flag}")

    print()
    print(f"  Total graded: {total}")
    all_won = sum(1 for r in rows if r["result"] == "won")
    all_lost = sum(1 for r in rows if r["result"] == "lost")
    print(f"  Overall: {all_won}W-{all_lost}L ({rate(all_won, all_lost):.0%})")
    print()
    print("Done.")


if __name__ == "__main__":
    run()
