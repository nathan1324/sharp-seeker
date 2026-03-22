"""Identify best combos and hours per signal type from a specific period.

Analyzes signal performance by type:sport:market combo and type:hour(MST)
to recommend optimal SIGNAL_BEST_COMBOS and SIGNAL_BEST_HOURS config.

Usage:
    docker compose exec sharp-seeker python /app/scripts/analyze_best_combos_hours.py
    docker compose exec sharp-seeker python /app/scripts/analyze_best_combos_hours.py 2026-03-03 2026-03-15
"""

import json
import os
import sqlite3
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

DB = os.getenv("DB_PATH", "/app/data/sharp_seeker.db")
MST = ZoneInfo("America/Phoenix")

MIN_SAMPLE = 5  # minimum decided signals for a combo/hour to be considered

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

MARKET_SHORT = {"h2h": "ML", "spreads": "Spread", "totals": "Total"}


def compute_units(price, result):
    if result == "push" or price is None:
        return 0.0
    if price < 0:
        risk = abs(price) / 100.0
    else:
        risk = 100.0 / price if price > 0 else 1.0
    if result == "won":
        return 1.0
    elif result == "lost":
        return -risk
    return 0.0


def get_price(row):
    details_raw = row.get("details_json")
    if not details_raw:
        return None
    try:
        details = json.loads(details_raw) if isinstance(details_raw, str) else details_raw
        vb = details.get("value_books", [])
        return vb[0].get("price") if vb else None
    except (json.JSONDecodeError, TypeError):
        return None


def fmt(w, l, p, u):
    n = w + l + p
    decided = w + l
    if decided == 0:
        return "(n={n:4d})  --".format(n=n)
    rate = w / decided
    sign = "+" if u >= 0 else ""
    return "(n={n:4d})  {w}W-{l}L-{p}P  ({rate:.0%})  {sign}{u:.1f}u".format(
        n=n, w=w, l=l, p=p, rate=rate, sign=sign, u=u,
    )


def tally(rows):
    w = sum(1 for r in rows if r["result"] == "won")
    l = sum(1 for r in rows if r["result"] == "lost")
    p = sum(1 for r in rows if r["result"] == "push")
    u = sum(compute_units(get_price(r), r["result"]) for r in rows)
    return w, l, p, u


def rate(w, l):
    return w / (w + l) if (w + l) > 0 else 0


def section(title):
    print()
    print("=" * 70)
    print("  {t}".format(t=title))
    print("=" * 70)


def main():
    since = sys.argv[1] if len(sys.argv) > 1 else None
    until = sys.argv[2] if len(sys.argv) > 2 else None

    for attempt in range(10):
        try:
            conn = sqlite3.connect(DB, timeout=10)
            conn.row_factory = sqlite3.Row
            break
        except sqlite3.OperationalError:
            print("  DB locked, retrying ({a}/10)...".format(a=attempt + 1))
            time.sleep(3)
    else:
        raise SystemExit("ERROR: Could not acquire DB lock.")

    where = "WHERE result IS NOT NULL"
    params = []
    if since:
        where += " AND signal_at >= ?"
        params.append(since)
    if until:
        where += " AND signal_at < ?"
        params.append(until)

    rows = [dict(r) for r in conn.execute(
        "SELECT * FROM signal_results {w} ORDER BY signal_at".format(w=where),
        tuple(params),
    ).fetchall()]
    conn.close()

    if not rows:
        print("No graded signals found.")
        return

    # Parse MST hour
    for row in rows:
        dt = datetime.fromisoformat(row["signal_at"])
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        row["mst_hour"] = dt.astimezone(MST).hour

    label = ""
    if since:
        label += " since {s}".format(s=since)
    if until:
        label += " until {u}".format(u=until)
    print("Analyzing {n} graded signals{l}".format(n=len(rows), l=label))

    types = sorted(set(r["signal_type"] for r in rows))

    # ── 1. Per signal type: combo performance ────────────────
    section("1. COMBOS BY SIGNAL TYPE (type:sport:market)")
    recommended_combos = []

    for st in types:
        st_rows = [r for r in rows if r["signal_type"] == st]
        label = SIGNAL_LABELS.get(st, st)
        print("\n  {l} ({n} signals):".format(l=label, n=len(st_rows)))

        combos = defaultdict(list)
        for r in st_rows:
            key = "{st}:{sp}:{mk}".format(st=st, sp=r["sport_key"], mk=r["market_key"])
            combos[key].append(r)

        ranked = []
        for key, c_rows in sorted(combos.items()):
            w, l, p, u = tally(c_rows)
            decided = w + l
            sp = key.split(":")[1]
            mk = key.split(":")[2]
            sp_short = SPORT_SHORT.get(sp, sp)
            mk_short = MARKET_SHORT.get(mk, mk)
            short_key = "{sp:6s} {mk}".format(sp=sp_short, mk=mk_short)
            r = rate(w, l)
            ranked.append((key, short_key, w, l, p, u, decided, r))

        # Sort by units
        ranked.sort(key=lambda x: x[5], reverse=True)
        for key, short_key, w, l, p, u, decided, r in ranked:
            flag = ""
            if decided >= MIN_SAMPLE and r >= 0.54 and u > 0:
                flag = " <-- RECOMMEND"
                recommended_combos.append(key)
            elif decided >= MIN_SAMPLE and r < 0.45:
                flag = " <-- AVOID"
            print("    {sk:20s} {f}{flag}".format(
                sk=short_key, f=fmt(w, l, p, u), flag=flag,
            ))

    # ── 2. Per signal type: hour performance ─────────────────
    section("2. HOURS BY SIGNAL TYPE (MST)")
    recommended_hours = defaultdict(list)

    for st in types:
        st_rows = [r for r in rows if r["signal_type"] == st]
        label = SIGNAL_LABELS.get(st, st)
        print("\n  {l}:".format(l=label))

        hours = defaultdict(list)
        for r in st_rows:
            hours[r["mst_hour"]].append(r)

        ranked = []
        for h, h_rows in sorted(hours.items()):
            w, l, p, u = tally(h_rows)
            decided = w + l
            r = rate(w, l)
            ranked.append((h, w, l, p, u, decided, r))

        for h, w, l, p, u, decided, r in ranked:
            ampm = "AM" if h < 12 else "PM"
            display = h % 12 or 12
            flag = ""
            if decided >= MIN_SAMPLE and r >= 0.54 and u > 0:
                flag = " <-- RECOMMEND"
                recommended_hours[st].append(h)
            elif decided >= MIN_SAMPLE and r < 0.45:
                flag = " <-- AVOID"
            print("    {d:2d} {ap} MST  {f}{flag}".format(
                d=display, ap=ampm, f=fmt(w, l, p, u), flag=flag,
            ))

    # ── 3. Cross-book hold vs performance ────────────────────
    section("3. CROSS-BOOK HOLD vs PERFORMANCE")

    hold_buckets = [
        (None, 0.0, "Negative (Efficient)"),
        (0.0, 0.02, "0-2% (Tight)"),
        (0.02, 0.03, "2-3% (Edge)"),
        (0.03, 1.0, "3%+ (Wide Edge)"),
    ]

    # Parse cross_book_hold from details
    for r in rows:
        r["_cbh"] = None
        details_raw = r.get("details_json")
        if details_raw:
            try:
                details = json.loads(details_raw) if isinstance(details_raw, str) else details_raw
                r["_cbh"] = details.get("cross_book_hold")
            except (json.JSONDecodeError, TypeError):
                pass

    has_hold = [r for r in rows if r["_cbh"] is not None]
    print("  Signals with cross-book hold data: {n}/{t}".format(n=len(has_hold), t=len(rows)))

    print("\n  All signal types:")
    for lo, hi, label in hold_buckets:
        if lo is None:
            bucket_rows = [r for r in has_hold if r["_cbh"] < 0]
        else:
            bucket_rows = [r for r in has_hold if lo <= r["_cbh"] < hi]
        if not bucket_rows:
            continue
        w, l, p, u = tally(bucket_rows)
        print("    {label:25s} {f}".format(label=label, f=fmt(w, l, p, u)))

    # By signal type
    for st in types:
        st_hold = [r for r in has_hold if r["signal_type"] == st]
        if not st_hold:
            continue
        sl = SIGNAL_LABELS.get(st, st)
        print("\n  {l}:".format(l=sl))
        for lo, hi, label in hold_buckets:
            if lo is None:
                bucket_rows = [r for r in st_hold if r["_cbh"] < 0]
            else:
                bucket_rows = [r for r in st_hold if lo <= r["_cbh"] < hi]
            if not bucket_rows:
                continue
            w, l, p, u = tally(bucket_rows)
            print("    {label:25s} {f}".format(label=label, f=fmt(w, l, p, u)))

    # ── 4. Dispersion vs performance ─────────────────────────
    section("4. PRICE DISPERSION vs PERFORMANCE")

    # Parse dispersion from details
    for r in rows:
        r["_disp"] = None
        details_raw = r.get("details_json")
        if details_raw:
            try:
                details = json.loads(details_raw) if isinstance(details_raw, str) else details_raw
                r["_disp"] = details.get("dispersion")
            except (json.JSONDecodeError, TypeError):
                pass

    has_disp = [r for r in rows if r["_disp"] is not None]
    print("  Signals with dispersion data: {n}/{t}".format(n=len(has_disp), t=len(rows)))

    if has_disp:
        # Split into point-based (spreads/totals) and ML separately
        point_rows = [r for r in has_disp if r["market_key"] in ("spreads", "totals")]
        ml_rows = [r for r in has_disp if r["market_key"] == "h2h"]

        if point_rows:
            point_buckets = [
                (0.0, 0.001, "0 (No dispersion)"),
                (0.001, 0.5, "0-0.5 pts"),
                (0.5, 1.0, "0.5-1 pts"),
                (1.0, 2.0, "1-2 pts"),
                (2.0, 100.0, "2+ pts"),
            ]
            print("\n  Spreads/Totals (point dispersion):")
            for lo, hi, label in point_buckets:
                bucket_rows = [r for r in point_rows if lo <= r["_disp"] < hi]
                if not bucket_rows:
                    continue
                w, l, p, u = tally(bucket_rows)
                print("    {label:25s} {f}".format(label=label, f=fmt(w, l, p, u)))

        if ml_rows:
            ml_buckets = [
                (0.0, 0.001, "0 (No dispersion)"),
                (0.001, 0.02, "0-2%"),
                (0.02, 0.05, "2-5%"),
                (0.05, 0.10, "5-10%"),
                (0.10, 1.0, "10%+"),
            ]
            print("\n  Moneyline (implied prob dispersion):")
            for lo, hi, label in ml_buckets:
                bucket_rows = [r for r in ml_rows if lo <= r["_disp"] < hi]
                if not bucket_rows:
                    continue
                w, l, p, u = tally(bucket_rows)
                print("    {label:25s} {f}".format(label=label, f=fmt(w, l, p, u)))

        # Dispersion by signal type (spreads/totals only since that's most data)
        if point_rows:
            print("\n  Spreads/Totals dispersion by signal type:")
            for st in types:
                st_disp = [r for r in point_rows if r["signal_type"] == st]
                if not st_disp:
                    continue
                sl = SIGNAL_LABELS.get(st, st)
                print("\n    {l}:".format(l=sl))
                for lo, hi, label in point_buckets:
                    bucket_rows = [r for r in st_disp if lo <= r["_disp"] < hi]
                    if not bucket_rows:
                        continue
                    w, l, p, u = tally(bucket_rows)
                    print("      {label:25s} {f}".format(label=label, f=fmt(w, l, p, u)))
    else:
        print("  (no dispersion data — this is a newer metric)")

    # ── 5. Recommended config ────────────────────────────────
    section("5. RECOMMENDED CONFIG")
    print()
    print("  SIGNAL_BEST_COMBOS:")
    if recommended_combos:
        combo_json = json.dumps(sorted(recommended_combos))
        print("    {c}".format(c=combo_json))
    else:
        print("    (no combos meet criteria)")

    print()
    print("  SIGNAL_BEST_HOURS:")
    if recommended_hours:
        hours_json = json.dumps(
            {k: sorted(v) for k, v in sorted(recommended_hours.items())}
        )
        print("    {h}".format(h=hours_json))
    else:
        print("    (no hours meet criteria)")

    # ── 6. What Elite would look like with these settings ────
    section("6. SIMULATED ELITE WITH RECOMMENDED CONFIG")
    rec_combos_set = set(recommended_combos)
    rec_hours_dict = {k: set(v) for k, v in recommended_hours.items()}

    elite = []
    one_q = []
    zero_q = []
    for r in rows:
        combo_key = "{st}:{sp}:{mk}".format(
            st=r["signal_type"], sp=r["sport_key"], mk=r["market_key"],
        )
        is_combo = combo_key in rec_combos_set
        is_hour = r["mst_hour"] in rec_hours_dict.get(r["signal_type"], set())
        q = int(is_combo) + int(is_hour)
        if q >= 2:
            elite.append(r)
        elif q == 1:
            one_q.append(r)
        else:
            zero_q.append(r)

    w, l, p, u = tally(elite)
    print("  Elite (2q):     {f}".format(f=fmt(w, l, p, u)))
    w, l, p, u = tally(one_q)
    print("  1 qualifier:    {f}".format(f=fmt(w, l, p, u)))
    w, l, p, u = tally(zero_q)
    print("  0 qualifiers:   {f}".format(f=fmt(w, l, p, u)))

    if elite:
        print("\n  Elite by signal type:")
        for st in types:
            st_elite = [r for r in elite if r["signal_type"] == st]
            if not st_elite:
                continue
            w, l, p, u = tally(st_elite)
            label = SIGNAL_LABELS.get(st, st)
            print("    {l:12s} {f}".format(l=label, f=fmt(w, l, p, u)))

    print()
    print("Done.")


if __name__ == "__main__":
    main()
