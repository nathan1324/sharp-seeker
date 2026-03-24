"""Deep diagnostic of Pinnacle Divergence signals.

Strips all assumptions and analyzes what distinguishes winning PD
signals from losing ones across every available dimension.

Usage:
    docker compose exec sharp-seeker python /app/scripts/diagnose_pd.py
    docker compose exec sharp-seeker python /app/scripts/diagnose_pd.py 2026-03-20
    docker compose exec sharp-seeker python /app/scripts/diagnose_pd.py 2026-03-03 2026-03-15
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
    return 1.0 if result == "won" else -risk if result == "lost" else 0.0


def get_detail(row, key, default=None):
    details_raw = row.get("details_json")
    if not details_raw:
        return default
    try:
        details = json.loads(details_raw) if isinstance(details_raw, str) else details_raw
        return details.get(key, default)
    except (json.JSONDecodeError, TypeError):
        return default


def get_price(row):
    vb = get_detail(row, "value_books", [])
    return vb[0].get("price") if vb else None


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


def section(title):
    print()
    print("=" * 70)
    print("  {t}".format(t=title))
    print("=" * 70)


def bucket_rows(rows, key_fn, buckets):
    """Group rows into named buckets based on a numeric key."""
    result = {}
    for lo, hi, label in buckets:
        if lo is None:
            b = [r for r in rows if key_fn(r) is not None and key_fn(r) < hi]
        elif hi is None:
            b = [r for r in rows if key_fn(r) is not None and key_fn(r) >= lo]
        else:
            b = [r for r in rows if key_fn(r) is not None and lo <= key_fn(r) < hi]
        if b:
            result[label] = b
    return result


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

    where = "WHERE result IS NOT NULL AND signal_type = 'pinnacle_divergence'"
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
        print("No PD signals found.")
        return

    # Enrich
    for r in rows:
        dt = datetime.fromisoformat(r["signal_at"])
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        r["mst_hour"] = dt.astimezone(MST).hour
        r["_cbh"] = get_detail(r, "cross_book_hold")
        r["_us_hold"] = get_detail(r, "us_hold")
        r["_pin_hold"] = get_detail(r, "pinnacle_hold")
        r["_delta"] = get_detail(r, "delta")
        r["_disp"] = get_detail(r, "dispersion")
        r["_us_book"] = get_detail(r, "us_book", "?")
        r["_strength"] = r["signal_strength"]

        # Time to game: how far before commence_time was the signal?
        commence = r.get("commence_time") or ""
        if commence:
            try:
                ct = datetime.fromisoformat(commence)
                if ct.tzinfo is None:
                    ct = ct.replace(tzinfo=timezone.utc)
                r["_hours_before"] = (ct - dt).total_seconds() / 3600
            except (ValueError, TypeError):
                r["_hours_before"] = None
        else:
            r["_hours_before"] = None

    label = ""
    if since:
        label += " since {s}".format(s=since)
    if until:
        label += " until {u}".format(u=until)
    print("PD Diagnostic: {n} signals{l}".format(n=len(rows), l=label))

    wins = [r for r in rows if r["result"] == "won"]
    losses = [r for r in rows if r["result"] == "lost"]

    w, l, p, u = tally(rows)
    print("Overall: {f}".format(f=fmt(w, l, p, u)))

    # ── 1. By sport x market ─────────────────────────────────
    section("1. BY SPORT x MARKET")
    combos = defaultdict(list)
    for r in rows:
        key = "{sp:6s} {mk}".format(
            sp=SPORT_SHORT.get(r["sport_key"], r["sport_key"]),
            mk=MARKET_SHORT.get(r["market_key"], r["market_key"]),
        )
        combos[key].append(r)

    for key in sorted(combos, key=lambda k: tally(combos[k])[3], reverse=True):
        w, l, p, u = tally(combos[key])
        print("  {k:20s} {f}".format(k=key, f=fmt(w, l, p, u)))

    # ── 2. By value book ─────────────────────────────────────
    section("2. BY VALUE BOOK")
    by_book = defaultdict(list)
    for r in rows:
        by_book[r["_us_book"]].append(r)
    for bk in sorted(by_book, key=lambda k: tally(by_book[k])[3], reverse=True):
        w, l, p, u = tally(by_book[bk])
        print("  {bk:20s} {f}".format(bk=bk, f=fmt(w, l, p, u)))

    # ── 3. By outcome direction ──────────────────────────────
    section("3. BY OUTCOME (Over/Under/Team)")
    by_outcome = defaultdict(list)
    for r in rows:
        if r["market_key"] == "totals":
            by_outcome[r["outcome_name"]].append(r)
        else:
            by_outcome["Team (spreads/ML)"].append(r)
    for oc in sorted(by_outcome):
        w, l, p, u = tally(by_outcome[oc])
        print("  {o:20s} {f}".format(o=oc, f=fmt(w, l, p, u)))

    # ── 4. By delta size ─────────────────────────────────────
    section("4. BY DELTA SIZE (divergence from Pinnacle)")
    delta_buckets = [
        (0, 0.5, "< 0.5"),
        (0.5, 1.0, "0.5 - 1.0"),
        (1.0, 1.5, "1.0 - 1.5"),
        (1.5, 2.0, "1.5 - 2.0"),
        (2.0, None, "2.0+"),
    ]
    # Separate ML (prob delta) from point-based
    point_rows = [r for r in rows if r["market_key"] != "h2h" and r["_delta"] is not None]
    ml_rows = [r for r in rows if r["market_key"] == "h2h" and r["_delta"] is not None]

    if point_rows:
        print("\n  Spreads/Totals (point delta):")
        grouped = bucket_rows(point_rows, lambda r: r["_delta"], delta_buckets)
        for label, b_rows in grouped.items():
            w, l, p, u = tally(b_rows)
            print("    {lb:20s} {f}".format(lb=label, f=fmt(w, l, p, u)))

    if ml_rows:
        ml_delta_buckets = [
            (0, 0.02, "< 2%"),
            (0.02, 0.04, "2-4%"),
            (0.04, 0.06, "4-6%"),
            (0.06, None, "6%+"),
        ]
        print("\n  Moneyline (implied prob delta):")
        grouped = bucket_rows(ml_rows, lambda r: r["_delta"], ml_delta_buckets)
        for label, b_rows in grouped.items():
            w, l, p, u = tally(b_rows)
            print("    {lb:20s} {f}".format(lb=label, f=fmt(w, l, p, u)))

    # ── 5. By cross-book hold ────────────────────────────────
    section("5. BY CROSS-BOOK HOLD")
    hold_buckets = [
        (None, 0, "Negative (Efficient)"),
        (0, 0.02, "0-2% (Tight)"),
        (0.02, 0.03, "2-3% (Edge)"),
        (0.03, None, "3%+ (Wide Edge)"),
    ]
    has_cbh = [r for r in rows if r["_cbh"] is not None]
    if has_cbh:
        grouped = bucket_rows(has_cbh, lambda r: r["_cbh"], hold_buckets)
        for label, b_rows in grouped.items():
            w, l, p, u = tally(b_rows)
            print("  {lb:25s} {f}".format(lb=label, f=fmt(w, l, p, u)))

        # By sport
        for sp_key in sorted(SPORT_SHORT):
            sp_rows = [r for r in has_cbh if r["sport_key"] == sp_key]
            if not sp_rows:
                continue
            print("\n  {sp}:".format(sp=SPORT_SHORT[sp_key]))
            grouped = bucket_rows(sp_rows, lambda r: r["_cbh"], hold_buckets)
            for label, b_rows in grouped.items():
                w, l, p, u = tally(b_rows)
                print("    {lb:25s} {f}".format(lb=label, f=fmt(w, l, p, u)))

    # ── 6. By US book hold ───────────────────────────────────
    section("6. BY US BOOK HOLD (single book vig)")
    us_hold_buckets = [
        (0, 0.035, "< 3.5% (Sharp)"),
        (0.035, 0.045, "3.5-4.5%"),
        (0.045, 0.05, "4.5-5.0%"),
        (0.05, None, "5%+ (Wide)"),
    ]
    has_ush = [r for r in rows if r["_us_hold"] is not None]
    if has_ush:
        grouped = bucket_rows(has_ush, lambda r: r["_us_hold"], us_hold_buckets)
        for label, b_rows in grouped.items():
            w, l, p, u = tally(b_rows)
            print("  {lb:25s} {f}".format(lb=label, f=fmt(w, l, p, u)))

    # ── 7. By strength ───────────────────────────────────────
    section("7. BY STRENGTH")
    str_buckets = [
        (0, 0.34, "< 34%"),
        (0.34, 0.50, "34-49%"),
        (0.50, 0.67, "50-66%"),
        (0.67, 0.80, "67-79%"),
    ]
    grouped = bucket_rows(rows, lambda r: r["_strength"], str_buckets)
    for label, b_rows in grouped.items():
        w, l, p, u = tally(b_rows)
        print("  {lb:20s} {f}".format(lb=label, f=fmt(w, l, p, u)))

    # ── 8. By hours before game ──────────────────────────────
    section("8. BY HOURS BEFORE GAME START")
    time_buckets = [
        (0, 2, "< 2 hours"),
        (2, 6, "2-6 hours"),
        (6, 12, "6-12 hours"),
        (12, 24, "12-24 hours"),
        (24, None, "24+ hours"),
    ]
    has_time = [r for r in rows if r["_hours_before"] is not None and r["_hours_before"] > 0]
    if has_time:
        grouped = bucket_rows(has_time, lambda r: r["_hours_before"], time_buckets)
        for label, b_rows in grouped.items():
            w, l, p, u = tally(b_rows)
            print("  {lb:20s} {f}".format(lb=label, f=fmt(w, l, p, u)))

    # ── 9. By MST hour ───────────────────────────────────────
    section("9. BY MST HOUR")
    by_hour = defaultdict(list)
    for r in rows:
        by_hour[r["mst_hour"]].append(r)
    for h in sorted(by_hour):
        w, l, p, u = tally(by_hour[h])
        ampm = "AM" if h < 12 else "PM"
        display = h % 12 or 12
        print("  {d:2d} {ap} MST  {f}".format(d=display, ap=ampm, f=fmt(w, l, p, u)))

    # ── 10. Dispersion ────────────────────────────────────────
    section("10. BY DISPERSION")
    has_disp = [r for r in rows if r["_disp"] is not None]
    if has_disp:
        disp_buckets = [
            (0, 0.001, "Zero"),
            (0.001, 0.5, "0-0.5"),
            (0.5, 1.0, "0.5-1.0"),
            (1.0, 2.0, "1.0-2.0"),
            (2.0, None, "2.0+"),
        ]
        grouped = bucket_rows(has_disp, lambda r: r["_disp"], disp_buckets)
        for label, b_rows in grouped.items():
            w, l, p, u = tally(b_rows)
            print("  {lb:20s} {f}".format(lb=label, f=fmt(w, l, p, u)))
    else:
        print("  (no dispersion data)")

    # ── 11. Winning vs Losing profile ─────────────────────────
    section("11. WINNING vs LOSING SIGNAL PROFILE")

    def avg(vals):
        clean = [v for v in vals if v is not None]
        return sum(clean) / len(clean) if clean else None

    def fmt_avg(val):
        if val is None:
            return "N/A"
        return "{v:.3f}".format(v=val)

    print("  {metric:30s} {wins:>12s} {losses:>12s}".format(
        metric="Metric", wins="WINS", losses="LOSSES",
    ))
    print("  " + "-" * 56)

    metrics = [
        ("Avg strength", lambda r: r["_strength"]),
        ("Avg delta", lambda r: r["_delta"]),
        ("Avg cross-book hold", lambda r: r["_cbh"]),
        ("Avg US hold", lambda r: r["_us_hold"]),
        ("Avg hours before game", lambda r: r["_hours_before"]),
        ("Avg dispersion", lambda r: r["_disp"]),
    ]

    for name, fn in metrics:
        w_avg = avg([fn(r) for r in wins])
        l_avg = avg([fn(r) for r in losses])
        print("  {name:30s} {w:>12s} {l:>12s}".format(
            name=name, w=fmt_avg(w_avg), l=fmt_avg(l_avg),
        ))

    # Count by book
    print("\n  Value book distribution:")
    for bk in sorted(set(r["_us_book"] for r in rows)):
        wc = sum(1 for r in wins if r["_us_book"] == bk)
        lc = sum(1 for r in losses if r["_us_book"] == bk)
        total = wc + lc
        if total > 0:
            wr = wc / total * 100
            print("    {bk:20s} {wc}W-{lc}L ({wr:.0f}%)".format(
                bk=bk, wc=wc, lc=lc, wr=wr,
            ))

    print()
    print("Done.")


if __name__ == "__main__":
    main()
