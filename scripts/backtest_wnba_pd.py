"""Backtest WNBA pinnacle_divergence under NBA-style guardrails.

WNBA PD has been collecting in the raw channel under NBA-aligned detector
filters since 2026-05-31 (strength 0.25, totals/spread thresholds 0.5,
high-cross-book-hold totals suppression >= 0.025). This script answers:
would WNBA PD clear the same PROMOTION standards NBA PD did — i.e. is there a
profitable combo/hour worth moving onto the main channel — or is it still too
thin / unprofitable?

Reports, for graded WNBA PD signals (and NBA PD as a benchmark over the same
window): volume, win%/units/ROI overall, by market, by MST hour, and by
cross-book hold bucket. Then applies the same promotion criteria used by
analyze_best_combos_hours.py (n >= 5 decided, WR >= 54%, units > 0) and emits
recommended SIGNAL_BEST_COMBOS / SIGNAL_BEST_HOURS for WNBA.

ROI% follows the codebase convention: (net units / decided plays) * 100,
to-win-1 unit model (win +1.0, loss -risk, push 0).

Run on server:
    docker compose exec sharp-seeker python /app/scripts/backtest_wnba_pd.py
    docker compose exec sharp-seeker python /app/scripts/backtest_wnba_pd.py 2026-05-31
    docker compose exec sharp-seeker python /app/scripts/backtest_wnba_pd.py 2026-05-31 2026-06-06
"""

import json
import os
import sqlite3
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

DB = os.getenv("DB_PATH", "/app/data/sharp_seeker.db")
MST = ZoneInfo("America/Phoenix")

PD = "pinnacle_divergence"
WNBA = "basketball_wnba"
NBA = "basketball_nba"

# Same promotion bar analyze_best_combos_hours.py uses.
MIN_SAMPLE = 5
PROMOTE_WR = 0.54

MARKET_SHORT = {"h2h": "ML", "spreads": "Spread", "totals": "Total"}

# NBA's signature guardrail: drop totals at cross-book hold >= this. WNBA
# inherited it by analogy (no WNBA data at the time) — this backtest is the
# chance to confirm whether WNBA totals actually bleed at high hold.
HIGH_HOLD = 0.025


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


def get_hold(row):
    details_raw = row.get("details_json")
    if not details_raw:
        return None
    try:
        details = json.loads(details_raw) if isinstance(details_raw, str) else details_raw
        return details.get("cross_book_hold")
    except (json.JSONDecodeError, TypeError):
        return None


def tally(rows):
    w = sum(1 for r in rows if r["result"] == "won")
    l = sum(1 for r in rows if r["result"] == "lost")
    p = sum(1 for r in rows if r["result"] == "push")
    u = sum(compute_units(get_price(r), r["result"]) for r in rows)
    return w, l, p, u


def fmt(w, l, p, u):
    n = w + l + p
    decided = w + l
    if decided == 0:
        return "(n={n:3d})  no decided".format(n=n)
    wr = w / decided
    roi = (u / decided) * 100.0
    sign = "+" if u >= 0 else ""
    rsign = "+" if roi >= 0 else ""
    return ("(n={n:3d})  {w}W-{l}L-{p}P  WR {wr:5.1%}  {sign}{u:6.2f}u  "
            "ROI {rsign}{roi:5.1f}%").format(
        n=n, w=w, l=l, p=p, wr=wr, sign=sign, u=u, rsign=rsign, roi=roi)


def section(title):
    print()
    print("=" * 72)
    print("  {t}".format(t=title))
    print("=" * 72)


def market_breakdown(rows, indent="    "):
    by_market = defaultdict(list)
    for r in rows:
        by_market[r["market_key"]].append(r)
    for mk in sorted(by_market):
        w, l, p, u = tally(by_market[mk])
        label = MARKET_SHORT.get(mk, mk)
        print("{i}{m:8s} {f}".format(i=indent, m=label, f=fmt(w, l, p, u)))


def connect():
    for attempt in range(10):
        try:
            conn = sqlite3.connect(DB, timeout=10)
            conn.row_factory = sqlite3.Row
            return conn
        except sqlite3.OperationalError:
            print("  DB locked, retrying ({a}/10)...".format(a=attempt + 1))
            time.sleep(3)
    raise SystemExit("ERROR: Could not acquire DB lock.")


def load(conn, sport, since, until):
    where = "WHERE signal_type = ? AND sport_key = ? AND result IS NOT NULL"
    params = [PD, sport]
    if since:
        where += " AND signal_at >= ?"
        params.append(since)
    if until:
        where += " AND signal_at < ?"
        params.append(until)
    rows = [dict(r) for r in conn.execute(
        "SELECT signal_type, sport_key, market_key, result, signal_at, "
        "details_json FROM signal_results {w} ORDER BY signal_at".format(w=where),
        tuple(params),
    ).fetchall()]
    for r in rows:
        dt = datetime.fromisoformat(r["signal_at"])
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        r["mst_hour"] = dt.astimezone(MST).hour
    return rows


def main():
    since = sys.argv[1] if len(sys.argv) > 1 else None
    until = sys.argv[2] if len(sys.argv) > 2 else None

    conn = connect()
    wnba = load(conn, WNBA, since, until)
    nba = load(conn, NBA, since, until)
    conn.close()

    span = ""
    if since:
        span += " since {s}".format(s=since)
    if until:
        span += " until {u}".format(u=until)

    section("WNBA PD BACKTEST{span}".format(span=span))
    if not wnba:
        print("  No graded WNBA PD signals in range. Nothing to evaluate.")
        print("  (WNBA PD volume is structurally thin — Pinnacle WNBA price")
        print("   coverage is ~half of FD/DK. Widen the date range or wait.)")
        return

    dates = [r["signal_at"][:10] for r in wnba]
    print("  Graded WNBA PD signals: {n}  ({a} -> {b})".format(
        n=len(wnba), a=min(dates), b=max(dates)))
    w, l, p, u = tally(wnba)
    print("  Overall: {f}".format(f=fmt(w, l, p, u)))
    if (w + l) < MIN_SAMPLE:
        print("  ** Sample below MIN_SAMPLE ({m}) decided — treat everything below".format(m=MIN_SAMPLE))
        print("     as directional only, NOT a basis for promotion. **")

    # ── By market ────────────────────────────────────────────
    section("WNBA PD BY MARKET")
    market_breakdown(wnba)

    # ── By MST hour ──────────────────────────────────────────
    section("WNBA PD BY HOUR (MST)")
    by_hour = defaultdict(list)
    for r in wnba:
        by_hour[r["mst_hour"]].append(r)
    for h in sorted(by_hour):
        ampm = "AM" if h < 12 else "PM"
        disp = h % 12 or 12
        w, l, p, u = tally(by_hour[h])
        print("    {d:2d} {ap} MST  {f}".format(d=disp, ap=ampm, f=fmt(w, l, p, u)))

    # ── Cross-book hold (validate the high-hold totals suppression) ──
    section("WNBA PD BY CROSS-BOOK HOLD (totals suppression check)")
    print("  NBA guardrail drops TOTALS at hold >= {h:.3f}. Since WNBA already".format(h=HIGH_HOLD))
    print("  inherits that, high-hold totals should be near-absent here; this")
    print("  shows the surviving distribution, not the dropped one.")
    buckets = [
        (None, 0.0, "Negative (efficient)"),
        (0.0, 0.02, "0.000-0.020 (tight)"),
        (0.02, HIGH_HOLD, "0.020-0.025"),
        (HIGH_HOLD, 1.0, ">= 0.025 (NBA drops totals)"),
    ]
    for label_rows, tag in ((wnba, "All markets"),
                            ([r for r in wnba if r["market_key"] == "totals"], "Totals only")):
        has_hold = [r for r in label_rows if get_hold(r) is not None]
        print("\n  {t} (hold data on {n}/{m}):".format(
            t=tag, n=len(has_hold), m=len(label_rows)))
        for lo, hi, name in buckets:
            if lo is None:
                br = [r for r in has_hold if get_hold(r) < 0]
            else:
                br = [r for r in has_hold if lo <= get_hold(r) < hi]
            if not br:
                continue
            w, l, p, u = tally(br)
            print("    {name:28s} {f}".format(name=name, f=fmt(w, l, p, u)))

    # ── NBA benchmark (the guardrail target) ─────────────────
    section("NBA PD BENCHMARK (same window — the bar WNBA is measured against)")
    if nba:
        w, l, p, u = tally(nba)
        print("  Overall: {f}".format(f=fmt(w, l, p, u)))
        print("  By market:")
        market_breakdown(nba, indent="    ")
    else:
        print("  No graded NBA PD signals in range (NBA Finals end ~June 22,")
        print("  PD volume drying up). Benchmark unavailable for this window.")

    # ── Promotion verdict ────────────────────────────────────
    section("PROMOTION VERDICT (criteria: n>={m} decided, WR>={wr:.0%}, units>0)".format(
        m=MIN_SAMPLE, wr=PROMOTE_WR))
    rec_combos = []
    rec_hours = []

    combos = defaultdict(list)
    for r in wnba:
        combos["{t}:{s}:{mk}".format(t=PD, s=WNBA, mk=r["market_key"])].append(r)
    print("\n  Combos (type:sport:market):")
    any_combo = False
    for key in sorted(combos):
        w, l, p, u = tally(combos[key])
        decided = w + l
        verdict = "thin"
        if decided >= MIN_SAMPLE:
            wr = w / decided
            if wr >= PROMOTE_WR and u > 0:
                verdict = "PROMOTE"
                rec_combos.append(key)
                any_combo = True
            elif wr < 0.45:
                verdict = "AVOID"
            else:
                verdict = "hold"
        print("    {k:42s} {f}  -> {v}".format(k=key, f=fmt(w, l, p, u), v=verdict))
    if not any_combo:
        print("    (no combo clears the promotion bar)")

    hours = defaultdict(list)
    for r in wnba:
        hours[r["mst_hour"]].append(r)
    for h in sorted(hours):
        w, l, p, u = tally(hours[h])
        decided = w + l
        if decided >= MIN_SAMPLE and (w / decided) >= PROMOTE_WR and u > 0:
            rec_hours.append(h)

    section("RECOMMENDED CONFIG (only if a combo cleared the bar)")
    if rec_combos:
        print("\n  Add to SIGNAL_BEST_COMBOS:")
        print("    " + json.dumps(sorted(rec_combos)))
        if rec_hours:
            print("\n  Add to SIGNAL_BEST_HOURS (MST):")
            print("    " + json.dumps({PD + ":" + WNBA: sorted(rec_hours)}))
        print("\n  NOTE: promoting moves WNBA PD off the raw channel onto the main")
        print("  PD channel through the normal qualifier gate. Decide 2U Elite")
        print("  sizing explicitly before flipping (see 2026-05-09 review note).")
    else:
        print("\n  No promotion. Keep WNBA PD on the raw collection channel and")
        print("  re-evaluate at the next review. Either volume is still too thin")
        print("  or the surviving signals don't beat break-even (~52.4% at -110).")

    print()
    print("Done.")


if __name__ == "__main__":
    main()
