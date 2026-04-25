"""Diagnose why MLB barely produces signals vs NBA.

Usage:
    python /app/scripts/diagnose_mlb_volume.py            # default 21 days
    python /app/scripts/diagnose_mlb_volume.py 30

Note: signal_results is POST-filter (recorded after the 8-stage pipeline). We
can't see pre-filter detector firings from the DB. Instead we compare:

  1. Snapshot polling volume per sport — are we even fetching MLB odds?
  2. Distinct events per sport — how many MLB games entered the system?
  3. MLB game commence_time distribution vs polling quiet hours
  4. signal_results breakdown per sport × type × market (post-filter survivors)
  5. Per-sport "events polled -> signals fired" funnel

The combination tells us whether MLB is starved at the poll layer (no data),
detector layer (data but nothing fires), or filter layer (fires but everything
gets dropped — would manifest as low signal_results despite plenty of polling).
"""

import sqlite3
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone

DB = "/app/data/sharp_seeker.db"
MST = timezone(timedelta(hours=-7))

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
    "arbitrage": "Arbitrage",
}

MARKET_LABELS = {"spreads": "Spread", "h2h": "ML", "totals": "Total"}


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


def section(title):
    print()
    print("=" * 78)
    print(f"  {title}")
    print("=" * 78)


def utc_to_mst_hour(utc_h):
    return (utc_h - 7) % 24


def mst_hour_label(utc_h):
    mst_h = utc_to_mst_hour(utc_h)
    ampm = "AM" if mst_h < 12 else "PM"
    display = mst_h % 12 or 12
    return f"{display:2d} {ampm} MST"


def run():
    days = int(sys.argv[1]) if len(sys.argv) > 1 else 21
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()

    conn = connect()

    print(f"MLB volume diagnostic — last {days} days")
    print(f"Cutoff: {cutoff}")

    # ── 1. Snapshot polling volume per sport ──────────────────────
    section("1. SNAPSHOT POLLING VOLUME (rows in odds_snapshots)")
    cur = conn.execute(
        """
        SELECT sport_key, COUNT(*) AS n
        FROM odds_snapshots
        WHERE fetched_at >= ?
        GROUP BY sport_key
        ORDER BY n DESC
        """,
        (cutoff,),
    )
    rows = cur.fetchall()
    if not rows:
        print("  (no snapshots in window)")
    for r in rows:
        sk = r["sport_key"]
        print(f"  {SPORT_SHORT.get(sk, sk):8s}  {r['n']:>9d} rows")

    # ── 2. Distinct events polled per sport ───────────────────────
    section("2. DISTINCT EVENTS POLLED")
    cur = conn.execute(
        """
        SELECT sport_key, COUNT(DISTINCT event_id) AS n
        FROM odds_snapshots
        WHERE fetched_at >= ?
        GROUP BY sport_key
        ORDER BY n DESC
        """,
        (cutoff,),
    )
    for r in cur.fetchall():
        sk = r["sport_key"]
        print(f"  {SPORT_SHORT.get(sk, sk):8s}  {r['n']:>4d} events")

    # ── 3. Polling cadence: when did we fetch MLB odds? ───────────
    section("3. MLB POLLING — fetches by UTC hour")
    cur = conn.execute(
        """
        SELECT substr(fetched_at, 12, 2) AS utc_hour, COUNT(*) AS n
        FROM odds_snapshots
        WHERE sport_key = 'baseball_mlb' AND fetched_at >= ?
        GROUP BY utc_hour
        ORDER BY utc_hour
        """,
        (cutoff,),
    )
    fetch_by_hour = {int(r["utc_hour"]): r["n"] for r in cur.fetchall()}
    if fetch_by_hour:
        print("  UTC | MST       | rows")
        for utc_h in range(24):
            n = fetch_by_hour.get(utc_h, 0)
            bar = "#" * min(50, n // max(1, max(fetch_by_hour.values()) // 50))
            print(f"  {utc_h:02d}  | {mst_hour_label(utc_h)} | {n:6d}  {bar}")
    else:
        print("  (no MLB snapshots — sport may not be in SPORTS list on server)")

    # ── 4. MLB commence_time distribution (when do MLB games start?) ──
    section("4. MLB GAME START TIMES (from commence_time)")
    cur = conn.execute(
        """
        SELECT substr(commence_time, 12, 2) AS utc_hour,
               COUNT(DISTINCT event_id) AS n
        FROM odds_snapshots
        WHERE sport_key = 'baseball_mlb' AND fetched_at >= ?
        GROUP BY utc_hour
        ORDER BY utc_hour
        """,
        (cutoff,),
    )
    start_by_hour = {int(r["utc_hour"]): r["n"] for r in cur.fetchall()}
    if start_by_hour:
        print("  UTC | MST       | events")
        for utc_h in range(24):
            n = start_by_hour.get(utc_h, 0)
            if n == 0:
                continue
            bar = "#" * min(50, n)
            print(f"  {utc_h:02d}  | {mst_hour_label(utc_h)} | {n:4d}  {bar}")
    else:
        print("  (no MLB games)")

    # Polling quiet hours from .env (default 1-13 UTC)
    print()
    print("  Reference: default polling quiet hours = UTC 1-13 (6pm-6am MST)")

    # ── 5. signal_results post-filter survivors per sport × type × market ──
    section("5. POST-FILTER SIGNALS (signal_results) per sport x type x market")
    cur = conn.execute(
        """
        SELECT sport_key, signal_type, market_key, COUNT(*) AS n,
               SUM(CASE WHEN result IS NOT NULL THEN 1 ELSE 0 END) AS graded,
               SUM(CASE WHEN result = 'won' THEN 1 ELSE 0 END) AS wins,
               SUM(CASE WHEN result = 'lost' THEN 1 ELSE 0 END) AS losses
        FROM signal_results
        WHERE signal_at >= ?
        GROUP BY sport_key, signal_type, market_key
        ORDER BY sport_key, signal_type, market_key
        """,
        (cutoff,),
    )

    by_sport = defaultdict(list)
    for r in cur.fetchall():
        by_sport[r["sport_key"]].append(dict(r))

    for sport in sorted(by_sport.keys(), key=lambda s: SPORT_SHORT.get(s, s)):
        print(f"\n  {SPORT_SHORT.get(sport, sport)}")
        for r in by_sport[sport]:
            t = SIGNAL_LABELS.get(r["signal_type"], r["signal_type"])
            m = MARKET_LABELS.get(r["market_key"], r["market_key"])
            decided = (r["wins"] or 0) + (r["losses"] or 0)
            wr = f"{(r['wins'] or 0) / decided:.0%}" if decided else "--"
            print(
                f"    {t:10s} {m:7s}  total={r['n']:4d}  graded={r['graded']:4d}  "
                f"({r['wins'] or 0}W-{r['losses'] or 0}L, {wr})"
            )

    # ── 6. Funnel summary: events polled -> signals fired ─────────
    section("6. FUNNEL — events polled vs post-filter signals")
    print("  Sport     events  signals  ratio")
    cur = conn.execute(
        """
        SELECT o.sport_key,
               COUNT(DISTINCT o.event_id) AS events,
               (SELECT COUNT(*) FROM signal_results s
                WHERE s.sport_key = o.sport_key AND s.signal_at >= ?) AS signals
        FROM odds_snapshots o
        WHERE o.fetched_at >= ?
        GROUP BY o.sport_key
        ORDER BY events DESC
        """,
        (cutoff, cutoff),
    )
    for r in cur.fetchall():
        sk = r["sport_key"]
        events = r["events"] or 0
        signals = r["signals"] or 0
        ratio = f"{signals / events:.2f}" if events else "--"
        print(f"  {SPORT_SHORT.get(sk, sk):8s}  {events:>5d}  {signals:>6d}  {ratio}")

    # ── 7. MLB Rapid specifically — any post-filter signals? ──────
    section("7. MLB RAPID — post-filter signals (any market)")
    cur = conn.execute(
        """
        SELECT market_key, COUNT(*) AS n,
               SUM(CASE WHEN result IS NOT NULL THEN 1 ELSE 0 END) AS graded
        FROM signal_results
        WHERE sport_key = 'baseball_mlb'
          AND signal_type = 'rapid_change'
          AND signal_at >= ?
        GROUP BY market_key
        """,
        (cutoff,),
    )
    rows = cur.fetchall()
    if not rows:
        print("  ZERO MLB Rapid signals survived filters in this window.")
        print("  Means: detector didn't fire OR pipeline dropped all of them.")
    else:
        for r in rows:
            m = MARKET_LABELS.get(r["market_key"], r["market_key"])
            print(f"  {m:7s}  total={r['n']}  graded={r['graded']}")

    conn.close()
    print()
    print("Done.")


if __name__ == "__main__":
    run()
