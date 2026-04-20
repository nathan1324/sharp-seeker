"""High market hold performance analysis (sent signals only).

Filters graded signals to those that were actually sent as alerts, then
buckets by cross-book hold to test whether high-hold signals outperform.
Reports record (W-L-P, win%) and units per sport, per market, per signal type.

Usage:
    docker compose exec sharp-seeker python /app/scripts/analyze_high_hold_sent.py [threshold]

    threshold   Hold % cutoff for the "high" bucket (default 3.0 = 3%+)
"""

import json
import sqlite3
import sys
import time
from collections import defaultdict

DB = "/app/data/sharp_seeker.db"

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
    "americanfootball_nfl": "NFL",
    "americanfootball_ncaaf": "NCAAF",
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


def fmt(wins, losses, pushes, units):
    n = wins + losses + pushes
    decided = wins + losses
    wr = f"{wins / decided:.0%}" if decided else "--"
    sign = "+" if units >= 0 else ""
    roi = ""
    if decided:
        roi_val = (units / decided) * 100
        roi_sign = "+" if roi_val >= 0 else ""
        roi = f"  ROI {roi_sign}{roi_val:.1f}%"
    return f"(n={n:4d})  {wins:3d}W-{losses:3d}L-{pushes:2d}P  ({wr})  [{sign}{units:6.2f}u]{roi}"


def section(title):
    print()
    print("=" * 78)
    print(f"  {title}")
    print("=" * 78)


def tally(rows):
    d = {"won": 0, "lost": 0, "push": 0, "units": 0.0}
    for r in rows:
        d[r["result"]] += 1
        d["units"] += compute_units(r["best_price"], r["result"], r["multiplier"])
    return d


def print_line(label, d, indent=4):
    print(f"{' ' * indent}{label:28s} {fmt(d['won'], d['lost'], d['push'], d['units'])}")


def run():
    threshold = float(sys.argv[1]) if len(sys.argv) > 1 else 3.0

    conn = connect()

    cur = conn.execute("""
        SELECT event_id, sport_key, signal_type, market_key, outcome_name,
               signal_strength, signal_at, result, details_json
        FROM signal_results
        WHERE result IS NOT NULL
        ORDER BY signal_at
    """)
    rows = [dict(r) for r in cur.fetchall()]

    # Sent-only filter: key by (event_id, alert_type=signal_type, market_key, outcome_name)
    sent_cur = conn.execute("""
        SELECT event_id, alert_type, market_key, outcome_name
        FROM sent_alerts
    """)
    sent_keys = set()
    for s in sent_cur.fetchall():
        s_dict = dict(s)
        sent_keys.add(
            (s_dict["event_id"], s_dict["alert_type"],
             s_dict["market_key"], s_dict["outcome_name"])
        )
    conn.close()

    # Enrich + filter to sent
    enriched = []
    for row in rows:
        was_sent = (
            row["event_id"], row["signal_type"],
            row["market_key"], row["outcome_name"],
        ) in sent_keys
        if not was_sent:
            continue
        row["best_price"] = None
        row["multiplier"] = 1
        row["cross_hold"] = None
        details_raw = row.get("details_json")
        if details_raw:
            try:
                details = json.loads(details_raw) if isinstance(details_raw, str) else details_raw
                row["cross_hold"] = details.get("cross_book_hold")
                vb = details.get("value_books", [])
                if vb:
                    row["best_price"] = vb[0].get("price")
                if details.get("qualifier_count", 0) >= 2:
                    row["multiplier"] = 2
            except (json.JSONDecodeError, TypeError):
                pass
        enriched.append(row)

    sent_with_hold = [r for r in enriched if r["cross_hold"] is not None]
    high = [r for r in sent_with_hold if r["cross_hold"] >= threshold]
    low = [r for r in sent_with_hold if r["cross_hold"] < threshold]

    print(f"High-Hold Performance (sent signals only)")
    print(f"Threshold: cross-book hold >= {threshold:.1f}%")
    print(f"Sent & graded: {len(enriched)}")
    print(f"  with hold data: {len(sent_with_hold)}")
    print(f"  high-hold  (>= {threshold:.1f}%): {len(high)}")
    print(f"  lower-hold (<  {threshold:.1f}%): {len(low)}")
    if sent_with_hold:
        first = sent_with_hold[0]["signal_at"][:10]
        last = sent_with_hold[-1]["signal_at"][:10]
        print(f"  range: {first} to {last}")

    # ── 1. High vs low overall ──────────────────────────────
    section(f"1. HIGH (>= {threshold:.1f}%) vs LOW (< {threshold:.1f}%) — OVERALL")
    print_line(f"HIGH >= {threshold:.1f}%", tally(high))
    print_line(f"LOW  <  {threshold:.1f}%", tally(low))

    # ── 2. By sport ─────────────────────────────────────────
    section(f"2. HIGH HOLD (>= {threshold:.1f}%) BY SPORT")
    sports = sorted(set(r["sport_key"] for r in sent_with_hold))
    for sport in sports:
        hs = [r for r in high if r["sport_key"] == sport]
        ls = [r for r in low if r["sport_key"] == sport]
        label = SPORT_SHORT.get(sport, sport)
        print(f"\n  {label}")
        print_line("HIGH", tally(hs), indent=6)
        print_line("LOW ", tally(ls), indent=6)

    # ── 3. By sport x market (high only) ────────────────────
    section(f"3. HIGH HOLD (>= {threshold:.1f}%) BY SPORT x MARKET")
    for sport in sports:
        printed_sport = False
        for mkt in ["h2h", "spreads", "totals"]:
            hs = [r for r in high if r["sport_key"] == sport and r["market_key"] == mkt]
            if not hs:
                continue
            if not printed_sport:
                print(f"\n  {SPORT_SHORT.get(sport, sport)}")
                printed_sport = True
            print_line(MARKET_SHORT[mkt], tally(hs), indent=6)

    # ── 4. By sport x signal type (high only) ───────────────
    section(f"4. HIGH HOLD (>= {threshold:.1f}%) BY SPORT x SIGNAL TYPE")
    for sport in sports:
        printed_sport = False
        sig_types = sorted(set(r["signal_type"] for r in high if r["sport_key"] == sport))
        for st in sig_types:
            hs = [r for r in high if r["sport_key"] == sport and r["signal_type"] == st]
            if not hs:
                continue
            if not printed_sport:
                print(f"\n  {SPORT_SHORT.get(sport, sport)}")
                printed_sport = True
            print_line(SIGNAL_LABELS.get(st, st), tally(hs), indent=6)

    # ── 5. Fine-grained hold tiers per sport ────────────────
    section("5. FINE-GRAINED HOLD TIERS BY SPORT (sent only)")
    tiers = [
        ("< 0%", lambda h: h < 0),
        ("0-1%", lambda h: 0 <= h < 1),
        ("1-2%", lambda h: 1 <= h < 2),
        ("2-3%", lambda h: 2 <= h < 3),
        ("3-4%", lambda h: 3 <= h < 4),
        ("4-5%", lambda h: 4 <= h < 5),
        ("5-7%", lambda h: 5 <= h < 7),
        ("7%+ ", lambda h: h >= 7),
    ]
    for sport in sports:
        sport_rows = [r for r in sent_with_hold if r["sport_key"] == sport]
        if not sport_rows:
            continue
        print(f"\n  {SPORT_SHORT.get(sport, sport)}")
        for label, pred in tiers:
            bucket = [r for r in sport_rows if pred(r["cross_hold"])]
            if not bucket:
                continue
            print_line(label, tally(bucket), indent=6)

    print("\nDone.")


if __name__ == "__main__":
    run()
