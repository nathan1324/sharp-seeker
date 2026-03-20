"""Validate whether config changes correlate with performance shifts.

Splits signal performance into time periods aligned with major config
deployments to identify which changes helped and which hurt.

Key deployment dates (UTC):
  Feb 19       System launch
  Feb 26       Tiered strength filters, quiet hours, max strength caps
  Feb 28       Sport-specific PD thresholds (NHL ML 1.5%, NBA ML 2%,
               totals/spreads 0.5 for NHL/NBA), sport strength overrides
               (PD NBA/NHL min 0.25)
  Mar 02       Signal blocklist deployed
  Mar 15       Hold boost added to PD strength (+0.04/+0.08)
  Mar 18       Major tuning (quiet hours, best hours, 2U logic)
  Mar 19       Added Caesars + BetRivers bookmakers

Usage:
    docker compose exec sharp-seeker python /app/scripts/validate_config_changes.py
"""

import json
import sqlite3
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone

DB = "/app/data/sharp_seeker.db"
MST = timezone(timedelta(hours=-7))

# Time periods aligned with config deployments (UTC dates)
PERIODS = [
    ("Feb 19-25 (baseline)", "2026-02-19", "2026-02-26"),
    ("Feb 26-28 (thresholds lowered)", "2026-02-26", "2026-03-01"),
    ("Mar 01-02 (pre-blocklist)", "2026-03-01", "2026-03-03"),
    ("Mar 03-14 (blocklist, stable)", "2026-03-03", "2026-03-15"),
    ("Mar 15-17 (hold boost)", "2026-03-15", "2026-03-18"),
    ("Mar 18-19 (tuning + new books)", "2026-03-18", "2026-03-20"),
]

SIGNAL_LABELS = {
    "steam_move": "Steam",
    "rapid_change": "Rapid",
    "pinnacle_divergence": "PinDiv",
    "reverse_line": "RevLine",
    "exchange_shift": "ExchShift",
    "arbitrage": "Arb",
}

SPORT_SHORT = {
    "basketball_nba": "NBA",
    "basketball_ncaab": "NCAAB",
    "icehockey_nhl": "NHL",
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


def get_qualifier_count(row):
    details_raw = row.get("details_json")
    if not details_raw:
        return 0
    try:
        details = json.loads(details_raw) if isinstance(details_raw, str) else details_raw
        return details.get("qualifier_count", 0)
    except (json.JSONDecodeError, TypeError):
        return 0


def get_hold_boost(row):
    details_raw = row.get("details_json")
    if not details_raw:
        return None
    try:
        details = json.loads(details_raw) if isinstance(details_raw, str) else details_raw
        return details.get("hold_boost")
    except (json.JSONDecodeError, TypeError):
        return None


def fmt_record(w, l, p, u):
    n = w + l + p
    decided = w + l
    if decided == 0:
        return f"(n={n:4d})  --"
    rate = w / decided
    sign = "+" if u >= 0 else ""
    return f"(n={n:4d})  {w}W-{l}L-{p}P  ({rate:.0%})  {sign}{u:.1f}u"


def tally_rows(rows):
    w = sum(1 for r in rows if r["result"] == "won")
    l = sum(1 for r in rows if r["result"] == "lost")
    p = sum(1 for r in rows if r["result"] == "push")
    u = sum(compute_units(get_price(r), r["result"]) for r in rows)
    return w, l, p, u


def section(title):
    print()
    print("=" * 78)
    print(f"  {title}")
    print("=" * 78)


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


def main():
    conn = connect()
    cur = conn.execute("""
        SELECT event_id, sport_key, signal_type, market_key, outcome_name,
               signal_strength, signal_at, result, details_json
        FROM signal_results
        WHERE result IS NOT NULL
        ORDER BY signal_at
    """)
    all_rows = [dict(r) for r in cur.fetchall()]

    # Also get free play keys
    fp_cur = conn.execute("""
        SELECT event_id, market_key, outcome_name
        FROM sent_alerts WHERE is_free_play = 1
    """)
    fp_keys = set()
    for fp in fp_cur.fetchall():
        d = dict(fp)
        fp_keys.add((d["event_id"], d["market_key"], d["outcome_name"]))
    conn.close()

    for row in all_rows:
        row["is_free_play"] = (
            row["event_id"], row["market_key"], row["outcome_name"]
        ) in fp_keys

    print(f"Total graded signals: {len(all_rows)}")

    # ── 1. Overall by time period ─────────────────────────────
    section("1. PERFORMANCE BY TIME PERIOD")
    for label, start, end in PERIODS:
        period_rows = [r for r in all_rows if start <= r["signal_at"] < end]
        if not period_rows:
            print(f"  {label:45s} (no data)")
            continue
        w, l, p, u = tally_rows(period_rows)
        print(f"  {label:45s} {fmt_record(w, l, p, u)}")

    # ── 2. PinDiv by time period ──────────────────────────────
    section("2. PINNACLE DIVERGENCE BY TIME PERIOD")
    pd_rows = [r for r in all_rows if r["signal_type"] == "pinnacle_divergence"]
    for label, start, end in PERIODS:
        period_rows = [r for r in pd_rows if start <= r["signal_at"] < end]
        if not period_rows:
            print(f"  {label:45s} (no data)")
            continue
        w, l, p, u = tally_rows(period_rows)
        print(f"  {label:45s} {fmt_record(w, l, p, u)}")

    # ── 3. PinDiv by market by time period ────────────────────
    section("3. PINDIV BY MARKET x TIME PERIOD")
    for mk in ["h2h", "spreads", "totals"]:
        mk_rows = [r for r in pd_rows if r["market_key"] == mk]
        mname = MARKET_SHORT.get(mk, mk)
        print(f"\n  {mname}:")
        for label, start, end in PERIODS:
            period_rows = [r for r in mk_rows if start <= r["signal_at"] < end]
            if not period_rows:
                continue
            w, l, p, u = tally_rows(period_rows)
            print(f"    {label:43s} {fmt_record(w, l, p, u)}")

    # ── 4. PinDiv by sport by time period ─────────────────────
    section("4. PINDIV BY SPORT x TIME PERIOD")
    for sk in sorted(SPORT_SHORT.keys()):
        sk_rows = [r for r in pd_rows if r["sport_key"] == sk]
        sname = SPORT_SHORT.get(sk, sk)
        print(f"\n  {sname}:")
        for label, start, end in PERIODS:
            period_rows = [r for r in sk_rows if start <= r["signal_at"] < end]
            if not period_rows:
                continue
            w, l, p, u = tally_rows(period_rows)
            print(f"    {label:43s} {fmt_record(w, l, p, u)}")

    # ── 5. PinDiv strength distribution by period ─────────────
    section("5. PINDIV STRENGTH DISTRIBUTION BY PERIOD")
    buckets = [
        (0.0, 0.34, "<34%"),
        (0.34, 0.50, "34-49%"),
        (0.50, 0.67, "50-66%"),
        (0.67, 0.80, "67-79%"),
        (0.80, 1.01, "80%+"),
    ]
    for label, start, end in PERIODS:
        period_rows = [r for r in pd_rows if start <= r["signal_at"] < end]
        if not period_rows:
            continue
        print(f"\n  {label}:")
        for lo, hi, blabel in buckets:
            b_rows = [r for r in period_rows if lo <= r["signal_strength"] < hi]
            if not b_rows:
                continue
            w, l, p, u = tally_rows(b_rows)
            print(f"    {blabel:10s} {fmt_record(w, l, p, u)}")

    # ── 6. Hold boost impact (PD signals only) ───────────────
    section("6. PINDIV HOLD BOOST IMPACT (Mar 15+)")
    pd_post_boost = [r for r in pd_rows if r["signal_at"] >= "2026-03-15"]
    if pd_post_boost:
        boosted = [r for r in pd_post_boost if (get_hold_boost(r) or 0) > 0]
        unboosted = [r for r in pd_post_boost if (get_hold_boost(r) or 0) == 0]

        if boosted:
            w, l, p, u = tally_rows(boosted)
            print(f"  Hold boost applied (boost > 0):  {fmt_record(w, l, p, u)}")
        if unboosted:
            w, l, p, u = tally_rows(unboosted)
            print(f"  No hold boost (boost = 0/None):  {fmt_record(w, l, p, u)}")

        # Break boosted by market
        print("\n  Boosted by market:")
        for mk in ["h2h", "spreads", "totals"]:
            mk_rows = [r for r in boosted if r["market_key"] == mk]
            if not mk_rows:
                continue
            w, l, p, u = tally_rows(mk_rows)
            mname = MARKET_SHORT.get(mk, mk)
            print(f"    {mname:12s} {fmt_record(w, l, p, u)}")
    else:
        print("  No PD signals after Mar 15.")

    # ── 7. Low-strength PD signals (0.25-0.49) ───────────────
    section("7. LOW-STRENGTH PD SIGNALS (0.25-0.49)")
    low_str = [r for r in pd_rows if 0.25 <= r["signal_strength"] < 0.50]
    if low_str:
        w, l, p, u = tally_rows(low_str)
        print(f"  All low-strength PD:  {fmt_record(w, l, p, u)}")

        print("\n  By sport:")
        for sk in sorted(SPORT_SHORT.keys()):
            sk_rows = [r for r in low_str if r["sport_key"] == sk]
            if not sk_rows:
                continue
            w, l, p, u = tally_rows(sk_rows)
            sname = SPORT_SHORT.get(sk, sk)
            print(f"    {sname:12s} {fmt_record(w, l, p, u)}")

        print("\n  By market:")
        for mk in ["h2h", "spreads", "totals"]:
            mk_rows = [r for r in low_str if r["market_key"] == mk]
            if not mk_rows:
                continue
            w, l, p, u = tally_rows(mk_rows)
            mname = MARKET_SHORT.get(mk, mk)
            print(f"    {mname:12s} {fmt_record(w, l, p, u)}")
    else:
        print("  No PD signals at 0.25-0.49 strength.")

    # ── 8. All detectors by period ────────────────────────────
    section("8. ALL DETECTORS BY TIME PERIOD")
    for st_val, st_label in sorted(SIGNAL_LABELS.items()):
        st_rows = [r for r in all_rows if r["signal_type"] == st_val]
        if not st_rows:
            continue
        print(f"\n  {st_label}:")
        for label, start, end in PERIODS:
            period_rows = [r for r in st_rows if start <= r["signal_at"] < end]
            if not period_rows:
                continue
            w, l, p, u = tally_rows(period_rows)
            print(f"    {label:43s} {fmt_record(w, l, p, u)}")

    # ── 9. Free plays by period ───────────────────────────────
    section("9. FREE PLAYS BY TIME PERIOD")
    fp_rows = [r for r in all_rows if r["is_free_play"]]
    for label, start, end in PERIODS:
        period_rows = [r for r in fp_rows if start <= r["signal_at"] < end]
        if not period_rows:
            continue
        w, l, p, u = tally_rows(period_rows)
        print(f"  {label:45s} {fmt_record(w, l, p, u)}")

    # ── 10. "What if" analysis ────────────────────────────────
    section("10. WHAT-IF: REMOVE LOW-STRENGTH + HOLD-BOOSTED SIGNALS")
    print("  Simulates reverting sport-specific low thresholds and hold boost.")
    print("  Excludes: PD signals with strength < 0.50 OR hold_boost > 0")
    print()

    # Simulate: remove PD signals where strength < 0.50 or hold_boost > 0
    # Keep all non-PD signals as-is
    simulated = []
    removed_count = 0
    for r in all_rows:
        if r["signal_type"] == "pinnacle_divergence":
            boost = get_hold_boost(r) or 0
            if r["signal_strength"] < 0.50 or boost > 0:
                removed_count += 1
                continue
        simulated.append(r)

    w, l, p, u = tally_rows(all_rows)
    print(f"  Current (all signals):   {fmt_record(w, l, p, u)}")
    w2, l2, p2, u2 = tally_rows(simulated)
    print(f"  Simulated (filtered):    {fmt_record(w2, l2, p2, u2)}")
    print(f"  Signals removed: {removed_count}")
    print(f"  Unit improvement: {u2 - u:+.1f}u")

    # Also simulate just removing hold boost (keep low-strength)
    print()
    simulated2 = []
    removed2 = 0
    for r in all_rows:
        if r["signal_type"] == "pinnacle_divergence":
            boost = get_hold_boost(r) or 0
            if boost > 0:
                removed2 += 1
                continue
        simulated2.append(r)
    w3, l3, p3, u3 = tally_rows(simulated2)
    print(f"  Sim: remove ONLY hold-boosted PD:  {fmt_record(w3, l3, p3, u3)}")
    print(f"  Signals removed: {removed2},  Unit change: {u3 - u:+.1f}u")

    # Simulate removing only low-strength PD (keep hold boost)
    simulated3 = []
    removed3 = 0
    for r in all_rows:
        if r["signal_type"] == "pinnacle_divergence":
            if r["signal_strength"] < 0.50:
                removed3 += 1
                continue
        simulated3.append(r)
    w4, l4, p4, u4 = tally_rows(simulated3)
    print(f"  Sim: remove ONLY low-strength PD:  {fmt_record(w4, l4, p4, u4)}")
    print(f"  Signals removed: {removed3},  Unit change: {u4 - u:+.1f}u")

    # ── 11. PinDiv NHL ML deep dive ───────────────────────────
    section("11. PINDIV NHL ML DEEP DIVE (worst combo: -33.0u)")
    nhl_ml = [r for r in pd_rows
              if r["sport_key"] == "icehockey_nhl" and r["market_key"] == "h2h"]
    if nhl_ml:
        print(f"  Total: {len(nhl_ml)} signals")
        print("\n  By strength:")
        for lo, hi, blabel in buckets:
            b_rows = [r for r in nhl_ml if lo <= r["signal_strength"] < hi]
            if not b_rows:
                continue
            w, l, p, u = tally_rows(b_rows)
            print(f"    {blabel:10s} {fmt_record(w, l, p, u)}")

        print("\n  By period:")
        for label, start, end in PERIODS:
            period_rows = [r for r in nhl_ml if start <= r["signal_at"] < end]
            if not period_rows:
                continue
            w, l, p, u = tally_rows(period_rows)
            print(f"    {label:43s} {fmt_record(w, l, p, u)}")

    # ── 12. Stable period vs post-change comparison ──────────
    section("12. STABLE PERIOD (Mar 3-14) vs POST-CHANGES (Mar 15+)")
    stable = [r for r in all_rows
              if "2026-03-03" <= r["signal_at"] < "2026-03-15"]
    post = [r for r in all_rows if r["signal_at"] >= "2026-03-15"]

    if stable and post:
        sw, sl, sp, su = tally_rows(stable)
        pw, pl, pp, pu = tally_rows(post)
        print(f"  Stable (Mar 3-14):  {fmt_record(sw, sl, sp, su)}")
        print(f"  Post (Mar 15+):     {fmt_record(pw, pl, pp, pu)}")

        # By detector
        print("\n  Stable by detector:")
        for st_val, st_label in sorted(SIGNAL_LABELS.items()):
            st_rows = [r for r in stable if r["signal_type"] == st_val]
            if not st_rows:
                continue
            w, l, p, u = tally_rows(st_rows)
            print(f"    {st_label:12s} {fmt_record(w, l, p, u)}")

        print("\n  Post-change by detector:")
        for st_val, st_label in sorted(SIGNAL_LABELS.items()):
            st_rows = [r for r in post if r["signal_type"] == st_val]
            if not st_rows:
                continue
            w, l, p, u = tally_rows(st_rows)
            print(f"    {st_label:12s} {fmt_record(w, l, p, u)}")

        # By market
        print("\n  Stable by market:")
        for mk in ["h2h", "spreads", "totals"]:
            mk_rows = [r for r in stable if r["market_key"] == mk]
            if not mk_rows:
                continue
            w, l, p, u = tally_rows(mk_rows)
            print(f"    {MARKET_SHORT[mk]:12s} {fmt_record(w, l, p, u)}")

        print("\n  Post-change by market:")
        for mk in ["h2h", "spreads", "totals"]:
            mk_rows = [r for r in post if r["market_key"] == mk]
            if not mk_rows:
                continue
            w, l, p, u = tally_rows(mk_rows)
            print(f"    {MARKET_SHORT[mk]:12s} {fmt_record(w, l, p, u)}")

    print()
    print("Done.")


if __name__ == "__main__":
    main()
