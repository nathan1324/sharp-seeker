"""Analyze ALL MLB signal performance (not just free plays) to identify profitable combos."""

import json
import sqlite3
from collections import defaultdict

DB = "/app/data/sharp_seeker.db"


def _get_price(details_json):
    if not details_json:
        return None
    try:
        details = json.loads(details_json) if isinstance(details_json, str) else details_json
        vb = details.get("value_books", [])
        if vb:
            return vb[0].get("price")
    except (json.JSONDecodeError, TypeError):
        pass
    return None


def _unit_pnl(result, price):
    if result == "push" or price is None:
        return 0.0
    if price < 0:
        risk = abs(price) / 100.0
    elif price > 0:
        risk = 100.0 / price
    else:
        risk = 1.0
    if result == "won":
        return 1.0
    elif result == "lost":
        return -risk
    return 0.0


def _fmt(units, w, l, p):
    decided = w + l
    wr = f"{w/decided:.0%}" if decided else "N/A"
    sign = "+" if units >= 0 else ""
    return f"{sign}{units:.2f}u | {wr} ({w}W/{l}L/{p}P)"


def run():
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row

    # All resolved MLB signals — use sr.details_json (always populated)
    # instead of sa.details_json (only exists for non-suppressed signals)
    rows = conn.execute("""
        SELECT sr.signal_type, sr.market_key, sr.outcome_name, sr.result,
               sr.signal_strength, sr.sport_key, sr.signal_at,
               sr.details_json
        FROM signal_results sr
        WHERE sr.sport_key = 'baseball_mlb'
          AND sr.result IN ('won', 'lost', 'push')
        ORDER BY sr.signal_at ASC
    """).fetchall()

    # Count unresolved
    unresolved = conn.execute("""
        SELECT COUNT(*) FROM signal_results
        WHERE sport_key = 'baseball_mlb' AND result IS NULL
    """).fetchone()[0]

    if not rows:
        print(f"No resolved MLB signals found. ({unresolved} unresolved pending)")
        return

    print(f"=== MLB SIGNAL ANALYSIS ({len(rows)} resolved, {unresolved} pending) ===\n")

    # Overall
    total_u, tw, tl, tp = 0.0, 0, 0, 0
    for r in rows:
        price = _get_price(r["details_json"])
        total_u += _unit_pnl(r["result"], price)
        if r["result"] == "won": tw += 1
        elif r["result"] == "lost": tl += 1
        else: tp += 1
    print(f"Overall: {_fmt(total_u, tw, tl, tp)}\n")

    # By signal type
    print("=== BY SIGNAL TYPE ===")
    by_type = defaultdict(lambda: {"u": 0.0, "w": 0, "l": 0, "p": 0})
    for r in rows:
        price = _get_price(r["details_json"])
        k = r["signal_type"]
        by_type[k]["u"] += _unit_pnl(r["result"], price)
        if r["result"] == "won": by_type[k]["w"] += 1
        elif r["result"] == "lost": by_type[k]["l"] += 1
        else: by_type[k]["p"] += 1
    for k, c in sorted(by_type.items(), key=lambda x: x[1]["u"]):
        print(f"  {k}: {_fmt(c['u'], c['w'], c['l'], c['p'])}")

    # By market
    print("\n=== BY MARKET ===")
    by_mkt = defaultdict(lambda: {"u": 0.0, "w": 0, "l": 0, "p": 0})
    for r in rows:
        price = _get_price(r["details_json"])
        k = r["market_key"]
        by_mkt[k]["u"] += _unit_pnl(r["result"], price)
        if r["result"] == "won": by_mkt[k]["w"] += 1
        elif r["result"] == "lost": by_mkt[k]["l"] += 1
        else: by_mkt[k]["p"] += 1
    for k, c in sorted(by_mkt.items(), key=lambda x: x[1]["u"]):
        print(f"  {k}: {_fmt(c['u'], c['w'], c['l'], c['p'])}")

    # By type:market combo
    print("\n=== BY TYPE:MARKET COMBO ===")
    by_combo = defaultdict(lambda: {"u": 0.0, "w": 0, "l": 0, "p": 0})
    for r in rows:
        price = _get_price(r["details_json"])
        k = f"{r['signal_type']}:{r['market_key']}"
        by_combo[k]["u"] += _unit_pnl(r["result"], price)
        if r["result"] == "won": by_combo[k]["w"] += 1
        elif r["result"] == "lost": by_combo[k]["l"] += 1
        else: by_combo[k]["p"] += 1
    for k, c in sorted(by_combo.items(), key=lambda x: x[1]["u"]):
        print(f"  {k}: {_fmt(c['u'], c['w'], c['l'], c['p'])}")

    # By week
    print("\n=== BY WEEK ===")
    by_week = defaultdict(lambda: {"u": 0.0, "w": 0, "l": 0, "p": 0})
    for r in rows:
        price = _get_price(r["details_json"])
        sa = r["signal_at"] or ""
        week = sa[:10] if sa else "unknown"
        try:
            from datetime import datetime
            dt = datetime.fromisoformat(sa.replace("Z", "+00:00"))
            week = dt.strftime("%Y-W%U")
        except Exception:
            pass
        by_week[week]["u"] += _unit_pnl(r["result"], price)
        if r["result"] == "won": by_week[week]["w"] += 1
        elif r["result"] == "lost": by_week[week]["l"] += 1
        else: by_week[week]["p"] += 1
    for k in sorted(by_week.keys()):
        c = by_week[k]
        print(f"  {k}: {_fmt(c['u'], c['w'], c['l'], c['p'])}")

    # By strength bucket
    print("\n=== BY STRENGTH BUCKET ===")
    by_str = defaultdict(lambda: {"u": 0.0, "w": 0, "l": 0, "p": 0})
    for r in rows:
        price = _get_price(r["details_json"])
        s = r["signal_strength"] or 0
        if s >= 0.60:
            bucket = "0.60+"
        elif s >= 0.40:
            bucket = "0.40-0.59"
        else:
            bucket = "<0.40"
        by_str[bucket]["u"] += _unit_pnl(r["result"], price)
        if r["result"] == "won": by_str[bucket]["w"] += 1
        elif r["result"] == "lost": by_str[bucket]["l"] += 1
        else: by_str[bucket]["p"] += 1
    for bucket in ["0.60+", "0.40-0.59", "<0.40"]:
        if bucket in by_str:
            c = by_str[bucket]
            print(f"  {bucket}: {_fmt(c['u'], c['w'], c['l'], c['p'])}")

    # By MST hour (for best_hours config)
    print("\n=== BY MST HOUR ===")
    from datetime import datetime as dt2, timezone as tz2
    from zoneinfo import ZoneInfo
    MST = ZoneInfo("America/Phoenix")
    by_hour = defaultdict(lambda: {"u": 0.0, "w": 0, "l": 0, "p": 0})
    for r in rows:
        price = _get_price(r["details_json"])
        sa = r["signal_at"] or ""
        try:
            sig_dt = dt2.fromisoformat(sa.replace("Z", "+00:00"))
            mst_hour = sig_dt.astimezone(MST).hour
        except Exception:
            continue
        by_hour[mst_hour]["u"] += _unit_pnl(r["result"], price)
        if r["result"] == "won": by_hour[mst_hour]["w"] += 1
        elif r["result"] == "lost": by_hour[mst_hour]["l"] += 1
        else: by_hour[mst_hour]["p"] += 1
    for h in sorted(by_hour.keys()):
        c = by_hour[h]
        decided = c["w"] + c["l"]
        wr = f"{c['w']/decided:.0%}" if decided else "N/A"
        sign = "+" if c["u"] >= 0 else ""
        print(f"  {h:02d}:00 MST: {sign}{c['u']:.2f}u | {wr} ({c['w']}W/{c['l']}L/{c['p']}P)")

    # By MST hour per signal type
    print("\n=== BY MST HOUR PER TYPE ===")
    by_type_hour = defaultdict(lambda: defaultdict(lambda: {"u": 0.0, "w": 0, "l": 0, "p": 0}))
    for r in rows:
        price = _get_price(r["details_json"])
        sa = r["signal_at"] or ""
        try:
            sig_dt = dt2.fromisoformat(sa.replace("Z", "+00:00"))
            mst_hour = sig_dt.astimezone(MST).hour
        except Exception:
            continue
        stype = r["signal_type"]
        by_type_hour[stype][mst_hour]["u"] += _unit_pnl(r["result"], price)
        if r["result"] == "won": by_type_hour[stype][mst_hour]["w"] += 1
        elif r["result"] == "lost": by_type_hour[stype][mst_hour]["l"] += 1
        else: by_type_hour[stype][mst_hour]["p"] += 1
    for stype in sorted(by_type_hour.keys()):
        print(f"  --- {stype} ---")
        for h in sorted(by_type_hour[stype].keys()):
            c = by_type_hour[stype][h]
            decided = c["w"] + c["l"]
            wr = f"{c['w']/decided:.0%}" if decided else "N/A"
            sign = "+" if c["u"] >= 0 else ""
            print(f"    {h:02d}:00 MST: {sign}{c['u']:.2f}u | {wr} ({c['w']}W/{c['l']}L/{c['p']}P)")

    # By qualifier tags
    print("\n=== BY QUALIFIER TAGS ===")
    by_tags = defaultdict(lambda: {"u": 0.0, "w": 0, "l": 0, "p": 0})
    for r in rows:
        price = _get_price(r["details_json"])
        details = {}
        if r["details_json"]:
            try:
                details = json.loads(r["details_json"])
            except (json.JSONDecodeError, TypeError):
                pass
        tags = details.get("qualifier_tags", [])
        tag_key = " + ".join(sorted(tags)) if tags else "(none)"
        by_tags[tag_key]["u"] += _unit_pnl(r["result"], price)
        if r["result"] == "won": by_tags[tag_key]["w"] += 1
        elif r["result"] == "lost": by_tags[tag_key]["l"] += 1
        else: by_tags[tag_key]["p"] += 1
    for k, c in sorted(by_tags.items(), key=lambda x: x[1]["u"]):
        print(f"  {k}: {_fmt(c['u'], c['w'], c['l'], c['p'])}")

    # Suppressed signals (0 qualifiers — never sent to Discord)
    print("\n=== SUPPRESSED SIGNALS (0 qualifiers) ===")
    suppressed = 0
    sent = 0
    for r in rows:
        details = {}
        if r["details_json"]:
            try:
                details = json.loads(r["details_json"])
            except (json.JSONDecodeError, TypeError):
                pass
        qc = details.get("qualifier_count", 0)
        if qc == 0:
            suppressed += 1
        else:
            sent += 1
    print(f"  Sent to Discord: {sent}")
    print(f"  Suppressed (0 qualifiers): {suppressed}")
    print(f"  Total: {suppressed + sent}")

    # Also show NHL for comparison
    print("\n\n=== NHL SIGNAL ANALYSIS (for comparison) ===")
    nhl_rows = conn.execute("""
        SELECT sr.signal_type, sr.market_key, sr.result, sr.details_json
        FROM signal_results sr
        WHERE sr.sport_key = 'icehockey_nhl'
          AND sr.result IN ('won', 'lost', 'push')
    """).fetchall()

    if nhl_rows:
        print(f"({len(nhl_rows)} resolved)\n")
        by_combo2 = defaultdict(lambda: {"u": 0.0, "w": 0, "l": 0, "p": 0})
        for r in nhl_rows:
            price = _get_price(r["details_json"])
            k = f"{r['signal_type']}:{r['market_key']}"
            by_combo2[k]["u"] += _unit_pnl(r["result"], price)
            if r["result"] == "won": by_combo2[k]["w"] += 1
            elif r["result"] == "lost": by_combo2[k]["l"] += 1
            else: by_combo2[k]["p"] += 1
        for k, c in sorted(by_combo2.items(), key=lambda x: x[1]["u"]):
            print(f"  {k}: {_fmt(c['u'], c['w'], c['l'], c['p'])}")

    conn.close()


if __name__ == "__main__":
    run()
