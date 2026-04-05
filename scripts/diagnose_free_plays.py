"""Diagnose free play performance — unit P/L assuming every play is to win 1u."""

import json
import sqlite3
from collections import defaultdict
from datetime import datetime

DB = "/app/data/sharp_seeker.db"


def _get_price(details_json):
    """Extract best book price (American odds) from details_json."""
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
    """Calculate unit P/L for a single play sized to win 1u.

    To win 1u:
      - Favorite (price < 0): risk |price|/100, win +1u, lose -|price|/100
      - Underdog (price > 0): risk 100/price, win +1u, lose -100/price
      - Even (price = 100/-100): risk 1u
    """
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


def _fmt(units, w, l, p, n_no_odds=0):
    """Format a summary line with units, record, and avg odds."""
    decided = w + l
    wr = f"{w/decided:.0%}" if decided else "N/A"
    sign = "+" if units >= 0 else ""
    no_odds_note = f" ({n_no_odds} missing odds)" if n_no_odds else ""
    return f"{sign}{units:.2f}u | {wr} ({w}W/{l}L/{p}P){no_odds_note}"


def _parse_details(row):
    details = {}
    if row["details_json"]:
        try:
            details = json.loads(row["details_json"])
        except (json.JSONDecodeError, TypeError):
            pass
    return details


def run():
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row

    rows = conn.execute("""
        SELECT sa.event_id, sa.market_key, sa.outcome_name, sa.sent_at,
               sa.details_json, sa.alert_type,
               sr.result, sr.signal_strength,
               sr.sport_key
        FROM sent_alerts sa
        LEFT JOIN signal_results sr
          ON sa.event_id = sr.event_id
         AND sa.alert_type = sr.signal_type
         AND sa.market_key = sr.market_key
         AND sa.outcome_name = sr.outcome_name
        WHERE sa.is_free_play = 1
        ORDER BY sa.sent_at ASC
    """).fetchall()

    if not rows:
        print("No free plays found.")
        return

    resolved = [r for r in rows if r["result"] in ("won", "lost", "push")]
    pending = [r for r in rows if r["result"] is None]

    print(f"=== FREE PLAY UNIT ANALYSIS ({len(rows)} total, {len(pending)} pending) ===\n")

    # --- Overall ---
    total_units = 0.0
    total_w, total_l, total_p, total_no_odds = 0, 0, 0, 0
    for r in resolved:
        price = _get_price(r["details_json"])
        total_units += _unit_pnl(r["result"], price)
        if r["result"] == "won": total_w += 1
        elif r["result"] == "lost": total_l += 1
        else: total_p += 1
        if price is None and r["result"] != "push": total_no_odds += 1

    print(f"Overall: {_fmt(total_units, total_w, total_l, total_p, total_no_odds)}\n")

    # --- By week ---
    print("=== BY WEEK ===")
    weekly = defaultdict(lambda: {"units": 0.0, "w": 0, "l": 0, "p": 0, "no_odds": 0})
    for r in resolved:
        dt = datetime.fromisoformat(r["sent_at"].replace("Z", "+00:00"))
        week = dt.strftime("%Y-W%U")
        price = _get_price(r["details_json"])
        weekly[week]["units"] += _unit_pnl(r["result"], price)
        if r["result"] == "won": weekly[week]["w"] += 1
        elif r["result"] == "lost": weekly[week]["l"] += 1
        else: weekly[week]["p"] += 1
        if price is None and r["result"] != "push": weekly[week]["no_odds"] += 1

    for week in sorted(weekly.keys()):
        c = weekly[week]
        print(f"  {week}: {_fmt(c['units'], c['w'], c['l'], c['p'], c['no_odds'])}")

    # --- By signal type ---
    print("\n=== BY SIGNAL TYPE ===")
    by_type = defaultdict(lambda: {"units": 0.0, "w": 0, "l": 0, "p": 0, "no_odds": 0})
    for r in resolved:
        price = _get_price(r["details_json"])
        k = r["alert_type"]
        by_type[k]["units"] += _unit_pnl(r["result"], price)
        if r["result"] == "won": by_type[k]["w"] += 1
        elif r["result"] == "lost": by_type[k]["l"] += 1
        else: by_type[k]["p"] += 1
        if price is None and r["result"] != "push": by_type[k]["no_odds"] += 1
    for st, c in sorted(by_type.items(), key=lambda x: x[1]["units"]):
        print(f"  {st}: {_fmt(c['units'], c['w'], c['l'], c['p'], c['no_odds'])}")

    # --- By sport ---
    print("\n=== BY SPORT ===")
    by_sport = defaultdict(lambda: {"units": 0.0, "w": 0, "l": 0, "p": 0, "no_odds": 0})
    for r in resolved:
        price = _get_price(r["details_json"])
        sport = r["sport_key"] or _parse_details(r).get("sport_key", "unknown")
        by_sport[sport]["units"] += _unit_pnl(r["result"], price)
        if r["result"] == "won": by_sport[sport]["w"] += 1
        elif r["result"] == "lost": by_sport[sport]["l"] += 1
        else: by_sport[sport]["p"] += 1
        if price is None and r["result"] != "push": by_sport[sport]["no_odds"] += 1
    for sp, c in sorted(by_sport.items(), key=lambda x: x[1]["units"]):
        print(f"  {sp}: {_fmt(c['units'], c['w'], c['l'], c['p'], c['no_odds'])}")

    # --- By market ---
    print("\n=== BY MARKET ===")
    by_market = defaultdict(lambda: {"units": 0.0, "w": 0, "l": 0, "p": 0, "no_odds": 0})
    for r in resolved:
        price = _get_price(r["details_json"])
        k = r["market_key"]
        by_market[k]["units"] += _unit_pnl(r["result"], price)
        if r["result"] == "won": by_market[k]["w"] += 1
        elif r["result"] == "lost": by_market[k]["l"] += 1
        else: by_market[k]["p"] += 1
        if price is None and r["result"] != "push": by_market[k]["no_odds"] += 1
    for mk, c in sorted(by_market.items(), key=lambda x: x[1]["units"]):
        print(f"  {mk}: {_fmt(c['units'], c['w'], c['l'], c['p'], c['no_odds'])}")

    # --- By qualifier tags ---
    print("\n=== BY QUALIFIER TAGS ===")
    by_tags = defaultdict(lambda: {"units": 0.0, "w": 0, "l": 0, "p": 0, "no_odds": 0})
    for r in resolved:
        price = _get_price(r["details_json"])
        details = _parse_details(r)
        tags = details.get("qualifier_tags", [])
        tag_key = " + ".join(sorted(tags)) if tags else "(none)"
        by_tags[tag_key]["units"] += _unit_pnl(r["result"], price)
        if r["result"] == "won": by_tags[tag_key]["w"] += 1
        elif r["result"] == "lost": by_tags[tag_key]["l"] += 1
        else: by_tags[tag_key]["p"] += 1
        if price is None and r["result"] != "push": by_tags[tag_key]["no_odds"] += 1
    for tags, c in sorted(by_tags.items(), key=lambda x: x[1]["units"]):
        print(f"  {tags}: {_fmt(c['units'], c['w'], c['l'], c['p'], c['no_odds'])}")

    # --- By type:sport:market combo ---
    print("\n=== BY TYPE:SPORT:MARKET COMBO ===")
    by_combo = defaultdict(lambda: {"units": 0.0, "w": 0, "l": 0, "p": 0, "no_odds": 0})
    for r in resolved:
        price = _get_price(r["details_json"])
        sport = r["sport_key"] or _parse_details(r).get("sport_key", "unknown")
        combo = f"{r['alert_type']}:{sport}:{r['market_key']}"
        by_combo[combo]["units"] += _unit_pnl(r["result"], price)
        if r["result"] == "won": by_combo[combo]["w"] += 1
        elif r["result"] == "lost": by_combo[combo]["l"] += 1
        else: by_combo[combo]["p"] += 1
        if price is None and r["result"] != "push": by_combo[combo]["no_odds"] += 1
    for combo, c in sorted(by_combo.items(), key=lambda x: x[1]["units"]):
        print(f"  {combo}: {_fmt(c['units'], c['w'], c['l'], c['p'], c['no_odds'])}")

    # --- Recent individual results ---
    print("\n=== LAST 4 WEEKS — INDIVIDUAL FREE PLAYS ===")
    recent = resolved[-60:]
    running = 0.0
    for r in recent:
        details = _parse_details(r)
        price = _get_price(r["details_json"])
        sport = r["sport_key"] or details.get("sport_key", "unknown")
        tags = details.get("qualifier_tags", [])
        tag_str = "+".join(t[:5] for t in tags)
        pnl = _unit_pnl(r["result"], price)
        running += pnl
        emoji = {"won": "W", "lost": "L", "push": "P"}.get(r["result"], "?")
        odds_str = f"{int(price):+d}" if price else "n/a"
        dt = r["sent_at"][:16]
        print(f"  {emoji} {pnl:+6.2f}u (run:{running:+6.2f}u) {dt} {r['alert_type'][:8]:8s} {sport:25s} {r['market_key']:8s} {r['outcome_name']:20s} {odds_str:>6s} [{tag_str}]")

    # --- Strength buckets ---
    print("\n=== BY STRENGTH BUCKET ===")
    by_str = defaultdict(lambda: {"units": 0.0, "w": 0, "l": 0, "p": 0})
    for r in resolved:
        price = _get_price(r["details_json"])
        s = r["signal_strength"] or 0
        if s >= 0.60:
            bucket = "0.60+"
        elif s >= 0.40:
            bucket = "0.40-0.59"
        else:
            bucket = "<0.40"
        by_str[bucket]["units"] += _unit_pnl(r["result"], price)
        if r["result"] == "won": by_str[bucket]["w"] += 1
        elif r["result"] == "lost": by_str[bucket]["l"] += 1
        else: by_str[bucket]["p"] += 1
    for bucket in ["0.60+", "0.40-0.59", "<0.40"]:
        if bucket in by_str:
            c = by_str[bucket]
            print(f"  {bucket}: {_fmt(c['units'], c['w'], c['l'], c['p'])}")

    # --- Odds range buckets ---
    print("\n=== BY ODDS RANGE ===")
    by_odds = defaultdict(lambda: {"units": 0.0, "w": 0, "l": 0, "p": 0})
    for r in resolved:
        price = _get_price(r["details_json"])
        if price is None:
            bucket = "no_odds"
        elif price <= -200:
            bucket = "heavy fav (-200+)"
        elif price < -100:
            bucket = "fav (-199 to -101)"
        elif price <= 100:
            bucket = "pick'em (-100 to +100)"
        elif price <= 200:
            bucket = "dog (+101 to +200)"
        else:
            bucket = "big dog (+200+)"
        by_odds[bucket]["units"] += _unit_pnl(r["result"], price)
        if r["result"] == "won": by_odds[bucket]["w"] += 1
        elif r["result"] == "lost": by_odds[bucket]["l"] += 1
        else: by_odds[bucket]["p"] += 1
    for bucket in ["heavy fav (-200+)", "fav (-199 to -101)", "pick'em (-100 to +100)", "dog (+101 to +200)", "big dog (+200+)", "no_odds"]:
        if bucket in by_odds:
            c = by_odds[bucket]
            print(f"  {bucket}: {_fmt(c['units'], c['w'], c['l'], c['p'])}")

    conn.close()


if __name__ == "__main__":
    run()
