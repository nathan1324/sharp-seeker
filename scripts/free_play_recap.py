"""Dump full free play history with graded results for recap analysis."""

import json
import sqlite3
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

DB = "/app/data/sharp_seeker.db"
MST = ZoneInfo("America/Phoenix")

MARKET_NAMES = {"spreads": "Spread", "totals": "Total", "h2h": "Moneyline"}
RESULT_EMOJI = {"won": "W", "lost": "L", "push": "P"}


def compute_units(price, result):
    """Compute units won/lost assuming bet-to-win-1u."""
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


def main():
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    rows = cur.execute("""
        SELECT sa.event_id, sa.alert_type, sa.market_key, sa.outcome_name,
               sa.sent_at, sa.details_json,
               sr.result, sr.signal_strength, sr.sport_key
        FROM sent_alerts sa
        LEFT JOIN signal_results sr
          ON sa.event_id = sr.event_id
         AND sa.alert_type = sr.signal_type
         AND sa.market_key = sr.market_key
         AND sa.outcome_name = sr.outcome_name
        WHERE sa.is_free_play = 1
        ORDER BY sa.sent_at ASC
    """).fetchall()

    wins = 0
    losses = 0
    pushes = 0
    pending = 0
    total_units = 0.0
    total = len(rows)

    print(f"=== FREE PLAY HISTORY ({total} picks) ===\n")

    for r in rows:
        row = dict(r)
        result = row["result"]
        if result == "won":
            wins += 1
        elif result == "lost":
            losses += 1
        elif result == "push":
            pushes += 1
        else:
            pending += 1

        # Parse sent time to MST
        sent_utc = datetime.fromisoformat(row["sent_at"])
        if sent_utc.tzinfo is None:
            sent_utc = sent_utc.replace(tzinfo=timezone.utc)
        sent_mst = sent_utc.astimezone(MST)
        date_str = sent_mst.strftime("%b %d %I:%M%p")

        # Parse details for matchup and odds
        details = json.loads(row["details_json"]) if row["details_json"] else {}
        value_books = details.get("value_books", [])
        best = value_books[0] if value_books else {}
        book = best.get("bookmaker", "?").title()
        price = best.get("price")
        point = best.get("point")

        market = row["market_key"]
        market_name = MARKET_NAMES.get(market, market)

        # Compute units for this pick
        pick_units = compute_units(price, result)
        total_units += pick_units
        if result in ("won", "lost"):
            unit_str = f"{pick_units:+.2f}u"
        else:
            unit_str = "     "

        # Format odds
        if market == "h2h" and price is not None:
            odds_str = f"{price:+.0f}"
        elif point is not None and price is not None:
            if market == "totals":
                odds_str = f"{point} ({price:+.0f})"
            else:
                odds_str = f"{point:+.1f} ({price:+.0f})"
        elif price is not None:
            odds_str = f"{price:+.0f}"
        else:
            odds_str = "?"

        tag = RESULT_EMOJI.get(result, "PENDING")
        sport = row.get("sport_key", "?")
        signal_type = row["alert_type"]

        # Try to get team names from details
        home = details.get("home_team", "")
        away = details.get("away_team", "")
        matchup = f"{away} @ {home}" if home and away else row["event_id"][:20]

        print(
            f"[{tag:7s}] {unit_str:7s} {date_str:16s} | {sport:25s} | {signal_type:25s} | "
            f"{market_name:10s} | {row['outcome_name']:20s} | "
            f"{odds_str:15s} @ {book:15s} | {matchup}"
        )

    print(f"\n=== SUMMARY ===")
    print(f"Record: {wins}-{losses} (W-L)")
    sign = "+" if total_units >= 0 else ""
    print(f"Units: {sign}{total_units:.2f}u (to-win-1u)")
    if pushes:
        print(f"Pushes: {pushes}")
    if pending:
        print(f"Pending: {pending}")
    resolved = wins + losses
    if resolved > 0:
        pct = wins / resolved * 100
        print(f"Win rate: {pct:.1f}% ({wins}/{resolved})")
    print()

    # Streaks
    results_list = [dict(r)["result"] for r in rows if dict(r)["result"] in ("won", "lost")]
    if results_list:
        # Current streak
        current = results_list[-1]
        streak = 0
        for res in reversed(results_list):
            if res == current:
                streak += 1
            else:
                break
        print(f"Current streak: {streak} {'wins' if current == 'won' else 'losses'} in a row")

        # Best win streak
        best_streak = 0
        cur_streak = 0
        for res in results_list:
            if res == "won":
                cur_streak += 1
                best_streak = max(best_streak, cur_streak)
            else:
                cur_streak = 0
        print(f"Best win streak: {best_streak}")

    # Helper to extract price from a row
    def _get_price(row):
        details = json.loads(row["details_json"]) if row.get("details_json") else {}
        vb = details.get("value_books", [])
        return vb[0].get("price") if vb else None

    # By signal type
    print(f"\n=== BY SIGNAL TYPE ===")
    type_stats = {}
    for r in rows:
        row = dict(r)
        st = row["alert_type"]
        if st not in type_stats:
            type_stats[st] = {"won": 0, "lost": 0, "push": 0, "pending": 0, "units": 0.0}
        result = row["result"]
        if result in ("won", "lost", "push"):
            type_stats[st][result] += 1
            type_stats[st]["units"] += compute_units(_get_price(row), result)
        else:
            type_stats[st]["pending"] += 1

    for st, stats in sorted(type_stats.items()):
        w, l = stats["won"], stats["lost"]
        u = stats["units"]
        resolved = w + l
        pct = f"{w/resolved*100:.0f}%" if resolved else "N/A"
        print(f"  {st:30s} {w}-{l} ({pct})  {u:+.2f}u")

    # By sport
    print(f"\n=== BY SPORT ===")
    sport_stats = {}
    for r in rows:
        row = dict(r)
        sp = row.get("sport_key") or "unknown"
        if sp not in sport_stats:
            sport_stats[sp] = {"won": 0, "lost": 0, "push": 0, "pending": 0, "units": 0.0}
        result = row["result"]
        if result in ("won", "lost", "push"):
            sport_stats[sp][result] += 1
            sport_stats[sp]["units"] += compute_units(_get_price(row), result)
        else:
            sport_stats[sp]["pending"] += 1

    for sp, stats in sorted(sport_stats.items()):
        w, l = stats["won"], stats["lost"]
        u = stats["units"]
        resolved = w + l
        pct = f"{w/resolved*100:.0f}%" if resolved else "N/A"
        print(f"  {sp:30s} {w}-{l} ({pct})  {u:+.2f}u")

    # By market
    print(f"\n=== BY MARKET ===")
    mkt_stats = {}
    for r in rows:
        row = dict(r)
        mk = row["market_key"]
        if mk not in mkt_stats:
            mkt_stats[mk] = {"won": 0, "lost": 0, "push": 0, "pending": 0, "units": 0.0}
        result = row["result"]
        if result in ("won", "lost", "push"):
            mkt_stats[mk][result] += 1
            mkt_stats[mk]["units"] += compute_units(_get_price(row), result)
        else:
            mkt_stats[mk]["pending"] += 1

    for mk, stats in sorted(mkt_stats.items()):
        w, l = stats["won"], stats["lost"]
        u = stats["units"]
        resolved = w + l
        pct = f"{w/resolved*100:.0f}%" if resolved else "N/A"
        name = MARKET_NAMES.get(mk, mk)
        print(f"  {name:30s} {w}-{l} ({pct})  {u:+.2f}u")

    # Recent 10
    print(f"\n=== LAST 10 RESOLVED ===")
    resolved_rows = [dict(r) for r in rows if dict(r)["result"] in ("won", "lost", "push")]
    for r in resolved_rows[-10:]:
        tag = RESULT_EMOJI.get(r["result"], "?")
        details = json.loads(r["details_json"]) if r["details_json"] else {}
        home = details.get("home_team", "")
        away = details.get("away_team", "")
        matchup = f"{away} @ {home}" if home and away else r["event_id"][:20]
        market_name = MARKET_NAMES.get(r["market_key"], r["market_key"])
        price = _get_price(r)
        u = compute_units(price, r["result"])
        print(f"  [{tag}] {u:+.2f}u  {matchup} — {market_name} {r['outcome_name']}")

    conn.close()


if __name__ == "__main__":
    main()
