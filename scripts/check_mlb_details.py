"""Check what's actually in signal_results.details_json for MLB signals."""

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


def run():
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row

    rows = conn.execute("""
        SELECT signal_type, market_key, outcome_name, result,
               signal_strength, details_json, signal_at
        FROM signal_results
        WHERE sport_key = 'baseball_mlb'
          AND result IN ('won', 'lost', 'push')
        ORDER BY signal_at ASC
    """).fetchall()

    # Manual unit calc per week
    from datetime import datetime
    from zoneinfo import ZoneInfo
    MST = ZoneInfo("America/Phoenix")

    by_week = defaultdict(lambda: {"u": 0.0, "w": 0, "l": 0, "p": 0,
                                    "prices": [], "no_price": 0})
    for r in rows:
        price = _get_price(r["details_json"])
        sa = r["signal_at"] or ""
        try:
            dt = datetime.fromisoformat(sa.replace("Z", "+00:00"))
            week = dt.strftime("%Y-W%U")
        except Exception:
            week = "unknown"

        pnl = _unit_pnl(r["result"], price)
        by_week[week]["u"] += pnl
        if r["result"] == "won":
            by_week[week]["w"] += 1
        elif r["result"] == "lost":
            by_week[week]["l"] += 1
        else:
            by_week[week]["p"] += 1
        if price is not None:
            by_week[week]["prices"].append(price)
        else:
            by_week[week]["no_price"] += 1

    print("=== UNIT PNL BY WEEK (manual calc) ===")
    for wk in sorted(by_week.keys()):
        c = by_week[wk]
        decided = c["w"] + c["l"]
        wr = f"{c['w']/decided:.0%}" if decided else "N/A"
        sign = "+" if c["u"] >= 0 else ""
        avg_price = sum(c["prices"]) / len(c["prices"]) if c["prices"] else 0
        print(f"  {wk}: {sign}{c['u']:.2f}u | {wr} "
              f"({c['w']}W/{c['l']}L/{c['p']}P) "
              f"avg_price={avg_price:.0f} "
              f"no_price={c['no_price']} "
              f"total_prices={len(c['prices'])}")

    # Show 5 individual won + 5 lost with their computed PnL
    print("\n=== SAMPLE INDIVIDUAL PNL ===")
    won_samples = [r for r in rows if r["result"] == "won"][-5:]
    lost_samples = [r for r in rows if r["result"] == "lost"][-5:]
    for r in won_samples + lost_samples:
        price = _get_price(r["details_json"])
        pnl = _unit_pnl(r["result"], price)
        print(f"  {r['result']:5s} price={price} pnl={pnl:+.2f} "
              f"{r['signal_type']} {r['market_key']}")

    conn.close()


if __name__ == "__main__":
    run()
