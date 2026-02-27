"""Analyze NHL Pinnacle divergence near-misses to find the right thresholds."""

import sqlite3
from collections import defaultdict
from datetime import datetime, timedelta, timezone

DB = "/app/data/sharp_seeker.db"
MST = timezone(timedelta(hours=-7))

PINNACLE_KEY = "pinnacle"
US_BOOKS = {"draftkings", "fanduel", "betmgm", "caesars", "williamhill_us"}
NHL_SPORT = "icehockey_nhl"

# Current thresholds
SPREAD_THRESHOLD = 1.0
ML_PROB_THRESHOLD = 0.03


def american_to_implied_prob(price: float) -> float:
    if price >= 100:
        return 100 / (price + 100)
    else:
        return abs(price) / (abs(price) + 100)


def run():
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row

    # Look at last 7 days of data
    since = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()

    print("=== NHL Pinnacle Divergence Analysis (last 7 days) ===\n")

    # Get all distinct fetch times for NHL
    cur = conn.execute(
        "SELECT DISTINCT fetched_at FROM odds_snapshots WHERE sport_key = ? AND fetched_at >= ? ORDER BY fetched_at",
        (NHL_SPORT, since),
    )
    fetch_times = [r["fetched_at"] for r in cur.fetchall()]
    print(f"Fetch cycles analyzed: {len(fetch_times)}\n")

    # Track divergences by bucket
    spread_deltas: list[dict] = []
    ml_deltas: list[dict] = []

    for fetched_at in fetch_times:
        cur = conn.execute(
            """SELECT event_id, bookmaker_key, market_key, outcome_name,
                      price, point, home_team, away_team
               FROM odds_snapshots
               WHERE sport_key = ? AND fetched_at = ?""",
            (NHL_SPORT, fetched_at),
        )
        rows = cur.fetchall()

        # Index by (event_id, market, outcome) → {book: row}
        by_market: dict[tuple, dict[str, dict]] = {}
        for r in rows:
            row = dict(r)
            key = (row["event_id"], row["market_key"], row["outcome_name"])
            by_market.setdefault(key, {})[row["bookmaker_key"]] = row

        for (event_id, market_key, outcome_name), books in by_market.items():
            pinnacle = books.get(PINNACLE_KEY)
            if pinnacle is None:
                continue

            for bm_key, row in books.items():
                if bm_key not in US_BOOKS:
                    continue

                if market_key == "h2h":
                    us_prob = american_to_implied_prob(row["price"])
                    pin_prob = american_to_implied_prob(pinnacle["price"])
                    delta = abs(us_prob - pin_prob)
                    # Check if US has better value (higher price = better payout)
                    us_better = row["price"] > pinnacle["price"]
                    if us_better and delta > 0.005:  # ignore trivial
                        ml_deltas.append({
                            "delta": delta,
                            "matchup": f"{row['away_team']} @ {row['home_team']}",
                            "book": bm_key,
                            "outcome": outcome_name,
                            "us_price": row["price"],
                            "pin_price": pinnacle["price"],
                            "fetched_at": fetched_at,
                        })
                elif market_key in ("spreads", "totals"):
                    if row["point"] is not None and pinnacle["point"] is not None:
                        delta = abs(row["point"] - pinnacle["point"])
                        # Check direction
                        if market_key == "spreads":
                            us_better = row["point"] > pinnacle["point"]
                        elif outcome_name.lower() == "over":
                            us_better = row["point"] < pinnacle["point"]
                        else:
                            us_better = row["point"] > pinnacle["point"]
                        if us_better and delta > 0.1:  # ignore trivial
                            spread_deltas.append({
                                "delta": delta,
                                "market": market_key,
                                "matchup": f"{row['away_team']} @ {row['home_team']}",
                                "book": bm_key,
                                "outcome": outcome_name,
                                "us_point": row["point"],
                                "pin_point": pinnacle["point"],
                                "fetched_at": fetched_at,
                            })

    # --- Spread/total analysis ---
    print("── Spread/Total Divergences (US book better) ──")
    if spread_deltas:
        # Bucket by threshold
        buckets = [0.25, 0.5, 0.75, 1.0, 1.5, 2.0]
        for threshold in buckets:
            count = sum(1 for d in spread_deltas if d["delta"] >= threshold)
            marker = " ← current" if threshold == SPREAD_THRESHOLD else ""
            print(f"  >= {threshold:.2f} pts:  {count:5d} occurrences{marker}")

        # Show top 10 largest
        print(f"\n  Top 10 largest spread/total divergences:")
        for d in sorted(spread_deltas, key=lambda x: -x["delta"])[:10]:
            dt = datetime.fromisoformat(d["fetched_at"]).astimezone(MST)
            print(f"    {dt.strftime('%m/%d %I:%M %p')}  {d['delta']:.1f} pts  "
                  f"{d['matchup']}  {d['outcome']} {d['market']}  "
                  f"({d['book']}: {d['us_point']} vs pin: {d['pin_point']})")
    else:
        print("  No spread/total divergences found.")

    # --- ML analysis ---
    print(f"\n── Moneyline Divergences (US book better) ──")
    if ml_deltas:
        buckets = [0.01, 0.015, 0.02, 0.025, 0.03, 0.04, 0.05]
        for threshold in buckets:
            count = sum(1 for d in ml_deltas if d["delta"] >= threshold)
            marker = " ← current" if threshold == ML_PROB_THRESHOLD else ""
            print(f"  >= {threshold:.1%} prob:  {count:5d} occurrences{marker}")

        # Show top 10 largest
        print(f"\n  Top 10 largest ML divergences:")
        for d in sorted(ml_deltas, key=lambda x: -x["delta"])[:10]:
            dt = datetime.fromisoformat(d["fetched_at"]).astimezone(MST)
            print(f"    {dt.strftime('%m/%d %I:%M %p')}  {d['delta']:.2%}  "
                  f"{d['matchup']}  {d['outcome']}  "
                  f"({d['book']}: {d['us_price']:+.0f} vs pin: {d['pin_price']:+.0f})")
    else:
        print("  No ML divergences found.")

    # --- How many actual NHL PD alerts fired ---
    cur = conn.execute(
        """SELECT COUNT(*) FROM sent_alerts
           WHERE alert_type = 'pinnacle_divergence'
             AND event_id IN (
                 SELECT DISTINCT event_id FROM odds_snapshots WHERE sport_key = ?
             )
             AND sent_at >= ?""",
        (NHL_SPORT, since),
    )
    fired = cur.fetchone()[0]
    print(f"\n── NHL PD alerts that actually fired (last 7 days): {fired} ──")

    conn.close()


if __name__ == "__main__":
    run()
