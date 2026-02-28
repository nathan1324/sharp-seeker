"""Diagnose why NHL PD signals aren't firing despite the sport override."""

import sqlite3
from datetime import datetime, timedelta, timezone

DB = "/app/data/sharp_seeker.db"
MST = timezone(timedelta(hours=-7))
NHL_SPORT = "icehockey_nhl"
PINNACLE_KEY = "pinnacle"
US_BOOKS = {"draftkings", "fanduel", "betmgm", "caesars", "williamhill_us"}


def american_to_implied_prob(price):
    if price >= 100:
        return 100 / (price + 100)
    else:
        return abs(price) / (abs(price) + 100)


def run():
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row

    since_24h = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
    since_7d = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()

    # 1. Check if config override is being loaded
    print("=" * 60)
    print("1. CONFIG CHECK")
    print("=" * 60)
    try:
        from sharp_seeker.config import Settings
        s = Settings()
        print(f"   pd_sport_ml_prob_overrides = {s.pd_sport_ml_prob_overrides}")
        print(f"   pinnacle_ml_prob_threshold = {s.pinnacle_ml_prob_threshold}")
        nhl_threshold = s.pd_sport_ml_prob_overrides.get(
            NHL_SPORT, s.pinnacle_ml_prob_threshold
        )
        print(f"   Effective NHL ML threshold = {nhl_threshold}")
        if NHL_SPORT not in s.pd_sport_ml_prob_overrides:
            print("   *** WARNING: No NHL override found! Using global threshold. ***")
    except Exception as e:
        print(f"   Could not load Settings: {e}")
        nhl_threshold = 0.03
    print()

    # 2. Check if NHL data is being polled
    print("=" * 60)
    print("2. NHL DATA (last 24h)")
    print("=" * 60)
    cur = conn.execute(
        "SELECT COUNT(*) FROM odds_snapshots WHERE sport_key = ? AND fetched_at >= ?",
        (NHL_SPORT, since_24h),
    )
    snap_count = cur.fetchone()[0]
    print(f"   NHL snapshots (24h): {snap_count}")

    cur = conn.execute(
        "SELECT COUNT(DISTINCT event_id) FROM odds_snapshots WHERE sport_key = ? AND fetched_at >= ?",
        (NHL_SPORT, since_24h),
    )
    event_count = cur.fetchone()[0]
    print(f"   NHL events (24h): {event_count}")

    cur = conn.execute(
        "SELECT COUNT(DISTINCT bookmaker_key) FROM odds_snapshots WHERE sport_key = ? AND fetched_at >= ?",
        (NHL_SPORT, since_24h),
    )
    book_count = cur.fetchone()[0]
    print(f"   Distinct books: {book_count}")

    cur = conn.execute(
        """SELECT DISTINCT bookmaker_key FROM odds_snapshots
           WHERE sport_key = ? AND fetched_at >= ?
           ORDER BY bookmaker_key""",
        (NHL_SPORT, since_24h),
    )
    books = [r[0] for r in cur.fetchall()]
    print(f"   Books: {', '.join(books)}")
    if PINNACLE_KEY not in books:
        print("   *** WARNING: Pinnacle not in NHL data! No PD signals possible. ***")
    print()

    # 3. Check for NHL h2h divergences right now
    print("=" * 60)
    print("3. NHL H2H DIVERGENCES (last 24h, threshold={:.1%})".format(nhl_threshold))
    print("=" * 60)

    cur = conn.execute(
        "SELECT DISTINCT fetched_at FROM odds_snapshots WHERE sport_key = ? AND fetched_at >= ? ORDER BY fetched_at DESC",
        (NHL_SPORT, since_24h),
    )
    fetch_times = [r[0] for r in cur.fetchall()]

    near_misses = []
    would_fire = []

    for fetched_at in fetch_times:
        cur = conn.execute(
            """SELECT event_id, bookmaker_key, market_key, outcome_name,
                      price, point, home_team, away_team
               FROM odds_snapshots
               WHERE sport_key = ? AND fetched_at = ? AND market_key = 'h2h'""",
            (NHL_SPORT, fetched_at),
        )
        rows = [dict(r) for r in cur.fetchall()]

        by_outcome = {}
        for r in rows:
            key = (r["event_id"], r["outcome_name"])
            by_outcome.setdefault(key, {})[r["bookmaker_key"]] = r

        for (event_id, outcome), bks in by_outcome.items():
            pin = bks.get(PINNACLE_KEY)
            if not pin:
                continue
            for bm, row in bks.items():
                if bm not in US_BOOKS:
                    continue
                us_prob = american_to_implied_prob(row["price"])
                pin_prob = american_to_implied_prob(pin["price"])
                delta = abs(us_prob - pin_prob)
                us_better = row["price"] > pin["price"]
                if not us_better:
                    continue

                matchup = "{} @ {}".format(row["away_team"], row["home_team"])
                entry = {
                    "delta": delta,
                    "matchup": matchup,
                    "outcome": outcome,
                    "book": bm,
                    "us_price": row["price"],
                    "pin_price": pin["price"],
                    "fetched_at": fetched_at,
                }

                if delta >= nhl_threshold:
                    would_fire.append(entry)
                elif delta >= 0.01:
                    near_misses.append(entry)

    print(f"   Would fire (>= {nhl_threshold:.1%}): {len(would_fire)}")
    print(f"   Near misses (1%-{nhl_threshold:.1%}): {len(near_misses)}")

    if would_fire:
        print(f"\n   Top 10 that SHOULD fire:")
        for d in sorted(would_fire, key=lambda x: -x["delta"])[:10]:
            dt = datetime.fromisoformat(d["fetched_at"]).astimezone(MST)
            print(
                "     {} {:.2%}  {}  {} ({}: {:+.0f} vs pin: {:+.0f})".format(
                    dt.strftime("%m/%d %I:%M %p"), d["delta"],
                    d["matchup"], d["outcome"],
                    d["book"], d["us_price"], d["pin_price"],
                )
            )
    print()

    # 4. Check sent alerts — any NHL PD?
    print("=" * 60)
    print("4. NHL PD ALERTS SENT")
    print("=" * 60)
    cur = conn.execute(
        """SELECT COUNT(*) FROM sent_alerts sa
           WHERE sa.alert_type = 'pinnacle_divergence'
             AND sa.sent_at >= ?
             AND EXISTS (
                 SELECT 1 FROM odds_snapshots os
                 WHERE os.event_id = sa.event_id AND os.sport_key = ?
             )""",
        (since_7d, NHL_SPORT),
    )
    fired_7d = cur.fetchone()[0]
    print(f"   NHL PD alerts (7 days): {fired_7d}")

    cur = conn.execute(
        """SELECT COUNT(*) FROM sent_alerts sa
           WHERE sa.alert_type = 'pinnacle_divergence'
             AND sa.sent_at >= ?
             AND EXISTS (
                 SELECT 1 FROM odds_snapshots os
                 WHERE os.event_id = sa.event_id AND os.sport_key = ?
             )""",
        (since_24h, NHL_SPORT),
    )
    fired_24h = cur.fetchone()[0]
    print(f"   NHL PD alerts (24h): {fired_24h}")
    print()

    # 5. Check pipeline filters — are NHL signals being generated but filtered?
    print("=" * 60)
    print("5. PIPELINE FILTER CHECK")
    print("=" * 60)
    try:
        from sharp_seeker.config import Settings
        s = Settings()
        # Min strength for PD
        sport_key_str = "pinnacle_divergence:" + NHL_SPORT
        min_str = s.signal_sport_strength_overrides.get(
            sport_key_str,
            s.signal_strength_overrides.get("pinnacle_divergence", s.min_signal_strength),
        )
        print(f"   Min strength for NHL PD: {min_str}")

        # Max strength cap
        max_str = s.max_signal_strength_overrides.get("pinnacle_divergence")
        print(f"   Max strength cap for PD: {max_str or 'none'}")

        # Quiet hours
        pd_quiet = s.signal_quiet_hours.get("pinnacle_divergence", [])
        print(f"   PD quiet hours (UTC): {pd_quiet}")

        # Excluded books
        print(f"   PD excluded books: {s.pd_excluded_books}")

        # Check strength of signals that would fire
        if would_fire:
            for d in would_fire[:5]:
                strength = min(1.0, d["delta"] / (nhl_threshold * 3))
                status = "PASS" if min_str <= strength < (max_str or 1.0) else "FILTERED"
                print(
                    "   Signal strength {:.2f} -> {} (min={}, max={})".format(
                        strength, status, min_str, max_str or "none"
                    )
                )
    except Exception as e:
        print(f"   Could not check pipeline: {e}")

    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    run()
