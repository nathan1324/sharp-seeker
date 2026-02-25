"""Export yesterday's signal results."""

import asyncio
from datetime import datetime, timedelta, timezone

from sharp_seeker.config import Settings
from sharp_seeker.db.migrations import init_db
from sharp_seeker.db.repository import Repository


async def main():
    s = Settings()
    db = await init_db(s.db_path)
    repo = Repository(db)

    since = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()

    # Resolved signals
    resolved = await repo.get_resolved_signals_since(since)
    if resolved:
        print("=== Resolved Signals (last 24h) ===\n")
        for row in resolved:
            d = dict(row)
            teams = await repo.get_event_teams(d["event_id"])
            matchup = f"{teams[1]} vs {teams[0]}" if teams else d["event_id"]
            print(f"  {d['result'].upper():5s}  {matchup}  |  {d['signal_type']}  "
                  f"{d['market_key']} {d['outcome_name']}  |  strength={d['signal_strength']}")
    else:
        print("No resolved signals in the last 24h.")

    # Unresolved signals
    unresolved = await repo.get_unresolved_signals()
    if unresolved:
        print(f"\n=== Unresolved Signals ({len(unresolved)} total) ===\n")
        for row in unresolved:
            d = dict(row)
            teams = await repo.get_event_teams(d["event_id"])
            matchup = f"{teams[1]} vs {teams[0]}" if teams else d["event_id"]
            print(f"  PENDING  {matchup}  |  {d['signal_type']}  "
                  f"{d['market_key']} {d['outcome_name']}  |  strength={d['signal_strength']}")

    # Summary stats
    stats = await repo.get_performance_stats(since)
    if stats:
        print("\n=== Summary ===\n")
        total_w = total_l = total_p = 0
        for st, counts in sorted(stats.items()):
            w, l, p = counts.get("won", 0), counts.get("lost", 0), counts.get("push", 0)
            total_w += w
            total_l += l
            total_p += p
            decided = w + l
            rate = f"{w / decided:.0%}" if decided else "N/A"
            print(f"  {st}: {rate} ({w}W/{l}L/{p}P)")
        total_d = total_w + total_l
        overall = f"{total_w / total_d:.0%}" if total_d else "N/A"
        print(f"\n  Overall: {overall} ({total_w}W/{total_l}L/{total_p}P)")

    await db.close()


if __name__ == "__main__":
    asyncio.run(main())
