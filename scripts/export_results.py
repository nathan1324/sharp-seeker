"""Export yesterday's signal results."""

import asyncio
import json
from datetime import datetime, timedelta, timezone

from sharp_seeker.config import Settings
from sharp_seeker.db.migrations import init_db
from sharp_seeker.db.repository import Repository


def _format_odds(d):
    """Extract best odds string from details_json."""
    details_raw = d.get("details_json")
    if not details_raw:
        return ""
    try:
        details = json.loads(details_raw) if isinstance(details_raw, str) else details_raw
        vb = details.get("value_books", [])
        if not vb:
            return ""
        best = vb[0]
        parts = []
        if best.get("point") is not None:
            parts.append(f"{best['point']:+g}")
        if best.get("price") is not None:
            parts.append(f"({best['price']:+g})")
        bm = best.get("bookmaker", "").title()
        return f"{' '.join(parts)} @ {bm}" if parts else ""
    except (json.JSONDecodeError, TypeError):
        return ""


def _sport_label(sport_key):
    parts = sport_key.split("_", 1)
    return parts[-1].upper() if len(parts) > 1 else sport_key.upper()


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
            sport = _sport_label(d.get("sport_key", ""))
            odds = _format_odds(d)
            print(f"  {d['result'].upper():5s}  {sport:6s}  {matchup}  |  {d['signal_type']}  "
                  f"{d['market_key']} {d['outcome_name']}  |  {odds}  |  strength={d['signal_strength']}")
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
            sport = _sport_label(d.get("sport_key", ""))
            odds = _format_odds(d)
            print(f"  PENDING  {sport:6s}  {matchup}  |  {d['signal_type']}  "
                  f"{d['market_key']} {d['outcome_name']}  |  {odds}  |  strength={d['signal_strength']}")

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
