"""Compare today's recap: old sent_at logic vs new resolved_at logic."""
import asyncio
import sqlite3
from datetime import datetime, timedelta, timezone

DB_PATH = "/app/data/sharp_seeker.db"


def main():
    now = datetime.now(timezone.utc)
    since_24h = (now - timedelta(hours=24)).isoformat()

    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row

    # Old logic: sent_at >= now - 24h
    old_rows = db.execute("""
        SELECT sa.event_id, sa.market_key, sa.outcome_name, sa.sent_at,
               sr.result, sr.signal_strength, sr.resolved_at
        FROM sent_alerts sa
        LEFT JOIN signal_results sr
          ON sa.event_id = sr.event_id
         AND sa.alert_type = sr.signal_type
         AND sa.market_key = sr.market_key
         AND sa.outcome_name = sr.outcome_name
        WHERE sa.is_free_play = 1
          AND sa.sent_at >= ?
        ORDER BY sa.sent_at ASC
    """, (since_24h,)).fetchall()

    # New logic: resolved_at >= now - 24h
    new_rows = db.execute("""
        SELECT sa.event_id, sa.market_key, sa.outcome_name, sa.sent_at,
               sr.result, sr.signal_strength, sr.resolved_at
        FROM sent_alerts sa
        JOIN signal_results sr
          ON sa.event_id = sr.event_id
         AND sa.alert_type = sr.signal_type
         AND sa.market_key = sr.market_key
         AND sa.outcome_name = sr.outcome_name
        WHERE sa.is_free_play = 1
          AND sr.resolved_at >= ?
        ORDER BY sa.sent_at ASC
    """, (since_24h,)).fetchall()

    db.close()

    def show(label, rows):
        print(f"\n{'='*60}")
        print(f" {label} ({len(rows)} plays)")
        print(f"{'='*60}")
        w = l = 0
        units = 0.0
        for r in rows:
            d = dict(r)
            result = d.get("result") or "PENDING"
            name = d["outcome_name"]
            market = d["market_key"]
            sent = d["sent_at"][:16]
            resolved = (d.get("resolved_at") or "")[:16]
            emoji = {"won": "W", "lost": "L", "push": "P"}.get(result, "?")
            print(f"  [{emoji}] {name} ({market}) | sent={sent} resolved={resolved}")
            if result == "won":
                w += 1
                units += 0.91
            elif result == "lost":
                l += 1
                units -= 1.0
        print(f"\n  Record: {w}-{l}  |  Units: {units:+.2f}u")

    show("OLD (sent_at) — what today's recap showed", old_rows)
    show("NEW (resolved_at) — what it would have shown", new_rows)

    # Show plays only in new but not old
    old_ids = {(dict(r)["event_id"], dict(r)["outcome_name"]) for r in old_rows}
    new_ids = {(dict(r)["event_id"], dict(r)["outcome_name"]) for r in new_rows}
    missing = new_ids - old_ids
    if missing:
        print(f"\n  ** {len(missing)} play(s) missed by old logic:")
        for eid, name in missing:
            print(f"     - {name} ({eid})")


if __name__ == "__main__":
    main()
