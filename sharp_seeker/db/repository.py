"""Data access layer for Sharp Seeker."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import aiosqlite
import structlog

log = structlog.get_logger()


class Repository:
    def __init__(self, db: aiosqlite.Connection) -> None:
        self._db = db

    # ── Odds snapshots ──────────────────────────────────────────────

    async def insert_snapshots(self, rows: list[dict[str, Any]]) -> int:
        """Bulk-insert snapshot rows, ignoring duplicates. Returns count inserted."""
        if not rows:
            return 0
        sql = """
            INSERT OR IGNORE INTO odds_snapshots
                (event_id, sport_key, home_team, away_team, commence_time,
                 bookmaker_key, market_key, outcome_name, price, point, fetched_at)
            VALUES
                (:event_id, :sport_key, :home_team, :away_team, :commence_time,
                 :bookmaker_key, :market_key, :outcome_name, :price, :point, :fetched_at)
        """
        cursor = await self._db.executemany(sql, rows)
        await self._db.commit()
        inserted = cursor.rowcount  # type: ignore[union-attr]
        log.debug("snapshots_inserted", count=inserted, total=len(rows))
        return inserted

    async def get_snapshots_since(
        self, event_id: str, since: str
    ) -> list[aiosqlite.Row]:
        """Get all snapshots for an event since a given ISO timestamp."""
        sql = """
            SELECT * FROM odds_snapshots
            WHERE event_id = ? AND fetched_at >= ?
            ORDER BY fetched_at ASC
        """
        cursor = await self._db.execute(sql, (event_id, since))
        return await cursor.fetchall()

    async def get_latest_snapshots(self, event_id: str) -> list[aiosqlite.Row]:
        """Get the most recent snapshot for each bookmaker/market/outcome combo."""
        sql = """
            SELECT * FROM odds_snapshots
            WHERE event_id = ? AND fetched_at = (
                SELECT MAX(fetched_at) FROM odds_snapshots WHERE event_id = ?
            )
        """
        cursor = await self._db.execute(sql, (event_id, event_id))
        return await cursor.fetchall()

    async def get_previous_snapshots(
        self, event_id: str, before: str
    ) -> list[aiosqlite.Row]:
        """Get the snapshot immediately before the given timestamp for each combo."""
        sql = """
            SELECT s.* FROM odds_snapshots s
            INNER JOIN (
                SELECT bookmaker_key, market_key, outcome_name, MAX(fetched_at) AS prev_at
                FROM odds_snapshots
                WHERE event_id = ? AND fetched_at < ?
                GROUP BY bookmaker_key, market_key, outcome_name
            ) prev ON s.event_id = ?
                AND s.bookmaker_key = prev.bookmaker_key
                AND s.market_key = prev.market_key
                AND s.outcome_name = prev.outcome_name
                AND s.fetched_at = prev.prev_at
        """
        cursor = await self._db.execute(sql, (event_id, before, event_id))
        return await cursor.fetchall()

    async def get_distinct_event_ids_at(self, fetched_at: str) -> list[str]:
        """Get all distinct event IDs from a specific fetch timestamp."""
        sql = "SELECT DISTINCT event_id FROM odds_snapshots WHERE fetched_at = ?"
        cursor = await self._db.execute(sql, (fetched_at,))
        rows = await cursor.fetchall()
        return [row["event_id"] for row in rows]

    # ── Sent alerts (dedup) ─────────────────────────────────────────

    async def was_alert_sent_recently(
        self,
        event_id: str,
        alert_type: str,
        market_key: str,
        outcome_name: str,
        cooldown_minutes: int,
    ) -> bool:
        """Check if an alert was sent within the cooldown window."""
        cutoff = datetime.now(timezone.utc).isoformat()
        sql = """
            SELECT 1 FROM sent_alerts
            WHERE event_id = ? AND alert_type = ? AND market_key = ? AND outcome_name = ?
              AND sent_at >= datetime(?, '-' || ? || ' minutes')
            LIMIT 1
        """
        cursor = await self._db.execute(
            sql, (event_id, alert_type, market_key, outcome_name, cutoff, cooldown_minutes)
        )
        return (await cursor.fetchone()) is not None

    async def record_alert(
        self,
        event_id: str,
        alert_type: str,
        market_key: str,
        outcome_name: str,
        details_json: str | None = None,
    ) -> None:
        now = datetime.now(timezone.utc).isoformat()
        sql = """
            INSERT INTO sent_alerts (event_id, alert_type, market_key, outcome_name, sent_at, details_json)
            VALUES (?, ?, ?, ?, ?, ?)
        """
        await self._db.execute(
            sql, (event_id, alert_type, market_key, outcome_name, now, details_json)
        )
        await self._db.commit()

    # ── API usage ───────────────────────────────────────────────────

    async def record_api_usage(
        self, endpoint: str, credits_used: int, credits_remaining: int
    ) -> None:
        now = datetime.now(timezone.utc).isoformat()
        sql = """
            INSERT INTO api_usage (timestamp, endpoint, credits_used, credits_remaining)
            VALUES (?, ?, ?, ?)
        """
        await self._db.execute(sql, (now, endpoint, credits_used, credits_remaining))
        await self._db.commit()

    async def get_credits_remaining(self) -> int | None:
        """Get the most recently recorded credits remaining."""
        sql = "SELECT credits_remaining FROM api_usage ORDER BY id DESC LIMIT 1"
        cursor = await self._db.execute(sql)
        row = await cursor.fetchone()
        return row["credits_remaining"] if row else None

    # ── Aggregate queries (for daily summaries) ────────────────────

    async def get_alerts_count_since(self, since: str) -> int:
        """Count alerts sent since the given ISO timestamp."""
        sql = "SELECT COUNT(*) AS cnt FROM sent_alerts WHERE sent_at >= ?"
        cursor = await self._db.execute(sql, (since,))
        row = await cursor.fetchone()
        return row["cnt"] if row else 0

    async def get_poll_count_since(self, since: str) -> int:
        """Count API polls since the given ISO timestamp."""
        sql = "SELECT COUNT(*) AS cnt FROM api_usage WHERE timestamp >= ?"
        cursor = await self._db.execute(sql, (since,))
        row = await cursor.fetchone()
        return row["cnt"] if row else 0
