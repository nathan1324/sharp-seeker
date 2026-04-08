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
                 bookmaker_key, market_key, outcome_name, price, point, deep_link, fetched_at)
            VALUES
                (:event_id, :sport_key, :home_team, :away_team, :commence_time,
                 :bookmaker_key, :market_key, :outcome_name, :price, :point, :deep_link, :fetched_at)
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
        cooldown_minutes: int,
        outcome_name: str | None = None,
    ) -> bool:
        """Check if an alert was sent within the cooldown window.

        When outcome_name is None, checks at market level (any side).
        """
        cutoff = datetime.now(timezone.utc).isoformat()
        if outcome_name is not None:
            sql = """
                SELECT 1 FROM sent_alerts
                WHERE event_id = ? AND alert_type = ? AND market_key = ? AND outcome_name = ?
                  AND sent_at >= datetime(?, '-' || ? || ' minutes')
                LIMIT 1
            """
            params = (event_id, alert_type, market_key, outcome_name, cutoff, cooldown_minutes)
        else:
            sql = """
                SELECT 1 FROM sent_alerts
                WHERE event_id = ? AND alert_type = ? AND market_key = ?
                  AND sent_at >= datetime(?, '-' || ? || ' minutes')
                LIMIT 1
            """
            params = (event_id, alert_type, market_key, cutoff, cooldown_minutes)
        cursor = await self._db.execute(sql, params)
        return (await cursor.fetchone()) is not None

    async def record_alert(
        self,
        event_id: str,
        alert_type: str,
        market_key: str,
        outcome_name: str,
        details_json: str | None = None,
        is_free_play: bool = False,
    ) -> None:
        now = datetime.now(timezone.utc).isoformat()
        sql = """
            INSERT INTO sent_alerts (event_id, alert_type, market_key, outcome_name, sent_at, details_json, is_free_play)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        await self._db.execute(
            sql, (event_id, alert_type, market_key, outcome_name, now, details_json, int(is_free_play))
        )
        await self._db.commit()

    async def get_free_play_event_ids(self) -> set[str]:
        """Return event IDs of all past free play alerts."""
        sql = "SELECT DISTINCT event_id FROM sent_alerts WHERE is_free_play = 1"
        cursor = await self._db.execute(sql)
        rows = await cursor.fetchall()
        return {row[0] for row in rows}

    async def count_free_plays_since(self, since: str) -> int:
        """Count free plays sent since the given ISO timestamp."""
        sql = "SELECT COUNT(*) FROM sent_alerts WHERE is_free_play = 1 AND sent_at >= ?"
        cursor = await self._db.execute(sql, (since,))
        row = await cursor.fetchone()
        return row[0] if row else 0

    async def get_free_play_details_since(self, since: str) -> list[dict]:
        """Get sport_key for each free play since the given timestamp.

        Extracts sport from the event's snapshots since sent_alerts doesn't
        store sport_key directly.
        """
        sql = """
            SELECT sa.event_id, sr.sport_key
            FROM sent_alerts sa
            LEFT JOIN signal_results sr
              ON sa.event_id = sr.event_id
             AND sa.alert_type = sr.signal_type
             AND sa.market_key = sr.market_key
             AND sa.outcome_name = sr.outcome_name
            WHERE sa.is_free_play = 1 AND sa.sent_at >= ?
            GROUP BY sa.event_id
        """
        cursor = await self._db.execute(sql, (since,))
        rows = await cursor.fetchall()
        return [{"event_id": r[0], "sport_key": r[1] or ""} for r in rows]

    async def mark_alert_free_play(
        self, event_id: str, market_key: str, outcome_name: str,
    ) -> None:
        """Set is_free_play=1 on the most recent matching sent_alerts row."""
        sql = """
            UPDATE sent_alerts SET is_free_play = 1
            WHERE id = (
                SELECT id FROM sent_alerts
                WHERE event_id = ? AND market_key = ? AND outcome_name = ?
                ORDER BY sent_at DESC LIMIT 1
            )
        """
        await self._db.execute(sql, (event_id, market_key, outcome_name))
        await self._db.commit()

    async def count_alerts_by_type(self, alert_type: str) -> int:
        """Count total sent alerts of a given type."""
        sql = "SELECT COUNT(*) AS cnt FROM sent_alerts WHERE alert_type = ?"
        cursor = await self._db.execute(sql, (alert_type,))
        row = await cursor.fetchone()
        return row["cnt"] if row else 0

    async def count_alerts_by_types(self, alert_types: list[str]) -> int:
        """Count total sent alerts across multiple types."""
        placeholders = ",".join("?" for _ in alert_types)
        sql = f"SELECT COUNT(*) AS cnt FROM sent_alerts WHERE alert_type IN ({placeholders})"
        cursor = await self._db.execute(sql, alert_types)
        row = await cursor.fetchone()
        return row["cnt"] if row else 0

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

    # ── Backtesting ────────────────────────────────────────────────

    async def get_distinct_fetch_times(
        self, start: str, end: str
    ) -> list[str]:
        """Get all distinct fetched_at timestamps in a date range."""
        sql = """
            SELECT DISTINCT fetched_at FROM odds_snapshots
            WHERE fetched_at >= ? AND fetched_at <= ?
            ORDER BY fetched_at ASC
        """
        cursor = await self._db.execute(sql, (start, end))
        rows = await cursor.fetchall()
        return [row["fetched_at"] for row in rows]

    # ── Signal results (performance tracking) ──────────────────────

    async def record_signal_result(
        self,
        event_id: str,
        signal_type: str,
        market_key: str,
        outcome_name: str,
        signal_direction: str,
        signal_strength: float,
        signal_at: str,
        details_json: str | None = None,
        sport_key: str | None = None,
        is_live: bool | None = None,
    ) -> None:
        is_live_int = None if is_live is None else int(is_live)
        sql = """
            INSERT OR IGNORE INTO signal_results
                (event_id, sport_key, signal_type, market_key, outcome_name,
                 signal_direction, signal_strength, signal_at, is_live, details_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        await self._db.execute(
            sql,
            (
                event_id, sport_key, signal_type, market_key, outcome_name,
                signal_direction, signal_strength, signal_at, is_live_int,
                details_json,
            ),
        )
        await self._db.commit()

    async def resolve_signal(
        self, event_id: str, signal_type: str, market_key: str,
        outcome_name: str, signal_at: str, result: str,
    ) -> None:
        """Mark a signal as won/lost/push."""
        now = datetime.now(timezone.utc).isoformat()
        sql = """
            UPDATE signal_results
            SET result = ?, resolved_at = ?
            WHERE event_id = ? AND signal_type = ? AND market_key = ?
              AND outcome_name = ? AND signal_at = ?
        """
        await self._db.execute(
            sql, (result, now, event_id, signal_type, market_key, outcome_name, signal_at)
        )
        await self._db.commit()

    async def get_unresolved_signals(self) -> list[aiosqlite.Row]:
        """Get signals that haven't been resolved yet."""
        sql = "SELECT * FROM signal_results WHERE result IS NULL"
        cursor = await self._db.execute(sql)
        return await cursor.fetchall()

    async def get_performance_stats(
        self, since: str | None = None, sport_key: str | None = None,
        exclude_sports: list[str] | None = None,
    ) -> dict[str, dict[str, int]]:
        """Get win/loss/push counts grouped by signal type.

        Deduplicates by (event_id, signal_type, market_key, outcome_name),
        counting each unique play only once.
        """
        where = "WHERE result IS NOT NULL"
        params: list[str] = []
        if since:
            where += " AND signal_at >= ?"
            params.append(since)
        if sport_key:
            where += " AND sport_key = ?"
            params.append(sport_key)
        if exclude_sports:
            placeholders = ",".join("?" * len(exclude_sports))
            where += f" AND sport_key NOT IN ({placeholders})"
            params.extend(exclude_sports)

        sql = f"""
            SELECT signal_type, result, COUNT(*) AS cnt
            FROM (
                SELECT event_id, signal_type, market_key, outcome_name, result,
                       ROW_NUMBER() OVER (
                           PARTITION BY event_id, signal_type, market_key, outcome_name
                           ORDER BY signal_at DESC
                       ) AS rn
                FROM signal_results
                {where}
            )
            WHERE rn = 1
            GROUP BY signal_type, result
        """
        cursor = await self._db.execute(sql, tuple(params))
        rows = await cursor.fetchall()

        stats: dict[str, dict[str, int]] = {}
        for row in rows:
            st = row["signal_type"]
            stats.setdefault(st, {"won": 0, "lost": 0, "push": 0, "total": 0})
            stats[st][row["result"]] = row["cnt"]
            stats[st]["total"] += row["cnt"]
        return stats

    async def get_performance_stats_by_market(
        self,
        since: str | None = None,
        signal_type: str | None = None,
        sport_key: str | None = None,
        exclude_sports: list[str] | None = None,
    ) -> dict[str, dict[str, int]]:
        """Get win/loss/push counts grouped by market_key.

        Deduplicates by (event_id, signal_type, market_key, outcome_name).
        """
        where = "WHERE result IS NOT NULL"
        params: list[str] = []
        if since:
            where += " AND signal_at >= ?"
            params.append(since)
        if signal_type:
            where += " AND signal_type = ?"
            params.append(signal_type)
        if sport_key:
            where += " AND sport_key = ?"
            params.append(sport_key)
        if exclude_sports:
            placeholders = ",".join("?" * len(exclude_sports))
            where += f" AND sport_key NOT IN ({placeholders})"
            params.extend(exclude_sports)

        sql = f"""
            SELECT market_key, result, COUNT(*) AS cnt
            FROM (
                SELECT event_id, signal_type, market_key, outcome_name, result,
                       ROW_NUMBER() OVER (
                           PARTITION BY event_id, signal_type, market_key, outcome_name
                           ORDER BY signal_at DESC
                       ) AS rn
                FROM signal_results
                {where}
            )
            WHERE rn = 1
            GROUP BY market_key, result
        """
        cursor = await self._db.execute(sql, tuple(params))
        rows = await cursor.fetchall()

        stats: dict[str, dict[str, int]] = {}
        for row in rows:
            mk = row["market_key"]
            stats.setdefault(mk, {"won": 0, "lost": 0, "push": 0, "total": 0})
            stats[mk][row["result"]] = row["cnt"]
            stats[mk]["total"] += row["cnt"]
        return stats

    async def get_signal_count_since(self, since: str) -> int:
        """Count signals recorded since the given timestamp."""
        sql = "SELECT COUNT(*) AS cnt FROM signal_results WHERE signal_at >= ?"
        cursor = await self._db.execute(sql, (since,))
        row = await cursor.fetchone()
        return row["cnt"] if row else 0

    async def get_reference_line(
        self,
        event_id: str,
        market_key: str,
        outcome_name: str,
        signal_at: str,
    ) -> float | None:
        """Get the spread/total point closest to signal time.

        Prefers Pinnacle, falls back to any bookmaker.
        """
        # Try Pinnacle first
        sql = """
            SELECT point FROM odds_snapshots
            WHERE event_id = ? AND market_key = ? AND outcome_name = ?
              AND fetched_at <= ? AND point IS NOT NULL
              AND bookmaker_key = 'pinnacle'
            ORDER BY fetched_at DESC
            LIMIT 1
        """
        cursor = await self._db.execute(
            sql, (event_id, market_key, outcome_name, signal_at)
        )
        row = await cursor.fetchone()
        if row:
            return row["point"]

        # Fall back to any bookmaker
        sql = """
            SELECT point FROM odds_snapshots
            WHERE event_id = ? AND market_key = ? AND outcome_name = ?
              AND fetched_at <= ? AND point IS NOT NULL
            ORDER BY fetched_at DESC
            LIMIT 1
        """
        cursor = await self._db.execute(
            sql, (event_id, market_key, outcome_name, signal_at)
        )
        row = await cursor.fetchone()
        return row["point"] if row else None

    async def get_resolved_signals_since(
        self, since: str, signal_type: str | None = None,
        sport_key: str | None = None,
        exclude_sports: list[str] | None = None,
    ) -> list[aiosqlite.Row]:
        """Get resolved signals since a timestamp, optionally filtered by type/sport.

        Deduplicates by (event_id, signal_type, market_key, outcome_name),
        keeping only the latest signal_at per group to avoid repeated entries
        from multiple poll cycles.
        """
        where = "WHERE result IS NOT NULL AND signal_at >= ?"
        params: list[str] = [since]
        if signal_type:
            where += " AND signal_type = ?"
            params.append(signal_type)
        if sport_key:
            where += " AND sport_key = ?"
            params.append(sport_key)
        if exclude_sports:
            placeholders = ",".join("?" * len(exclude_sports))
            where += f" AND sport_key NOT IN ({placeholders})"
            params.extend(exclude_sports)
        sql = f"""
            SELECT * FROM signal_results
            {where}
              AND signal_at = (
                SELECT MAX(s2.signal_at) FROM signal_results s2
                WHERE s2.event_id = signal_results.event_id
                  AND s2.signal_type = signal_results.signal_type
                  AND s2.market_key = signal_results.market_key
                  AND s2.outcome_name = signal_results.outcome_name
                  AND s2.result IS NOT NULL
              )
            ORDER BY resolved_at DESC
        """
        cursor = await self._db.execute(sql, tuple(params))
        return await cursor.fetchall()

    async def get_event_teams(self, event_id: str) -> tuple[str, str] | None:
        """Get (home_team, away_team) for an event from snapshots."""
        sql = """
            SELECT home_team, away_team FROM odds_snapshots
            WHERE event_id = ?
            LIMIT 1
        """
        cursor = await self._db.execute(sql, (event_id,))
        row = await cursor.fetchone()
        if row:
            return row["home_team"], row["away_team"]
        return None

    # ── Free play recap ─────────────────────────────────────────────

    async def get_free_play_results_since(self, since: str) -> list[aiosqlite.Row]:
        """Get free play alerts from the last N hours with their graded results."""
        sql = """
            SELECT sa.event_id, sa.market_key, sa.outcome_name, sa.sent_at,
                   sa.details_json,
                   sr.result, sr.signal_strength
            FROM sent_alerts sa
            LEFT JOIN signal_results sr
              ON sa.event_id = sr.event_id
             AND sa.alert_type = sr.signal_type
             AND sa.market_key = sr.market_key
             AND sa.outcome_name = sr.outcome_name
            WHERE sa.is_free_play = 1
              AND sa.sent_at >= ?
            ORDER BY sa.sent_at ASC
        """
        cursor = await self._db.execute(sql, (since,))
        return await cursor.fetchall()

    async def get_free_play_results_resolved_since(
        self, since: str
    ) -> list[aiosqlite.Row]:
        """Get free play alerts resolved (graded) since the given timestamp.

        Unlike get_free_play_results_since which filters on sent_at, this
        filters on resolved_at — so a play shared Monday for a Tuesday game
        appears in Tuesday's recap when it was graded, not Monday's.
        """
        sql = """
            SELECT sa.event_id, sa.market_key, sa.outcome_name, sa.sent_at,
                   sa.details_json,
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
        """
        cursor = await self._db.execute(sql, (since,))
        return await cursor.fetchall()
