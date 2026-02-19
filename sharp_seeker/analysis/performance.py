"""Signal performance tracking: record signals and resolve outcomes."""

from __future__ import annotations

import json

import structlog

from sharp_seeker.db.repository import Repository
from sharp_seeker.engine.base import Signal

log = structlog.get_logger()


class PerformanceTracker:
    def __init__(self, repo: Repository) -> None:
        self._repo = repo

    async def record_signal(self, signal: Signal) -> None:
        """Record a signal for later performance evaluation."""
        direction = self._extract_direction(signal)
        await self._repo.record_signal_result(
            event_id=signal.event_id,
            signal_type=signal.signal_type.value,
            market_key=signal.market_key,
            outcome_name=signal.outcome_name,
            signal_direction=direction,
            signal_strength=signal.strength,
            signal_at=signal.details.get("fetched_at", ""),
            details_json=json.dumps(signal.details),
        )

    async def record_signals(self, signals: list[Signal], fetched_at: str) -> None:
        """Record multiple signals with a shared fetched_at timestamp."""
        for sig in signals:
            direction = self._extract_direction(sig)
            await self._repo.record_signal_result(
                event_id=sig.event_id,
                signal_type=sig.signal_type.value,
                market_key=sig.market_key,
                outcome_name=sig.outcome_name,
                signal_direction=direction,
                signal_strength=sig.strength,
                signal_at=fetched_at,
                details_json=json.dumps(sig.details),
            )

    async def get_stats(self, since: str | None = None) -> dict[str, dict[str, int]]:
        """Get win/loss/push stats grouped by signal type."""
        return await self._repo.get_performance_stats(since)

    async def get_win_rate(self, since: str | None = None) -> dict[str, float]:
        """Get win rate per signal type."""
        stats = await self.get_stats(since)
        rates: dict[str, float] = {}
        for st, counts in stats.items():
            decided = counts.get("won", 0) + counts.get("lost", 0)
            if decided > 0:
                rates[st] = round(counts.get("won", 0) / decided, 4)
            else:
                rates[st] = 0.0
        return rates

    @staticmethod
    def _extract_direction(signal: Signal) -> str:
        """Extract a directional label from the signal details."""
        details = signal.details
        if "direction" in details:
            return details["direction"]
        if "us_direction" in details:
            return f"us:{details['us_direction']}_pin:{details.get('pinnacle_direction', '?')}"
        if "delta" in details:
            return "up" if details["delta"] > 0 else "down"
        return "unknown"
