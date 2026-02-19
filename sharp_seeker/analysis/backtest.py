"""Backtesting framework: replay stored snapshots through detectors."""

from __future__ import annotations

from dataclasses import dataclass, field

import structlog

from sharp_seeker.config import Settings
from sharp_seeker.db.repository import Repository
from sharp_seeker.engine.base import Signal
from sharp_seeker.engine.pipeline import DetectionPipeline

log = structlog.get_logger()


@dataclass
class BacktestResult:
    start: str
    end: str
    fetch_cycles: int
    total_signals: int = 0
    signals_by_type: dict[str, int] = field(default_factory=dict)
    signals_by_sport: dict[str, int] = field(default_factory=dict)
    all_signals: list[Signal] = field(default_factory=list)

    @property
    def summary(self) -> str:
        lines = [
            f"Backtest: {self.start} → {self.end}",
            f"  Fetch cycles: {self.fetch_cycles}",
            f"  Total signals: {self.total_signals}",
            "",
            "  By type:",
        ]
        for st, count in sorted(self.signals_by_type.items(), key=lambda x: -x[1]):
            lines.append(f"    {st}: {count}")
        lines.append("")
        lines.append("  By sport:")
        for sport, count in sorted(self.signals_by_sport.items(), key=lambda x: -x[1]):
            lines.append(f"    {sport}: {count}")
        return "\n".join(lines)


class Backtester:
    def __init__(self, settings: Settings, repo: Repository) -> None:
        self._settings = settings
        self._repo = repo
        self._pipeline = DetectionPipeline(settings, repo)

    async def run(self, start: str, end: str) -> BacktestResult:
        """Replay all stored snapshots in the date range through detectors."""
        fetch_times = await self._repo.get_distinct_fetch_times(start, end)
        log.info("backtest_start", start=start, end=end, cycles=len(fetch_times))

        result = BacktestResult(start=start, end=end, fetch_cycles=len(fetch_times))

        for fetched_at in fetch_times:
            signals = await self._pipeline.run(fetched_at)
            result.total_signals += len(signals)

            for sig in signals:
                st = sig.signal_type.value
                result.signals_by_type[st] = result.signals_by_type.get(st, 0) + 1
                result.signals_by_sport[sig.sport_key] = (
                    result.signals_by_sport.get(sig.sport_key, 0) + 1
                )
                result.all_signals.append(sig)

        log.info(
            "backtest_complete",
            cycles=result.fetch_cycles,
            signals=result.total_signals,
        )
        return result
