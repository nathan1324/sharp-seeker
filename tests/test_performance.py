"""Tests for signal performance tracking."""

from __future__ import annotations

import pytest

from sharp_seeker.analysis.performance import PerformanceTracker
from sharp_seeker.engine.base import Signal, SignalType


def _signal(
    signal_type: SignalType = SignalType.STEAM_MOVE,
    event_id: str = "evt1",
    direction: str = "down",
) -> Signal:
    return Signal(
        signal_type=signal_type,
        event_id=event_id,
        sport_key="basketball_nba",
        home_team="Lakers",
        away_team="Celtics",
        market_key="spreads",
        outcome_name="Lakers",
        strength=0.75,
        description="test signal",
        details={"direction": direction, "delta": -0.5},
    )


@pytest.mark.asyncio
async def test_record_and_resolve(settings, repo):
    """Record a signal, resolve it, and check stats."""
    tracker = PerformanceTracker(repo)
    sig = _signal()

    await tracker.record_signals([sig], "2025-01-15T12:00:00+00:00")

    # Unresolved signals should exist
    unresolved = await repo.get_unresolved_signals()
    assert len(unresolved) == 1

    # Resolve it as a win
    row = dict(unresolved[0])
    await repo.resolve_signal(
        event_id=row["event_id"],
        signal_type=row["signal_type"],
        market_key=row["market_key"],
        outcome_name=row["outcome_name"],
        signal_at=row["signal_at"],
        result="won",
    )

    stats = await tracker.get_stats()
    assert stats["steam_move"]["won"] == 1
    assert stats["steam_move"]["total"] == 1


@pytest.mark.asyncio
async def test_win_rate(settings, repo):
    """Win rate should be calculated correctly."""
    tracker = PerformanceTracker(repo)

    # Record 3 signals
    for i in range(3):
        sig = _signal(event_id=f"evt_{i}")
        await tracker.record_signals([sig], f"2025-01-15T12:{i:02d}:00+00:00")

    # Resolve: 2 wins, 1 loss
    unresolved = await repo.get_unresolved_signals()
    for i, row in enumerate(unresolved):
        r = dict(row)
        result = "won" if i < 2 else "lost"
        await repo.resolve_signal(
            event_id=r["event_id"],
            signal_type=r["signal_type"],
            market_key=r["market_key"],
            outcome_name=r["outcome_name"],
            signal_at=r["signal_at"],
            result=result,
        )

    rates = await tracker.get_win_rate()
    assert abs(rates["steam_move"] - 0.6667) < 0.01


@pytest.mark.asyncio
async def test_direction_extraction(settings, repo):
    """Direction should be extracted from signal details."""
    tracker = PerformanceTracker(repo)

    sig = _signal(direction="up")
    await tracker.record_signals([sig], "2025-01-15T12:00:00+00:00")

    unresolved = await repo.get_unresolved_signals()
    assert dict(unresolved[0])["signal_direction"] == "up"
