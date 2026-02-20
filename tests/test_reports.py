"""Tests for report generation (stats queries only, no Discord calls)."""

from __future__ import annotations

import pytest

from sharp_seeker.analysis.performance import PerformanceTracker
from sharp_seeker.engine.base import Signal, SignalType


def _signal(
    event_id: str,
    signal_type: SignalType,
    market_key: str = "spreads",
) -> Signal:
    return Signal(
        signal_type=signal_type,
        event_id=event_id,
        sport_key="basketball_nba",
        home_team="Lakers",
        away_team="Celtics",
        market_key=market_key,
        outcome_name="Lakers",
        strength=0.7,
        description="test",
        details={"direction": "down"},
    )


@pytest.mark.asyncio
async def test_stats_multiple_types(settings, repo):
    """Stats should be grouped by signal type."""
    tracker = PerformanceTracker(repo)

    # Record signals of different types
    await tracker.record_signals(
        [_signal("e1", SignalType.STEAM_MOVE)], "2025-01-15T12:00:00+00:00"
    )
    await tracker.record_signals(
        [_signal("e2", SignalType.RAPID_CHANGE)], "2025-01-15T12:01:00+00:00"
    )
    await tracker.record_signals(
        [_signal("e3", SignalType.STEAM_MOVE)], "2025-01-15T12:02:00+00:00"
    )

    # Resolve all
    for row in await repo.get_unresolved_signals():
        r = dict(row)
        await repo.resolve_signal(
            r["event_id"], r["signal_type"], r["market_key"],
            r["outcome_name"], r["signal_at"], "won",
        )

    stats = await tracker.get_stats()
    assert stats["steam_move"]["won"] == 2
    assert stats["rapid_change"]["won"] == 1


@pytest.mark.asyncio
async def test_signal_count_since(settings, repo):
    """Signal count since a timestamp should work correctly."""
    tracker = PerformanceTracker(repo)

    await tracker.record_signals(
        [_signal("e1", SignalType.STEAM_MOVE)], "2025-01-15T12:00:00+00:00"
    )
    await tracker.record_signals(
        [_signal("e2", SignalType.STEAM_MOVE)], "2025-01-16T12:00:00+00:00"
    )

    count = await repo.get_signal_count_since("2025-01-16T00:00:00+00:00")
    assert count == 1

    count_all = await repo.get_signal_count_since("2025-01-01T00:00:00+00:00")
    assert count_all == 2


@pytest.mark.asyncio
async def test_performance_stats_by_market(settings, repo):
    """Stats should be grouped by market_key."""
    tracker = PerformanceTracker(repo)

    # Record signals across different markets
    await tracker.record_signals(
        [_signal("e1", SignalType.STEAM_MOVE, market_key="h2h")],
        "2025-01-15T12:00:00+00:00",
    )
    await tracker.record_signals(
        [_signal("e2", SignalType.STEAM_MOVE, market_key="h2h")],
        "2025-01-15T12:01:00+00:00",
    )
    await tracker.record_signals(
        [_signal("e3", SignalType.STEAM_MOVE, market_key="spreads")],
        "2025-01-15T12:02:00+00:00",
    )
    await tracker.record_signals(
        [_signal("e4", SignalType.RAPID_CHANGE, market_key="totals")],
        "2025-01-15T12:03:00+00:00",
    )

    # Resolve with mixed results
    unresolved = await repo.get_unresolved_signals()
    results_map = {"e1": "won", "e2": "lost", "e3": "won", "e4": "push"}
    for row in unresolved:
        r = dict(row)
        await repo.resolve_signal(
            r["event_id"], r["signal_type"], r["market_key"],
            r["outcome_name"], r["signal_at"], results_map[r["event_id"]],
        )

    # All markets
    stats = await repo.get_performance_stats_by_market()
    assert stats["h2h"]["won"] == 1
    assert stats["h2h"]["lost"] == 1
    assert stats["h2h"]["total"] == 2
    assert stats["spreads"]["won"] == 1
    assert stats["spreads"]["total"] == 1
    assert stats["totals"]["push"] == 1

    # Filtered by signal_type
    stats_steam = await repo.get_performance_stats_by_market(
        signal_type="steam_move"
    )
    assert "h2h" in stats_steam
    assert "spreads" in stats_steam
    assert "totals" not in stats_steam

    # Filtered by since
    stats_recent = await repo.get_performance_stats_by_market(
        since="2025-01-15T12:02:00+00:00"
    )
    assert "h2h" not in stats_recent
    assert "spreads" in stats_recent
