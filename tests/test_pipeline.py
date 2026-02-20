"""Tests for the detection pipeline deduplication."""

from __future__ import annotations

import pytest

from sharp_seeker.engine.pipeline import DetectionPipeline


def _snap(
    event_id: str,
    bookmaker: str,
    market: str,
    outcome: str,
    price: float,
    point: float | None,
    fetched_at: str,
) -> dict:
    return {
        "event_id": event_id,
        "sport_key": "basketball_nba",
        "home_team": "Lakers",
        "away_team": "Celtics",
        "commence_time": "2025-01-15T00:00:00Z",
        "bookmaker_key": bookmaker,
        "market_key": market,
        "outcome_name": outcome,
        "price": price,
        "point": point,
        "fetched_at": fetched_at,
    }


@pytest.mark.asyncio
async def test_deduplication(settings, repo):
    """A signal that was already alerted within cooldown should be filtered out."""
    event = "evt_dedup"
    t1 = "2025-01-15T12:00:00+00:00"
    t2 = "2025-01-15T12:20:00+00:00"

    # Create data that would trigger a steam move
    snapshots = [
        _snap(event, "draftkings", "spreads", "Lakers", -110, -3.5, t1),
        _snap(event, "fanduel", "spreads", "Lakers", -110, -3.5, t1),
        _snap(event, "betmgm", "spreads", "Lakers", -110, -3.5, t1),
        _snap(event, "draftkings", "spreads", "Lakers", -110, -4.0, t2),
        _snap(event, "fanduel", "spreads", "Lakers", -110, -4.0, t2),
        _snap(event, "betmgm", "spreads", "Lakers", -110, -4.0, t2),
    ]
    await repo.insert_snapshots(snapshots)

    pipeline = DetectionPipeline(settings, repo)

    # First run should produce signals
    signals1 = await pipeline.run(t2)
    assert len(signals1) > 0

    # Record the alert as sent
    for sig in signals1:
        await repo.record_alert(
            event_id=sig.event_id,
            alert_type=sig.signal_type.value,
            market_key=sig.market_key,
            outcome_name=sig.outcome_name,
        )

    # Second run should be deduped
    signals2 = await pipeline.run(t2)
    assert len(signals2) == 0


@pytest.mark.asyncio
async def test_market_side_dedup(settings, repo):
    """Both sides of the same market should be deduped to one signal."""
    event = "evt_sides"
    t1 = "2025-01-15T12:00:00+00:00"
    t2 = "2025-01-15T12:20:00+00:00"

    # Create data for BOTH sides of a spread that trigger a steam move.
    # Three books all move Lakers from -3.5 to -4.0 AND Celtics from +3.5 to +4.0.
    snapshots = []
    for bm in ("draftkings", "fanduel", "betmgm"):
        # Lakers side
        snapshots.append(_snap(event, bm, "spreads", "Lakers", -110, -3.5, t1))
        snapshots.append(_snap(event, bm, "spreads", "Lakers", -110, -4.0, t2))
        # Celtics side (mirror)
        snapshots.append(_snap(event, bm, "spreads", "Celtics", -110, 3.5, t1))
        snapshots.append(_snap(event, bm, "spreads", "Celtics", -110, 4.0, t2))

    await repo.insert_snapshots(snapshots)

    pipeline = DetectionPipeline(settings, repo)
    signals = await pipeline.run(t2)

    # Should only get ONE steam move signal for spreads, not two
    steam_spread = [
        s for s in signals
        if s.signal_type.value == "steam_move" and s.market_key == "spreads"
    ]
    assert len(steam_spread) == 1
