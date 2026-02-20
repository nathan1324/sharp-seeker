"""Tests for the detection pipeline deduplication."""

from __future__ import annotations

import pytest

from sharp_seeker.engine.base import Signal, SignalType
from sharp_seeker.engine.pipeline import DetectionPipeline, _pick_best_signal


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


@pytest.mark.asyncio
async def test_mirror_side_suppressed_by_cooldown(settings, repo):
    """After alerting one side of an h2h market, the mirror side should be
    suppressed within the cooldown window (market-level dedup)."""
    event = "evt_mirror"
    t1 = "2025-01-15T12:00:00+00:00"
    t2 = "2025-01-15T12:20:00+00:00"

    # Create h2h data for both sides that would trigger a steam move.
    # Three books all shorten Pacers and drift Wizards.
    snapshots = []
    for bm in ("draftkings", "fanduel", "betmgm"):
        snapshots.append(_snap(event, bm, "h2h", "Pacers", -150, None, t1))
        snapshots.append(_snap(event, bm, "h2h", "Pacers", -170, None, t2))
        snapshots.append(_snap(event, bm, "h2h", "Wizards", 130, None, t1))
        snapshots.append(_snap(event, bm, "h2h", "Wizards", 150, None, t2))
    await repo.insert_snapshots(snapshots)

    pipeline = DetectionPipeline(settings, repo)

    # First run — should fire a signal for one side only (market-side dedup).
    signals1 = await pipeline.run(t2)
    h2h_signals = [s for s in signals1 if s.market_key == "h2h"]
    assert len(h2h_signals) == 1
    alerted_side = h2h_signals[0].outcome_name

    # Record the alert (as the alert dispatcher would).
    await repo.record_alert(
        event_id=h2h_signals[0].event_id,
        alert_type=h2h_signals[0].signal_type.value,
        market_key=h2h_signals[0].market_key,
        outcome_name=alerted_side,
    )

    # Second run — the mirror side should be suppressed by market-level cooldown.
    signals2 = await pipeline.run(t2)
    h2h_signals2 = [s for s in signals2 if s.market_key == "h2h"]
    assert len(h2h_signals2) == 0, (
        f"Mirror side should be suppressed but got: "
        f"{[s.outcome_name for s in h2h_signals2]}"
    )


def _make_signal(
    signal_type: SignalType,
    outcome: str = "Team A",
    market: str = "spreads",
    strength: float = 0.7,
    details: dict | None = None,
) -> Signal:
    return Signal(
        signal_type=signal_type,
        event_id="evt1",
        sport_key="basketball_nba",
        home_team="Team A",
        away_team="Team B",
        market_key=market,
        outcome_name=outcome,
        strength=strength,
        description="test",
        details=details or {},
    )


def test_pick_best_reverse_line_follows_pinnacle():
    """Reverse line: keep the side where Pinnacle delta is positive."""
    sigs = [
        _make_signal(
            SignalType.REVERSE_LINE,
            outcome="Team A",
            details={"pinnacle_delta": -0.5, "value_books": [{"bookmaker": "dk"}]},
        ),
        _make_signal(
            SignalType.REVERSE_LINE,
            outcome="Team B",
            details={"pinnacle_delta": 0.5, "value_books": [{"bookmaker": "dk"}]},
        ),
    ]
    best = _pick_best_signal(sigs)
    assert best.outcome_name == "Team B"


def test_pick_best_steam_spread_prefers_down():
    """Steam move on spreads: keep the side where direction is 'down'."""
    sigs = [
        _make_signal(
            SignalType.STEAM_MOVE,
            outcome="Lakers",
            market="spreads",
            details={"direction": "down", "value_books": []},
        ),
        _make_signal(
            SignalType.STEAM_MOVE,
            outcome="Celtics",
            market="spreads",
            details={"direction": "up", "value_books": []},
        ),
    ]
    best = _pick_best_signal(sigs)
    assert best.outcome_name == "Lakers"


def test_pick_best_steam_h2h_prefers_down():
    """Steam move on h2h: keep the side where direction is 'down'."""
    sigs = [
        _make_signal(
            SignalType.STEAM_MOVE,
            outcome="Favorite",
            market="h2h",
            details={"direction": "down", "value_books": []},
        ),
        _make_signal(
            SignalType.STEAM_MOVE,
            outcome="Underdog",
            market="h2h",
            details={"direction": "up", "value_books": []},
        ),
    ]
    best = _pick_best_signal(sigs)
    assert best.outcome_name == "Favorite"


def test_pick_best_steam_totals_over():
    """Steam move on totals going up: keep the Over side."""
    sigs = [
        _make_signal(
            SignalType.STEAM_MOVE,
            outcome="Over",
            market="totals",
            details={"direction": "up", "value_books": []},
        ),
        _make_signal(
            SignalType.STEAM_MOVE,
            outcome="Under",
            market="totals",
            details={"direction": "up", "value_books": []},
        ),
    ]
    best = _pick_best_signal(sigs)
    assert best.outcome_name == "Over"


def test_pick_best_steam_totals_under():
    """Steam move on totals going down: keep the Under side."""
    sigs = [
        _make_signal(
            SignalType.STEAM_MOVE,
            outcome="Over",
            market="totals",
            details={"direction": "down", "value_books": []},
        ),
        _make_signal(
            SignalType.STEAM_MOVE,
            outcome="Under",
            market="totals",
            details={"direction": "down", "value_books": []},
        ),
    ]
    best = _pick_best_signal(sigs)
    assert best.outcome_name == "Under"


def test_pick_best_exchange_prefers_shortened():
    """Exchange shift: keep the side that shortened."""
    sigs = [
        _make_signal(
            SignalType.EXCHANGE_SHIFT,
            outcome="Team A",
            market="h2h",
            details={"direction": "shortened", "value_books": []},
        ),
        _make_signal(
            SignalType.EXCHANGE_SHIFT,
            outcome="Team B",
            market="h2h",
            details={"direction": "drifted", "value_books": []},
        ),
    ]
    best = _pick_best_signal(sigs)
    assert best.outcome_name == "Team A"


def test_pick_best_rapid_change_prefers_larger_delta():
    """Rapid change: keep the side with the larger delta."""
    sigs = [
        _make_signal(
            SignalType.RAPID_CHANGE,
            outcome="Team A",
            details={"delta": 0.5, "value_books": []},
        ),
        _make_signal(
            SignalType.RAPID_CHANGE,
            outcome="Team B",
            details={"delta": 1.2, "value_books": []},
        ),
    ]
    best = _pick_best_signal(sigs)
    assert best.outcome_name == "Team B"


def test_pick_best_fallback_uses_value_books():
    """When no signal-specific logic matches, prefer more value books."""
    sigs = [
        _make_signal(
            SignalType.PINNACLE_DIVERGENCE,
            outcome="Team A",
            details={"value_books": [{"bookmaker": "dk"}]},
        ),
        _make_signal(
            SignalType.PINNACLE_DIVERGENCE,
            outcome="Team B",
            details={"value_books": [{"bookmaker": "dk"}, {"bookmaker": "fd"}]},
        ),
    ]
    best = _pick_best_signal(sigs)
    assert best.outcome_name == "Team B"
