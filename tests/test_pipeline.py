"""Tests for the detection pipeline deduplication."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from sharp_seeker.config import Settings
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
        "commence_time": "2099-01-15T00:00:00Z",
        "bookmaker_key": bookmaker,
        "market_key": market,
        "outcome_name": outcome,
        "price": price,
        "point": point,
        "deep_link": None,
        "fetched_at": fetched_at,
    }


@pytest.mark.asyncio
async def test_deduplication(settings, repo):
    """A signal that was already alerted within cooldown should be filtered out."""
    event = "evt_dedup"
    t1 = "2025-01-15T12:00:00+00:00"
    t2 = "2025-01-15T12:20:00+00:00"

    # Create data that would trigger a steam move (caesars stays on old line = value book)
    snapshots = [
        _snap(event, "draftkings", "spreads", "Lakers", -110, -3.5, t1),
        _snap(event, "fanduel", "spreads", "Lakers", -110, -3.5, t1),
        _snap(event, "betmgm", "spreads", "Lakers", -110, -3.5, t1),
        _snap(event, "caesars", "spreads", "Lakers", -110, -3.5, t1),
        _snap(event, "draftkings", "spreads", "Lakers", -110, -4.0, t2),
        _snap(event, "fanduel", "spreads", "Lakers", -110, -4.0, t2),
        _snap(event, "betmgm", "spreads", "Lakers", -110, -4.0, t2),
        _snap(event, "caesars", "spreads", "Lakers", -110, -3.5, t2),
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
    # Caesars stays on old line = value book.
    snapshots = []
    for bm in ("draftkings", "fanduel", "betmgm"):
        # Lakers side
        snapshots.append(_snap(event, bm, "spreads", "Lakers", -110, -3.5, t1))
        snapshots.append(_snap(event, bm, "spreads", "Lakers", -110, -4.0, t2))
        # Celtics side (mirror)
        snapshots.append(_snap(event, bm, "spreads", "Celtics", -110, 3.5, t1))
        snapshots.append(_snap(event, bm, "spreads", "Celtics", -110, 4.0, t2))
    # Caesars doesn't move (stale line)
    snapshots.append(_snap(event, "caesars", "spreads", "Lakers", -110, -3.5, t1))
    snapshots.append(_snap(event, "caesars", "spreads", "Lakers", -110, -3.5, t2))
    snapshots.append(_snap(event, "caesars", "spreads", "Celtics", -110, 3.5, t1))
    snapshots.append(_snap(event, "caesars", "spreads", "Celtics", -110, 3.5, t2))

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
    # Three books all shorten Pacers and drift Wizards. Caesars stays stale.
    snapshots = []
    for bm in ("draftkings", "fanduel", "betmgm"):
        snapshots.append(_snap(event, bm, "h2h", "Pacers", -150, None, t1))
        snapshots.append(_snap(event, bm, "h2h", "Pacers", -170, None, t2))
        snapshots.append(_snap(event, bm, "h2h", "Wizards", 130, None, t1))
        snapshots.append(_snap(event, bm, "h2h", "Wizards", 150, None, t2))
    # Caesars doesn't move (stale line = value book)
    snapshots.append(_snap(event, "caesars", "h2h", "Pacers", -150, None, t1))
    snapshots.append(_snap(event, "caesars", "h2h", "Pacers", -150, None, t2))
    snapshots.append(_snap(event, "caesars", "h2h", "Wizards", 130, None, t1))
    snapshots.append(_snap(event, "caesars", "h2h", "Wizards", 130, None, t2))
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


# ── Per-signal-type filtering tests ──────────────────────────────────


@pytest.mark.asyncio
async def test_strength_override_filters_weak_signal(repo):
    """A signal type with a strength override should be filtered at the higher threshold,
    while other types at the same strength pass through."""
    event = "evt_override"
    t1 = "2025-01-15T12:00:00+00:00"
    t2 = "2025-01-15T12:20:00+00:00"

    # Create data that triggers a steam move (caesars stays on old line = value book)
    snapshots = []
    for bm in ("draftkings", "fanduel", "betmgm"):
        snapshots.append(_snap(event, bm, "spreads", "Lakers", -110, -3.5, t1))
        snapshots.append(_snap(event, bm, "spreads", "Lakers", -110, -4.0, t2))
    snapshots.append(_snap(event, "caesars", "spreads", "Lakers", -110, -3.5, t1))
    snapshots.append(_snap(event, "caesars", "spreads", "Lakers", -110, -3.5, t2))
    await repo.insert_snapshots(snapshots)

    # Without override — signals should pass (global min is 0.5)
    settings_normal = Settings(
        odds_api_key="test_key",
        discord_webhook_url="https://discord.com/api/webhooks/test/test",
        db_path=":memory:",
        min_signal_strength=0.5,
    )
    pipeline = DetectionPipeline(settings_normal, repo)
    signals = await pipeline.run(t2)
    steam_signals = [s for s in signals if s.signal_type == SignalType.STEAM_MOVE]
    assert len(steam_signals) > 0, "Steam move should pass with default threshold"

    # Record the strength so we can set the override above it
    strength = steam_signals[0].strength

    # With override set above the signal's strength — should be filtered out
    settings_override = Settings(
        odds_api_key="test_key",
        discord_webhook_url="https://discord.com/api/webhooks/test/test",
        db_path=":memory:",
        min_signal_strength=0.5,
        signal_strength_overrides={"steam_move": strength + 0.01},
    )
    pipeline_override = DetectionPipeline(settings_override, repo)
    signals_override = await pipeline_override.run(t2)
    steam_override = [s for s in signals_override if s.signal_type == SignalType.STEAM_MOVE]
    assert len(steam_override) == 0, "Steam move should be filtered by strength override"


@pytest.mark.asyncio
async def test_signal_quiet_hours_suppresses(repo):
    """A signal type configured as quiet at the current hour should be suppressed,
    while other signal types at the same hour pass through."""
    from datetime import datetime, timezone

    event = "evt_quiet"
    t1 = "2025-01-15T12:00:00+00:00"
    t2 = "2025-01-15T12:20:00+00:00"

    # Create data that triggers a steam move
    snapshots = []
    for bm in ("draftkings", "fanduel", "betmgm"):
        snapshots.append(_snap(event, bm, "spreads", "Lakers", -110, -3.5, t1))
        snapshots.append(_snap(event, bm, "spreads", "Lakers", -110, -4.0, t2))
    snapshots.append(_snap(event, "caesars", "spreads", "Lakers", -110, -3.5, t1))
    snapshots.append(_snap(event, "caesars", "spreads", "Lakers", -110, -3.5, t2))
    await repo.insert_snapshots(snapshots)

    # Configure quiet hours for steam_move at hour 14
    settings_quiet = Settings(
        odds_api_key="test_key",
        discord_webhook_url="https://discord.com/api/webhooks/test/test",
        db_path=":memory:",
        signal_quiet_hours={"steam_move": [14]},
    )
    pipeline = DetectionPipeline(settings_quiet, repo)

    # Mock datetime.now to return hour 14 UTC
    fake_now = datetime(2025, 1, 15, 14, 30, 0, tzinfo=timezone.utc)
    with patch("sharp_seeker.engine.pipeline.datetime") as mock_dt:
        mock_dt.now.return_value = fake_now
        mock_dt.fromisoformat = datetime.fromisoformat
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
        signals = await pipeline.run(t2)

    steam_signals = [s for s in signals if s.signal_type == SignalType.STEAM_MOVE]
    assert len(steam_signals) == 0, "Steam move should be suppressed during quiet hour"


@pytest.mark.asyncio
async def test_signal_quiet_hours_no_config(repo):
    """When signal_quiet_hours is empty, all signals pass through (no regression)."""
    event = "evt_noq"
    t1 = "2025-01-15T12:00:00+00:00"
    t2 = "2025-01-15T12:20:00+00:00"

    snapshots = []
    for bm in ("draftkings", "fanduel", "betmgm"):
        snapshots.append(_snap(event, bm, "spreads", "Lakers", -110, -3.5, t1))
        snapshots.append(_snap(event, bm, "spreads", "Lakers", -110, -4.0, t2))
    snapshots.append(_snap(event, "caesars", "spreads", "Lakers", -110, -3.5, t1))
    snapshots.append(_snap(event, "caesars", "spreads", "Lakers", -110, -3.5, t2))
    await repo.insert_snapshots(snapshots)

    settings_empty = Settings(
        odds_api_key="test_key",
        discord_webhook_url="https://discord.com/api/webhooks/test/test",
        db_path=":memory:",
        signal_quiet_hours={},
    )
    pipeline = DetectionPipeline(settings_empty, repo)
    signals = await pipeline.run(t2)
    steam_signals = [s for s in signals if s.signal_type == SignalType.STEAM_MOVE]
    assert len(steam_signals) > 0, "Signals should pass when no quiet hours configured"
