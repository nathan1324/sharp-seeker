"""Tests for X (Twitter) poster."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from sharp_seeker.alerts.x_poster import XPoster, _format_odds
from sharp_seeker.engine.base import Signal, SignalType


def _make_signal(
    signal_type: SignalType = SignalType.STEAM_MOVE,
    market_key: str = "spreads",
    outcome_name: str = "Lakers",
    strength: float = 0.75,
    details: dict | None = None,
) -> Signal:
    return Signal(
        signal_type=signal_type,
        event_id="evt_123",
        sport_key="basketball_nba",
        home_team="Lakers",
        away_team="Celtics",
        market_key=market_key,
        outcome_name=outcome_name,
        strength=strength,
        description="Test signal",
        commence_time="2099-01-15T00:00:00Z",
        details=details or {},
    )


# ── Disabled when credentials missing ───────────────────────────


def test_disabled_without_credentials(settings, repo):
    """XPoster should gracefully disable when X credentials are not set."""
    poster = XPoster(settings, repo)
    assert poster._enabled is False


@pytest.mark.asyncio
async def test_post_signals_skips_when_disabled(settings, repo):
    """post_signals should be a no-op when disabled."""
    poster = XPoster(settings, repo)
    sig = _make_signal()
    # Should not raise
    await poster.post_signals([sig])


# ── Free play logic ─────────────────────────────────────────────


@pytest.mark.asyncio
async def test_is_free_play_non_pinnacle(settings, repo):
    """Non-Pinnacle signals are never free plays."""
    poster = XPoster(settings, repo)
    sig = _make_signal(signal_type=SignalType.STEAM_MOVE)
    assert await poster._is_free_play(sig) is False


@pytest.mark.asyncio
async def test_is_free_play_at_interval(settings, repo):
    """Pinnacle signal is a free play when count is divisible by interval."""
    poster = XPoster(settings, repo)
    poster._free_play_interval = 10

    sig = _make_signal(signal_type=SignalType.PINNACLE_DIVERGENCE)

    # Insert 10 pinnacle_divergence rows into sent_alerts
    for i in range(10):
        await repo.record_alert(
            event_id=f"evt_{i}",
            alert_type="pinnacle_divergence",
            market_key="spreads",
            outcome_name="Lakers",
        )

    assert await poster._is_free_play(sig) is True


@pytest.mark.asyncio
async def test_is_free_play_not_at_interval(settings, repo):
    """Pinnacle signal is NOT a free play when count isn't divisible."""
    poster = XPoster(settings, repo)
    poster._free_play_interval = 10

    sig = _make_signal(signal_type=SignalType.PINNACLE_DIVERGENCE)

    # Insert 7 rows
    for i in range(7):
        await repo.record_alert(
            event_id=f"evt_{i}",
            alert_type="pinnacle_divergence",
            market_key="spreads",
            outcome_name="Lakers",
        )

    assert await poster._is_free_play(sig) is False


@pytest.mark.asyncio
async def test_is_free_play_zero_count(settings, repo):
    """Zero pinnacle_divergence alerts means not a free play."""
    poster = XPoster(settings, repo)
    sig = _make_signal(signal_type=SignalType.PINNACLE_DIVERGENCE)
    assert await poster._is_free_play(sig) is False


# ── Tweet formatting ─────────────────────────────────────────────


def test_format_teaser_steam_move(settings, repo):
    poster = XPoster(settings, repo)
    poster._cta_url = "https://discord.gg/test"
    sig = _make_signal(signal_type=SignalType.STEAM_MOVE)

    text = poster._format_teaser(sig)
    assert "Celtics vs Lakers" in text
    assert "Steam Move" in text
    assert "https://discord.gg/test" in text
    assert "Sharp money detected" in text


def test_format_teaser_all_signal_types(settings, repo):
    """Every signal type should produce a teaser with its label."""
    poster = XPoster(settings, repo)
    poster._cta_url = "https://discord.gg/test"

    for st in SignalType:
        sig = _make_signal(signal_type=st)
        text = poster._format_teaser(sig)
        assert "Celtics vs Lakers" in text
        assert "discord.gg/test" in text


def test_format_teaser_no_cta_url(settings, repo):
    """Teaser without CTA URL should not include a link line."""
    poster = XPoster(settings, repo)
    poster._cta_url = ""
    sig = _make_signal()

    text = poster._format_teaser(sig)
    assert "discord" not in text.lower()
    assert "Sharp money detected" in text


def test_format_free_play_with_value_books(settings, repo):
    poster = XPoster(settings, repo)
    poster._cta_url = "https://discord.gg/test"

    sig = _make_signal(
        signal_type=SignalType.PINNACLE_DIVERGENCE,
        market_key="spreads",
        outcome_name="Lakers",
        strength=0.85,
        details={
            "value_books": [
                {"bookmaker": "draftkings", "price": -110, "point": -3.5},
            ],
        },
    )

    text = poster._format_free_play(sig)
    assert "FREE PLAY" in text
    assert "Celtics vs Lakers" in text
    assert "Spread" in text
    assert "Lakers" in text
    assert "-3.5" in text
    assert "Draftkings" in text
    assert "85%" in text
    assert "discord.gg/test" in text


def test_format_free_play_moneyline(settings, repo):
    poster = XPoster(settings, repo)
    poster._cta_url = ""

    sig = _make_signal(
        signal_type=SignalType.PINNACLE_DIVERGENCE,
        market_key="h2h",
        outcome_name="Lakers",
        strength=0.90,
        details={
            "value_books": [
                {"bookmaker": "fanduel", "price": 150, "point": None},
            ],
        },
    )

    text = poster._format_free_play(sig)
    assert "FREE PLAY" in text
    assert "Moneyline" in text
    assert "+150" in text
    assert "Fanduel" in text


def test_format_free_play_no_value_books(settings, repo):
    """Free play without value books should still produce a valid tweet."""
    poster = XPoster(settings, repo)
    sig = _make_signal(
        signal_type=SignalType.PINNACLE_DIVERGENCE,
        details={},
    )

    text = poster._format_free_play(sig)
    assert "FREE PLAY" in text
    assert "Lakers" in text


# ── _format_odds helper ──────────────────────────────────────────


def test_format_odds_h2h():
    assert _format_odds("h2h", -150, None) == "-150"
    assert _format_odds("h2h", 200, None) == "+200"
    assert _format_odds("h2h", None, None) == "?"


def test_format_odds_spreads():
    assert _format_odds("spreads", -110, -3.5) == "-3.5 (-110)"
    assert _format_odds("spreads", -110, 7.0) == "+7 (-110)"


def test_format_odds_totals():
    assert _format_odds("totals", -110, 220.5) == "220.5 (-110)"


# ── Integration: post_signals calls _post_tweet ──────────────────


@pytest.mark.asyncio
async def test_post_signals_calls_tweepy(settings, repo):
    """When enabled, post_signals should call create_tweet for each signal."""
    poster = XPoster(settings, repo)
    # Force-enable with a mock client
    poster._enabled = True
    poster._client = MagicMock()
    poster._client.create_tweet = MagicMock()
    poster._cta_url = "https://discord.gg/test"

    sig = _make_signal()
    await poster.post_signals([sig])

    poster._client.create_tweet.assert_called_once()
    call_text = poster._client.create_tweet.call_args.kwargs["text"]
    assert "Sharp money detected" in call_text


@pytest.mark.asyncio
async def test_post_signals_free_play_tweet(settings, repo):
    """A free play signal should produce a FREE PLAY tweet."""
    poster = XPoster(settings, repo)
    poster._enabled = True
    poster._client = MagicMock()
    poster._client.create_tweet = MagicMock()
    poster._free_play_interval = 5

    sig = _make_signal(
        signal_type=SignalType.PINNACLE_DIVERGENCE,
        details={"value_books": [{"bookmaker": "draftkings", "price": -110, "point": -3.5}]},
    )

    # Insert exactly 5 pinnacle_divergence alerts
    for i in range(5):
        await repo.record_alert(
            event_id=f"evt_{i}",
            alert_type="pinnacle_divergence",
            market_key="spreads",
            outcome_name="Lakers",
        )

    await poster.post_signals([sig])

    poster._client.create_tweet.assert_called_once()
    call_text = poster._client.create_tweet.call_args.kwargs["text"]
    assert "FREE PLAY" in call_text


@pytest.mark.asyncio
async def test_post_signals_handles_tweepy_error(settings, repo):
    """If tweepy raises, it should log the error but not crash."""
    poster = XPoster(settings, repo)
    poster._enabled = True
    poster._client = MagicMock()
    poster._client.create_tweet = MagicMock(side_effect=Exception("API error"))

    sig = _make_signal()
    # Should not raise
    await poster.post_signals([sig])
