"""Tests for X (Twitter) poster."""

from __future__ import annotations

import json
from datetime import datetime, timezone
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


# ── Only Pinnacle Divergence signals are tweeted ─────────────────


@pytest.mark.asyncio
async def test_non_pinnacle_signals_skipped(settings, repo):
    """Non-Pinnacle Divergence signals should not be tweeted."""
    poster = XPoster(settings, repo)
    poster._enabled = True
    poster._client = MagicMock()
    poster._client.create_tweet = MagicMock()

    for st in (SignalType.STEAM_MOVE, SignalType.RAPID_CHANGE, SignalType.REVERSE_LINE, SignalType.EXCHANGE_SHIFT):
        await poster.post_signals([_make_signal(signal_type=st)])

    poster._client.create_tweet.assert_not_called()


# ── Free play logic ─────────────────────────────────────────────


def test_is_free_play_seq(settings, repo):
    """Sequence number divisible by interval should be a free play."""
    poster = XPoster(settings, repo)
    poster._free_play_interval = 5
    assert poster._is_free_play_seq(5) is True
    assert poster._is_free_play_seq(10) is True
    assert poster._is_free_play_seq(3) is False
    assert poster._is_free_play_seq(0) is False


@pytest.mark.asyncio
async def test_free_play_batch_sequencing(settings, repo):
    """When a batch of PD signals fires, the one landing on the interval gets free play."""
    poster = XPoster(settings, repo)
    poster._enabled = True
    poster._client = MagicMock()
    poster._client.create_tweet = MagicMock()
    poster._free_play_interval = 5

    # Pre-insert 3 PD alerts (count = 3 before this batch)
    for i in range(3):
        await repo.record_alert(
            event_id=f"evt_{i}",
            alert_type="pinnacle_divergence",
            market_key="spreads",
            outcome_name="Lakers",
        )

    # Batch of 4 PD signals: seq 4, 5, 6, 7 — only seq 5 is free play
    batch = [_make_signal(signal_type=SignalType.PINNACLE_DIVERGENCE) for _ in range(4)]
    # Record them in sent_alerts (as Discord alerter would)
    for i in range(4):
        await repo.record_alert(
            event_id=f"evt_batch_{i}",
            alert_type="pinnacle_divergence",
            market_key="spreads",
            outcome_name="Lakers",
        )

    await poster.post_signals(batch)

    assert poster._client.create_tweet.call_count == 4
    calls = [c.kwargs["text"] for c in poster._client.create_tweet.call_args_list]
    free_play_count = sum(1 for t in calls if "FREE PLAY" in t)
    assert free_play_count == 1  # only seq 5


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
    poster._enabled = True
    poster._client = MagicMock()
    poster._client.create_tweet = MagicMock()
    poster._cta_url = "https://discord.gg/test"

    # Record 1 alert in sent_alerts (as Discord alerter would)
    await repo.record_alert(
        event_id="evt_123", alert_type="pinnacle_divergence",
        market_key="spreads", outcome_name="Lakers",
    )

    sig = _make_signal(signal_type=SignalType.PINNACLE_DIVERGENCE)
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

    # Insert exactly 5 alerts (batch of 1 signal, total=5, seq=5, 5%5==0)
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

    sig = _make_signal(signal_type=SignalType.PINNACLE_DIVERGENCE)
    # Should not raise
    await poster.post_signals([sig])


# ── Daily recap ──────────────────────────────────────────────────


def test_format_recap_with_results(settings, repo):
    """Won/lost/push signals format correctly in the recap."""
    poster = XPoster(settings, repo)
    poster._cta_url = "https://discord.gg/test"

    results = [
        {"outcome_name": "Lakers", "market_key": "spreads", "result": "won",
         "signal_strength": 0.85, "event_id": "e1", "sent_at": "2099-01-15",
         "details_json": json.dumps({"value_books": [{"bookmaker": "draftkings", "price": -110, "point": -3.5}]})},
        {"outcome_name": "Chiefs", "market_key": "h2h", "result": "lost",
         "signal_strength": 0.70, "event_id": "e2", "sent_at": "2099-01-15",
         "details_json": json.dumps({"value_books": [{"bookmaker": "fanduel", "price": 150}]})},
        {"outcome_name": "Over", "market_key": "totals", "result": "push",
         "signal_strength": 0.60, "event_id": "e3", "sent_at": "2099-01-15",
         "details_json": json.dumps({"value_books": [{"bookmaker": "betmgm", "price": -110, "point": 220.5}]})},
    ]

    text = poster._format_recap(results)
    assert "Yesterday's Free Plays" in text
    assert "\u2705" in text  # won
    assert "\u274c" in text  # lost
    assert "\u21a9\ufe0f" in text  # push
    assert "Lakers" in text
    assert "Chiefs" in text
    assert "Record: 1-1" in text  # push doesn't count
    assert "discord.gg/test" in text


def test_format_recap_pending(settings, repo):
    """Unresolved signals show as pending."""
    poster = XPoster(settings, repo)
    poster._cta_url = ""

    results = [
        {"outcome_name": "Cowboys", "market_key": "spreads", "result": None,
         "signal_strength": 0.80, "event_id": "e1", "sent_at": "2099-01-15",
         "details_json": None},
    ]

    text = poster._format_recap(results)
    assert "\u23f3" in text  # hourglass
    assert "PENDING" in text
    assert "Cowboys" in text
    assert "Record:" not in text  # no decided games


@pytest.mark.asyncio
async def test_post_daily_recap_empty(settings, repo):
    """No free plays in window → no tweet posted."""
    poster = XPoster(settings, repo)
    poster._enabled = True
    poster._client = MagicMock()
    poster._client.create_tweet = MagicMock()

    await poster.post_daily_recap()

    poster._client.create_tweet.assert_not_called()


@pytest.mark.asyncio
async def test_post_daily_recap_calls_tweepy(settings, repo):
    """When free plays exist, recap tweet is posted via tweepy."""
    poster = XPoster(settings, repo)
    poster._enabled = True
    poster._client = MagicMock()
    poster._client.create_tweet = MagicMock()
    poster._cta_url = "https://discord.gg/test"

    # Insert a free play alert
    await repo.record_alert(
        event_id="evt_recap",
        alert_type="pinnacle_divergence",
        market_key="spreads",
        outcome_name="Lakers",
        is_free_play=True,
    )

    await poster.post_daily_recap()

    poster._client.create_tweet.assert_called_once()
    call_text = poster._client.create_tweet.call_args.kwargs["text"]
    assert "Yesterday's Free Plays" in call_text
    assert "Lakers" in call_text


@pytest.mark.asyncio
async def test_mark_alert_free_play(settings, repo):
    """mark_alert_free_play should update is_free_play on the correct row."""
    # Insert two alerts for same event (different times)
    await repo.record_alert(
        event_id="evt_fp", alert_type="pinnacle_divergence",
        market_key="spreads", outcome_name="Lakers",
    )
    await repo.record_alert(
        event_id="evt_fp", alert_type="pinnacle_divergence",
        market_key="spreads", outcome_name="Lakers",
    )

    await repo.mark_alert_free_play("evt_fp", "spreads", "Lakers")

    # Verify only the most recent row is marked
    cursor = await repo._db.execute(
        "SELECT is_free_play FROM sent_alerts WHERE event_id = 'evt_fp' ORDER BY sent_at ASC"
    )
    rows = await cursor.fetchall()
    assert len(rows) == 2
    assert rows[0]["is_free_play"] == 0
    assert rows[1]["is_free_play"] == 1


@pytest.mark.asyncio
async def test_post_signals_marks_free_play(settings, repo):
    """post_signals should mark the alert as a free play in the DB when it posts one."""
    poster = XPoster(settings, repo)
    poster._enabled = True
    poster._client = MagicMock()
    poster._client.create_tweet = MagicMock()
    poster._free_play_interval = 5

    sig = _make_signal(
        signal_type=SignalType.PINNACLE_DIVERGENCE,
        details={"value_books": [{"bookmaker": "draftkings", "price": -110, "point": -3.5}]},
    )

    # Insert exactly 5 alerts (batch of 1 signal, total=5, seq=5, 5%5==0)
    for i in range(5):
        await repo.record_alert(
            event_id="evt_123" if i == 4 else f"evt_{i}",
            alert_type="pinnacle_divergence",
            market_key="spreads",
            outcome_name="Lakers",
        )

    await poster.post_signals([sig])

    # The alert for evt_123 should now be marked as free play
    cursor = await repo._db.execute(
        "SELECT is_free_play FROM sent_alerts WHERE event_id = 'evt_123'"
    )
    row = await cursor.fetchone()
    assert row["is_free_play"] == 1


# ── Teaser time-window gating ──────────────────────────────────


@pytest.mark.asyncio
async def test_teaser_skipped_outside_hours(settings, repo):
    """Teaser should NOT post when current UTC hour is outside x_teaser_hours."""
    poster = XPoster(settings, repo)
    poster._enabled = True
    poster._client = MagicMock()
    poster._client.create_tweet = MagicMock()
    poster._teaser_hours = [14]

    # Record 1 alert so seq=1 (not a free play)
    await repo.record_alert(
        event_id="evt_123", alert_type="pinnacle_divergence",
        market_key="spreads", outcome_name="Lakers",
    )

    sig = _make_signal(signal_type=SignalType.PINNACLE_DIVERGENCE)

    # Mock UTC hour to 18 — outside the allowed [14]
    fake_now = datetime(2026, 2, 26, 18, 30, 0, tzinfo=timezone.utc)
    with patch("sharp_seeker.alerts.x_poster.datetime") as mock_dt:
        mock_dt.now.return_value = fake_now
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
        await poster.post_signals([sig])

    poster._client.create_tweet.assert_not_called()


@pytest.mark.asyncio
async def test_teaser_posted_within_hours(settings, repo):
    """Teaser SHOULD post when current UTC hour is inside x_teaser_hours."""
    poster = XPoster(settings, repo)
    poster._enabled = True
    poster._client = MagicMock()
    poster._client.create_tweet = MagicMock()
    poster._teaser_hours = [14]

    # Record 1 alert so seq=1 (not a free play)
    await repo.record_alert(
        event_id="evt_123", alert_type="pinnacle_divergence",
        market_key="spreads", outcome_name="Lakers",
    )

    sig = _make_signal(signal_type=SignalType.PINNACLE_DIVERGENCE)

    # Mock UTC hour to 14 — inside the allowed [14]
    fake_now = datetime(2026, 2, 26, 14, 15, 0, tzinfo=timezone.utc)
    with patch("sharp_seeker.alerts.x_poster.datetime") as mock_dt:
        mock_dt.now.return_value = fake_now
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
        await poster.post_signals([sig])

    poster._client.create_tweet.assert_called_once()
    call_text = poster._client.create_tweet.call_args.kwargs["text"]
    assert "Sharp money detected" in call_text
