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
    sport_key: str = "basketball_nba",
) -> Signal:
    return Signal(
        signal_type=signal_type,
        event_id="evt_123",
        sport_key=sport_key,
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


# ── Only tweetable signal types are tweeted ──────────────────────


@pytest.mark.asyncio
async def test_non_tweetable_signals_skipped(settings, repo):
    """Signal types not in x_tweet_signal_types should not be tweeted."""
    poster = XPoster(settings, repo)
    poster._enabled = True
    poster._client = MagicMock()
    poster._client.create_tweet = MagicMock()

    for st in (SignalType.STEAM_MOVE, SignalType.REVERSE_LINE, SignalType.EXCHANGE_SHIFT):
        await poster.post_signals([_make_signal(signal_type=st)])

    poster._client.create_tweet.assert_not_called()


# ── Free play logic (Elite 2+ / 2U 3+ with daily cap) ─────────


@pytest.mark.asyncio
async def test_2u_signal_becomes_free_play(settings, repo):
    """A 2U signal (3+ qualifiers) should automatically become a free play."""
    poster = XPoster(settings, repo)
    poster._enabled = True
    poster._digest_mode = False
    poster._client = MagicMock()
    poster._client.create_tweet = MagicMock()

    sig = _make_signal(
        signal_type=SignalType.PINNACLE_DIVERGENCE,
        details={
            "qualifier_count": 3,
            "value_books": [{"bookmaker": "draftkings", "price": -110, "point": -3.5}],
        },
    )

    await poster.post_signals([sig])

    poster._client.create_tweet.assert_called_once()
    call_text = poster._client.create_tweet.call_args.kwargs["text"]
    assert "FREE PLAY" in call_text


@pytest.mark.asyncio
async def test_elite_signal_becomes_free_play(settings, repo):
    """An Elite signal (2 qualifiers) should become a free play."""
    poster = XPoster(settings, repo)
    poster._enabled = True
    poster._digest_mode = False
    poster._client = MagicMock()
    poster._client.create_tweet = MagicMock()

    sig = _make_signal(
        signal_type=SignalType.PINNACLE_DIVERGENCE,
        details={
            "qualifier_count": 2,
            "value_books": [{"bookmaker": "draftkings", "price": -110, "point": -3.5}],
        },
    )

    await poster.post_signals([sig])

    poster._client.create_tweet.assert_called_once()
    call_text = poster._client.create_tweet.call_args.kwargs["text"]
    assert "FREE PLAY" in call_text


@pytest.mark.asyncio
async def test_zero_qualifiers_no_free_play(settings, repo):
    """Signals with 0 qualifiers should NOT become free plays."""
    poster = XPoster(settings, repo)
    poster._enabled = True
    poster._digest_mode = False
    poster._client = MagicMock()
    poster._client.create_tweet = MagicMock()

    sig = _make_signal(
        signal_type=SignalType.PINNACLE_DIVERGENCE,
        details={
            "qualifier_count": 0,
            "value_books": [{"bookmaker": "draftkings", "price": -110, "point": -3.5}],
        },
    )

    await poster.post_signals([sig])

    calls = [c.kwargs["text"] for c in poster._client.create_tweet.call_args_list]
    assert all("FREE PLAY" not in t for t in calls)


@pytest.mark.asyncio
async def test_steam_move_elite_not_free_play(settings, repo):
    """Steam move with 2 qualifiers should NOT become a free play — PD only."""
    poster = XPoster(settings, repo)
    poster._enabled = True
    poster._digest_mode = False
    poster._client = MagicMock()
    poster._client.create_tweet = MagicMock()

    sig = _make_signal(
        signal_type=SignalType.STEAM_MOVE,
        details={
            "qualifier_count": 2,
            "qualifier_tags": ["Best Combo", "Best Hour"],
            "value_books": [{"bookmaker": "draftkings", "price": -110, "point": -3.5}],
        },
    )

    await poster.post_signals([sig])

    calls = [c.kwargs["text"] for c in poster._client.create_tweet.call_args_list]
    assert all("FREE PLAY" not in t for t in calls)


@pytest.mark.asyncio
async def test_elite_capped_but_2u_punches_through(settings, repo):
    """Elite signals respect daily cap; 2U signals always fire."""
    poster = XPoster(settings, repo)
    poster._enabled = True
    poster._digest_mode = False
    poster._free_play_daily_cap = 2
    poster._client = MagicMock()
    poster._client.create_tweet = MagicMock()

    # Pre-record 2 free plays today to hit the cap
    for i in range(2):
        await repo.record_alert(
            event_id="fp_{}".format(i), alert_type="pinnacle_divergence",
            market_key="spreads", outcome_name="Lakers", is_free_play=True,
        )

    # Elite signal (1q) should be capped
    elite_sig = _make_signal(
        signal_type=SignalType.PINNACLE_DIVERGENCE,
        details={
            "qualifier_count": 1,
            "value_books": [{"bookmaker": "draftkings", "price": -110, "point": -3.5}],
        },
    )
    # 2U signal (2q) should punch through
    two_u_sig = _make_signal(
        signal_type=SignalType.PINNACLE_DIVERGENCE,
        details={
            "qualifier_count": 2,
            "value_books": [{"bookmaker": "fanduel", "price": -105, "point": -4.5}],
        },
    )
    two_u_sig.event_id = "evt_2u"

    await poster.post_signals([elite_sig, two_u_sig])

    calls = [c.kwargs["text"] for c in poster._client.create_tweet.call_args_list]
    free_plays = [t for t in calls if "FREE PLAY" in t]
    assert len(free_plays) == 1  # only 2U, not elite
    assert "FanDuel" in free_plays[0]


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
    assert "DraftKings" in text
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
    assert "FanDuel" in text


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
    poster._digest_mode = False
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
async def test_2u_excluded_book_skipped(settings, repo):
    """2U signal from an excluded book should NOT become a free play."""
    poster = XPoster(settings, repo)
    poster._enabled = True
    poster._digest_mode = False
    poster._client = MagicMock()
    poster._client.create_tweet = MagicMock()
    poster._excluded_books = {"betmgm"}

    sig = _make_signal(
        signal_type=SignalType.PINNACLE_DIVERGENCE,
        details={
            "qualifier_count": 3,
            "value_books": [{"bookmaker": "betmgm", "price": -110, "point": -3.5}],
        },
    )

    await poster.post_signals([sig])

    # Should get a teaser, not a free play
    calls = [c.kwargs["text"] for c in poster._client.create_tweet.call_args_list]
    assert all("FREE PLAY" not in t for t in calls)


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
async def test_post_signals_marks_2u_free_play(settings, repo):
    """post_signals should mark 2U signals as free plays in the DB."""
    poster = XPoster(settings, repo)
    poster._enabled = True
    poster._client = MagicMock()
    poster._client.create_tweet = MagicMock()

    sig = _make_signal(
        signal_type=SignalType.PINNACLE_DIVERGENCE,
        details={
            "qualifier_count": 3,
            "value_books": [{"bookmaker": "draftkings", "price": -110, "point": -3.5}],
        },
    )

    # Record the alert (as Discord alerter would)
    await repo.record_alert(
        event_id="evt_123", alert_type="pinnacle_divergence",
        market_key="spreads", outcome_name="Lakers",
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
    poster._digest_mode = False
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
    poster._digest_mode = False
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


# ── Strength cap ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_strength_cap_filters_signals(settings, repo):
    """Signals at or above the cap should be filtered out of X tweets."""
    poster = XPoster(settings, repo)
    poster._enabled = True
    poster._digest_mode = False
    poster._client = MagicMock()
    poster._client.create_tweet = MagicMock()
    poster._max_strength = 0.80

    # Record 3 alerts (batch of 3)
    for i in range(3):
        await repo.record_alert(
            event_id=f"evt_{i}", alert_type="pinnacle_divergence",
            market_key="spreads", outcome_name="Lakers",
        )

    batch = [
        _make_signal(signal_type=SignalType.PINNACLE_DIVERGENCE, strength=0.60),
        _make_signal(signal_type=SignalType.PINNACLE_DIVERGENCE, strength=0.85),
        _make_signal(signal_type=SignalType.PINNACLE_DIVERGENCE, strength=0.92),
    ]
    await poster.post_signals(batch)

    # Only the 0.60 signal should produce a tweet
    assert poster._client.create_tweet.call_count == 1


@pytest.mark.asyncio
async def test_strength_cap_all_filtered(settings, repo):
    """When all signals are above the cap, no tweets and log batch skipped."""
    poster = XPoster(settings, repo)
    poster._enabled = True
    poster._client = MagicMock()
    poster._client.create_tweet = MagicMock()
    poster._max_strength = 0.80

    await repo.record_alert(
        event_id="evt_0", alert_type="pinnacle_divergence",
        market_key="spreads", outcome_name="Lakers",
    )

    batch = [
        _make_signal(signal_type=SignalType.PINNACLE_DIVERGENCE, strength=0.85),
        _make_signal(signal_type=SignalType.PINNACLE_DIVERGENCE, strength=0.92),
    ]
    await poster.post_signals(batch)

    poster._client.create_tweet.assert_not_called()


@pytest.mark.asyncio
async def test_strength_cap_default_no_filter(settings, repo):
    """Default cap of 1.0 should let all signals through."""
    poster = XPoster(settings, repo)
    poster._enabled = True
    poster._digest_mode = False
    poster._client = MagicMock()
    poster._client.create_tweet = MagicMock()
    # default _max_strength is 1.0

    for i in range(2):
        await repo.record_alert(
            event_id=f"evt_{i}", alert_type="pinnacle_divergence",
            market_key="spreads", outcome_name="Lakers",
        )

    batch = [
        _make_signal(signal_type=SignalType.PINNACLE_DIVERGENCE, strength=0.60),
        _make_signal(signal_type=SignalType.PINNACLE_DIVERGENCE, strength=0.95),
    ]
    await poster.post_signals(batch)

    assert poster._client.create_tweet.call_count == 2


@pytest.mark.asyncio
async def test_strength_cap_boundary(settings, repo):
    """A signal at exactly the cap value should be filtered (strict <)."""
    poster = XPoster(settings, repo)
    poster._enabled = True
    poster._digest_mode = False
    poster._client = MagicMock()
    poster._client.create_tweet = MagicMock()
    poster._max_strength = 0.80

    await repo.record_alert(
        event_id="evt_0", alert_type="pinnacle_divergence",
        market_key="spreads", outcome_name="Lakers",
    )

    batch = [_make_signal(signal_type=SignalType.PINNACLE_DIVERGENCE, strength=0.80)]
    await poster.post_signals(batch)

    poster._client.create_tweet.assert_not_called()


@pytest.mark.asyncio
async def test_multiple_2u_signals_all_become_free_plays(settings, repo):
    """All 2U signals in a batch should become free plays."""
    poster = XPoster(settings, repo)
    poster._enabled = True
    poster._digest_mode = False
    poster._client = MagicMock()
    poster._client.create_tweet = MagicMock()

    sig1 = Signal(
        signal_type=SignalType.PINNACLE_DIVERGENCE,
        event_id="evt_1", sport_key="basketball_nba",
        home_team="Lakers", away_team="Celtics",
        market_key="totals", outcome_name="Over", strength=0.60,
        description="Test", commence_time="2099-01-15T00:00:00Z",
        details={
            "qualifier_count": 3,
            "value_books": [{"bookmaker": "draftkings", "price": -110, "point": 220.5}],
        },
    )
    sig2 = Signal(
        signal_type=SignalType.PINNACLE_DIVERGENCE,
        event_id="evt_2", sport_key="basketball_nba",
        home_team="Warriors", away_team="Suns",
        market_key="spreads", outcome_name="Warriors", strength=0.55,
        description="Test", commence_time="2099-01-15T00:00:00Z",
        details={
            "qualifier_count": 3,
            "value_books": [{"bookmaker": "fanduel", "price": -105, "point": -3.5}],
        },
    )

    await poster.post_signals([sig1, sig2])

    calls = [c.kwargs["text"] for c in poster._client.create_tweet.call_args_list]
    free_play_count = sum(1 for t in calls if "FREE PLAY" in t)
    assert free_play_count == 2


# ── Same-game free play dedup ──────────────────────────────────


@pytest.mark.asyncio
async def test_free_play_skips_repeat_game(settings, repo):
    """2U free play should skip a game that already had a free play."""
    poster = XPoster(settings, repo)
    poster._enabled = True
    poster._digest_mode = False
    poster._client = MagicMock()
    poster._client.create_tweet = MagicMock()

    # Record a past free play for evt_123
    await repo.record_alert(
        event_id="evt_123", alert_type="pinnacle_divergence",
        market_key="spreads", outcome_name="Lakers", is_free_play=True,
    )

    # New batch: same game (evt_123) with 3 qualifiers — should be skipped for free play
    sig = _make_signal(
        signal_type=SignalType.PINNACLE_DIVERGENCE,
        outcome_name="Celtics",
        market_key="h2h",
        details={
            "qualifier_count": 3,
            "value_books": [{"bookmaker": "draftkings", "price": 150}],
        },
    )

    await poster.post_signals([sig])

    # Should only get a teaser, not a free play
    calls = [c.kwargs["text"] for c in poster._client.create_tweet.call_args_list]
    assert all("FREE PLAY" not in t for t in calls)


@pytest.mark.asyncio
async def test_free_play_picks_different_game(settings, repo):
    """When one game already has a free play, the other 2U game gets picked."""
    poster = XPoster(settings, repo)
    poster._enabled = True
    poster._digest_mode = False
    poster._client = MagicMock()
    poster._client.create_tweet = MagicMock()

    # Past free play for evt_123
    await repo.record_alert(
        event_id="evt_123", alert_type="pinnacle_divergence",
        market_key="spreads", outcome_name="Lakers", is_free_play=True,
    )

    # Batch: evt_123 (repeat, 2U) + evt_other (new, 2U)
    repeat = Signal(
        signal_type=SignalType.PINNACLE_DIVERGENCE,
        event_id="evt_123", sport_key="basketball_nba",
        home_team="Lakers", away_team="Celtics",
        market_key="h2h", outcome_name="Celtics", strength=0.50,
        description="Test", commence_time="2099-01-15T00:00:00Z",
        details={
            "qualifier_count": 3,
            "value_books": [{"bookmaker": "draftkings", "price": 150}],
        },
    )
    new_game = Signal(
        signal_type=SignalType.PINNACLE_DIVERGENCE,
        event_id="evt_other", sport_key="basketball_nba",
        home_team="Warriors", away_team="Suns",
        market_key="spreads", outcome_name="Warriors", strength=0.70,
        description="Test", commence_time="2099-01-15T00:00:00Z",
        details={
            "qualifier_count": 3,
            "value_books": [{"bookmaker": "fanduel", "price": -110, "point": -3.5}],
        },
    )

    await poster.post_signals([repeat, new_game])

    calls = [c.kwargs["text"] for c in poster._client.create_tweet.call_args_list]
    free_plays = [t for t in calls if "FREE PLAY" in t]
    assert len(free_plays) == 1
    assert "Warriors" in free_plays[0]  # new game picked, not repeat




# ── Rapid change tweeting ──────────────────────────────────────


@pytest.mark.asyncio
async def test_rapid_change_gets_teaser(settings, repo):
    """Rapid change signal should produce a teaser tweet."""
    poster = XPoster(settings, repo)
    poster._enabled = True
    poster._digest_mode = False
    poster._client = MagicMock()
    poster._client.create_tweet = MagicMock()

    # Record 1 alert (as Discord alerter would)
    await repo.record_alert(
        event_id="evt_123", alert_type="rapid_change",
        market_key="spreads", outcome_name="Lakers",
    )

    sig = _make_signal(signal_type=SignalType.RAPID_CHANGE)
    await poster.post_signals([sig])

    poster._client.create_tweet.assert_called_once()
    call_text = poster._client.create_tweet.call_args.kwargs["text"]
    assert "Sharp money detected" in call_text
    assert "Rapid Change" in call_text


@pytest.mark.asyncio
async def test_rapid_change_not_free_play(settings, repo):
    """Rapid change signal should NOT become a free play — PD only."""
    poster = XPoster(settings, repo)
    poster._enabled = True
    poster._digest_mode = False
    poster._client = MagicMock()
    poster._client.create_tweet = MagicMock()

    sig = _make_signal(
        signal_type=SignalType.RAPID_CHANGE,
        details={
            "value_books": [{"bookmaker": "draftkings", "price": -110, "point": -3.5}],
            "qualifier_count": 2,
            "qualifier_tags": ["Best Combo", "Best Hour"],
        },
    )

    await poster.post_signals([sig])

    calls = [c.kwargs["text"] for c in poster._client.create_tweet.call_args_list]
    assert all("FREE PLAY" not in t for t in calls)


@pytest.mark.asyncio
async def test_custom_tweet_types_config(settings, repo):
    """Setting x_tweet_signal_types to PD-only excludes rapid changes."""
    poster = XPoster(settings, repo)
    poster._enabled = True
    poster._digest_mode = False
    poster._client = MagicMock()
    poster._client.create_tweet = MagicMock()
    # Override to PD only
    poster._tweet_types = {"pinnacle_divergence"}

    await poster.post_signals([_make_signal(signal_type=SignalType.RAPID_CHANGE)])
    poster._client.create_tweet.assert_not_called()

    # PD should still work
    await repo.record_alert(
        event_id="evt_123", alert_type="pinnacle_divergence",
        market_key="spreads", outcome_name="Lakers",
    )
    await poster.post_signals([_make_signal(signal_type=SignalType.PINNACLE_DIVERGENCE)])
    poster._client.create_tweet.assert_called_once()


# ── Digest mode ─────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_digest_buffers_teasers(settings, repo):
    """In digest mode, post_signals() should buffer teasers instead of tweeting."""
    poster = XPoster(settings, repo)
    poster._enabled = True
    poster._digest_mode = True
    poster._client = MagicMock()
    poster._client.create_tweet = MagicMock()

    # Record 1 alert (not a free play sequence hit)
    await repo.record_alert(
        event_id="evt_123", alert_type="pinnacle_divergence",
        market_key="spreads", outcome_name="Lakers",
    )

    sig = _make_signal(signal_type=SignalType.PINNACLE_DIVERGENCE)
    await poster.post_signals([sig])

    # No tweet should be posted (teaser is buffered)
    poster._client.create_tweet.assert_not_called()
    # Signal should be in the buffer
    assert len(poster._digest_buffer) == 1
    assert poster._digest_buffer[0] is sig


@pytest.mark.asyncio
async def test_digest_posts_free_play_immediately(settings, repo):
    """2U free plays still tweet right away in digest mode."""
    poster = XPoster(settings, repo)
    poster._enabled = True
    poster._digest_mode = True
    poster._client = MagicMock()
    poster._client.create_tweet = MagicMock()

    sig = _make_signal(
        signal_type=SignalType.PINNACLE_DIVERGENCE,
        details={
            "value_books": [{"bookmaker": "draftkings", "price": -110, "point": -3.5}],
            "qualifier_count": 3,
            "qualifier_tags": ["Best Combo", "Best Hour", "Sharp Hold"],
        },
    )

    await poster.post_signals([sig])

    # Free play should be posted immediately
    poster._client.create_tweet.assert_called_once()
    call_text = poster._client.create_tweet.call_args.kwargs["text"]
    assert "FREE PLAY" in call_text
    # Teaser buffer should be empty (free play isn't a teaser)
    assert len(poster._digest_buffer) == 0
    # Free play should be in the digest free plays buffer
    assert len(poster._digest_free_plays) == 1


@pytest.mark.asyncio
async def test_post_digest_formats_and_posts(settings, repo):
    """post_digest() should format buffer and call tweepy."""
    poster = XPoster(settings, repo)
    poster._enabled = True
    poster._client = MagicMock()
    poster._client.create_tweet = MagicMock()
    poster._cta_url = "https://discord.gg/test"

    # Manually fill the buffer
    poster._digest_buffer = [
        _make_signal(signal_type=SignalType.PINNACLE_DIVERGENCE),
        _make_signal(signal_type=SignalType.RAPID_CHANGE),
    ]

    await poster.post_digest()

    poster._client.create_tweet.assert_called_once()
    call_text = poster._client.create_tweet.call_args.kwargs["text"]
    assert "Sharp Signals" in call_text
    assert "2 alerts" in call_text
    assert "\U0001f525 Discord Signals" in call_text
    assert "Pinnacle Divergence" in call_text
    assert "Rapid Change" in call_text
    assert "discord.gg/test" in call_text


@pytest.mark.asyncio
async def test_post_digest_empty_buffer_skips(settings, repo):
    """No tweet when buffer is empty."""
    poster = XPoster(settings, repo)
    poster._enabled = True
    poster._client = MagicMock()
    poster._client.create_tweet = MagicMock()

    await poster.post_digest()

    poster._client.create_tweet.assert_not_called()


@pytest.mark.asyncio
async def test_post_digest_clears_buffer(settings, repo):
    """Buffer should be empty after digest posts."""
    poster = XPoster(settings, repo)
    poster._enabled = True
    poster._client = MagicMock()
    poster._client.create_tweet = MagicMock()

    poster._digest_buffer = [_make_signal(signal_type=SignalType.PINNACLE_DIVERGENCE)]
    poster._digest_free_plays = [_make_signal(signal_type=SignalType.PINNACLE_DIVERGENCE)]
    await poster.post_digest()

    assert len(poster._digest_buffer) == 0
    assert len(poster._digest_free_plays) == 0


@pytest.mark.asyncio
async def test_digest_includes_free_plays(settings, repo):
    """Digest should include free plays posted during the window with pick details."""
    poster = XPoster(settings, repo)
    poster._enabled = True
    poster._client = MagicMock()
    poster._client.create_tweet = MagicMock()
    poster._cta_url = "https://discord.gg/test"

    free_play = _make_signal(
        signal_type=SignalType.PINNACLE_DIVERGENCE,
        market_key="spreads",
        outcome_name="Lakers",
        details={"value_books": [{"bookmaker": "draftkings", "price": -110, "point": -3.5}]},
    )
    teaser = _make_signal(signal_type=SignalType.RAPID_CHANGE)

    poster._digest_buffer = [teaser]
    poster._digest_free_plays = [free_play]

    await poster.post_digest()

    poster._client.create_tweet.assert_called_once()
    call_text = poster._client.create_tweet.call_args.kwargs["text"]
    assert "2 alerts" in call_text
    # Free plays section with header
    assert "\U0001f3af Free Plays" in call_text
    assert "DraftKings" in call_text
    assert "-3.5" in call_text
    # Signals section with header
    assert "\U0001f525 Discord Signals" in call_text
    assert "Rapid Change" in call_text


@pytest.mark.asyncio
async def test_digest_only_free_plays(settings, repo):
    """Digest should post even if there are only free plays and no teasers."""
    poster = XPoster(settings, repo)
    poster._enabled = True
    poster._client = MagicMock()
    poster._client.create_tweet = MagicMock()
    poster._cta_url = ""

    free_play = _make_signal(
        signal_type=SignalType.PINNACLE_DIVERGENCE,
        market_key="h2h",
        outcome_name="Lakers",
        details={"value_books": [{"bookmaker": "fanduel", "price": 150}]},
    )
    poster._digest_free_plays = [free_play]

    await poster.post_digest()

    poster._client.create_tweet.assert_called_once()
    call_text = poster._client.create_tweet.call_args.kwargs["text"]
    assert "1 alert" in call_text
    assert "\U0001f3af Free Plays" in call_text
    assert "FanDuel" in call_text
    assert "+150" in call_text
    # No signals section when there are no teasers
    assert "\U0001f525 Discord Signals" not in call_text


@pytest.mark.asyncio
async def test_digest_truncation(settings, repo):
    """Large batches should truncate with '...and N more'."""
    poster = XPoster(settings, repo)
    poster._enabled = True
    poster._client = MagicMock()
    poster._client.create_tweet = MagicMock()
    poster._cta_url = "https://discord.gg/test"

    # Create many signals with long team names to force truncation
    signals = []
    for i in range(15):
        sig = Signal(
            signal_type=SignalType.PINNACLE_DIVERGENCE,
            event_id=f"evt_{i}",
            sport_key="basketball_nba",
            home_team=f"TeamHome{i}LongName",
            away_team=f"TeamAway{i}LongName",
            market_key="spreads",
            outcome_name="Lakers",
            strength=0.75,
            description="Test signal",
            commence_time="2099-01-15T00:00:00Z",
            details={},
        )
        signals.append(sig)
    poster._digest_buffer = signals

    await poster.post_digest()

    poster._client.create_tweet.assert_called_once()
    call_text = poster._client.create_tweet.call_args.kwargs["text"]
    assert len(call_text) <= 280
    assert "more" in call_text


@pytest.mark.asyncio
async def test_legacy_mode_unchanged(settings, repo):
    """When digest mode is off (interval=0), behavior matches per-signal tweeting."""
    poster = XPoster(settings, repo)
    poster._enabled = True
    poster._digest_mode = False
    poster._client = MagicMock()
    poster._client.create_tweet = MagicMock()

    # Record 1 alert (not a free play)
    await repo.record_alert(
        event_id="evt_123", alert_type="pinnacle_divergence",
        market_key="spreads", outcome_name="Lakers",
    )

    sig = _make_signal(signal_type=SignalType.PINNACLE_DIVERGENCE)
    await poster.post_signals([sig])

    # Should tweet immediately (not buffer)
    poster._client.create_tweet.assert_called_once()
    call_text = poster._client.create_tweet.call_args.kwargs["text"]
    assert "Sharp money detected" in call_text
    # Buffer should be empty
    assert len(poster._digest_buffer) == 0


# ── Weekly recap ──────────────────────────────────────────────────


def test_format_weekly_recap_with_results(settings, repo):
    """Won/lost results format correctly in the weekly recap."""
    poster = XPoster(settings, repo)
    poster._cta_url = "https://discord.gg/test"

    results = [
        {"outcome_name": "Lakers", "market_key": "spreads", "result": "won",
         "signal_strength": 0.85, "event_id": "e1", "sent_at": "2099-01-15",
         "details_json": json.dumps({"value_books": [{"bookmaker": "draftkings", "price": -110, "point": -3.5}]})},
        {"outcome_name": "Chiefs", "market_key": "h2h", "result": "lost",
         "signal_strength": 0.70, "event_id": "e2", "sent_at": "2099-01-15",
         "details_json": json.dumps({"value_books": [{"bookmaker": "fanduel", "price": 150}]})},
        {"outcome_name": "Celtics", "market_key": "spreads", "result": "won",
         "signal_strength": 0.80, "event_id": "e3", "sent_at": "2099-01-16",
         "details_json": json.dumps({"value_books": [{"bookmaker": "betmgm", "price": -105, "point": -2.5}]})},
    ]

    text = poster._format_weekly_recap(results)
    assert "Weekly Free Plays" in text
    assert "\u2705" in text  # won emoji
    assert "\u274c" in text  # lost emoji
    assert "Lakers" in text
    assert "Chiefs" in text
    assert "Celtics" in text
    assert "Record: 2-1" in text
    assert "discord.gg/test" in text
    assert len(text) <= 280


def test_format_weekly_recap_truncation(settings, repo):
    """Many picks should truncate with '...and N more' and stay <= 280 chars."""
    poster = XPoster(settings, repo)
    poster._cta_url = "https://discord.gg/test"

    results = []
    for i in range(15):
        results.append({
            "outcome_name": f"Team{i}LongName",
            "market_key": "spreads",
            "result": "won" if i % 2 == 0 else "lost",
            "signal_strength": 0.70,
            "event_id": f"e{i}",
            "sent_at": "2099-01-15",
            "details_json": json.dumps({"value_books": [{"bookmaker": "draftkings", "price": -110, "point": -3.5}]}),
        })

    text = poster._format_weekly_recap(results)
    assert len(text) <= 280
    assert "...and" in text
    assert "more" in text
    assert "Weekly Free Plays" in text
    assert "Record:" in text


@pytest.mark.asyncio
async def test_post_weekly_recap_empty(settings, repo):
    """No free plays in 168-hour window → no tweet posted."""
    poster = XPoster(settings, repo)
    poster._enabled = True
    poster._client = MagicMock()
    poster._client.create_tweet = MagicMock()

    await poster.post_weekly_recap()

    poster._client.create_tweet.assert_not_called()


@pytest.mark.asyncio
async def test_post_weekly_recap_calls_tweepy(settings, repo):
    """When free plays exist in the past week, weekly recap tweet is posted."""
    poster = XPoster(settings, repo)
    poster._enabled = True
    poster._client = MagicMock()
    poster._client.create_tweet = MagicMock()
    poster._cta_url = "https://discord.gg/test"

    # Insert a free play alert
    await repo.record_alert(
        event_id="evt_weekly",
        alert_type="pinnacle_divergence",
        market_key="spreads",
        outcome_name="Lakers",
        is_free_play=True,
    )

    await poster.post_weekly_recap()

    poster._client.create_tweet.assert_called_once()
    call_text = poster._client.create_tweet.call_args.kwargs["text"]
    assert "Weekly Free Plays" in call_text
    assert "Lakers" in call_text


# ── Daily recap with card image attachment ─────────────────────────


@pytest.mark.asyncio
async def test_daily_recap_attaches_card(settings, repo):
    """When card_gen returns paths, recap tweet includes media_ids."""
    mock_card_gen = AsyncMock()
    mock_card_gen.generate_daily_cards.return_value = [
        "/tmp/results_2026-03-12_1080x1080.png",
        "/tmp/results_2026-03-12_1080x1920.png",
    ]
    poster = XPoster(settings, repo, card_gen=mock_card_gen)
    poster._enabled = True
    poster._client = MagicMock()
    poster._client.create_tweet = MagicMock()
    poster._api = MagicMock()
    mock_media = MagicMock()
    mock_media.media_id = 12345
    poster._api.media_upload.return_value = mock_media

    # Insert a free play alert
    await repo.record_alert(
        event_id="evt_card", alert_type="pinnacle_divergence",
        market_key="spreads", outcome_name="Lakers", is_free_play=True,
    )

    await poster.post_daily_recap()

    poster._api.media_upload.assert_called_once_with(
        filename="/tmp/results_2026-03-12_1080x1080.png"
    )
    call_kwargs = poster._client.create_tweet.call_args.kwargs
    assert call_kwargs["media_ids"] == [12345]


@pytest.mark.asyncio
async def test_daily_recap_text_fallback_on_upload_failure(settings, repo):
    """When media upload fails, recap posts text-only."""
    mock_card_gen = AsyncMock()
    mock_card_gen.generate_daily_cards.return_value = [
        "/tmp/results_2026-03-12_1080x1080.png",
    ]
    poster = XPoster(settings, repo, card_gen=mock_card_gen)
    poster._enabled = True
    poster._client = MagicMock()
    poster._client.create_tweet = MagicMock()
    poster._api = MagicMock()
    poster._api.media_upload.side_effect = Exception("upload failed")

    await repo.record_alert(
        event_id="evt_fail", alert_type="pinnacle_divergence",
        market_key="spreads", outcome_name="Lakers", is_free_play=True,
    )

    await poster.post_daily_recap()

    # Tweet should still be posted, without media_ids
    poster._client.create_tweet.assert_called_once()
    call_kwargs = poster._client.create_tweet.call_args.kwargs
    assert "media_ids" not in call_kwargs


@pytest.mark.asyncio
async def test_daily_recap_no_card_gen(settings, repo):
    """When card_gen is None, recap posts text-only (backwards compatible)."""
    poster = XPoster(settings, repo, card_gen=None)
    poster._enabled = True
    poster._client = MagicMock()
    poster._client.create_tweet = MagicMock()

    await repo.record_alert(
        event_id="evt_nocard", alert_type="pinnacle_divergence",
        market_key="spreads", outcome_name="Lakers", is_free_play=True,
    )

    await poster.post_daily_recap()

    poster._client.create_tweet.assert_called_once()
    call_kwargs = poster._client.create_tweet.call_args.kwargs
    assert "media_ids" not in call_kwargs
