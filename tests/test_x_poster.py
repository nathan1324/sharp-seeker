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


# ── Free play logic ─────────────────────────────────────────────


@pytest.mark.asyncio
async def test_free_play_batch_sequencing(settings, repo):
    """When a batch of PD signals fires, the one landing on the interval gets free play."""
    poster = XPoster(settings, repo)
    poster._enabled = True
    poster._digest_mode = False
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


# ── Smart free play selection ────────────────────────────────


@pytest.mark.asyncio
async def test_smart_free_play_prefers_sport(settings, repo):
    """NBA should be picked over NCAAB when NBA is in preferred sports, even with higher strength."""
    poster = XPoster(settings, repo)
    poster._enabled = True
    poster._digest_mode = False
    poster._client = MagicMock()
    poster._client.create_tweet = MagicMock()
    poster._free_play_interval = 1  # every signal is a free play
    poster._free_play_sports = ["basketball_nba"]
    poster._free_play_markets = []
    poster._max_strength = 1.0

    # Record 2 alerts for the batch
    for i in range(2):
        await repo.record_alert(
            event_id=f"evt_{i}", alert_type="pinnacle_divergence",
            market_key="spreads", outcome_name="Lakers",
        )

    nba = _make_signal(
        signal_type=SignalType.PINNACLE_DIVERGENCE, strength=0.70,
        sport_key="basketball_nba",
        details={"value_books": [{"bookmaker": "draftkings", "price": -110, "point": -3.5}]},
    )
    ncaab = _make_signal(
        signal_type=SignalType.PINNACLE_DIVERGENCE, strength=0.55,
        sport_key="basketball_ncaab",
        details={"value_books": [{"bookmaker": "fanduel", "price": -105, "point": -2.5}]},
    )

    await poster.post_signals([ncaab, nba])

    calls = [c.kwargs["text"] for c in poster._client.create_tweet.call_args_list]
    free_plays = [t for t in calls if "FREE PLAY" in t]
    assert len(free_plays) == 1
    assert "70%" in free_plays[0]  # NBA signal strength


@pytest.mark.asyncio
async def test_smart_free_play_prefers_market(settings, repo):
    """Moneyline should be picked over spreads when h2h is preferred market."""
    poster = XPoster(settings, repo)
    poster._enabled = True
    poster._digest_mode = False
    poster._client = MagicMock()
    poster._client.create_tweet = MagicMock()
    poster._free_play_interval = 1
    poster._free_play_sports = []
    poster._free_play_markets = ["h2h"]
    poster._max_strength = 1.0

    for i in range(2):
        await repo.record_alert(
            event_id=f"evt_{i}", alert_type="pinnacle_divergence",
            market_key="spreads", outcome_name="Lakers",
        )

    ml = _make_signal(
        signal_type=SignalType.PINNACLE_DIVERGENCE, strength=0.70, market_key="h2h",
        details={"value_books": [{"bookmaker": "draftkings", "price": 150}]},
    )
    spread = _make_signal(
        signal_type=SignalType.PINNACLE_DIVERGENCE, strength=0.55, market_key="spreads",
        details={"value_books": [{"bookmaker": "fanduel", "price": -110, "point": -3.5}]},
    )

    await poster.post_signals([spread, ml])

    calls = [c.kwargs["text"] for c in poster._client.create_tweet.call_args_list]
    free_plays = [t for t in calls if "FREE PLAY" in t]
    assert len(free_plays) == 1
    assert "Moneyline" in free_plays[0]


@pytest.mark.asyncio
async def test_smart_free_play_falls_back_to_strength(settings, repo):
    """With no preferences, the lower-strength signal should win."""
    poster = XPoster(settings, repo)
    poster._enabled = True
    poster._digest_mode = False
    poster._client = MagicMock()
    poster._client.create_tweet = MagicMock()
    poster._free_play_interval = 1
    poster._free_play_sports = []
    poster._free_play_markets = []
    poster._max_strength = 1.0

    for i in range(2):
        await repo.record_alert(
            event_id=f"evt_{i}", alert_type="pinnacle_divergence",
            market_key="spreads", outcome_name="Lakers",
        )

    low = _make_signal(
        signal_type=SignalType.PINNACLE_DIVERGENCE, strength=0.55,
        details={"value_books": [{"bookmaker": "draftkings", "price": -110, "point": -3.5}]},
    )
    high = _make_signal(
        signal_type=SignalType.PINNACLE_DIVERGENCE, strength=0.75,
        details={"value_books": [{"bookmaker": "fanduel", "price": -105, "point": -2.5}]},
    )

    await poster.post_signals([high, low])

    calls = [c.kwargs["text"] for c in poster._client.create_tweet.call_args_list]
    free_plays = [t for t in calls if "FREE PLAY" in t]
    assert len(free_plays) == 1
    assert "55%" in free_plays[0]


@pytest.mark.asyncio
async def test_free_play_counter_uses_unfiltered_count(settings, repo):
    """Seq should be computed from all tweetable signals, not just eligible (below cap)."""
    poster = XPoster(settings, repo)
    poster._enabled = True
    poster._digest_mode = False
    poster._client = MagicMock()
    poster._client.create_tweet = MagicMock()
    poster._free_play_interval = 5
    poster._max_strength = 0.80

    # 3 prior alerts + batch of 3 (unfiltered) = seq 4,5,6 → free play due at 5
    for i in range(3):
        await repo.record_alert(
            event_id=f"evt_{i}", alert_type="pinnacle_divergence",
            market_key="spreads", outcome_name="Lakers",
        )

    batch = [
        _make_signal(signal_type=SignalType.PINNACLE_DIVERGENCE, strength=0.60),  # eligible
        _make_signal(signal_type=SignalType.PINNACLE_DIVERGENCE, strength=0.90),  # filtered
        _make_signal(signal_type=SignalType.PINNACLE_DIVERGENCE, strength=0.92),  # filtered
    ]
    # Record them as Discord alerter would
    for i in range(3):
        await repo.record_alert(
            event_id=f"evt_batch_{i}", alert_type="pinnacle_divergence",
            market_key="spreads", outcome_name="Lakers",
        )

    await poster.post_signals(batch)

    calls = [c.kwargs["text"] for c in poster._client.create_tweet.call_args_list]
    # Should have 1 tweet: the free play (only 1 eligible signal, and it becomes the free play)
    assert len(calls) == 1
    assert "FREE PLAY" in calls[0]


def test_pick_best_free_play_excludes_book(settings, repo):
    """Signals from excluded books should be skipped for free play selection."""
    poster = XPoster(settings, repo)
    poster._free_play_sports = []
    poster._free_play_markets = []
    poster._excluded_books = {"betmgm"}

    mgm = _make_signal(
        signal_type=SignalType.PINNACLE_DIVERGENCE, strength=0.50,
        details={"value_books": [{"bookmaker": "betmgm", "price": -110, "point": -3.5}]},
    )
    dk = _make_signal(
        signal_type=SignalType.PINNACLE_DIVERGENCE, strength=0.70,
        details={"value_books": [{"bookmaker": "draftkings", "price": -110, "point": -3.5}]},
    )

    # MGM has lower strength (normally preferred), but is excluded
    result = poster._pick_best_free_play([mgm, dk])
    assert result is dk


def test_pick_best_free_play_fallback_when_all_excluded(settings, repo):
    """If all candidates are excluded, fall back to unfiltered list."""
    poster = XPoster(settings, repo)
    poster._free_play_sports = []
    poster._free_play_markets = []
    poster._excluded_books = {"betmgm"}

    mgm1 = _make_signal(
        signal_type=SignalType.PINNACLE_DIVERGENCE, strength=0.50,
        details={"value_books": [{"bookmaker": "betmgm", "price": -110, "point": -3.5}]},
    )
    mgm2 = _make_signal(
        signal_type=SignalType.PINNACLE_DIVERGENCE, strength=0.70,
        details={"value_books": [{"bookmaker": "betmgm", "price": -105, "point": -2.5}]},
    )

    # All excluded → falls back to unfiltered, picks lower strength
    result = poster._pick_best_free_play([mgm1, mgm2])
    assert result is mgm1


def test_pick_best_free_play_scoring(settings, repo):
    """Unit test for _pick_best_free_play scoring logic directly."""
    poster = XPoster(settings, repo)
    poster._free_play_sports = ["basketball_nba"]
    poster._free_play_markets = ["h2h"]

    # NBA + h2h + high strength (worst on strength, best on sport+market)
    s1 = _make_signal(
        signal_type=SignalType.PINNACLE_DIVERGENCE, strength=0.75,
        sport_key="basketball_nba", market_key="h2h",
    )
    # NCAAB + spreads + low strength (best on strength, worst on sport+market)
    s2 = _make_signal(
        signal_type=SignalType.PINNACLE_DIVERGENCE, strength=0.50,
        sport_key="basketball_ncaab", market_key="spreads",
    )
    # NBA + spreads + medium strength
    s3 = _make_signal(
        signal_type=SignalType.PINNACLE_DIVERGENCE, strength=0.60,
        sport_key="basketball_nba", market_key="spreads",
    )

    # s1 wins: sport(1) + market(1) beats s2's sport(0) + market(0)
    assert poster._pick_best_free_play([s1, s2, s3]) is s1

    # Without sport preference, market preference still wins
    poster._free_play_sports = []
    # s1: sport(0), market(1), 0.25  vs  s3: sport(0), market(0), 0.40
    assert poster._pick_best_free_play([s1, s2, s3]) is s1

    # Without any preferences, lowest strength wins
    poster._free_play_markets = []
    # s2: (0, 0, 0.50)  vs  s3: (0, 0, 0.40)  vs  s1: (0, 0, 0.25)
    assert poster._pick_best_free_play([s1, s2, s3]) is s2


# ── Same-game free play dedup ──────────────────────────────────


@pytest.mark.asyncio
async def test_free_play_skips_repeat_game(settings, repo):
    """Free play should not pick a game that already had a free play (avoids opposite-side picks)."""
    poster = XPoster(settings, repo)
    poster._enabled = True
    poster._digest_mode = False
    poster._client = MagicMock()
    poster._client.create_tweet = MagicMock()
    poster._free_play_interval = 1  # every signal is a free play

    # Record a past free play for evt_123
    await repo.record_alert(
        event_id="evt_123", alert_type="pinnacle_divergence",
        market_key="spreads", outcome_name="Lakers", is_free_play=True,
    )

    # New batch: same game (evt_123) but different side — should be skipped for free play
    sig = _make_signal(
        signal_type=SignalType.PINNACLE_DIVERGENCE,
        outcome_name="Celtics",
        market_key="h2h",
        details={"value_books": [{"bookmaker": "draftkings", "price": 150}]},
    )
    # Record the alert (as Discord alerter would)
    await repo.record_alert(
        event_id="evt_123", alert_type="pinnacle_divergence",
        market_key="h2h", outcome_name="Celtics",
    )

    await poster.post_signals([sig])

    # Should only get a teaser, not a free play
    calls = [c.kwargs["text"] for c in poster._client.create_tweet.call_args_list]
    assert all("FREE PLAY" not in t for t in calls)


@pytest.mark.asyncio
async def test_free_play_picks_different_game(settings, repo):
    """When one game already has a free play, pick the other game instead."""
    poster = XPoster(settings, repo)
    poster._enabled = True
    poster._digest_mode = False
    poster._client = MagicMock()
    poster._client.create_tweet = MagicMock()
    poster._free_play_interval = 1
    poster._free_play_sports = []
    poster._free_play_markets = []

    # Past free play for evt_123
    await repo.record_alert(
        event_id="evt_123", alert_type="pinnacle_divergence",
        market_key="spreads", outcome_name="Lakers", is_free_play=True,
    )

    # Batch: evt_123 (repeat game) + evt_other (new game)
    repeat = Signal(
        signal_type=SignalType.PINNACLE_DIVERGENCE,
        event_id="evt_123", sport_key="basketball_nba",
        home_team="Lakers", away_team="Celtics",
        market_key="h2h", outcome_name="Celtics", strength=0.50,
        description="Test", commence_time="2099-01-15T00:00:00Z",
        details={"value_books": [{"bookmaker": "draftkings", "price": 150}]},
    )
    new_game = Signal(
        signal_type=SignalType.PINNACLE_DIVERGENCE,
        event_id="evt_other", sport_key="basketball_nba",
        home_team="Warriors", away_team="Suns",
        market_key="spreads", outcome_name="Warriors", strength=0.70,
        description="Test", commence_time="2099-01-15T00:00:00Z",
        details={"value_books": [{"bookmaker": "fanduel", "price": -110, "point": -3.5}]},
    )

    for sig in [repeat, new_game]:
        await repo.record_alert(
            event_id=sig.event_id, alert_type="pinnacle_divergence",
            market_key=sig.market_key, outcome_name=sig.outcome_name,
        )

    await poster.post_signals([repeat, new_game])

    calls = [c.kwargs["text"] for c in poster._client.create_tweet.call_args_list]
    free_plays = [t for t in calls if "FREE PLAY" in t]
    assert len(free_plays) == 1
    assert "Warriors" in free_plays[0]  # new game picked, not repeat


# ── Weekend interval ──────────────────────────────────────────


@pytest.mark.asyncio
async def test_weekend_interval_used_on_saturday(settings, repo):
    """On weekends, the wider weekend interval should be used."""
    poster = XPoster(settings, repo)
    poster._enabled = True
    poster._digest_mode = False
    poster._client = MagicMock()
    poster._client.create_tweet = MagicMock()
    poster._free_play_interval = 5
    poster._free_play_weekend_interval = 10

    # Insert 10 alerts so seq=10 (hits weekend interval=10 but not weekday=5*3=15)
    for i in range(10):
        await repo.record_alert(
            event_id=f"evt_{i}", alert_type="pinnacle_divergence",
            market_key="spreads", outcome_name="Lakers",
        )

    sig = _make_signal(
        signal_type=SignalType.PINNACLE_DIVERGENCE,
        details={"value_books": [{"bookmaker": "draftkings", "price": -110, "point": -3.5}]},
    )

    # Saturday → weekend interval (10), seq=10, 10%10==0 → free play
    fake_saturday = datetime(2026, 2, 28, 18, 0, 0, tzinfo=timezone.utc)
    with patch("sharp_seeker.alerts.x_poster.datetime") as mock_dt:
        mock_dt.now.return_value = fake_saturday
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
        await poster.post_signals([sig])

    calls = [c.kwargs["text"] for c in poster._client.create_tweet.call_args_list]
    assert any("FREE PLAY" in t for t in calls)


@pytest.mark.asyncio
async def test_weekend_interval_not_used_on_weekday(settings, repo):
    """On weekdays, the regular interval should be used — weekend interval ignored."""
    poster = XPoster(settings, repo)
    poster._enabled = True
    poster._digest_mode = False
    poster._client = MagicMock()
    poster._client.create_tweet = MagicMock()
    poster._free_play_interval = 5
    poster._free_play_weekend_interval = 10

    # Insert 7 alerts: seq=7, hits neither 5*2=10 nor 10 → no free play
    for i in range(7):
        await repo.record_alert(
            event_id=f"evt_{i}", alert_type="pinnacle_divergence",
            market_key="spreads", outcome_name="Lakers",
        )

    sig = _make_signal(signal_type=SignalType.PINNACLE_DIVERGENCE)

    # Wednesday → weekday interval (5), seq=7, 7%5!=0 → no free play
    fake_wednesday = datetime(2026, 2, 25, 18, 0, 0, tzinfo=timezone.utc)
    with patch("sharp_seeker.alerts.x_poster.datetime") as mock_dt:
        mock_dt.now.return_value = fake_wednesday
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
        await poster.post_signals([sig])

    calls = [c.kwargs["text"] for c in poster._client.create_tweet.call_args_list]
    assert all("FREE PLAY" not in t for t in calls)


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
async def test_rapid_change_eligible_for_free_play(settings, repo):
    """Rapid change signal can become a free play pick."""
    poster = XPoster(settings, repo)
    poster._enabled = True
    poster._digest_mode = False
    poster._client = MagicMock()
    poster._client.create_tweet = MagicMock()
    poster._free_play_interval = 5

    sig = _make_signal(
        signal_type=SignalType.RAPID_CHANGE,
        details={"value_books": [{"bookmaker": "draftkings", "price": -110, "point": -3.5}]},
    )

    # Insert exactly 5 alerts of rapid_change type
    for i in range(5):
        await repo.record_alert(
            event_id=f"evt_{i}",
            alert_type="rapid_change",
            market_key="spreads",
            outcome_name="Lakers",
        )

    await poster.post_signals([sig])

    poster._client.create_tweet.assert_called_once()
    call_text = poster._client.create_tweet.call_args.kwargs["text"]
    assert "FREE PLAY" in call_text
    assert "Rapid Change" in call_text


@pytest.mark.asyncio
async def test_mixed_batch_counter(settings, repo):
    """Batch with PD + rapid changes counts both for seq numbering."""
    poster = XPoster(settings, repo)
    poster._enabled = True
    poster._digest_mode = False
    poster._client = MagicMock()
    poster._client.create_tweet = MagicMock()
    poster._free_play_interval = 5

    # Pre-insert 3 alerts (2 PD + 1 rapid)
    for i in range(2):
        await repo.record_alert(
            event_id=f"evt_pd_{i}", alert_type="pinnacle_divergence",
            market_key="spreads", outcome_name="Lakers",
        )
    await repo.record_alert(
        event_id="evt_rc_0", alert_type="rapid_change",
        market_key="spreads", outcome_name="Lakers",
    )

    # Batch of 3: 1 PD + 2 rapid (total = 3 prior + 3 batch = 6, seq 4,5,6)
    batch = [
        _make_signal(signal_type=SignalType.PINNACLE_DIVERGENCE),
        _make_signal(signal_type=SignalType.RAPID_CHANGE),
        _make_signal(signal_type=SignalType.RAPID_CHANGE),
    ]
    for i, sig in enumerate(batch):
        await repo.record_alert(
            event_id=f"evt_batch_{i}",
            alert_type=sig.signal_type.value,
            market_key="spreads",
            outcome_name="Lakers",
        )

    await poster.post_signals(batch)

    # seq 5 hits interval → 1 free play + 2 teasers = 3 tweets
    assert poster._client.create_tweet.call_count == 3
    calls = [c.kwargs["text"] for c in poster._client.create_tweet.call_args_list]
    free_play_count = sum(1 for t in calls if "FREE PLAY" in t)
    assert free_play_count == 1


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
    """Free plays still tweet right away in digest mode."""
    poster = XPoster(settings, repo)
    poster._enabled = True
    poster._digest_mode = True
    poster._client = MagicMock()
    poster._client.create_tweet = MagicMock()
    poster._free_play_interval = 5

    sig = _make_signal(
        signal_type=SignalType.PINNACLE_DIVERGENCE,
        details={"value_books": [{"bookmaker": "draftkings", "price": -110, "point": -3.5}]},
    )

    # Insert exactly 5 alerts so seq=5 triggers free play
    for i in range(5):
        await repo.record_alert(
            event_id=f"evt_{i}",
            alert_type="pinnacle_divergence",
            market_key="spreads",
            outcome_name="Lakers",
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
    assert "Draftkings" in call_text
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
    assert "Fanduel" in call_text
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
