"""Tests for X (Twitter) poster."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
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
    qualifier_count: int = 1,
) -> Signal:
    # Free plays require a 1+ qualifier (the same bar Discord uses); default the
    # helper to 1 so signals are eligible unless a test overrides it.
    details = dict(details or {})
    details.setdefault("qualifier_count", qualifier_count)
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
        details=details,
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


# ── Free play logic (combo whitelist) ──────────────────────────


@pytest.mark.asyncio
async def test_whitelisted_combo_becomes_free_play(settings, repo):
    """A signal matching a whitelisted combo should become a free play."""
    poster = XPoster(settings, repo)
    poster._enabled = True

    poster._free_play_combos = {"pinnacle_divergence:basketball_nba:totals"}
    poster._free_play_interval = 1
    poster._client = MagicMock()
    poster._client.create_tweet = MagicMock()

    sig = _make_signal(
        signal_type=SignalType.PINNACLE_DIVERGENCE,
        market_key="totals",
        outcome_name="Over",
        details={
            "value_books": [{"bookmaker": "draftkings", "price": -110, "point": 220.5}],
        },
    )

    await poster.post_signals([sig])

    poster._client.create_tweet.assert_called_once()
    call_text = poster._client.create_tweet.call_args.kwargs["text"]
    assert "FREE PLAY" in call_text


@pytest.mark.asyncio
async def test_non_whitelisted_combo_no_free_play(settings, repo):
    """A signal NOT matching any whitelisted combo should not become a free play."""
    poster = XPoster(settings, repo)
    poster._enabled = True

    poster._free_play_combos = {"pinnacle_divergence:basketball_nba:h2h"}
    poster._client = MagicMock()
    poster._client.create_tweet = MagicMock()

    sig = _make_signal(
        signal_type=SignalType.PINNACLE_DIVERGENCE,
        market_key="totals",
        details={
            "value_books": [{"bookmaker": "draftkings", "price": -110, "point": 220.5}],
        },
    )

    await poster.post_signals([sig])

    calls = [c.kwargs["text"] for c in poster._client.create_tweet.call_args_list]
    assert all("FREE PLAY" not in t for t in calls)


@pytest.mark.asyncio
async def test_excluded_sport_no_free_play(settings, repo):
    """A signal matching a combo but in an excluded sport is benched."""
    poster = XPoster(settings, repo)
    poster._enabled = True

    poster._free_play_combos = {"pinnacle_divergence:*:totals"}
    poster._fp_excluded_sports = {"basketball_wnba"}
    poster._free_play_interval = 1
    poster._client = MagicMock()
    poster._client.create_tweet = MagicMock()

    details = {
        "value_books": [{"bookmaker": "draftkings", "price": -110, "point": 165.5}],
    }
    wnba = _make_signal(
        signal_type=SignalType.PINNACLE_DIVERGENCE,
        market_key="totals",
        outcome_name="Over",
        sport_key="basketball_wnba",
        details=details,
    )
    nba = _make_signal(
        signal_type=SignalType.PINNACLE_DIVERGENCE,
        market_key="totals",
        outcome_name="Over",
        sport_key="basketball_nba",
        details=details,
    )
    nba.event_id = "evt_nba"

    await poster.post_signals([wnba, nba])

    calls = [c.kwargs["text"] for c in poster._client.create_tweet.call_args_list]
    free_plays = [t for t in calls if "FREE PLAY" in t]
    # WNBA benched, NBA (same combo, not excluded) still posts.
    assert len(free_plays) == 1


@pytest.mark.asyncio
async def test_excluded_combo_no_free_play(settings, repo):
    """An excluded type:sport:market combo is benched while the blanket combo
    still serves other sports — WNBA totals out, NBA totals through."""
    poster = XPoster(settings, repo)
    poster._enabled = True

    poster._free_play_combos = {"pinnacle_divergence:*:totals"}
    poster._fp_excluded_combos = {"pinnacle_divergence:basketball_wnba:totals"}
    poster._free_play_interval = 1
    poster._client = MagicMock()
    poster._client.create_tweet = MagicMock()

    details = {
        "value_books": [{"bookmaker": "draftkings", "price": -110, "point": 165.5}],
    }
    wnba = _make_signal(
        signal_type=SignalType.PINNACLE_DIVERGENCE,
        market_key="totals",
        outcome_name="Over",
        sport_key="basketball_wnba",
        details=details,
    )
    nba = _make_signal(
        signal_type=SignalType.PINNACLE_DIVERGENCE,
        market_key="totals",
        outcome_name="Over",
        sport_key="basketball_nba",
        details=details,
    )
    nba.event_id = "evt_nba"

    await poster.post_signals([wnba, nba])

    calls = [c.kwargs["text"] for c in poster._client.create_tweet.call_args_list]
    free_plays = [t for t in calls if "FREE PLAY" in t]
    # WNBA totals carved out by the combo exclusion; NBA totals still posts.
    assert len(free_plays) == 1


@pytest.mark.asyncio
async def test_interval_skips_early_signals(settings, repo):
    """With interval=3, only every 3rd eligible signal becomes a free play."""
    poster = XPoster(settings, repo)
    poster._enabled = True

    poster._free_play_combos = {"pinnacle_divergence:basketball_nba:totals"}
    poster._free_play_interval = 3
    poster._free_play_sport_cap = 10
    poster._free_play_hourly_cap = 10
    poster._client = MagicMock()
    poster._client.create_tweet = MagicMock()

    # Send 3 eligible signals one at a time (simulating 3 poll cycles)
    for i in range(3):
        sig = _make_signal(
            signal_type=SignalType.PINNACLE_DIVERGENCE,
            market_key="totals",
            outcome_name="Over",
            details={
                "value_books": [{"bookmaker": "draftkings", "price": -110, "point": 220.5}],
            },
        )
        sig.event_id = f"evt_{i}"
        await poster.post_signals([sig])

    calls = [c.kwargs["text"] for c in poster._client.create_tweet.call_args_list]
    free_plays = [t for t in calls if "FREE PLAY" in t]
    assert len(free_plays) == 1  # only the 3rd one fires


@pytest.mark.asyncio
async def test_empty_combo_list_no_free_plays(settings, repo):
    """Empty free_play_combos list should produce no free plays."""
    poster = XPoster(settings, repo)
    poster._enabled = True

    poster._free_play_combos = set()
    poster._client = MagicMock()
    poster._client.create_tweet = MagicMock()

    sig = _make_signal(
        signal_type=SignalType.PINNACLE_DIVERGENCE,
        market_key="spreads",
        details={
            "value_books": [{"bookmaker": "draftkings", "price": -110, "point": -3.5}],
        },
    )

    await poster.post_signals([sig])

    calls = [c.kwargs["text"] for c in poster._client.create_tweet.call_args_list]
    assert all("FREE PLAY" not in t for t in calls)


@pytest.mark.asyncio
async def test_sport_cap_limits_free_plays(settings, repo):
    """Sport cap should limit free plays from the same sport."""
    poster = XPoster(settings, repo)
    poster._enabled = True

    poster._free_play_combos = {"pinnacle_divergence:basketball_nba:totals"}
    poster._free_play_interval = 1
    poster._free_play_sport_cap = 1  # cap at 1 per sport
    poster._client = MagicMock()
    poster._client.create_tweet = MagicMock()

    sig1 = _make_signal(
        signal_type=SignalType.PINNACLE_DIVERGENCE,
        market_key="totals",
        outcome_name="Over",
        details={
            "value_books": [{"bookmaker": "draftkings", "price": -110, "point": 220.5}],
        },
    )
    sig2 = _make_signal(
        signal_type=SignalType.PINNACLE_DIVERGENCE,
        market_key="totals",
        outcome_name="Under",
        details={
            "value_books": [{"bookmaker": "fanduel", "price": -105, "point": 224.5}],
        },
    )
    sig2.event_id = "evt_2"

    await poster.post_signals([sig1, sig2])

    calls = [c.kwargs["text"] for c in poster._client.create_tweet.call_args_list]
    free_plays = [t for t in calls if "FREE PLAY" in t]
    assert len(free_plays) == 1  # second one sport-capped


# ── Tweet formatting ─────────────────────────────────────────────


def test_format_free_play_with_value_books(settings, repo):
    poster = XPoster(settings, repo)

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
    assert "Get all picks" not in text


def test_format_free_play_moneyline(settings, repo):
    poster = XPoster(settings, repo)

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
async def test_2u_excluded_book_skipped(settings, repo):
    """2U signal from an excluded book should NOT become a free play."""
    poster = XPoster(settings, repo)
    poster._enabled = True

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
    """Won/lost/push signals format correctly in the recap with units."""
    poster = XPoster(settings, repo)

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
    # Header now includes record + units (1W-1L, push doesn't count)
    # Lakers: won at -110 = +1.0u. Chiefs: lost at +150 = -0.67u. Net = +0.3u
    assert "Yesterday: 1-1" in text
    assert "+0.3u" in text
    assert "\u2705" in text  # won
    assert "\u274c" in text  # lost
    assert "\u21a9\ufe0f" in text  # push
    assert "Lakers" in text
    assert "Chiefs" in text
    # Per-pick units appear inline
    assert "(+1.0u)" in text  # Lakers win
    assert "Get all picks" not in text


def test_format_recap_includes_mtd_footer(settings, repo):
    """When mtd_results provided, footer shows month-to-date W-L and units."""
    poster = XPoster(settings, repo)

    yesterday = [
        {"outcome_name": "Lakers", "market_key": "spreads", "result": "won",
         "signal_strength": 0.85, "event_id": "e1", "sent_at": "2099-01-15",
         "details_json": json.dumps({"value_books": [{"price": -110}]})},
    ]
    # MTD: include yesterday's win plus 2 other prior wins and 1 loss
    mtd = yesterday + [
        {"outcome_name": "X", "market_key": "h2h", "result": "won",
         "details_json": json.dumps({"value_books": [{"price": -110}]})},
        {"outcome_name": "Y", "market_key": "h2h", "result": "won",
         "details_json": json.dumps({"value_books": [{"price": -110}]})},
        {"outcome_name": "Z", "market_key": "h2h", "result": "lost",
         "details_json": json.dumps({"value_books": [{"price": -110}]})},
    ]

    text = poster._format_recap(yesterday, mtd)
    # 3W-1L: +3.0u (wins) - 1.1u (loss at -110) = +1.9u
    assert "3-1" in text
    assert "+1.9u" in text


def test_format_recap_zero_plays_always_posts(settings, repo):
    """Empty results must still produce a tweet body (accountability beat)."""
    poster = XPoster(settings, repo)

    text = poster._format_recap([])
    assert text  # not empty
    assert "No free plays yesterday" in text


def test_format_recap_pending(settings, repo):
    """Unresolved signals show as pending."""
    poster = XPoster(settings, repo)

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
    """Zero free plays still posts an accountability tweet (no-play message)."""
    poster = XPoster(settings, repo)
    poster._enabled = True
    poster._client = MagicMock()
    poster._client.create_tweet = MagicMock()

    await poster.post_daily_recap()

    # Always-post behavior: tweet IS sent, with the no-plays message
    poster._client.create_tweet.assert_called_once()
    call_text = poster._client.create_tweet.call_args.kwargs["text"]
    assert "No free plays yesterday" in call_text


@pytest.mark.asyncio
async def test_post_daily_recap_calls_tweepy(settings, repo):
    """When free plays exist, recap tweet is posted via tweepy."""
    poster = XPoster(settings, repo)
    poster._enabled = True
    poster._client = MagicMock()
    poster._client.create_tweet = MagicMock()

    # Insert a free play alert + resolved signal result
    now = datetime.now(timezone.utc).isoformat()
    await repo.record_alert(
        event_id="evt_recap",
        alert_type="pinnacle_divergence",
        market_key="spreads",
        outcome_name="Lakers",
        details_json=json.dumps({"value_books": [{"bookmaker": "draftkings", "price": -110, "point": -3.5}]}),
        is_free_play=True,
    )
    await repo.record_signal_result(
        event_id="evt_recap", signal_type="pinnacle_divergence",
        market_key="spreads", outcome_name="Lakers",
        signal_direction="over", signal_strength=0.5, signal_at=now,
    )
    await repo.resolve_signal(
        event_id="evt_recap", signal_type="pinnacle_divergence",
        market_key="spreads", outcome_name="Lakers",
        signal_at=now, result="won",
    )

    await poster.post_daily_recap()

    poster._client.create_tweet.assert_called_once()
    call_text = poster._client.create_tweet.call_args.kwargs["text"]
    # New header format includes record + units
    assert "Yesterday: 1-0" in call_text
    assert "+1.0u" in call_text  # won at -110
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
async def test_free_play_results_dedup_multi_cycle_signal(settings, repo):
    """A free play whose signal fired across multiple poll cycles must count once.

    Regression for the recap fan-out: one free play with N signal_results rows
    (one per cycle, differing only by signal_at) was joined into N rows and
    counted N times — e.g. a single win shown as 4-0.
    """
    base = datetime(2026, 6, 3, 18, 0, tzinfo=timezone.utc)
    await repo.record_alert(
        event_id="evt_multi",
        alert_type="pinnacle_divergence",
        market_key="totals",
        outcome_name="Under",
        details_json=json.dumps(
            {"value_books": [{"bookmaker": "draftkings", "price": -114, "point": 218.5}]}
        ),
        is_free_play=True,
    )
    # Same signal recorded across four cycles (distinct signal_at) and all graded won.
    for i in range(4):
        signal_at = (base + timedelta(minutes=12 * i)).isoformat()
        await repo.record_signal_result(
            event_id="evt_multi", signal_type="pinnacle_divergence",
            market_key="totals", outcome_name="Under",
            signal_direction="under", signal_strength=0.5, signal_at=signal_at,
        )
        await repo.resolve_signal(
            event_id="evt_multi", signal_type="pinnacle_divergence",
            market_key="totals", outcome_name="Under",
            signal_at=signal_at, result="won",
        )

    since = (base - timedelta(days=2)).isoformat()
    resolved = await repo.get_free_play_results_resolved_since(since)
    sent_based = await repo.get_free_play_results_since(since)

    assert len(resolved) == 1, f"expected 1 row, got {len(resolved)} (fan-out)"
    assert len(sent_based) == 1, f"expected 1 row, got {len(sent_based)} (fan-out)"
    assert dict(resolved[0])["result"] == "won"


@pytest.mark.asyncio
async def test_post_signals_marks_free_play_in_db(settings, repo):
    """post_signals should mark whitelisted combo signals as free plays in the DB."""
    poster = XPoster(settings, repo)
    poster._enabled = True
    poster._free_play_combos = {"pinnacle_divergence:basketball_nba:totals"}
    poster._free_play_interval = 1
    poster._client = MagicMock()
    poster._client.create_tweet = MagicMock()

    sig = _make_signal(
        signal_type=SignalType.PINNACLE_DIVERGENCE,
        market_key="totals",
        outcome_name="Over",
        details={
            "value_books": [{"bookmaker": "draftkings", "price": -110, "point": 220.5}],
        },
    )

    # Record the alert (as Discord alerter would)
    await repo.record_alert(
        event_id="evt_123", alert_type="pinnacle_divergence",
        market_key="totals", outcome_name="Over",
    )

    await poster.post_signals([sig])

    # The alert for evt_123 should now be marked as free play
    cursor = await repo._db.execute(
        "SELECT is_free_play FROM sent_alerts WHERE event_id = 'evt_123'"
    )
    row = await cursor.fetchone()
    assert row["is_free_play"] == 1


@pytest.mark.asyncio
async def test_hourly_cap_limits_free_plays(settings, repo):
    """Only 1 free play per hour — second whitelisted signal should be hourly-capped."""
    poster = XPoster(settings, repo)
    poster._enabled = True

    poster._free_play_combos = {"pinnacle_divergence:basketball_nba:totals"}
    poster._free_play_interval = 1
    poster._free_play_hourly_cap = 1
    poster._free_play_sport_cap = 5  # not the limiting factor
    poster._client = MagicMock()
    poster._client.create_tweet = MagicMock()

    sig1 = Signal(
        signal_type=SignalType.PINNACLE_DIVERGENCE,
        event_id="evt_1", sport_key="basketball_nba",
        home_team="Lakers", away_team="Celtics",
        market_key="totals", outcome_name="Over", strength=0.60,
        description="Test", commence_time="2099-01-15T00:00:00Z",
        details={
            "qualifier_count": 1,
            "value_books": [{"bookmaker": "draftkings", "price": -110, "point": 220.5}],
        },
    )
    sig2 = Signal(
        signal_type=SignalType.PINNACLE_DIVERGENCE,
        event_id="evt_2", sport_key="basketball_nba",
        home_team="Warriors", away_team="Suns",
        market_key="totals", outcome_name="Under", strength=0.55,
        description="Test", commence_time="2099-01-15T00:00:00Z",
        details={
            "qualifier_count": 1,
            "value_books": [{"bookmaker": "fanduel", "price": -105, "point": 224.5}],
        },
    )

    await poster.post_signals([sig1, sig2])

    calls = [c.kwargs["text"] for c in poster._client.create_tweet.call_args_list]
    free_play_count = sum(1 for t in calls if "FREE PLAY" in t)
    assert free_play_count == 1  # second one hourly-capped


# ── Same-game free play dedup ──────────────────────────────────


@pytest.mark.asyncio
async def test_free_play_skips_repeat_game(settings, repo):
    """Free play should skip a game that already had a free play."""
    poster = XPoster(settings, repo)
    poster._enabled = True

    poster._free_play_combos = {"pinnacle_divergence:basketball_nba:h2h"}
    poster._client = MagicMock()
    poster._client.create_tweet = MagicMock()

    # Record a past free play for evt_123
    await repo.record_alert(
        event_id="evt_123", alert_type="pinnacle_divergence",
        market_key="spreads", outcome_name="Lakers", is_free_play=True,
    )

    # New batch: same game (evt_123), whitelisted combo — should be skipped
    sig = _make_signal(
        signal_type=SignalType.PINNACLE_DIVERGENCE,
        outcome_name="Celtics",
        market_key="h2h",
        details={
            "value_books": [{"bookmaker": "draftkings", "price": 150}],
        },
    )

    await poster.post_signals([sig])

    # Should only get a teaser, not a free play
    calls = [c.kwargs["text"] for c in poster._client.create_tweet.call_args_list]
    assert all("FREE PLAY" not in t for t in calls)


@pytest.mark.asyncio
async def test_free_play_picks_different_game(settings, repo):
    """When one game already has a free play, the other whitelisted game gets picked."""
    poster = XPoster(settings, repo)
    poster._enabled = True

    poster._free_play_combos = {
        "pinnacle_divergence:basketball_nba:h2h",
        "pinnacle_divergence:basketball_nba:totals",
    }
    poster._free_play_interval = 1
    poster._free_play_sport_cap = 10
    poster._free_play_hourly_cap = 10
    poster._client = MagicMock()
    poster._client.create_tweet = MagicMock()

    # Past free play for evt_123
    await repo.record_alert(
        event_id="evt_123", alert_type="pinnacle_divergence",
        market_key="spreads", outcome_name="Lakers", is_free_play=True,
    )

    # Batch: evt_123 (repeat) + evt_other (new) — both whitelisted
    repeat = Signal(
        signal_type=SignalType.PINNACLE_DIVERGENCE,
        event_id="evt_123", sport_key="basketball_nba",
        home_team="Lakers", away_team="Celtics",
        market_key="h2h", outcome_name="Celtics", strength=0.50,
        description="Test", commence_time="2099-01-15T00:00:00Z",
        details={
            "qualifier_count": 1,
            "value_books": [{"bookmaker": "draftkings", "price": 150}],
        },
    )
    new_game = Signal(
        signal_type=SignalType.PINNACLE_DIVERGENCE,
        event_id="evt_other", sport_key="basketball_nba",
        home_team="Warriors", away_team="Suns",
        market_key="totals", outcome_name="Over", strength=0.70,
        description="Test", commence_time="2099-01-15T00:00:00Z",
        details={
            "qualifier_count": 1,
            "value_books": [{"bookmaker": "fanduel", "price": -110, "point": 224.5}],
        },
    )

    await poster.post_signals([repeat, new_game])

    calls = [c.kwargs["text"] for c in poster._client.create_tweet.call_args_list]
    free_plays = [t for t in calls if "FREE PLAY" in t]
    assert len(free_plays) == 1
    assert "Warriors" in free_plays[0]  # new game picked, not repeat




# ── Wildcard combos & qualifier gate ───────────────────────────


@pytest.mark.asyncio
async def test_wildcard_combo_matches_all_sports(settings, repo):
    """`pinnacle_divergence:*:totals` should match PD totals in any sport."""
    poster = XPoster(settings, repo)
    poster._enabled = True

    poster._free_play_combos = {"pinnacle_divergence:*:totals"}
    poster._free_play_interval = 1
    poster._client = MagicMock()
    poster._client.create_tweet = MagicMock()

    # A sport not explicitly listed anywhere — only the wildcard can match it.
    sig = _make_signal(
        signal_type=SignalType.PINNACLE_DIVERGENCE,
        market_key="totals",
        outcome_name="Over",
        sport_key="basketball_ncaab",
        details={
            "value_books": [{"bookmaker": "draftkings", "price": -110, "point": 145.5}],
        },
    )

    await poster.post_signals([sig])

    calls = [c.kwargs["text"] for c in poster._client.create_tweet.call_args_list]
    assert any("FREE PLAY" in t for t in calls)


@pytest.mark.asyncio
async def test_wildcard_totals_does_not_match_h2h_or_spreads(settings, repo):
    """`pinnacle_divergence:*:totals` must not pull in PD h2h or PD spreads."""
    poster = XPoster(settings, repo)
    poster._enabled = True

    poster._free_play_combos = {"pinnacle_divergence:*:totals"}
    poster._free_play_interval = 1
    poster._client = MagicMock()
    poster._client.create_tweet = MagicMock()

    h2h = _make_signal(
        signal_type=SignalType.PINNACLE_DIVERGENCE,
        market_key="h2h", outcome_name="Lakers",
        details={"value_books": [{"bookmaker": "draftkings", "price": 150}]},
    )
    spreads = _make_signal(
        signal_type=SignalType.PINNACLE_DIVERGENCE,
        market_key="spreads", outcome_name="Lakers",
        details={"value_books": [{"bookmaker": "draftkings", "price": -110, "point": -3.5}]},
    )
    spreads.event_id = "evt_spreads"

    await poster.post_signals([h2h, spreads])

    calls = [c.kwargs["text"] for c in poster._client.create_tweet.call_args_list]
    assert all("FREE PLAY" not in t for t in calls)


@pytest.mark.asyncio
async def test_totals_wildcard_matches_any_signal_type(settings, repo):
    """`*:*:totals` should match totals from any signal type (PD, steam, rapid)."""
    poster = XPoster(settings, repo)
    poster._enabled = True

    poster._free_play_combos = {"*:*:totals"}
    poster._free_play_interval = 1
    poster._free_play_hourly_cap = 0  # unlimited
    poster._free_play_sport_cap = 0  # unlimited
    poster._client = MagicMock()
    poster._client.create_tweet = MagicMock()

    sigs = []
    for i, st in enumerate(
        (SignalType.PINNACLE_DIVERGENCE, SignalType.STEAM_MOVE, SignalType.RAPID_CHANGE)
    ):
        sig = _make_signal(
            signal_type=st,
            market_key="totals",
            outcome_name="Over",
            sport_key="basketball_nba",
            details={
                "value_books": [{"bookmaker": "draftkings", "price": -110, "point": 220.5}],
            },
        )
        sig.event_id = f"evt_type_{i}"
        sigs.append(sig)

    await poster.post_signals(sigs)

    free_plays = [
        c.kwargs["text"]
        for c in poster._client.create_tweet.call_args_list
        if "FREE PLAY" in c.kwargs["text"]
    ]
    assert len(free_plays) == 3  # all three types post


@pytest.mark.asyncio
async def test_pd_only_combo_excludes_steam_and_rapid_totals(settings, repo):
    """With `pinnacle_divergence:*:totals`, only PD totals post — not steam/rapid."""
    poster = XPoster(settings, repo)
    poster._enabled = True

    poster._free_play_combos = {"pinnacle_divergence:*:totals"}
    poster._free_play_interval = 1
    poster._free_play_hourly_cap = 0
    poster._free_play_sport_cap = 0
    poster._client = MagicMock()
    poster._client.create_tweet = MagicMock()

    sigs = []
    for i, st in enumerate(
        (SignalType.PINNACLE_DIVERGENCE, SignalType.STEAM_MOVE, SignalType.RAPID_CHANGE)
    ):
        sig = _make_signal(
            signal_type=st,
            market_key="totals",
            outcome_name="Over",
            sport_key="basketball_nba",
            details={
                "value_books": [{"bookmaker": "draftkings", "price": -110, "point": 220.5}],
            },
        )
        sig.event_id = f"evt_pdonly_{i}"
        sigs.append(sig)

    await poster.post_signals(sigs)

    free_plays = [
        c.kwargs["text"]
        for c in poster._client.create_tweet.call_args_list
        if "FREE PLAY" in c.kwargs["text"]
    ]
    assert len(free_plays) == 1  # only the PD totals signal


@pytest.mark.asyncio
async def test_caps_zero_means_unlimited(settings, repo):
    """Hourly/sport caps of 0 should not limit free plays (mirror-Discord mode)."""
    poster = XPoster(settings, repo)
    poster._enabled = True

    poster._free_play_combos = {"pinnacle_divergence:*:totals"}
    poster._free_play_interval = 1
    poster._free_play_hourly_cap = 0
    poster._free_play_sport_cap = 0
    poster._client = MagicMock()
    poster._client.create_tweet = MagicMock()

    # 4 PD totals, same sport, same poll cycle (same hour) — none should be capped.
    sigs = []
    for i in range(4):
        sig = _make_signal(
            signal_type=SignalType.PINNACLE_DIVERGENCE,
            market_key="totals",
            outcome_name="Over",
            sport_key="basketball_nba",
            details={
                "value_books": [{"bookmaker": "draftkings", "price": -110, "point": 220.5}],
            },
        )
        sig.event_id = f"evt_cap_{i}"
        sigs.append(sig)

    await poster.post_signals(sigs)

    free_plays = [
        c.kwargs["text"]
        for c in poster._client.create_tweet.call_args_list
        if "FREE PLAY" in c.kwargs["text"]
    ]
    assert len(free_plays) == 4  # no throttling


@pytest.mark.asyncio
async def test_zero_qualifier_skipped(settings, repo):
    """A combo-matching signal with 0 qualifiers must not become a free play."""
    poster = XPoster(settings, repo)
    poster._enabled = True

    poster._free_play_combos = {"pinnacle_divergence:*:totals"}
    poster._free_play_interval = 1
    poster._client = MagicMock()
    poster._client.create_tweet = MagicMock()

    sig = _make_signal(
        signal_type=SignalType.PINNACLE_DIVERGENCE,
        market_key="totals",
        outcome_name="Over",
        qualifier_count=0,
        details={
            "value_books": [{"bookmaker": "draftkings", "price": -110, "point": 220.5}],
        },
    )

    await poster.post_signals([sig])

    calls = [c.kwargs["text"] for c in poster._client.create_tweet.call_args_list]
    assert all("FREE PLAY" not in t for t in calls)


@pytest.mark.asyncio
async def test_rapid_change_not_free_play_unless_whitelisted(settings, repo):
    """Rapid change signal should NOT become a free play unless in combo whitelist."""
    poster = XPoster(settings, repo)
    poster._enabled = True

    poster._free_play_combos = {"pinnacle_divergence:basketball_nba:spreads"}
    poster._client = MagicMock()
    poster._client.create_tweet = MagicMock()

    sig = _make_signal(
        signal_type=SignalType.RAPID_CHANGE,
        details={
            "value_books": [{"bookmaker": "draftkings", "price": -110, "point": -3.5}],
        },
    )

    await poster.post_signals([sig])

    calls = [c.kwargs["text"] for c in poster._client.create_tweet.call_args_list]
    assert all("FREE PLAY" not in t for t in calls)




# ── Weekly recap ──────────────────────────────────────────────────


def test_format_weekly_recap_with_results(settings, repo):
    """Weekly recap is a summary: record + net units, no per-pick list."""
    poster = XPoster(settings, repo)

    results = [
        {"outcome_name": "Lakers", "market_key": "spreads", "result": "won",
         "details_json": json.dumps({"value_books": [{"bookmaker": "draftkings", "price": -110, "point": -3.5}]})},
        {"outcome_name": "Chiefs", "market_key": "h2h", "result": "lost",
         "details_json": json.dumps({"value_books": [{"bookmaker": "fanduel", "price": 150}]})},
        {"outcome_name": "Celtics", "market_key": "spreads", "result": "won",
         "details_json": json.dumps({"value_books": [{"bookmaker": "betmgm", "price": -105, "point": -2.5}]})},
    ]

    text = poster._format_weekly_recap(results)
    assert "Weekly Free Plays" in text
    # Units: Lakers won @-110 = +1.0u, Chiefs lost @+150 = -0.7u,
    # Celtics won @-105 = +1.0u -> net +1.3u
    assert "Record: 2-1 (+1.3u)" in text
    # Summary only: individual picks are not listed.
    assert "Lakers" not in text
    assert "Chiefs" not in text
    assert len(text) <= 280


def test_format_weekly_recap_pending(settings, repo):
    """Undecided plays are surfaced as a pending count, not hidden."""
    poster = XPoster(settings, repo)

    results = [
        {"outcome_name": "Lakers", "market_key": "spreads", "result": "won",
         "details_json": json.dumps({"value_books": [{"bookmaker": "draftkings", "price": -110, "point": -3.5}]})},
        {"outcome_name": "Bulls", "market_key": "spreads", "result": None,
         "details_json": json.dumps({"value_books": [{"bookmaker": "draftkings", "price": -110, "point": 2.5}]})},
    ]

    text = poster._format_weekly_recap(results)
    assert "Record: 1-0 (+1.0u)" in text
    assert "1 still pending" in text


def test_format_weekly_recap_stays_compact(settings, repo):
    """Many picks still collapse to a single record line under 280 chars."""
    poster = XPoster(settings, repo)

    results = []
    for i in range(40):
        results.append({
            "outcome_name": f"Team{i}LongName",
            "market_key": "spreads",
            "result": "won" if i % 2 == 0 else "lost",
            "details_json": json.dumps({"value_books": [{"bookmaker": "draftkings", "price": -110, "point": -3.5}]}),
        })

    text = poster._format_weekly_recap(results)
    assert len(text) <= 280
    assert "Record: 20-20" in text
    assert "...and" not in text  # no per-pick truncation, it is a summary
    assert "Team0LongName" not in text


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
    # Summary only — the ungraded play shows as pending, not by name.
    assert "1 still pending" in call_text


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

    # Insert a free play alert + resolved signal result
    now = datetime.now(timezone.utc).isoformat()
    await repo.record_alert(
        event_id="evt_card", alert_type="pinnacle_divergence",
        market_key="spreads", outcome_name="Lakers", is_free_play=True,
    )
    await repo.record_signal_result(
        event_id="evt_card", signal_type="pinnacle_divergence",
        market_key="spreads", outcome_name="Lakers",
        signal_direction="over", signal_strength=0.5, signal_at=now,
    )
    await repo.resolve_signal(
        event_id="evt_card", signal_type="pinnacle_divergence",
        market_key="spreads", outcome_name="Lakers",
        signal_at=now, result="won",
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

    now = datetime.now(timezone.utc).isoformat()
    await repo.record_alert(
        event_id="evt_fail", alert_type="pinnacle_divergence",
        market_key="spreads", outcome_name="Lakers", is_free_play=True,
    )
    await repo.record_signal_result(
        event_id="evt_fail", signal_type="pinnacle_divergence",
        market_key="spreads", outcome_name="Lakers",
        signal_direction="over", signal_strength=0.5, signal_at=now,
    )
    await repo.resolve_signal(
        event_id="evt_fail", signal_type="pinnacle_divergence",
        market_key="spreads", outcome_name="Lakers",
        signal_at=now, result="won",
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

    now = datetime.now(timezone.utc).isoformat()
    await repo.record_alert(
        event_id="evt_nocard", alert_type="pinnacle_divergence",
        market_key="spreads", outcome_name="Lakers", is_free_play=True,
    )
    await repo.record_signal_result(
        event_id="evt_nocard", signal_type="pinnacle_divergence",
        market_key="spreads", outcome_name="Lakers",
        signal_direction="over", signal_strength=0.5, signal_at=now,
    )
    await repo.resolve_signal(
        event_id="evt_nocard", signal_type="pinnacle_divergence",
        market_key="spreads", outcome_name="Lakers",
        signal_at=now, result="won",
    )

    await poster.post_daily_recap()

    poster._client.create_tweet.assert_called_once()
    call_kwargs = poster._client.create_tweet.call_args.kwargs
    assert "media_ids" not in call_kwargs
