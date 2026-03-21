"""Tests for Discord alerter — tiered badges and qualifier system."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from sharp_seeker.alerts.discord import DiscordAlerter
from sharp_seeker.config import Settings
from sharp_seeker.engine.base import Signal, SignalType


def _make_settings(**overrides) -> Settings:
    defaults = dict(
        odds_api_key="test_key",
        discord_webhook_url="https://discord.com/api/webhooks/test/test",
        db_path=":memory:",
    )
    defaults.update(overrides)
    return Settings(**defaults)


def _make_signal(
    signal_type: SignalType = SignalType.PINNACLE_DIVERGENCE,
    sport_key: str = "basketball_nba",
    market_key: str = "totals",
    details: dict | None = None,
) -> Signal:
    base_details = {
        "us_book": "draftkings",
        "us_value": 220.5,
        "pinnacle_value": 219.5,
        "delta": 1.0,
        "value_books": [
            {"bookmaker": "draftkings", "price": -110, "point": 220.5},
        ],
    }
    if details:
        base_details.update(details)
    return Signal(
        signal_type=signal_type,
        event_id="evt_1",
        sport_key=sport_key,
        home_team="Lakers",
        away_team="Celtics",
        market_key=market_key,
        outcome_name="Over",
        strength=0.70,
        description="test signal",
        commence_time="2099-01-15T00:00:00Z",
        details=base_details,
    )


def _utc_for_mst_hour(hour: int) -> datetime:
    """Return a UTC datetime whose MST equivalent has the given hour."""
    # MST = UTC-7, so UTC hour = MST hour + 7
    return datetime(2025, 6, 15, (hour + 7) % 24, 30, 0, tzinfo=timezone.utc)


# ── Best combo badge tests ────────────────────────────────────


@patch("sharp_seeker.alerts.discord.DiscordWebhook")
def test_best_combo_no_badge(mock_webhook_cls):
    """Signal with 1 qualifier (best combo only) shows no tier badge."""
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_instance = MagicMock()
    mock_instance.execute.return_value = mock_resp
    mock_webhook_cls.return_value = mock_instance

    settings = _make_settings(
        signal_best_combos=["pinnacle_divergence:basketball_nba:totals"],
    )
    alerter = DiscordAlerter(settings, repo=MagicMock())
    sig = _make_signal()  # PD, NBA, totals — matches
    sig.details["qualifier_count"] = 1
    sig.details["qualifier_tags"] = ["Best Combo"]

    alerter._send_embed(sig)

    embed = mock_instance.add_embed.call_args[0][0]
    field_names = [f["name"] for f in embed.fields]
    # 1 qualifier = no badge (only 2U gets a badge)
    assert "\U0001f525 2U PLAY" not in field_names
    assert "\U0001f3c6 Elite Signal" not in field_names


@patch("sharp_seeker.alerts.discord.DiscordWebhook")
def test_best_combo_badge_not_shown(mock_webhook_cls):
    """Signal NOT in best combos should have no badge field."""
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_instance = MagicMock()
    mock_instance.execute.return_value = mock_resp
    mock_webhook_cls.return_value = mock_instance

    settings = _make_settings(
        signal_best_combos=["pinnacle_divergence:basketball_nba:totals"],
    )
    alerter = DiscordAlerter(settings, repo=MagicMock())
    sig = _make_signal(market_key="spreads")  # PD, NBA, spreads — no match
    sig.details["qualifier_count"] = 0
    sig.details["qualifier_tags"] = []

    alerter._send_embed(sig)

    embed = mock_instance.add_embed.call_args[0][0]
    field_names = [f["name"] for f in embed.fields]
    assert "\U0001f3c6 Elite Signal" not in field_names
    assert "\U0001f525 2U PLAY" not in field_names


@patch("sharp_seeker.alerts.discord.DiscordWebhook")
def test_best_combo_empty_config(mock_webhook_cls):
    """Empty best combos list means no badge on any signal."""
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_instance = MagicMock()
    mock_instance.execute.return_value = mock_resp
    mock_webhook_cls.return_value = mock_instance

    settings = _make_settings(signal_best_combos=[])
    alerter = DiscordAlerter(settings, repo=MagicMock())
    sig = _make_signal()
    sig.details["qualifier_count"] = 0
    sig.details["qualifier_tags"] = []

    alerter._send_embed(sig)

    embed = mock_instance.add_embed.call_args[0][0]
    field_names = [f["name"] for f in embed.fields]
    assert "\U0001f3c6 Elite Signal" not in field_names
    assert "\U0001f525 2U PLAY" not in field_names


# ── Best hour badge tests ──────────────────────────────────────


@patch("sharp_seeker.alerts.discord.datetime")
@patch("sharp_seeker.alerts.discord.DiscordWebhook")
def test_best_hour_no_badge(mock_webhook_cls, mock_dt):
    """Signal with 1 qualifier (best hour only) shows no tier badge."""
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_instance = MagicMock()
    mock_instance.execute.return_value = mock_resp
    mock_webhook_cls.return_value = mock_instance

    mock_dt.now.return_value = _utc_for_mst_hour(16)
    mock_dt.fromisoformat = datetime.fromisoformat
    mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)

    settings = _make_settings(
        signal_best_hours={"pinnacle_divergence": [6, 12, 14, 16, 17]},
    )
    alerter = DiscordAlerter(settings, repo=MagicMock())
    sig = _make_signal()
    sig.details["qualifier_count"] = 1
    sig.details["qualifier_tags"] = ["Best Hour"]

    alerter._send_embed(sig)

    embed = mock_instance.add_embed.call_args[0][0]
    field_names = [f["name"] for f in embed.fields]
    assert "\U0001f525 2U PLAY" not in field_names
    assert "\U0001f3c6 Elite Signal" not in field_names


@patch("sharp_seeker.alerts.discord.datetime")
@patch("sharp_seeker.alerts.discord.DiscordWebhook")
def test_best_hour_badge_not_shown(mock_webhook_cls, mock_dt):
    """Signal NOT matching a best hour should have no badge."""
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_instance = MagicMock()
    mock_instance.execute.return_value = mock_resp
    mock_webhook_cls.return_value = mock_instance

    mock_dt.now.return_value = _utc_for_mst_hour(10)
    mock_dt.fromisoformat = datetime.fromisoformat
    mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)

    settings = _make_settings(
        signal_best_hours={"pinnacle_divergence": [6, 12, 14, 16, 17]},
    )
    alerter = DiscordAlerter(settings, repo=MagicMock())
    sig = _make_signal()
    sig.details["qualifier_count"] = 0
    sig.details["qualifier_tags"] = []

    alerter._send_embed(sig)

    embed = mock_instance.add_embed.call_args[0][0]
    field_names = [f["name"] for f in embed.fields]
    assert "\U0001f3c6 Elite Signal" not in field_names


@patch("sharp_seeker.alerts.discord.datetime")
@patch("sharp_seeker.alerts.discord.DiscordWebhook")
def test_best_hour_only_no_badge(mock_webhook_cls, mock_dt):
    """Signal matches best hour but NOT best combo — no badge (only 2U gets badge)."""
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_instance = MagicMock()
    mock_instance.execute.return_value = mock_resp
    mock_webhook_cls.return_value = mock_instance

    mock_dt.now.return_value = _utc_for_mst_hour(16)
    mock_dt.fromisoformat = datetime.fromisoformat
    mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)

    settings = _make_settings(
        signal_best_combos=["pinnacle_divergence:basketball_nba:spreads"],
        signal_best_hours={"pinnacle_divergence": [16]},
    )
    alerter = DiscordAlerter(settings, repo=MagicMock())
    sig = _make_signal(market_key="totals")
    sig.details["qualifier_count"] = 1
    sig.details["qualifier_tags"] = ["Best Hour"]

    alerter._send_embed(sig)

    embed = mock_instance.add_embed.call_args[0][0]
    field_names = [f["name"] for f in embed.fields]
    assert "\U0001f525 2U PLAY" not in field_names
    assert "\U0001f3c6 Elite Signal" not in field_names


# ── Tiered badge tests ────────────────────────────────────────


@patch("sharp_seeker.alerts.discord.DiscordWebhook")
def test_elite_badge_two_qualifiers(mock_webhook_cls):
    """Signal with 2 qualifiers (combo + hour) gets Elite Signal badge."""
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_instance = MagicMock()
    mock_instance.execute.return_value = mock_resp
    mock_webhook_cls.return_value = mock_instance

    settings = _make_settings()
    alerter = DiscordAlerter(settings, repo=MagicMock())
    sig = _make_signal()
    sig.details["qualifier_count"] = 2
    sig.details["qualifier_tags"] = ["Best Combo", "Best Hour"]

    alerter._send_embed(sig)

    embed = mock_instance.add_embed.call_args[0][0]
    field_names = [f["name"] for f in embed.fields]
    assert "\U0001f3c6 Elite Signal" in field_names


@patch("sharp_seeker.alerts.discord.DiscordWebhook")
def test_one_qualifier_no_badge(mock_webhook_cls):
    """Signal with 1 qualifier shows no tier badge (only 2U gets badge)."""
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_instance = MagicMock()
    mock_instance.execute.return_value = mock_resp
    mock_webhook_cls.return_value = mock_instance

    settings = _make_settings()
    alerter = DiscordAlerter(settings, repo=MagicMock())
    sig = _make_signal()
    sig.details["qualifier_count"] = 1
    sig.details["qualifier_tags"] = ["Best Combo"]

    alerter._send_embed(sig)

    embed = mock_instance.add_embed.call_args[0][0]
    field_names = [f["name"] for f in embed.fields]
    assert "\U0001f3c6 Elite Signal" not in field_names
    assert "\U0001f525 2U PLAY" not in field_names


@pytest.mark.asyncio
@patch("sharp_seeker.alerts.discord.DiscordWebhook")
async def test_zero_qualifiers_suppressed(mock_webhook_cls):
    """Signal with 0 qualifiers should not be sent to Discord."""
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_instance = MagicMock()
    mock_instance.execute.return_value = mock_resp
    mock_webhook_cls.return_value = mock_instance

    settings = _make_settings(
        signal_best_combos=[],
        signal_best_hours={},
    )
    repo = MagicMock()
    repo.record_alert = AsyncMock()
    alerter = DiscordAlerter(settings, repo=repo)
    sig = _make_signal()

    await alerter.send_signals([sig])

    # No embed should be sent, no alert recorded
    mock_webhook_cls.assert_not_called()
    repo.record_alert.assert_not_called()
    # But qualifier_count should still be annotated
    assert sig.details["qualifier_count"] == 0


@pytest.mark.asyncio
@patch("sharp_seeker.alerts.discord.datetime")
@patch("sharp_seeker.alerts.discord.DiscordWebhook")
async def test_qualifier_count_annotated_in_details(mock_webhook_cls, mock_dt):
    """send_signals annotates qualifier_count and qualifier_tags in signal details."""
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_instance = MagicMock()
    mock_instance.execute.return_value = mock_resp
    mock_webhook_cls.return_value = mock_instance

    mock_dt.now.return_value = _utc_for_mst_hour(16)
    mock_dt.fromisoformat = datetime.fromisoformat
    mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)

    settings = _make_settings(
        signal_best_combos=["pinnacle_divergence:basketball_nba:totals"],
        signal_best_hours={"pinnacle_divergence": [16]},
    )
    repo = MagicMock()
    repo.record_alert = AsyncMock()
    alerter = DiscordAlerter(settings, repo=repo)
    sig = _make_signal()

    await alerter.send_signals([sig])

    assert sig.details["qualifier_count"] == 2
    assert set(sig.details["qualifier_tags"]) == {"Best Combo", "Best Hour"}


@pytest.mark.asyncio
@patch("sharp_seeker.alerts.discord.DiscordWebhook")
async def test_arb_bypasses_zero_qualifier_suppression(mock_webhook_cls):
    """Arb signals should be sent even with 0 qualifiers."""
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_instance = MagicMock()
    mock_instance.execute.return_value = mock_resp
    mock_webhook_cls.return_value = mock_instance

    settings = _make_settings(
        signal_best_combos=[],
        signal_best_hours={},
    )
    repo = MagicMock()
    repo.record_alert = AsyncMock()
    alerter = DiscordAlerter(settings, repo=repo)
    sig = Signal(
        signal_type=SignalType.ARBITRAGE,
        event_id="evt_arb",
        sport_key="basketball_nba",
        home_team="Lakers",
        away_team="Celtics",
        market_key="h2h",
        outcome_name="Lakers",
        strength=0.5,
        description="Arb found",
        commence_time="2099-01-15T00:00:00Z",
        details={
            "cross_book_hold": -0.02,
            "profit_pct": 2.0,
            "side_a": {
                "outcome": "Lakers",
                "bookmaker": "draftkings",
                "price": 115,
                "point": None,
            },
            "side_b": {
                "outcome": "Celtics",
                "bookmaker": "fanduel",
                "price": 115,
                "point": None,
            },
        },
    )

    await alerter.send_signals([sig])

    # Should still be sent despite 0 qualifiers
    mock_webhook_cls.assert_called_once()
    repo.record_alert.assert_called_once()


@patch("sharp_seeker.alerts.discord.DiscordWebhook")
def test_cross_book_hold_display(mock_webhook_cls):
    """Cross-book hold should be displayed in the embed description."""
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_instance = MagicMock()
    mock_instance.execute.return_value = mock_resp
    mock_webhook_cls.return_value = mock_instance

    settings = _make_settings()
    alerter = DiscordAlerter(settings, repo=MagicMock())
    sig = _make_signal(details={"cross_book_hold": 0.015})
    sig.details["qualifier_count"] = 1
    sig.details["qualifier_tags"] = ["Best Combo"]

    alerter._send_embed(sig)

    embed = mock_instance.add_embed.call_args[0][0]
    assert "Market: 1.5%" in embed.description
    assert "Tight" in embed.description
