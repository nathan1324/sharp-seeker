"""Tests for Discord alerter — best combo and best hour badges."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

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
) -> Signal:
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
        details={
            "us_book": "draftkings",
            "us_value": 220.5,
            "pinnacle_value": 219.5,
            "delta": 1.0,
            "value_books": [
                {"bookmaker": "draftkings", "price": -110, "point": 220.5},
            ],
        },
    )


@patch("sharp_seeker.alerts.discord.DiscordWebhook")
def test_best_combo_badge_shown(mock_webhook_cls):
    """Signal matching a best combo gets the star field."""
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

    alerter._send_embed(sig)

    # Inspect the embed that was added
    embed = mock_instance.add_embed.call_args[0][0]
    field_names = [f["name"] for f in embed.fields]
    assert "\u2b50 Top Performer" in field_names


@patch("sharp_seeker.alerts.discord.DiscordWebhook")
def test_best_combo_badge_not_shown(mock_webhook_cls):
    """Signal NOT in best combos should have no star field."""
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

    alerter._send_embed(sig)

    embed = mock_instance.add_embed.call_args[0][0]
    field_names = [f["name"] for f in embed.fields]
    assert "\u2b50 Top Performer" not in field_names


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

    alerter._send_embed(sig)

    embed = mock_instance.add_embed.call_args[0][0]
    field_names = [f["name"] for f in embed.fields]
    assert "\u2b50 Top Performer" not in field_names


# ── Best hour badge tests ──────────────────────────────────────


def _utc_for_mst_hour(hour: int) -> datetime:
    """Return a UTC datetime whose MST equivalent has the given hour."""
    # MST = UTC-7, so UTC hour = MST hour + 7
    return datetime(2025, 6, 15, (hour + 7) % 24, 30, 0, tzinfo=timezone.utc)


@patch("sharp_seeker.alerts.discord.datetime")
@patch("sharp_seeker.alerts.discord.DiscordWebhook")
def test_best_hour_badge_shown(mock_webhook_cls, mock_dt):
    """Signal matching a best hour gets the badge."""
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_instance = MagicMock()
    mock_instance.execute.return_value = mock_resp
    mock_webhook_cls.return_value = mock_instance

    # Make datetime.now() return MST hour 16 (UTC 23)
    mock_dt.now.return_value = _utc_for_mst_hour(16)
    mock_dt.fromisoformat = datetime.fromisoformat
    mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)

    settings = _make_settings(
        signal_best_hours={"pinnacle_divergence": [6, 12, 14, 16, 17]},
    )
    alerter = DiscordAlerter(settings, repo=MagicMock())
    sig = _make_signal()  # PD — matches hour 16

    alerter._send_embed(sig)

    embed = mock_instance.add_embed.call_args[0][0]
    field_names = [f["name"] for f in embed.fields]
    assert "\u2b50 Top Performer" in field_names


@patch("sharp_seeker.alerts.discord.datetime")
@patch("sharp_seeker.alerts.discord.DiscordWebhook")
def test_best_hour_badge_not_shown(mock_webhook_cls, mock_dt):
    """Signal NOT matching a best hour should have no badge."""
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_instance = MagicMock()
    mock_instance.execute.return_value = mock_resp
    mock_webhook_cls.return_value = mock_instance

    # MST hour 10 — not in the best hours list
    mock_dt.now.return_value = _utc_for_mst_hour(10)
    mock_dt.fromisoformat = datetime.fromisoformat
    mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)

    settings = _make_settings(
        signal_best_hours={"pinnacle_divergence": [6, 12, 14, 16, 17]},
    )
    alerter = DiscordAlerter(settings, repo=MagicMock())
    sig = _make_signal()

    alerter._send_embed(sig)

    embed = mock_instance.add_embed.call_args[0][0]
    field_names = [f["name"] for f in embed.fields]
    assert "\u2b50 Top Performer" not in field_names


@patch("sharp_seeker.alerts.discord.datetime")
@patch("sharp_seeker.alerts.discord.DiscordWebhook")
def test_best_hour_or_combo(mock_webhook_cls, mock_dt):
    """Signal matches best hour but NOT best combo — still gets badge."""
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_instance = MagicMock()
    mock_instance.execute.return_value = mock_resp
    mock_webhook_cls.return_value = mock_instance

    # MST hour 16 — matches best hour for PD
    mock_dt.now.return_value = _utc_for_mst_hour(16)
    mock_dt.fromisoformat = datetime.fromisoformat
    mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)

    settings = _make_settings(
        signal_best_combos=["pinnacle_divergence:basketball_nba:spreads"],  # won't match totals
        signal_best_hours={"pinnacle_divergence": [16]},
    )
    alerter = DiscordAlerter(settings, repo=MagicMock())
    sig = _make_signal(market_key="totals")  # combo won't match, but hour will

    alerter._send_embed(sig)

    embed = mock_instance.add_embed.call_args[0][0]
    field_names = [f["name"] for f in embed.fields]
    assert "\u2b50 Top Performer" in field_names
