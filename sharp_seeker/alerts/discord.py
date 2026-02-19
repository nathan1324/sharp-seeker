"""Discord webhook alert sender."""

from __future__ import annotations

import json
from datetime import datetime, timezone

import structlog
from discord_webhook import DiscordEmbed, DiscordWebhook

from sharp_seeker.alerts.models import SIGNAL_COLORS, SIGNAL_LABELS
from sharp_seeker.config import Settings
from sharp_seeker.db.repository import Repository
from sharp_seeker.engine.base import Signal

log = structlog.get_logger()


class DiscordAlerter:
    def __init__(self, settings: Settings, repo: Repository) -> None:
        self._webhook_url = settings.discord_webhook_url
        self._repo = repo

    async def send_signals(self, signals: list[Signal]) -> None:
        """Send each signal as a Discord embed and record it."""
        for signal in signals:
            try:
                self._send_embed(signal)
                await self._repo.record_alert(
                    event_id=signal.event_id,
                    alert_type=signal.signal_type.value,
                    market_key=signal.market_key,
                    outcome_name=signal.outcome_name,
                    details_json=json.dumps(signal.details),
                )
                log.info(
                    "alert_sent",
                    signal_type=signal.signal_type.value,
                    event=signal.event_id,
                )
            except Exception:
                log.exception("alert_send_failed", event=signal.event_id)

    def _send_embed(self, signal: Signal) -> None:
        webhook = DiscordWebhook(url=self._webhook_url)

        label = SIGNAL_LABELS.get(signal.signal_type, signal.signal_type.value)
        color = SIGNAL_COLORS.get(signal.signal_type, 0x95A5A6)

        embed = DiscordEmbed(
            title=f"{label} Detected",
            description=signal.description,
            color=color,
        )

        embed.add_embed_field(name="Sport", value=signal.sport_key, inline=True)
        embed.add_embed_field(
            name="Matchup",
            value=f"{signal.away_team} @ {signal.home_team}",
            inline=True,
        )
        embed.add_embed_field(name="Market", value=signal.market_key, inline=True)
        embed.add_embed_field(name="Outcome", value=signal.outcome_name, inline=True)
        embed.add_embed_field(
            name="Strength", value=f"{signal.strength:.0%}", inline=True
        )

        # Add book-level details for steam moves
        if signal.signal_type.value == "steam_move":
            book_details = signal.details.get("book_details", [])
            if book_details:
                lines = [f"  {b['bookmaker']}: {b['delta']:+.1f}" for b in book_details]
                embed.add_embed_field(
                    name="Book Movements", value="\n".join(lines), inline=False
                )

        # Add delta for rapid changes
        if signal.signal_type.value == "rapid_change":
            bm = signal.details.get("bookmaker", "?")
            delta = signal.details.get("delta", 0)
            embed.add_embed_field(
                name="Details", value=f"Book: {bm} | Delta: {delta}", inline=False
            )

        # Pinnacle divergence details
        if signal.signal_type.value == "pinnacle_divergence":
            us_book = signal.details.get("us_book", "?")
            us_val = signal.details.get("us_value", "?")
            pin_val = signal.details.get("pinnacle_value", "?")
            embed.add_embed_field(
                name="Details",
                value=f"{us_book}: {us_val} vs Pinnacle: {pin_val}",
                inline=False,
            )

        # Reverse line movement details
        if signal.signal_type.value == "reverse_line":
            us_dir = signal.details.get("us_direction", "?")
            pin_dir = signal.details.get("pinnacle_direction", "?")
            movers = ", ".join(signal.details.get("us_movers", []))
            embed.add_embed_field(
                name="Details",
                value=f"US ({movers}): {us_dir} | Pinnacle: {pin_dir}",
                inline=False,
            )

        # Exchange shift details
        if signal.signal_type.value == "exchange_shift":
            direction = signal.details.get("direction", "?")
            shift = signal.details.get("shift", 0)
            embed.add_embed_field(
                name="Details",
                value=f"Betfair {direction} | Probability shift: {shift:.1%}",
                inline=False,
            )

        embed.set_timestamp(datetime.now(timezone.utc).isoformat())
        embed.set_footer(text="Sharp Seeker")

        webhook.add_embed(embed)
        resp = webhook.execute()
        if resp and hasattr(resp, "status_code") and resp.status_code >= 400:
            log.error("discord_webhook_error", status=resp.status_code)
