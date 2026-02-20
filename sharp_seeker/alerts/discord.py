"""Discord webhook alert sender."""

from __future__ import annotations

import json
from datetime import datetime, timezone

import structlog
from discord_webhook import DiscordEmbed, DiscordWebhook

from sharp_seeker.alerts.models import SIGNAL_COLORS, SIGNAL_LABELS
from sharp_seeker.config import Settings
from sharp_seeker.db.repository import Repository
from sharp_seeker.engine.base import Signal, SignalType

log = structlog.get_logger()

# Map market_key to a readable name
MARKET_NAMES = {
    "spreads": "Spread",
    "totals": "Total",
    "h2h": "Moneyline",
}


def _strength_bar(strength: float) -> str:
    """Render strength as a visual bar."""
    filled = round(strength * 10)
    return f"`{'█' * filled}{'░' * (10 - filled)}` **{strength:.0%}**"


def _format_line_value(point: float | None, price: float | None, market: str) -> str:
    """Format a line value for display."""
    if market == "h2h" and price is not None:
        return f"{price:+.0f}" if price < 0 else f"+{price:.0f}"
    if point is not None:
        return str(point)
    if price is not None:
        return f"{price:+.0f}" if price < 0 else f"+{price:.0f}"
    return "?"


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
                    event_id=signal.event_id,
                )
            except Exception:
                log.exception("alert_send_failed", event_id=signal.event_id)

    def _send_embed(self, sig: Signal) -> None:
        webhook = DiscordWebhook(url=self._webhook_url)

        label = SIGNAL_LABELS.get(sig.signal_type, sig.signal_type.value)
        color = SIGNAL_COLORS.get(sig.signal_type, 0x95A5A6)
        market_name = MARKET_NAMES.get(sig.market_key, sig.market_key)
        matchup = f"{sig.away_team} @ {sig.home_team}"

        # Title: signal type
        # Description: matchup + big line movement block
        title = f"{label}"
        desc = self._build_description(sig, matchup, market_name)

        embed = DiscordEmbed(title=title, description=desc, color=color)

        # Strength bar
        embed.add_embed_field(
            name="Strength", value=_strength_bar(sig.strength), inline=False
        )

        # Signal-type-specific details
        self._add_details(embed, sig, market_name)

        embed.set_timestamp(datetime.now(timezone.utc).isoformat())
        embed.set_footer(text=f"Sharp Seeker • {sig.sport_key.replace('_', ' ').title()}")

        webhook.add_embed(embed)
        resp = webhook.execute()
        if resp and hasattr(resp, "status_code") and resp.status_code >= 400:
            log.error("discord_webhook_error", status=resp.status_code)

    def _build_description(self, sig: Signal, matchup: str, market_name: str) -> str:
        """Build the main description block with prominent line movement."""
        d = sig.details
        lines = [f"**{matchup}**", ""]

        if sig.signal_type == SignalType.RAPID_CHANGE:
            bm = d.get("bookmaker", "?").title()
            old_val = _format_line_value(d.get("old_point"), d.get("old_price"), sig.market_key)
            new_val = _format_line_value(d.get("new_point"), d.get("new_price"), sig.market_key)
            delta = d.get("delta", 0)
            lines.append(f"📊 **{market_name}** — {sig.outcome_name}")
            lines.append(f"## {old_val}  →  {new_val}")
            lines.append(f"**Delta: {delta:+.1f}** at {bm}")

        elif sig.signal_type == SignalType.STEAM_MOVE:
            direction = d.get("direction", "?")
            arrow = "📈" if direction == "up" else "📉"
            books_moved = d.get("books_moved", 0)
            avg_delta = d.get("avg_delta", 0)
            lines.append(f"{arrow} **{market_name}** — {sig.outcome_name}")
            lines.append(f"## {books_moved} books moved {direction}")
            lines.append(f"**Avg delta: {avg_delta:+.1f}**")

        elif sig.signal_type == SignalType.PINNACLE_DIVERGENCE:
            us_book = d.get("us_book", "?").title()
            us_val = d.get("us_value", "?")
            pin_val = d.get("pinnacle_value", "?")
            delta = d.get("delta", 0)
            lines.append(f"💰 **{market_name}** — {sig.outcome_name}")
            lines.append(f"## {us_book}: {us_val}  vs  Pinnacle: {pin_val}")
            lines.append(f"**Value edge: {delta:+.1f}** — bet at {us_book}")

        elif sig.signal_type == SignalType.REVERSE_LINE:
            us_dir = d.get("us_direction", "?")
            pin_dir = d.get("pinnacle_direction", "?")
            us_avg = d.get("us_avg_delta", 0)
            pin_delta = d.get("pinnacle_delta", 0)
            lines.append(f"🔄 **{market_name}** — {sig.outcome_name}")
            lines.append(f"## US {us_dir} ({us_avg:+.1f})  vs  Pinnacle {pin_dir} ({pin_delta:+.1f})")
            lines.append("**Public vs Sharp money divergence**")

        elif sig.signal_type == SignalType.EXCHANGE_SHIFT:
            direction = d.get("direction", "?")
            shift = d.get("shift", 0)
            old_prob = d.get("old_implied_prob", 0)
            new_prob = d.get("new_implied_prob", 0)
            arrow = "📈" if direction == "up" else "📉"
            lines.append(f"{arrow} **{market_name}** — {sig.outcome_name}")
            lines.append(f"## {old_prob:.1%}  →  {new_prob:.1%}")
            lines.append(f"**Betfair shift: {shift:+.1%}**")

        else:
            lines.append(sig.description)

        return "\n".join(lines)

    def _add_details(self, embed: DiscordEmbed, sig: Signal, market_name: str) -> None:
        """Add signal-type-specific detail fields."""
        d = sig.details

        if sig.signal_type == SignalType.STEAM_MOVE:
            book_details = d.get("book_details", [])
            if book_details:
                lines = [f"`{b['bookmaker'].title():15s}` **{b['delta']:+.1f}**" for b in book_details]
                embed.add_embed_field(
                    name="Book Movements", value="\n".join(lines), inline=False
                )

        elif sig.signal_type == SignalType.REVERSE_LINE:
            movers = d.get("us_movers", [])
            if movers:
                embed.add_embed_field(
                    name="US Books Moving",
                    value=", ".join(m.title() for m in movers),
                    inline=False,
                )
