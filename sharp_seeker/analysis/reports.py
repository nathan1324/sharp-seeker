"""Daily and weekly summary reports sent to Discord."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import structlog
from discord_webhook import DiscordEmbed, DiscordWebhook

from sharp_seeker.alerts.models import SIGNAL_LABELS
from sharp_seeker.config import Settings
from sharp_seeker.db.repository import Repository
from sharp_seeker.engine.base import SignalType

log = structlog.get_logger()

LOGO_URL = "https://raw.githubusercontent.com/nathan1324/sharp-seeker/main/assets/logo-square.png"

# Map signal_type DB values to their per-channel webhook setting names
_SIGNAL_WEBHOOK_ATTRS: dict[str, str] = {
    SignalType.STEAM_MOVE.value: "discord_webhook_steam_move",
    SignalType.RAPID_CHANGE.value: "discord_webhook_rapid_change",
    SignalType.PINNACLE_DIVERGENCE.value: "discord_webhook_pinnacle_divergence",
    SignalType.REVERSE_LINE.value: "discord_webhook_reverse_line",
    SignalType.EXCHANGE_SHIFT.value: "discord_webhook_exchange_shift",
}

# Friendly names for signal types
_SIGNAL_FRIENDLY: dict[str, str] = {
    st.value: SIGNAL_LABELS.get(st, st.value) for st in SignalType
}

RESULT_EMOJI = {"won": "\u2705", "lost": "\u274c", "push": "\u2796"}

_MARKET_FRIENDLY: dict[str, str] = {
    "h2h": "Moneyline",
    "spreads": "Spreads",
    "totals": "Totals",
}


class ReportGenerator:
    def __init__(self, settings: Settings, repo: Repository) -> None:
        self._settings = settings
        self._repo = repo

    async def send_daily_report(self) -> None:
        """Send per-signal-type reports + combined summary."""
        since = self._hours_ago(24)
        await self._send_per_type_reports("Daily", since)
        await self._send_combined_report("Daily Signal Report", since)

    async def send_weekly_report(self) -> None:
        """Send per-signal-type reports + combined summary."""
        since = self._hours_ago(168)
        await self._send_per_type_reports("Weekly", since)
        await self._send_combined_report("Weekly Signal Report", since)

    # ── Per-signal-type reports ──────────────────────────────────

    async def _send_per_type_reports(self, period: str, since: str) -> None:
        """Send a report for each signal type to its dedicated channel."""
        stats = await self._repo.get_performance_stats(since)
        if not stats:
            return

        for signal_type_val, counts in sorted(stats.items()):
            webhook_url = self._get_webhook_for_type(signal_type_val)
            friendly = _SIGNAL_FRIENDLY.get(signal_type_val, signal_type_val)

            resolved = await self._repo.get_resolved_signals_since(
                since, signal_type=signal_type_val
            )

            won = counts.get("won", 0)
            lost = counts.get("lost", 0)
            push = counts.get("push", 0)
            decided = won + lost
            rate = f"{won / decided:.0%}" if decided else "N/A"

            embed = DiscordEmbed(
                title=f"{period} {friendly} Report",
                description=f"Period: since {since[:10]}",
                color=0x9B59B6,
            )

            embed.add_embed_field(
                name="Record",
                value=f"**{rate}** ({won}W / {lost}L / {push}P)",
                inline=True,
            )

            # Individual signal outcomes
            if resolved:
                lines = []
                for sig in resolved[:15]:  # cap at 15 to fit embed
                    sig_dict = dict(sig)
                    emoji = RESULT_EMOJI.get(sig_dict["result"], "?")
                    teams = await self._repo.get_event_teams(sig_dict["event_id"])
                    matchup = f"{teams[1]} vs {teams[0]}" if teams else sig_dict["event_id"]
                    lines.append(
                        f"{emoji} {matchup} — {sig_dict['market_key']} "
                        f"{sig_dict['outcome_name']}"
                    )
                embed.add_embed_field(
                    name="Results",
                    value="\n".join(lines),
                    inline=False,
                )

            # Per-market breakdown for this signal type
            market_stats = await self._repo.get_performance_stats_by_market(
                since, signal_type=signal_type_val
            )
            if market_stats:
                mlines = []
                for mk, mc in sorted(market_stats.items()):
                    mname = _MARKET_FRIENDLY.get(mk, mk)
                    mw = mc.get("won", 0)
                    ml = mc.get("lost", 0)
                    mp = mc.get("push", 0)
                    md = mw + ml
                    mr = f"{mw / md:.0%}" if md else "N/A"
                    mlines.append(f"**{mname}**: {mr} ({mw}W/{ml}L/{mp}P)")
                embed.add_embed_field(
                    name="By Market",
                    value="\n".join(mlines),
                    inline=False,
                )

            embed.set_timestamp(datetime.now(timezone.utc).isoformat())
            embed.set_footer(text="Sandbox Sports", icon_url=LOGO_URL)

            self._send_webhook(webhook_url, embed, f"{period} {friendly}")

    # ── Combined summary (default channel) ──────────────────────

    async def _send_combined_report(self, title: str, since: str) -> None:
        stats = await self._repo.get_performance_stats(since)
        signal_count = await self._repo.get_signal_count_since(since)
        alert_count = await self._repo.get_alerts_count_since(since)

        embed = DiscordEmbed(
            title=title,
            description=f"Period: since {since[:10]}",
            color=0x9B59B6,
        )

        embed.add_embed_field(
            name="Signals Detected", value=str(signal_count), inline=True
        )
        embed.add_embed_field(
            name="Alerts Sent", value=str(alert_count), inline=True
        )

        if stats:
            total_won = sum(s.get("won", 0) for s in stats.values())
            total_lost = sum(s.get("lost", 0) for s in stats.values())
            total_decided = total_won + total_lost
            overall_rate = f"{total_won / total_decided:.1%}" if total_decided else "N/A"
            embed.add_embed_field(
                name="Overall Win Rate",
                value=f"{overall_rate} ({total_won}W / {total_lost}L)",
                inline=True,
            )

            lines = []
            for st, counts in sorted(stats.items()):
                friendly = _SIGNAL_FRIENDLY.get(st, st)
                won = counts.get("won", 0)
                lost = counts.get("lost", 0)
                push = counts.get("push", 0)
                decided = won + lost
                rate = f"{won / decided:.0%}" if decided else "N/A"
                lines.append(f"**{friendly}**: {rate} ({won}W/{lost}L/{push}P)")

            embed.add_embed_field(
                name="By Detector",
                value="\n".join(lines) if lines else "No resolved signals",
                inline=False,
            )

            # Overall market breakdown
            market_stats = await self._repo.get_performance_stats_by_market(since)
            if market_stats:
                mlines = []
                for mk, mc in sorted(market_stats.items()):
                    mname = _MARKET_FRIENDLY.get(mk, mk)
                    mw = mc.get("won", 0)
                    ml = mc.get("lost", 0)
                    mp = mc.get("push", 0)
                    md = mw + ml
                    mr = f"{mw / md:.0%}" if md else "N/A"
                    mlines.append(f"**{mname}**: {mr} ({mw}W/{ml}L/{mp}P)")
                embed.add_embed_field(
                    name="By Market",
                    value="\n".join(mlines),
                    inline=False,
                )
        else:
            embed.add_embed_field(
                name="Performance", value="No resolved signals yet", inline=False
            )

        embed.set_timestamp(datetime.now(timezone.utc).isoformat())
        embed.set_footer(text="Sharp Seeker")

        self._send_webhook(
            self._settings.discord_webhook_url, embed, title
        )

    # ── Helpers ──────────────────────────────────────────────────

    def _get_webhook_for_type(self, signal_type_val: str) -> str:
        """Get the webhook URL for a signal type, falling back to default."""
        attr = _SIGNAL_WEBHOOK_ATTRS.get(signal_type_val)
        if attr:
            url = getattr(self._settings, attr, None)
            if url:
                return url
        return self._settings.discord_webhook_url

    @staticmethod
    def _send_webhook(url: str, embed: DiscordEmbed, label: str) -> None:
        webhook = DiscordWebhook(url=url)
        webhook.add_embed(embed)
        resp = webhook.execute()

        if resp and hasattr(resp, "status_code") and resp.status_code < 400:
            log.info("report_sent", title=label)
        else:
            log.error("report_send_failed", title=label)

    @staticmethod
    def _hours_ago(hours: int) -> str:
        return (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
