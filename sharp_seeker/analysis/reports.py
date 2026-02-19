"""Daily and weekly summary reports sent to Discord."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import structlog
from discord_webhook import DiscordEmbed, DiscordWebhook

from sharp_seeker.config import Settings
from sharp_seeker.db.repository import Repository

log = structlog.get_logger()


class ReportGenerator:
    def __init__(self, settings: Settings, repo: Repository) -> None:
        self._settings = settings
        self._repo = repo

    async def send_daily_report(self) -> None:
        """Send a daily performance summary to Discord."""
        since = self._hours_ago(24)
        await self._send_report("Daily Signal Report", since)

    async def send_weekly_report(self) -> None:
        """Send a weekly performance summary to Discord."""
        since = self._hours_ago(168)
        await self._send_report("Weekly Signal Report", since)

    async def _send_report(self, title: str, since: str) -> None:
        stats = await self._repo.get_performance_stats(since)
        signal_count = await self._repo.get_signal_count_since(since)
        alert_count = await self._repo.get_alerts_count_since(since)

        webhook = DiscordWebhook(url=self._settings.discord_webhook_url)

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
            # Overall win rate
            total_won = sum(s.get("won", 0) for s in stats.values())
            total_lost = sum(s.get("lost", 0) for s in stats.values())
            total_decided = total_won + total_lost
            overall_rate = f"{total_won / total_decided:.1%}" if total_decided else "N/A"
            embed.add_embed_field(
                name="Overall Win Rate",
                value=f"{overall_rate} ({total_won}W / {total_lost}L)",
                inline=True,
            )

            # Per-detector breakdown
            lines = []
            for st, counts in sorted(stats.items()):
                won = counts.get("won", 0)
                lost = counts.get("lost", 0)
                push = counts.get("push", 0)
                decided = won + lost
                rate = f"{won / decided:.0%}" if decided else "N/A"
                lines.append(f"**{st}**: {rate} ({won}W/{lost}L/{push}P)")

            embed.add_embed_field(
                name="By Detector",
                value="\n".join(lines) if lines else "No resolved signals",
                inline=False,
            )
        else:
            embed.add_embed_field(
                name="Performance", value="No resolved signals yet", inline=False
            )

        embed.set_timestamp(datetime.now(timezone.utc).isoformat())
        embed.set_footer(text="Sharp Seeker")

        webhook.add_embed(embed)
        resp = webhook.execute()

        if resp and hasattr(resp, "status_code") and resp.status_code < 400:
            log.info("report_sent", title=title)
        else:
            log.error("report_send_failed", title=title)

    @staticmethod
    def _hours_ago(hours: int) -> str:
        return (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
