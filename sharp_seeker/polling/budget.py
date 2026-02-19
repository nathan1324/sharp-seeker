"""API credit budget tracking, throttling, and daily summaries."""

from __future__ import annotations

from datetime import datetime, timezone

import structlog
from discord_webhook import DiscordEmbed, DiscordWebhook

from sharp_seeker.config import Settings
from sharp_seeker.db.repository import Repository

log = structlog.get_logger()

CREDITS_PER_POLL = 9  # 3 sports x 3 credits each (3 markets x 1 region-equivalent)


class BudgetTracker:
    def __init__(self, settings: Settings, repo: Repository) -> None:
        self._settings = settings
        self._repo = repo
        self._low_budget_warned = False

    async def should_poll(self) -> bool:
        """Check if we have enough budget to poll. Returns False if below 20% threshold."""
        remaining = await self._repo.get_credits_remaining()
        if remaining is None:
            return True  # no data yet, assume OK

        threshold = self._settings.odds_api_monthly_credits * 0.20
        if remaining <= threshold:
            log.warning(
                "budget_low",
                remaining=remaining,
                threshold=threshold,
                monthly=self._settings.odds_api_monthly_credits,
            )
            if not self._low_budget_warned:
                self._low_budget_warned = True
                self._send_budget_warning(remaining)
            return False

        if remaining < CREDITS_PER_POLL:
            log.warning("budget_exhausted", remaining=remaining)
            return False

        return True

    async def get_status(self) -> dict:
        """Return current budget status."""
        remaining = await self._repo.get_credits_remaining()
        monthly = self._settings.odds_api_monthly_credits
        used = (monthly - remaining) if remaining is not None else 0
        return {
            "monthly_limit": monthly,
            "credits_remaining": remaining,
            "credits_used": used,
            "pct_remaining": round((remaining / monthly) * 100, 1) if remaining else 100.0,
        }

    async def send_daily_summary(self) -> None:
        """Send a daily budget summary to Discord."""
        status = await self.get_status()
        alerts_today = await self._repo.get_alerts_count_since(
            self._today_start_iso()
        )
        polls_today = await self._repo.get_poll_count_since(
            self._today_start_iso()
        )

        webhook = DiscordWebhook(url=self._settings.discord_webhook_url)
        embed = DiscordEmbed(
            title="Daily Budget Summary",
            description="Sharp Seeker daily status report",
            color=0x3498DB,
        )
        embed.add_embed_field(
            name="Credits Used / Remaining",
            value=f"{status['credits_used']} / {status['credits_remaining'] or '?'}",
            inline=True,
        )
        embed.add_embed_field(
            name="Monthly Limit",
            value=str(status["monthly_limit"]),
            inline=True,
        )
        embed.add_embed_field(
            name="% Remaining",
            value=f"{status['pct_remaining']}%",
            inline=True,
        )
        embed.add_embed_field(
            name="Polls Today", value=str(polls_today), inline=True
        )
        embed.add_embed_field(
            name="Alerts Today", value=str(alerts_today), inline=True
        )
        embed.set_timestamp(datetime.now(timezone.utc).isoformat())
        embed.set_footer(text="Sharp Seeker")

        webhook.add_embed(embed)
        resp = webhook.execute()

        if resp and hasattr(resp, "status_code") and resp.status_code < 400:
            log.info("daily_summary_sent")
        else:
            log.error("daily_summary_failed")

    def _send_budget_warning(self, remaining: int) -> None:
        """Send a one-time low budget warning to Discord."""
        webhook = DiscordWebhook(url=self._settings.discord_webhook_url)
        embed = DiscordEmbed(
            title="Budget Warning",
            description=(
                f"API credits are below 20% threshold. "
                f"Remaining: **{remaining}** / {self._settings.odds_api_monthly_credits}. "
                f"Polling has been paused."
            ),
            color=0xE74C3C,
        )
        embed.set_timestamp(datetime.now(timezone.utc).isoformat())
        embed.set_footer(text="Sharp Seeker")
        webhook.add_embed(embed)
        webhook.execute()

    @staticmethod
    def _today_start_iso() -> str:
        now = datetime.now(timezone.utc)
        return now.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
