"""Daily and weekly summary reports sent to Discord."""

from __future__ import annotations

import csv
import io
import json
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


def _sport_friendly(sport_key: str) -> str:
    """Convert a sport_key like 'basketball_ncaab' to 'NCAAB'."""
    parts = sport_key.split("_", 1)
    return parts[-1].upper() if len(parts) > 1 else sport_key.upper()


def _parse_best_book(details_json_str: str | None) -> tuple[str, str, str]:
    """Extract best book's bookmaker, point, and price from details_json."""
    if not details_json_str:
        return "", "", ""
    try:
        details = json.loads(details_json_str) if isinstance(details_json_str, str) else details_json_str
        vb = details.get("value_books", [])
        if not vb:
            return "", "", ""
        best = vb[0]
        return (
            best.get("bookmaker", ""),
            best.get("point", ""),
            best.get("price", ""),
        )
    except (json.JSONDecodeError, TypeError):
        return "", "", ""


class ReportGenerator:
    def __init__(self, settings: Settings, repo: Repository) -> None:
        self._settings = settings
        self._repo = repo

    async def send_daily_report(self) -> None:
        """Send per-signal-type reports + combined summary + override reports."""
        since = self._hours_ago(48)
        await self._send_per_type_reports("Daily", since)
        await self._send_override_reports("Daily", since)
        await self._send_combined_report("Daily Signal Report", since)

    async def send_weekly_report(self) -> None:
        """Send per-signal-type reports + combined summary + override reports."""
        since = self._hours_ago(168)
        await self._send_per_type_reports("Weekly", since)
        await self._send_override_reports("Weekly", since)
        await self._send_combined_report("Weekly Signal Report", since)

    # ── Per-signal-type reports ──────────────────────────────────

    async def _send_per_type_reports(self, period: str, since: str) -> None:
        """Send a report for each signal type to its dedicated channel.

        Sports that have webhook overrides for a signal type get their own
        reports via _send_override_reports, so they are excluded here to
        avoid double-counting.
        """
        # Build a map: signal_type -> list of sports that have overrides
        override_sports: dict[str, list[str]] = {}
        for key in self._settings.discord_webhook_overrides:
            parts = key.split(":", 1)
            if len(parts) == 2:
                override_sports.setdefault(parts[0], []).append(parts[1])

        all_types_stats = await self._repo.get_performance_stats(since)
        if not all_types_stats:
            return

        for signal_type_val, _ in sorted(all_types_stats.items()):
            webhook_url = self._get_webhook_for_type(signal_type_val)
            friendly = _SIGNAL_FRIENDLY.get(signal_type_val, signal_type_val)
            exclude = override_sports.get(signal_type_val)

            # Re-query stats excluding overridden sports for this signal type
            type_stats = await self._repo.get_performance_stats(
                since, exclude_sports=exclude,
            )
            counts = type_stats.get(signal_type_val)
            if not counts:
                continue

            resolved = await self._repo.get_resolved_signals_since(
                since, signal_type=signal_type_val, exclude_sports=exclude,
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
                since, signal_type=signal_type_val, exclude_sports=exclude,
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

            csv_bytes = await self._build_results_csv(
                since, signal_type=signal_type_val, exclude_sports=exclude,
            )
            date_str = since[:10]
            csv_name = f"{signal_type_val}_results_{date_str}.csv"

            self._send_webhook(
                webhook_url, embed, f"{period} {friendly}",
                file_content=csv_bytes, filename=csv_name,
            )

            # Also send to default channel for centralized recap
            default_url = self._settings.discord_webhook_url
            if webhook_url != default_url:
                self._send_webhook(
                    default_url, embed, f"{period} {friendly} (misc)",
                    file_content=csv_bytes, filename=csv_name,
                )

    # ── Per-sport override reports ─────────────────────────────

    async def _send_override_reports(self, period: str, since: str) -> None:
        """Send sport-specific reports to each webhook override channel."""
        overrides = self._settings.discord_webhook_overrides
        if not overrides:
            return

        for key, webhook_url in overrides.items():
            parts = key.split(":", 1)
            if len(parts) != 2:
                log.warning("invalid_override_key", key=key)
                continue
            signal_type_val, sport_key = parts

            friendly_signal = _SIGNAL_FRIENDLY.get(signal_type_val, signal_type_val)
            friendly_sport = _sport_friendly(sport_key)

            stats = await self._repo.get_performance_stats(
                since, sport_key=sport_key,
            )
            counts = stats.get(signal_type_val)
            if not counts:
                continue

            won = counts.get("won", 0)
            lost = counts.get("lost", 0)
            push = counts.get("push", 0)
            decided = won + lost
            rate = f"{won / decided:.0%}" if decided else "N/A"

            title = f"{period} {friendly_signal} Report — {friendly_sport}"
            embed = DiscordEmbed(
                title=title,
                description=f"Period: since {since[:10]}",
                color=0x9B59B6,
            )

            embed.add_embed_field(
                name="Record",
                value=f"**{rate}** ({won}W / {lost}L / {push}P)",
                inline=True,
            )

            resolved = await self._repo.get_resolved_signals_since(
                since, signal_type=signal_type_val, sport_key=sport_key,
            )
            if resolved:
                lines = []
                for sig in resolved[:15]:
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

            market_stats = await self._repo.get_performance_stats_by_market(
                since, signal_type=signal_type_val, sport_key=sport_key,
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

            csv_bytes = await self._build_results_csv(
                since, signal_type=signal_type_val, sport_key=sport_key,
            )
            date_str = since[:10]
            sport_slug = sport_key.replace(":", "_")
            csv_name = f"{signal_type_val}_{sport_slug}_results_{date_str}.csv"

            self._send_webhook(
                webhook_url, embed, title,
                file_content=csv_bytes, filename=csv_name,
            )

            # Also send to default channel for centralized recap
            default_url = self._settings.discord_webhook_url
            if webhook_url != default_url:
                self._send_webhook(
                    default_url, embed, f"{title} (misc)",
                    file_content=csv_bytes, filename=csv_name,
                )

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
        embed.set_footer(text="Sandbox Sports", icon_url=LOGO_URL)

        csv_bytes = await self._build_results_csv(since)
        date_str = since[:10]
        csv_name = f"all_results_{date_str}.csv"

        self._send_webhook(
            self._settings.discord_webhook_url, embed, title,
            file_content=csv_bytes, filename=csv_name,
        )

    # ── CSV builder ──────────────────────────────────────────────

    async def _build_results_csv(
        self,
        since: str,
        signal_type: str | None = None,
        sport_key: str | None = None,
        exclude_sports: list[str] | None = None,
    ) -> bytes | None:
        """Build an in-memory CSV of resolved signals, returning UTF-8 bytes."""
        rows = await self._repo.get_resolved_signals_since(
            since, signal_type=signal_type, sport_key=sport_key,
            exclude_sports=exclude_sports,
        )
        if not rows:
            return None

        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow([
            "result", "sport", "matchup", "signal_type", "market",
            "outcome", "book", "point", "price", "strength", "signal_at",
        ])

        for row in rows:
            d = dict(row)
            teams = await self._repo.get_event_teams(d["event_id"])
            matchup = f"{teams[1]} vs {teams[0]}" if teams else d["event_id"]
            book, point, price = _parse_best_book(d.get("details_json"))
            writer.writerow([
                d["result"].upper(),
                _sport_friendly(d.get("sport_key", "")),
                matchup,
                d["signal_type"],
                d["market_key"],
                d["outcome_name"],
                book,
                point,
                price,
                d["signal_strength"],
                d.get("signal_at", ""),
            ])

        return buf.getvalue().encode("utf-8")

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
    def _send_webhook(
        url: str, embed: DiscordEmbed, label: str,
        file_content: bytes | None = None, filename: str | None = None,
    ) -> None:
        webhook = DiscordWebhook(url=url)
        webhook.add_embed(embed)
        if file_content and filename:
            webhook.add_file(file=file_content, filename=filename)
        resp = webhook.execute()

        if resp and hasattr(resp, "status_code") and resp.status_code < 400:
            log.info("report_sent", title=label)
        else:
            log.error("report_send_failed", title=label)

    @staticmethod
    def _hours_ago(hours: int) -> str:
        return (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
