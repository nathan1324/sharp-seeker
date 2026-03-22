"""Discord webhook alert sender."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import structlog
from discord_webhook import DiscordEmbed, DiscordWebhook

from sharp_seeker.alerts.models import SIGNAL_COLORS, SIGNAL_LABELS, display_book
from sharp_seeker.config import Settings
from sharp_seeker.db.repository import Repository
from sharp_seeker.engine.base import Signal, SignalType
from sharp_seeker.engine.exchange_monitor import american_to_implied_prob

log = structlog.get_logger()

LOGO_URL = "https://raw.githubusercontent.com/nathan1324/sharp-seeker/main/assets/logo-square.png"

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


def _format_odds(market: str, price: float | None, point: float | None) -> str:
    """Format odds for display: 'spreads -3.5 (-110)' or 'h2h +150'."""
    if market == "h2h":
        if price is not None:
            return f"{price:+.0f}"
        return "?"
    # spreads: signed (+/-) point; totals: unsigned point
    parts = []
    if point is not None:
        if market == "totals":
            parts.append(f"{point:.1f}" if point != int(point) else f"{point:.0f}")
        else:
            parts.append(f"{point:+.1f}" if point != int(point) else f"{point:+.0f}")
    if price is not None:
        parts.append(f"({price:+.0f})")
    return " ".join(parts) if parts else "?"


ET = ZoneInfo("America/New_York")
MST = ZoneInfo("America/Phoenix")


def _format_game_time(commence_time: str) -> str:
    """Format commence_time as a readable date/time in Eastern."""
    if not commence_time:
        return ""
    try:
        ct = datetime.fromisoformat(commence_time)
        if ct.tzinfo is None:
            ct = ct.replace(tzinfo=timezone.utc)
        ct_et = ct.astimezone(ET)
        day = ct_et.strftime("%a, %b")
        dom = ct_et.day
        hour = ct_et.strftime("%I:%M %p").lstrip("0")
        return f"{day} {dom} \u2022 {hour} (ET)"
    except (ValueError, TypeError):
        return ""


def _live_tag(commence_time: str) -> str:
    """Return a LIVE or PREGAME tag based on commence_time vs now."""
    if not commence_time:
        return ""
    try:
        ct = datetime.fromisoformat(commence_time)
        if ct.tzinfo is None:
            ct = ct.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        return "[LIVE]" if now >= ct else "[PREGAME]"
    except (ValueError, TypeError):
        return ""


def _bet_recommendation(sig: Signal, market_name: str) -> str | None:
    """Build a prominent bet recommendation line from the best value book."""
    value_books = sig.details.get("value_books", [])
    if not value_books:
        return None
    best = value_books[0]
    bm = display_book(best["bookmaker"])
    odds = _format_odds(sig.market_key, best.get("price"), best.get("point"))
    text = f"Bet {sig.outcome_name} {odds} @ {bm}"
    link = best.get("deep_link")
    if link:
        return f"💰 **[{text}]({link})**"
    return f"💰 **{text}**"


class DiscordAlerter:
    def __init__(self, settings: Settings, repo: Repository) -> None:
        self._default_url = settings.discord_webhook_url
        self._repo = repo
        # Per-signal-type webhook URLs; fall back to default if not set.
        self._webhook_urls: dict[SignalType, str] = {}
        _mapping = {
            SignalType.STEAM_MOVE: settings.discord_webhook_steam_move,
            SignalType.RAPID_CHANGE: settings.discord_webhook_rapid_change,
            SignalType.PINNACLE_DIVERGENCE: settings.discord_webhook_pinnacle_divergence,
            SignalType.REVERSE_LINE: settings.discord_webhook_reverse_line,
            SignalType.EXCHANGE_SHIFT: settings.discord_webhook_exchange_shift,
            SignalType.ARBITRAGE: settings.discord_webhook_arbitrage,
        }
        for sig_type, url in _mapping.items():
            if url:
                self._webhook_urls[sig_type] = url
        # Per-sport+signal overrides: "signal_type:sport_key" → webhook URL
        self._webhook_overrides: dict[str, str] = settings.discord_webhook_overrides
        # Best combos — high-confidence type:sport:market patterns
        self._best_combos: set[str] = set(settings.signal_best_combos)
        # Best hours — high-confidence type:hour(MST) patterns
        self._best_hours: dict[str, set[int]] = {
            k: set(v) for k, v in settings.signal_best_hours.items()
        }

    def _is_best_combo(self, sig: Signal) -> bool:
        key = f"{sig.signal_type.value}:{sig.sport_key}:{sig.market_key}"
        return key in self._best_combos

    def _is_best_hour(self, sig: Signal) -> bool:
        hours = self._best_hours.get(sig.signal_type.value)
        if not hours:
            return False
        mst_hour = datetime.now(timezone.utc).astimezone(MST).hour
        return mst_hour in hours

    def _count_qualifiers(self, sig: Signal) -> tuple[int, list[str]]:
        """Count how many quality qualifiers a signal meets.

        Qualifiers: best combo, best hour.
        Tiers: 2 = 2U PLAY, 1 = Elite, 0 = suppressed.
        """
        tags: list[str] = []
        if self._best_combos and self._is_best_combo(sig):
            tags.append("Best Combo")
        if self._is_best_hour(sig):
            tags.append("Best Hour")
        return len(tags), tags

    async def send_signals(self, signals: list[Signal]) -> None:
        """Send each signal as a Discord embed and record it.

        Signals with 0 qualifiers are suppressed (not sent to Discord).
        """
        for signal in signals:
            q_count, q_tags = self._count_qualifiers(signal)
            signal.details["qualifier_count"] = q_count
            signal.details["qualifier_tags"] = q_tags
            if q_count == 0 and signal.signal_type != SignalType.ARBITRAGE:
                log.info(
                    "signal_suppressed",
                    signal_type=signal.signal_type.value,
                    event_id=signal.event_id,
                    reason="zero_qualifiers",
                )
                continue
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
                    qualifier_count=q_count,
                )
            except Exception:
                log.exception("alert_send_failed", event_id=signal.event_id)

    def _send_embed(self, sig: Signal) -> None:
        override_key = f"{sig.signal_type.value}:{sig.sport_key}"
        url = self._webhook_overrides.get(
            override_key,
            self._webhook_urls.get(sig.signal_type, self._default_url),
        )
        webhook = DiscordWebhook(url=url)

        label = SIGNAL_LABELS.get(sig.signal_type, sig.signal_type.value)
        color = SIGNAL_COLORS.get(sig.signal_type, 0x95A5A6)
        market_name = MARKET_NAMES.get(sig.market_key, sig.market_key)
        matchup = f"{sig.away_team} @ {sig.home_team}"

        # Title: signal type + live/pregame tag
        live_tag = _live_tag(sig.commence_time)
        title = f"{label}  {live_tag}" if live_tag else f"{label}"
        desc = self._build_description(sig, matchup, market_name)

        embed = DiscordEmbed(title=title, description=desc, color=color)

        # Strength bar
        embed.add_embed_field(
            name="Strength", value=_strength_bar(sig.strength), inline=False
        )

        # Show Elite badge when both qualifiers match
        q_count = sig.details.get("qualifier_count", 0)
        q_tags = sig.details.get("qualifier_tags", [])
        if q_count >= 2:
            tag_str = " + ".join(q_tags)
            embed.add_embed_field(
                name="\U0001f3c6 Elite Signal", value=tag_str, inline=False,
            )

        # Signal-type-specific details
        self._add_details(embed, sig, market_name)

        embed.set_timestamp(datetime.now(timezone.utc).isoformat())
        embed.set_footer(
            text=f"Sandbox Sports • {sig.sport_key.split('_')[-1].upper()}",
            icon_url=LOGO_URL,
        )

        webhook.add_embed(embed)
        resp = webhook.execute()
        if resp and hasattr(resp, "status_code") and resp.status_code >= 400:
            log.error("discord_webhook_error", status=resp.status_code)

    def _build_description(self, sig: Signal, matchup: str, market_name: str) -> str:
        """Build the main description block with prominent line movement."""
        d = sig.details
        game_time = _format_game_time(sig.commence_time)
        lines = [f"**{matchup}**"]
        if game_time:
            lines.append(f"-# {game_time}")
        lines.append("")

        bet_line = _bet_recommendation(sig, market_name)

        if sig.signal_type == SignalType.RAPID_CHANGE:
            bm = display_book(d.get("bookmaker", "?"))
            old_val = _format_line_value(d.get("old_point"), d.get("old_price"), sig.market_key)
            new_val = _format_line_value(d.get("new_point"), d.get("new_price"), sig.market_key)
            delta = d.get("delta", 0)
            lines.append(bet_line or f"📊 **{market_name}** — {sig.outcome_name}")
            lines.append(f"## {old_val}  →  {new_val}")
            lines.append(f"**Delta: {delta:+.1f}** at {bm}")

        elif sig.signal_type == SignalType.STEAM_MOVE:
            direction = d.get("direction", "?")
            books_moved = d.get("books_moved", 0)
            avg_delta = d.get("avg_delta", 0)
            lines.append(bet_line or f"📉 **{market_name}** — {sig.outcome_name}")
            lines.append(f"## {books_moved} books moved {direction}")
            lines.append(f"**Avg delta: {avg_delta:+.1f}**")

        elif sig.signal_type == SignalType.PINNACLE_DIVERGENCE:
            us_book = display_book(d.get("us_book", "?"))
            us_val = d.get("us_value", "?")
            pin_val = d.get("pinnacle_value", "?")
            delta = d.get("delta", 0)
            lines.append(bet_line or f"💰 **{market_name}** — {sig.outcome_name}")
            if sig.market_key == "h2h":
                lines.append(f"## {us_book}: {us_val:+.0f}  vs  Pinnacle: {pin_val:+.0f}")
            else:
                lines.append(f"## {us_book}: {us_val}  vs  Pinnacle: {pin_val}")
            if sig.market_key == "h2h":
                lines.append(f"**Value edge: {delta:.1%}**")
            else:
                lines.append(f"**Value edge: {delta:+.1f}**")

        elif sig.signal_type == SignalType.REVERSE_LINE:
            us_dir = d.get("us_direction", "?")
            pin_dir = d.get("pinnacle_direction", "?")
            us_avg = d.get("us_avg_delta", 0)
            pin_delta = d.get("pinnacle_delta", 0)
            pin_odds = _format_odds(
                sig.market_key, d.get("pinnacle_price"), d.get("pinnacle_point")
            )
            lines.append(bet_line or f"🔄 **{market_name}** — {sig.outcome_name}")
            lines.append(f"## US {us_dir} ({us_avg:+.1f})  vs  Pinnacle {pin_dir} ({pin_delta:+.1f})")
            lines.append(f"**Pinnacle line: {pin_odds}**")

        elif sig.signal_type == SignalType.ARBITRAGE:
            profit = d.get("profit_pct", 0)
            side_a = d.get("side_a", {})
            side_b = d.get("side_b", {})
            bm_a = display_book(side_a.get("bookmaker", "?"))
            bm_b = display_book(side_b.get("bookmaker", "?"))
            odds_a = _format_odds(sig.market_key, side_a.get("price"), side_a.get("point"))
            odds_b = _format_odds(sig.market_key, side_b.get("price"), side_b.get("point"))
            out_a = side_a.get("outcome", "?")
            out_b = side_b.get("outcome", "?")
            lines.append(f"## {profit:.2f}% guaranteed profit")

            # Compute stake sizing for guaranteed equal payout
            price_a = side_a.get("price")
            price_b = side_b.get("price")
            stake_a_label = ""
            stake_b_label = ""
            if price_a is not None and price_b is not None:
                prob_a = american_to_implied_prob(price_a)
                prob_b = american_to_implied_prob(price_b)
                total_prob = prob_a + prob_b
                pct_a = prob_b / total_prob * 100
                pct_b = prob_a / total_prob * 100
                lines.append(f"**Per $100:**")
                stake_a_label = f" (${pct_a:.2f})"
                stake_b_label = f" (${pct_b:.2f})"
            else:
                stake_a_label = ""
                stake_b_label = ""

            link_a = side_a.get("deep_link")
            link_b = side_b.get("deep_link")
            if link_a:
                lines.append(f"[**{bm_a}**]({link_a}) — {out_a} **{odds_a}**{stake_a_label}")
            else:
                lines.append(f"**{bm_a}** — {out_a} **{odds_a}**{stake_a_label}")
            if link_b:
                lines.append(f"[**{bm_b}**]({link_b}) — {out_b} **{odds_b}**{stake_b_label}")
            else:
                lines.append(f"**{bm_b}** — {out_b} **{odds_b}**{stake_b_label}")

        elif sig.signal_type == SignalType.EXCHANGE_SHIFT:
            direction = d.get("direction", "?")
            shift = d.get("shift", 0)
            old_prob = d.get("old_implied_prob", 0)
            new_prob = d.get("new_implied_prob", 0)
            lines.append(bet_line or f"📈 **{market_name}** — {sig.outcome_name}")
            lines.append(f"## {old_prob:.1%}  →  {new_prob:.1%}")
            lines.append(f"**Betfair shift: {shift:+.1%}**")

        else:
            lines.append(sig.description)

        # Show hold (vig) if available — applies to PD, SM, RC
        us_hold = d.get("us_hold")
        if us_hold is not None:
            hold_pct = us_hold * 100
            if hold_pct < 4.5:
                hold_label = "Sharp"
            elif hold_pct < 5.0:
                hold_label = "Average"
            else:
                hold_label = "Wide"
            lines.append(
                "-# Hold: {pct:.1f}% ({label})".format(
                    pct=hold_pct, label=hold_label
                )
            )

        # Cross-book hold: synthetic hold from best odds across all books
        # Note: for PD signals, this compares best prices regardless of point
        # value, so negative hold doesn't always mean a real arb (points may differ).
        cross_hold = d.get("cross_book_hold")
        if cross_hold is not None:
            cross_pct = cross_hold * 100
            if cross_pct < 0:
                cross_label = "Efficient"
            elif cross_pct < 1.5:
                cross_label = "Tight"
            elif cross_pct < 3.0:
                cross_label = "Edge"
            else:
                cross_label = "Wide Edge"
            lines.append(
                "-# Market: {pct:.1f}% ({label})".format(
                    pct=cross_pct, label=cross_label
                )
            )

        # Price dispersion: how spread out are books on this side
        dispersion = d.get("dispersion")
        if dispersion is not None and dispersion > 0:
            if sig.market_key == "h2h":
                lines.append("-# Dispersion: {d:.1%}".format(d=dispersion))
            else:
                disp_str = "{d:.1f}".format(d=dispersion) if dispersion != int(dispersion) else "{d:.0f}".format(d=dispersion)
                lines.append("-# Dispersion: {d}pts".format(d=disp_str))

        return "\n".join(lines)

    def _add_details(self, embed: DiscordEmbed, sig: Signal, market_name: str) -> None:
        """Add signal-type-specific detail fields."""
        d = sig.details

        if sig.signal_type == SignalType.STEAM_MOVE:
            book_details = d.get("book_details", [])
            if book_details:
                lines = []
                for b in book_details:
                    bm = display_book(b["bookmaker"])
                    odds = _format_odds(sig.market_key, b.get("price"), b.get("point"))
                    link = b.get("deep_link")
                    if link:
                        lines.append(f"[**{bm}**]({link}) — **{odds}**")
                    else:
                        lines.append(f"`{bm:15s}` **{odds}**")
                embed.add_embed_field(
                    name="Book Movements", value="\n".join(lines), inline=False
                )

        # Additional value books (best one is already shown in description)
        value_books = d.get("value_books", [])
        remaining = value_books[1:]
        if remaining:
            lines = []
            for vb in remaining:
                bm = display_book(vb["bookmaker"])
                odds = _format_odds(sig.market_key, vb.get("price"), vb.get("point"))
                link = vb.get("deep_link")
                if link:
                    lines.append(f"[**{bm}**]({link}) — {sig.outcome_name} **{odds}**")
                else:
                    lines.append(f"**{bm}** — {sig.outcome_name} **{odds}**")
            embed.add_embed_field(
                name="💰 More Value Bets",
                value="\n".join(lines),
                inline=False,
            )

        # Context books (not beating Pinnacle, but useful for comparison)
        context_books = d.get("context_books", [])
        if context_books:
            lines = []
            for cb in context_books:
                bm = display_book(cb["bookmaker"])
                odds = _format_odds(sig.market_key, cb.get("price"), cb.get("point"))
                link = cb.get("deep_link")
                if link:
                    lines.append(f"[**{bm}**]({link}) — {sig.outcome_name} **{odds}**")
                else:
                    lines.append(f"**{bm}** — {sig.outcome_name} **{odds}**")
            embed.add_embed_field(
                name="📋 Other Books",
                value="\n".join(lines),
                inline=False,
            )
