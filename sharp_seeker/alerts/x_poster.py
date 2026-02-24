"""X (Twitter) alert poster — tweets teasers and occasional free plays."""

from __future__ import annotations

import structlog
import tweepy

from sharp_seeker.config import Settings
from sharp_seeker.db.repository import Repository
from sharp_seeker.engine.base import Signal, SignalType

log = structlog.get_logger()

# Human-readable labels for signal types
_SIGNAL_LABELS: dict[SignalType, str] = {
    SignalType.STEAM_MOVE: "Steam Move",
    SignalType.RAPID_CHANGE: "Rapid Change",
    SignalType.PINNACLE_DIVERGENCE: "Pinnacle Divergence",
    SignalType.REVERSE_LINE: "Reverse Line Movement",
    SignalType.EXCHANGE_SHIFT: "Exchange Shift",
}


def _format_odds(market: str, price: float | None, point: float | None) -> str:
    """Format odds for tweet display: '-3.5 (-110)' or '+150'."""
    if market == "h2h":
        return f"{price:+.0f}" if price is not None else "?"
    parts = []
    if point is not None:
        if market == "totals":
            parts.append(f"{point:.1f}" if point != int(point) else f"{point:.0f}")
        else:
            parts.append(f"{point:+.1f}" if point != int(point) else f"{point:+.0f}")
    if price is not None:
        parts.append(f"({price:+.0f})")
    return " ".join(parts) if parts else "?"


class XPoster:
    """Posts signal teasers (and occasional free plays) to X."""

    def __init__(self, settings: Settings, repo: Repository) -> None:
        self._repo = repo
        self._cta_url = settings.x_cta_url
        self._free_play_interval = settings.x_free_play_interval
        self._enabled = False

        if all([
            settings.x_consumer_key,
            settings.x_consumer_secret,
            settings.x_access_token,
            settings.x_access_token_secret,
        ]):
            self._client = tweepy.Client(
                consumer_key=settings.x_consumer_key,
                consumer_secret=settings.x_consumer_secret,
                access_token=settings.x_access_token,
                access_token_secret=settings.x_access_token_secret,
            )
            self._enabled = True
            log.info("x_poster_enabled")
        else:
            self._client = None
            log.info("x_poster_disabled", reason="missing credentials")

    async def post_signals(self, signals: list[Signal]) -> None:
        """Post a tweet for each signal. Skips gracefully if disabled."""
        if not self._enabled:
            return

        pd_signals = [s for s in signals if s.signal_type == SignalType.PINNACLE_DIVERGENCE]
        if not pd_signals:
            return

        # Discord alerter already recorded all signals before we run,
        # so total_pd includes the entire current batch. Assign each
        # signal its own sequence number to avoid skipping a multiple.
        total_pd = await self._repo.count_alerts_by_type("pinnacle_divergence")
        batch_size = len(pd_signals)

        for i, signal in enumerate(pd_signals):
            seq = total_pd - batch_size + i + 1
            free_play = seq > 0 and seq % self._free_play_interval == 0
            try:
                if free_play:
                    text = self._format_free_play(signal)
                else:
                    text = self._format_teaser(signal)
                self._post_tweet(text)
                log.info(
                    "x_tweet_posted",
                    signal_type=signal.signal_type.value,
                    event_id=signal.event_id,
                    free_play=free_play,
                    seq=seq,
                )
            except Exception:
                log.exception(
                    "x_tweet_failed",
                    event_id=signal.event_id,
                )

    def _is_free_play_seq(self, seq: int) -> bool:
        """Check if a given sequence number should be a free play."""
        return seq > 0 and seq % self._free_play_interval == 0

    def _format_teaser(self, signal: Signal) -> str:
        matchup = f"{signal.away_team} vs {signal.home_team}"
        label = _SIGNAL_LABELS.get(signal.signal_type, signal.signal_type.value)
        lines = [
            f"\U0001f525 Sharp money detected \u2014 {matchup} ({label})",
        ]
        if self._cta_url:
            lines.append("")
            lines.append(f"Get real-time signals in Discord \u2192 {self._cta_url}")
        return "\n".join(lines)

    def _format_free_play(self, signal: Signal) -> str:
        matchup = f"{signal.away_team} vs {signal.home_team}"
        d = signal.details
        # Find best value book for the recommendation
        value_books = d.get("value_books", [])
        if value_books:
            best = value_books[0]
            bm = best["bookmaker"].title()
            odds = _format_odds(signal.market_key, best.get("price"), best.get("point"))
            bet_line = f"\U0001f4b0 Bet {signal.outcome_name} {odds} @ {bm}"
        else:
            bet_line = f"\U0001f4b0 Bet {signal.outcome_name}"

        market_name = {"spreads": "Spread", "totals": "Total", "h2h": "Moneyline"}.get(
            signal.market_key, signal.market_key
        )
        lines = [
            f"\U0001f3af FREE PLAY \u2014 {matchup} {market_name}",
            "",
            bet_line,
            f"Pinnacle Divergence \u2022 {signal.strength:.0%} strength",
        ]
        if self._cta_url:
            lines.append("")
            lines.append(f"Get real-time signals in Discord \u2192 {self._cta_url}")
        return "\n".join(lines)

    def _post_tweet(self, text: str) -> None:
        """Send a tweet via the X API v2."""
        assert self._client is not None
        self._client.create_tweet(text=text)
