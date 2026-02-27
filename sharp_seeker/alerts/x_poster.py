"""X (Twitter) alert poster — tweets teasers and occasional free plays."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

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
        self._teaser_hours: list[int] = settings.x_teaser_hours
        self._max_strength = settings.x_max_strength
        self._free_play_sports: list[str] = settings.x_free_play_sports
        self._free_play_markets: list[str] = settings.x_free_play_markets
        self._tweet_types: set[str] = set(settings.x_tweet_signal_types)
        self._excluded_books: set[str] = set(settings.x_excluded_books)
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

        tweetable = [s for s in signals if s.signal_type.value in self._tweet_types]
        if not tweetable:
            return

        # Filter by strength cap — eligible signals are below the max
        eligible = [s for s in tweetable if s.strength < self._max_strength]
        if not eligible:
            log.info("x_batch_skipped", reason="all_above_strength_cap", cap=self._max_strength)
            return

        # Discord alerter already recorded all signals before we run,
        # so total includes the entire current batch.  Compute seq
        # range from *unfiltered* batch size to keep counter predictable.
        total = await self._repo.count_alerts_by_types(list(self._tweet_types))
        batch_size = len(tweetable)
        seq_start = total - batch_size + 1
        seq_end = total  # inclusive

        # Check if any seq in this batch hits the free play interval
        free_play_due = any(
            seq > 0 and seq % self._free_play_interval == 0
            for seq in range(seq_start, seq_end + 1)
        )

        now_hour = datetime.now(timezone.utc).hour

        # Post free play (always, regardless of teaser hours)
        free_play_pick: Signal | None = None
        if free_play_due:
            free_play_pick = self._pick_best_free_play(eligible)
            try:
                text = self._format_free_play(free_play_pick)
                self._post_tweet(text)
                await self._repo.mark_alert_free_play(
                    free_play_pick.event_id, free_play_pick.market_key,
                    free_play_pick.outcome_name,
                )
                log.info(
                    "x_tweet_posted",
                    signal_type=free_play_pick.signal_type.value,
                    event_id=free_play_pick.event_id,
                    free_play=True,
                )
            except Exception:
                log.exception("x_tweet_failed", event_id=free_play_pick.event_id)

        # Post teasers (subject to teaser hours)
        if self._teaser_hours and now_hour not in self._teaser_hours:
            log.debug("x_teaser_skipped", reason="outside_teaser_hours", hour_utc=now_hour)
            return

        for signal in eligible:
            if signal is free_play_pick:
                continue  # already posted as free play
            try:
                text = self._format_teaser(signal)
                self._post_tweet(text)
                log.info(
                    "x_tweet_posted",
                    signal_type=signal.signal_type.value,
                    event_id=signal.event_id,
                    free_play=False,
                )
            except Exception:
                log.exception("x_tweet_failed", event_id=signal.event_id)

    @staticmethod
    def _get_book(signal: Signal) -> str | None:
        """Extract the recommended bookmaker from signal details."""
        value_books = signal.details.get("value_books", [])
        if value_books:
            return value_books[0].get("bookmaker")
        return signal.details.get("us_book")

    def _pick_best_free_play(self, eligible: list[Signal]) -> Signal:
        """Score eligible signals and pick the best candidate for free play."""
        if self._excluded_books:
            filtered = [
                s for s in eligible
                if self._get_book(s) not in self._excluded_books
            ]
            # Fall back to unfiltered if all candidates are excluded
            if filtered:
                eligible = filtered

        def score(s: Signal) -> tuple[int, int, float]:
            sport_bonus = 1 if self._free_play_sports and s.sport_key in self._free_play_sports else 0
            market_bonus = 1 if self._free_play_markets and s.market_key in self._free_play_markets else 0
            strength_score = 1.0 - s.strength  # lower strength → higher score
            return (sport_bonus, market_bonus, strength_score)
        return max(eligible, key=score)

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
            f"{_SIGNAL_LABELS.get(signal.signal_type, signal.signal_type.value)} \u2022 {signal.strength:.0%} strength",
        ]
        if self._cta_url:
            lines.append("")
            lines.append(f"Get real-time signals in Discord \u2192 {self._cta_url}")
        return "\n".join(lines)

    async def post_daily_recap(self) -> None:
        """Post a daily recap of yesterday's free plays to X."""
        if not self._enabled:
            log.info("x_recap_skipped", reason="disabled")
            return

        since = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
        results = await self._repo.get_free_play_results_since(since)
        if not results:
            log.info("x_recap_skipped", reason="no_free_plays")
            return

        text = self._format_recap(results)
        self._post_tweet(text)
        log.info("x_recap_posted", free_plays=len(results))

    def _format_recap(self, results: list) -> str:
        """Format a recap tweet from free play results."""
        _RESULT_EMOJI = {"won": "\u2705", "lost": "\u274c", "push": "\u21a9\ufe0f"}
        lines = ["\U0001f4ca Yesterday's Free Plays", ""]

        wins = losses = 0
        for row in results:
            row_dict = dict(row) if not isinstance(row, dict) else row
            result = row_dict.get("result")
            outcome = row_dict["outcome_name"]
            market = row_dict["market_key"]

            # Extract odds from details_json
            odds_str = ""
            details_raw = row_dict.get("details_json")
            if details_raw:
                try:
                    details = json.loads(details_raw) if isinstance(details_raw, str) else details_raw
                    value_books = details.get("value_books", [])
                    if value_books:
                        best = value_books[0]
                        odds_str = " " + _format_odds(market, best.get("price"), best.get("point"))
                except (json.JSONDecodeError, TypeError):
                    pass

            if result:
                emoji = _RESULT_EMOJI.get(result, "\u2753")
                label = result.upper()
                lines.append(f"{emoji} {outcome}{odds_str} \u2014 {label}")
                if result == "won":
                    wins += 1
                elif result == "lost":
                    losses += 1
            else:
                lines.append(f"\u23f3 {outcome}{odds_str} \u2014 PENDING")

        decided = wins + losses
        if decided > 0:
            lines.append("")
            lines.append(f"Record: {wins}-{losses}")

        if self._cta_url:
            lines.append("")
            lines.append(f"Get all picks in Discord \u2192 {self._cta_url}")

        return "\n".join(lines)

    def _post_tweet(self, text: str) -> None:
        """Send a tweet via the X API v2."""
        assert self._client is not None
        self._client.create_tweet(text=text)
