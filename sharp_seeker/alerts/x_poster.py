"""X (Twitter) alert poster — tweets teasers and occasional free plays."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import structlog
import tweepy
from discord_webhook import DiscordWebhook

from sharp_seeker.config import Settings
from sharp_seeker.db.repository import Repository
from sharp_seeker.engine.base import Signal, SignalType

TYPE_CHECKING = False
if TYPE_CHECKING:
    from sharp_seeker.analysis.card_generator import CardGenerator

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

    def __init__(
        self,
        settings: Settings,
        repo: Repository,
        card_gen: CardGenerator | None = None,
    ) -> None:
        self._repo = repo
        self._card_gen = card_gen
        self._cta_url = settings.x_cta_url
        self._teaser_hours: list[int] = settings.x_teaser_hours
        self._max_strength = settings.x_max_strength
        self._free_play_sports: list[str] = settings.x_free_play_sports
        self._free_play_markets: list[str] = settings.x_free_play_markets
        self._tweet_types: set[str] = set(settings.x_tweet_signal_types)
        self._excluded_books: set[str] = set(settings.x_excluded_books)
        self._digest_mode: bool = settings.x_digest_interval_hours > 0
        self._digest_buffer: list[Signal] = []
        self._digest_free_plays: list[Signal] = []
        self._discord_webhook_url: str = settings.discord_webhook_url
        self._enabled = False
        self._api: tweepy.API | None = None

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
            # v1.1 API for media uploads (create_tweet only accepts media_ids)
            auth = tweepy.OAuth1UserHandler(
                settings.x_consumer_key,
                settings.x_consumer_secret,
                settings.x_access_token,
                settings.x_access_token_secret,
            )
            self._api = tweepy.API(auth)
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

        now_utc = datetime.now(timezone.utc)
        now_hour = now_utc.hour

        # Free plays: every 2U signal (3+ qualifiers) becomes a free play
        past_fp_events = await self._repo.get_free_play_event_ids()
        free_play_picks: list[Signal] = []
        for s in eligible:
            q_count = (s.details or {}).get("qualifier_count", 0)
            if q_count < 3:
                continue
            if s.event_id in past_fp_events:
                continue
            if self._excluded_books and self._get_book(s) in self._excluded_books:
                continue
            free_play_picks.append(s)

        for pick in free_play_picks:
            try:
                text = self._format_free_play(pick)
                tweet_url = self._post_tweet(text)
                await self._repo.mark_alert_free_play(
                    pick.event_id, pick.market_key, pick.outcome_name,
                )
                if tweet_url:
                    self._notify_discord(tweet_url)
                if self._digest_mode:
                    self._digest_free_plays.append(pick)
                log.info(
                    "x_tweet_posted",
                    signal_type=pick.signal_type.value,
                    event_id=pick.event_id,
                    free_play=True,
                    qualifier_count=3,
                )
            except Exception:
                log.exception("x_tweet_failed", event_id=pick.event_id)

        # Collect teaser-eligible signals (exclude free play picks)
        fp_set = set(id(s) for s in free_play_picks)
        teasers = [s for s in eligible if id(s) not in fp_set]

        if self._digest_mode:
            # Buffer teasers for the next digest tweet
            self._digest_buffer.extend(teasers)
            if teasers:
                log.info("x_teasers_buffered", count=len(teasers), buffer_size=len(self._digest_buffer))
        else:
            # Legacy per-signal mode (subject to teaser hours)
            if self._teaser_hours and now_hour not in self._teaser_hours:
                log.debug("x_teaser_skipped", reason="outside_teaser_hours", hour_utc=now_hour)
                return

            for signal in teasers:
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

    async def post_digest(self) -> None:
        """Post a single digest tweet for all buffered teasers + free plays, then clear."""
        if not self._enabled:
            return
        if not self._digest_buffer and not self._digest_free_plays:
            log.debug("x_digest_skipped", reason="empty_buffer")
            return

        text = self._format_digest(self._digest_buffer, self._digest_free_plays)
        try:
            self._post_tweet(text)
            log.info(
                "x_digest_posted",
                teasers=len(self._digest_buffer),
                free_plays=len(self._digest_free_plays),
            )
        except Exception:
            log.exception("x_digest_failed")
        self._digest_buffer.clear()
        self._digest_free_plays.clear()

    def _format_digest(
        self, signals: list[Signal], free_plays: list[Signal] | None = None,
    ) -> str:
        """Format buffered signals + free plays into a single digest tweet (max 280 chars)."""
        free_plays = free_plays or []
        total = len(signals) + len(free_plays)
        cta = f"\n\nGet real-time signals in Discord \u2192 {self._cta_url}" if self._cta_url else ""
        header = f"\U0001f4ca Sharp Signals \u2014 {total} alert{'s' if total != 1 else ''}"

        lines: list[str] = []
        # Free plays section — show the pick since it's already public
        if free_plays:
            lines.append("\U0001f3af Free Plays")
            for sig in free_plays:
                d = sig.details
                value_books = d.get("value_books", [])
                if value_books:
                    best = value_books[0]
                    bm = best["bookmaker"].title()
                    odds = _format_odds(sig.market_key, best.get("price"), best.get("point"))
                    pick = f"{sig.outcome_name} {odds} @ {bm}"
                else:
                    pick = sig.outcome_name
                lines.append(f"  {sig.away_team} @ {sig.home_team} \u2014 {pick}")
        # Signals section
        if signals:
            if free_plays:
                lines.append("")
            lines.append("\U0001f525 Discord Signals")
            for sig in signals:
                label = _SIGNAL_LABELS.get(sig.signal_type, sig.signal_type.value)
                lines.append(f"  {sig.away_team} @ {sig.home_team} \u2014 {label}")

        # Try all lines first
        body = "\n".join(lines)
        tweet = f"{header}\n\n{body}{cta}"
        if len(tweet) <= 280:
            return tweet

        # Remove lines from the end until it fits with "...and N more"
        for show in range(len(lines) - 1, 0, -1):
            omitted = len(lines) - show
            body = "\n".join(lines[:show])
            tweet = f"{header}\n\n{body}\n...and {omitted} more{cta}"
            if len(tweet) <= 280:
                return tweet

        # Fallback: header + count + cta only
        return f"{header}\n\n...and {total} more{cta}"

    async def post_daily_recap(self) -> None:
        """Post a daily recap of yesterday's free plays to X, with card image."""
        if not self._enabled:
            log.info("x_recap_skipped", reason="disabled")
            return

        since = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
        results = await self._repo.get_free_play_results_since(since)
        if not results:
            log.info("x_recap_skipped", reason="no_free_plays")
            return

        text = self._format_recap(results)

        # Generate card images and attach the square one to the tweet
        media_ids = None
        if self._card_gen is not None:
            try:
                paths = await self._card_gen.generate_daily_cards()
                square = [p for p in paths if "1080x1080" in p]
                if square:
                    media_id = self._upload_media(square[0])
                    if media_id is not None:
                        media_ids = [media_id]
            except Exception:
                log.exception("x_recap_card_error")

        self._post_tweet(text, media_ids=media_ids)
        log.info("x_recap_posted", free_plays=len(results), has_card=media_ids is not None)

    async def post_weekly_recap(self) -> None:
        """Post a weekly recap of this week's free plays to X."""
        if not self._enabled:
            log.info("x_weekly_recap_skipped", reason="disabled")
            return

        since = (datetime.now(timezone.utc) - timedelta(hours=168)).isoformat()
        results = await self._repo.get_free_play_results_since(since)
        if not results:
            log.info("x_weekly_recap_skipped", reason="no_free_plays")
            return

        text = self._format_weekly_recap(results)
        self._post_tweet(text)
        log.info("x_weekly_recap_posted", free_plays=len(results))

    def _format_weekly_recap(self, results: list) -> str:
        """Format a weekly recap tweet from free play results (max 280 chars)."""
        _RESULT_EMOJI = {
            "won": "\u2705",
            "lost": "\u274c",
            "push": "\u21a9\ufe0f",
        }
        header = "\U0001f4ca Weekly Free Plays"

        # Build per-pick lines
        lines: list[str] = []
        wins = losses = 0
        for row in results:
            row_dict = dict(row) if not isinstance(row, dict) else row
            result = row_dict.get("result")
            outcome = row_dict["outcome_name"]
            market = row_dict["market_key"]

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

        # Fixed footer — always shown
        footer_parts: list[str] = []
        decided = wins + losses
        if decided > 0:
            footer_parts.append(f"Record: {wins}-{losses}")
        if self._cta_url:
            footer_parts.append(f"Get all picks \u2192 {self._cta_url}")
        footer = "\n".join(footer_parts)

        # Try all lines first
        body = "\n".join(lines)
        tweet = f"{header}\n\n{body}\n\n{footer}" if footer else f"{header}\n\n{body}"
        if len(tweet) <= 280:
            return tweet

        # Remove lines from the end until it fits with "...and N more"
        for show in range(len(lines) - 1, 0, -1):
            omitted = len(lines) - show
            body = "\n".join(lines[:show])
            suffix = f"\n...and {omitted} more"
            tweet = f"{header}\n\n{body}{suffix}\n\n{footer}" if footer else f"{header}\n\n{body}{suffix}"
            if len(tweet) <= 280:
                return tweet

        # Fallback: header + count + footer only
        total = len(lines)
        tweet = f"{header}\n\n...and {total} more\n\n{footer}" if footer else f"{header}\n\n...and {total} more"
        return tweet

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

    def _upload_media(self, filepath: str) -> int | None:
        """Upload an image via the v1.1 API. Returns media_id or None."""
        if self._api is None:
            return None
        try:
            media = self._api.media_upload(filename=filepath)
            log.info("x_media_uploaded", media_id=media.media_id, path=filepath)
            return media.media_id
        except Exception:
            log.exception("x_media_upload_failed", path=filepath)
            return None

    def _post_tweet(self, text: str, media_ids: list[int] | None = None) -> str | None:
        """Send a tweet via the X API v2. Returns the tweet URL if available."""
        assert self._client is not None
        kwargs: dict = {"text": text}
        if media_ids:
            kwargs["media_ids"] = media_ids
        resp = self._client.create_tweet(**kwargs)
        try:
            tweet_id = resp.data["id"]
            return "https://x.com/i/status/{tid}".format(tid=tweet_id)
        except (TypeError, KeyError, AttributeError):
            return None

    def _notify_discord(self, tweet_url: str) -> None:
        """Send the tweet link to the default Discord webhook."""
        try:
            webhook = DiscordWebhook(url=self._discord_webhook_url, content=tweet_url)
            webhook.execute()
        except Exception:
            log.exception("discord_free_play_notify_failed")
