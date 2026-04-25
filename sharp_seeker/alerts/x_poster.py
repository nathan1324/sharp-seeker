"""X (Twitter) alert poster — tweets teasers and occasional free plays."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import structlog
import tweepy
from discord_webhook import DiscordWebhook

from sharp_seeker.alerts.models import display_book
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
    SignalType.ARBITRAGE: "Arbitrage",
}


_TEAM_ABBR: dict[str, str] = {
    # NBA
    "Atlanta Hawks": "ATL", "Boston Celtics": "BOS", "Brooklyn Nets": "BKN",
    "Charlotte Hornets": "CHA", "Chicago Bulls": "CHI", "Cleveland Cavaliers": "CLE",
    "Dallas Mavericks": "DAL", "Denver Nuggets": "DEN", "Detroit Pistons": "DET",
    "Golden State Warriors": "GSW", "Houston Rockets": "HOU", "Indiana Pacers": "IND",
    "Los Angeles Clippers": "LAC", "Los Angeles Lakers": "LAL", "LA Clippers": "LAC",
    "Memphis Grizzlies": "MEM", "Miami Heat": "MIA", "Milwaukee Bucks": "MIL",
    "Minnesota Timberwolves": "MIN", "New Orleans Pelicans": "NOP",
    "New York Knicks": "NYK", "Oklahoma City Thunder": "OKC",
    "Orlando Magic": "ORL", "Philadelphia 76ers": "PHI", "Phoenix Suns": "PHX",
    "Portland Trail Blazers": "POR", "Sacramento Kings": "SAC",
    "San Antonio Spurs": "SAS", "Toronto Raptors": "TOR", "Utah Jazz": "UTA",
    "Washington Wizards": "WAS",
    # NHL
    "Anaheim Ducks": "ANA", "Arizona Coyotes": "ARI", "Boston Bruins": "BOS",
    "Buffalo Sabres": "BUF", "Calgary Flames": "CGY", "Carolina Hurricanes": "CAR",
    "Chicago Blackhawks": "CHI", "Colorado Avalanche": "COL",
    "Columbus Blue Jackets": "CBJ", "Dallas Stars": "DAL", "Detroit Red Wings": "DET",
    "Edmonton Oilers": "EDM", "Florida Panthers": "FLA", "Los Angeles Kings": "LAK",
    "Minnesota Wild": "MIN", "Montreal Canadiens": "MTL", "Nashville Predators": "NSH",
    "New Jersey Devils": "NJD", "New York Islanders": "NYI",
    "New York Rangers": "NYR", "Ottawa Senators": "OTT",
    "Philadelphia Flyers": "PHI", "Pittsburgh Penguins": "PIT",
    "San Jose Sharks": "SJS", "Seattle Kraken": "SEA", "St. Louis Blues": "STL",
    "St Louis Blues": "STL",
    "Tampa Bay Lightning": "TBL", "Toronto Maple Leafs": "TOR",
    "Utah Hockey Club": "UTA",
    "Vancouver Canucks": "VAN", "Vegas Golden Knights": "VGK",
    "Washington Capitals": "WSH", "Winnipeg Jets": "WPG",
    # MLB
    "Arizona Diamondbacks": "ARI", "Atlanta Braves": "ATL",
    "Baltimore Orioles": "BAL", "Boston Red Sox": "BOS", "Chicago Cubs": "CHC",
    "Chicago White Sox": "CWS", "Cincinnati Reds": "CIN",
    "Cleveland Guardians": "CLE", "Colorado Rockies": "COL",
    "Detroit Tigers": "DET", "Houston Astros": "HOU", "Kansas City Royals": "KC",
    "Los Angeles Angels": "LAA", "Los Angeles Dodgers": "LAD",
    "Miami Marlins": "MIA", "Milwaukee Brewers": "MIL",
    "Minnesota Twins": "MIN", "New York Mets": "NYM", "New York Yankees": "NYY",
    "Oakland Athletics": "OAK", "Philadelphia Phillies": "PHI",
    "Pittsburgh Pirates": "PIT", "San Diego Padres": "SD",
    "San Francisco Giants": "SF", "Seattle Mariners": "SEA",
    "St. Louis Cardinals": "STL", "St Louis Cardinals": "STL",
    "Tampa Bay Rays": "TB", "Texas Rangers": "TEX",
    "Toronto Blue Jays": "TOR", "Washington Nationals": "WSH",
}


def _abbr(team: str) -> str:
    """Return 2-4 letter team abbreviation, falling back to last word."""
    return _TEAM_ABBR.get(team, team.split()[-1].upper()[:4])


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
        self._free_play_sport_cap = settings.x_free_play_sport_cap
        self._free_play_hourly_cap = settings.x_free_play_hourly_cap
        self._free_play_interval = settings.x_free_play_interval
        self._free_play_combos: set[str] = set(settings.x_free_play_combos)
        self._fp_eligible_count = 0
        self._fp_eligible_date: str = ""
        self._max_strength = settings.x_max_strength
        self._tweet_types: set[str] = set(settings.x_tweet_signal_types)
        self._excluded_books: set[str] = set(settings.x_excluded_books)
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

        now_utc = datetime.now(timezone.utc)

        # Free plays: signals matching whitelisted type:sport:market combos.
        # Every Nth eligible signal becomes a free play (interval).
        # Additional caps: per sport per day, per hour, unique event.
        if not self._free_play_combos:
            free_play_picks: list[Signal] = []
        else:
            # Reset eligible counter at the start of each UTC day
            today_str = now_utc.strftime("%Y-%m-%d")
            if self._fp_eligible_date != today_str:
                self._fp_eligible_count = 0
                self._fp_eligible_date = today_str

            past_fp_events = await self._repo.get_free_play_event_ids()
            today_start = now_utc.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
            hour_start = now_utc.replace(minute=0, second=0, microsecond=0).isoformat()
            hour_fp_count = await self._repo.count_free_plays_since(hour_start)

            # Count today's free plays by sport
            today_fp_rows = await self._repo.get_free_play_details_since(today_start)
            sport_fp_counts: dict[str, int] = {}
            for fp_row in today_fp_rows:
                fp_sport = fp_row.get("sport_key", "")
                sport_fp_counts[fp_sport] = sport_fp_counts.get(fp_sport, 0) + 1

            free_play_picks: list[Signal] = []
            for s in signals:
                combo_key = f"{s.signal_type.value}:{s.sport_key}:{s.market_key}"
                if combo_key not in self._free_play_combos:
                    continue
                # Policy: spread free plays must be Steam type only.
                if s.market_key == "spreads" and s.signal_type != SignalType.STEAM_MOVE:
                    log.info(
                        "x_free_play_spreads_non_steam_skip",
                        event_id=s.event_id,
                        signal_type=s.signal_type.value,
                    )
                    continue
                if s.event_id in past_fp_events:
                    continue
                if self._excluded_books and self._get_book(s) in self._excluded_books:
                    continue
                # Count every eligible signal, but only post every Nth one
                self._fp_eligible_count += 1
                if self._fp_eligible_count % self._free_play_interval != 0:
                    log.info(
                        "x_free_play_interval_skip",
                        event_id=s.event_id,
                        eligible=self._fp_eligible_count,
                        interval=self._free_play_interval,
                    )
                    continue
                # Hourly cap
                if (hour_fp_count + len(free_play_picks)) >= self._free_play_hourly_cap:
                    log.info("x_free_play_hourly_capped", event_id=s.event_id)
                    continue
                # Per-sport daily cap
                sport_count = sport_fp_counts.get(s.sport_key, 0) + sum(
                    1 for p in free_play_picks if p.sport_key == s.sport_key
                )
                if sport_count >= self._free_play_sport_cap:
                    log.info("x_free_play_sport_capped", event_id=s.event_id, sport=s.sport_key)
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
                log.info(
                    "x_tweet_posted",
                    signal_type=pick.signal_type.value,
                    event_id=pick.event_id,
                    free_play=True,
                    qualifier_count=(pick.details or {}).get("qualifier_count", 0),
                )
            except Exception:
                log.exception("x_tweet_failed", event_id=pick.event_id)

        # Teasers disabled — only free plays are posted to X.

    @staticmethod
    def _get_book(signal: Signal) -> str | None:
        """Extract the recommended bookmaker from signal details."""
        value_books = signal.details.get("value_books", [])
        if value_books:
            return value_books[0].get("bookmaker")
        return signal.details.get("us_book")

    def _format_free_play(self, signal: Signal) -> str:
        matchup = f"{signal.away_team} vs {signal.home_team}"
        d = signal.details
        # Find best value book for the recommendation
        value_books = d.get("value_books", [])
        if value_books:
            best = value_books[0]
            bm = display_book(best["bookmaker"])
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
        """No-op — teasers disabled, digest is no longer needed."""
        return

    async def post_daily_recap(self) -> None:
        """Post a daily recap of yesterday's free plays to X, with card image."""
        if not self._enabled:
            log.info("x_recap_skipped", reason="disabled")
            return

        since = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
        results = await self._repo.get_free_play_results_resolved_since(since)
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
            tier_badge = ""
            details_raw = row_dict.get("details_json")
            if details_raw:
                try:
                    details = json.loads(details_raw) if isinstance(details_raw, str) else details_raw
                    value_books = details.get("value_books", [])
                    if value_books:
                        best = value_books[0]
                        odds_str = " " + _format_odds(market, best.get("price"), best.get("point"))
                    q_count = details.get("qualifier_count", 0)
                    if q_count >= 2:
                        tier_badge = " \U0001f3c6"  # Elite
                except (json.JSONDecodeError, TypeError):
                    pass

            if result:
                emoji = _RESULT_EMOJI.get(result, "\u2753")
                label = result.upper()
                lines.append(f"{emoji} {outcome}{odds_str}{tier_badge} \u2014 {label}")
                if result == "won":
                    wins += 1
                elif result == "lost":
                    losses += 1
            else:
                lines.append(f"\u23f3 {outcome}{odds_str}{tier_badge} \u2014 PENDING")

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

            # Extract odds, tier, and matchup from details_json
            odds_str = ""
            tier_badge = ""
            matchup_prefix = ""
            details_raw = row_dict.get("details_json")
            if details_raw:
                try:
                    details = json.loads(details_raw) if isinstance(details_raw, str) else details_raw
                    value_books = details.get("value_books", [])
                    if value_books:
                        best = value_books[0]
                        odds_str = " " + _format_odds(market, best.get("price"), best.get("point"))
                    q_count = details.get("qualifier_count", 0)
                    if q_count >= 2:
                        tier_badge = " \U0001f3c6"  # Elite
                    # Build compact matchup prefix for totals: "LAL/BOS"
                    home = details.get("home_team")
                    away = details.get("away_team")
                    if home and away and market == "totals":
                        matchup_prefix = _abbr(away) + "/" + _abbr(home) + " "
                except (json.JSONDecodeError, TypeError):
                    pass

            # For totals, show "LAL/BOS O 215.5" instead of just "Over 215.5"
            if market == "totals":
                direction = "O" if outcome.lower() == "over" else "U"
                pick_label = f"{matchup_prefix}{direction}{odds_str}"
            else:
                pick_label = f"{outcome}{odds_str}"

            if result:
                emoji = _RESULT_EMOJI.get(result, "\u2753")
                label = result.upper()
                lines.append(f"{emoji} {pick_label}{tier_badge} \u2014 {label}")
                if result == "won":
                    wins += 1
                elif result == "lost":
                    losses += 1
            else:
                lines.append(f"\u23f3 {pick_label}{tier_badge} \u2014 PENDING")

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
