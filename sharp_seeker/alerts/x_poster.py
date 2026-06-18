"""X (Twitter) alert poster — tweets teasers and occasional free plays."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

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


_ET = ZoneInfo("America/New_York")


def _format_game_time(commence_time: str) -> str:
    """Format commence_time as a readable date/time in Eastern.

    Mirrors the Discord alert so a free play can never be mistaken for a live
    bet — the reader always sees when the game starts.
    """
    if not commence_time:
        return ""
    try:
        ct = datetime.fromisoformat(commence_time)
        if ct.tzinfo is None:
            ct = ct.replace(tzinfo=timezone.utc)
        ct_et = ct.astimezone(_ET)
        day = ct_et.strftime("%a, %b")
        dom = ct_et.day
        hour = ct_et.strftime("%I:%M %p").lstrip("0")
        return f"{day} {dom} • {hour} (ET)"
    except (ValueError, TypeError):
        return ""


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


def _compute_units(price: float | None, result: str, multiplier: int = 1) -> float:
    """Risk-adjusted units. Mirrors analysis/reports.py for tweet recaps."""
    if result == "push" or price is None:
        return 0.0
    risk = abs(price) / 100.0 if price < 0 else 100.0 / price
    if result == "won":
        return 1.0 * multiplier
    if result == "lost":
        return -risk * multiplier
    return 0.0


def _units_from_row(row_dict: dict) -> float:
    """Compute units for a single free-play recap row."""
    details_raw = row_dict.get("details_json")
    price = None
    qualifier_count = 0
    if details_raw:
        try:
            details = json.loads(details_raw) if isinstance(details_raw, str) else details_raw
            vb = details.get("value_books", [])
            if vb:
                price = vb[0].get("price")
            qualifier_count = details.get("qualifier_count", 0)
        except (json.JSONDecodeError, TypeError):
            pass
    multiplier = 2 if qualifier_count >= 2 else 1
    return _compute_units(price, row_dict.get("result"), multiplier)


def _fmt_units(u: float) -> str:
    """Format units as `+1.4u` (no brackets — tweet-friendly)."""
    sign = "+" if u >= 0 else ""
    return f"{sign}{u:.1f}u"


def _month_start_iso(now: datetime | None = None) -> str:
    """ISO timestamp of the first day of the current UTC month at 00:00:00."""
    now = now or datetime.now(timezone.utc)
    return now.replace(day=1, hour=0, minute=0, second=0, microsecond=0).isoformat()


def _month_label(now: datetime | None = None) -> str:
    """Short month label like 'May' for the running-total footer."""
    now = now or datetime.now(timezone.utc)
    return now.strftime("%b")


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
        self._free_play_sport_cap = settings.x_free_play_sport_cap
        self._free_play_hourly_cap = settings.x_free_play_hourly_cap
        self._free_play_interval = settings.x_free_play_interval
        self._free_play_combos: set[str] = set(settings.x_free_play_combos)
        self._fp_excluded_sports: set[str] = set(settings.x_free_play_excluded_sports)
        self._fp_excluded_combos: set[str] = set(settings.x_free_play_excluded_combos)
        self._fp_raw_combos: set[str] = set(settings.x_free_play_raw_combos)
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
        if not self._free_play_combos and not self._fp_raw_combos:
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
                # A "raw" combo mirrors the Discord raw-PD channel: it makes the
                # signal eligible on its own and bypasses the qualifier gate, the
                # spreads policy, and the interval/caps below. The excluded-sport/
                # combo kill switches and per-event dedup still apply.
                is_raw = self._combo_matches(s, self._fp_raw_combos)
                if not is_raw and not self._matches_free_play_combo(s):
                    continue
                # Sport parked out of free plays (edge still under test).
                if s.sport_key in self._fp_excluded_sports:
                    log.info(
                        "x_free_play_excluded_sport_skip",
                        event_id=s.event_id,
                        sport_key=s.sport_key,
                    )
                    continue
                # Sport+market combo carved out (this market has no edge for this
                # sport, even though the blanket combo still serves other sports).
                if self._fp_excluded_combos and self._combo_matches(
                    s, self._fp_excluded_combos
                ):
                    log.info(
                        "x_free_play_excluded_combo_skip",
                        event_id=s.event_id,
                        signal_type=s.signal_type.value,
                        sport_key=s.sport_key,
                        market_key=s.market_key,
                    )
                    continue
                # Policy: spread free plays must be Steam type only.
                # Raw combos opt out (the Discord raw channel carries PD spreads).
                if (
                    not is_raw
                    and s.market_key == "spreads"
                    and s.signal_type != SignalType.STEAM_MOVE
                ):
                    log.info(
                        "x_free_play_spreads_non_steam_skip",
                        event_id=s.event_id,
                        signal_type=s.signal_type.value,
                    )
                    continue
                # Mimic Discord: only post signals that cleared Discord's send
                # gate (1+ qualifier). 0-qualifier signals are suppressed from
                # the main Discord alert, so they don't become free plays either.
                # Raw combos mirror the Discord raw-PD channel, which bypasses the
                # qualifier gate — so they post the full population, gate and all.
                q_count = (s.details or {}).get("qualifier_count", 0)
                if not is_raw and q_count < 1:
                    log.info(
                        "x_free_play_qualifier_skip",
                        event_id=s.event_id,
                        qualifier_count=q_count,
                    )
                    continue
                if s.event_id in past_fp_events:
                    continue
                if self._excluded_books and self._get_book(s) in self._excluded_books:
                    continue
                # Raw combos mirror the Discord raw channel: post every one,
                # skipping the interval throttle and the hourly/sport caps.
                if not is_raw:
                    # Count every eligible signal; throttle to every Nth.
                    # interval <= 1 disables throttling (post every eligible signal).
                    self._fp_eligible_count += 1
                    if (
                        self._free_play_interval > 1
                        and self._fp_eligible_count % self._free_play_interval != 0
                    ):
                        log.info(
                            "x_free_play_interval_skip",
                            event_id=s.event_id,
                            eligible=self._fp_eligible_count,
                            interval=self._free_play_interval,
                        )
                        continue
                    # Hourly cap (0 = unlimited)
                    if (
                        self._free_play_hourly_cap > 0
                        and (hour_fp_count + len(free_play_picks)) >= self._free_play_hourly_cap
                    ):
                        log.info("x_free_play_hourly_capped", event_id=s.event_id)
                        continue
                    # Per-sport daily cap (0 = unlimited)
                    sport_count = sport_fp_counts.get(s.sport_key, 0) + sum(
                        1 for p in free_play_picks if p.sport_key == s.sport_key
                    )
                    if self._free_play_sport_cap > 0 and sport_count >= self._free_play_sport_cap:
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

    def _matches_free_play_combo(self, signal: Signal) -> bool:
        """True if the signal matches a whitelisted free-play combo.

        Combos are `signal_type:sport_key:market_key`. A `*` in any segment is a
        wildcard, so `pinnacle_divergence:*:totals` matches PD totals in every
        sport (used to open all-sport PD totals to free plays).
        """
        return self._combo_matches(signal, self._free_play_combos)

    @staticmethod
    def _combo_matches(signal: Signal, combos: set[str]) -> bool:
        """True if the signal matches any `type:sport:market` combo in `combos`.

        A `*` in any of the 3 segments is a wildcard. Shared by the free-play
        whitelist and the exclusion list.
        """
        parts = (signal.signal_type.value, signal.sport_key, signal.market_key)
        for combo in combos:
            cp = combo.split(":")
            if len(cp) == 3 and all(c == "*" or c == p for c, p in zip(cp, parts)):
                return True
        return False

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
        ]
        # Game date/time so a pregame play can't be mistaken for a live bet.
        game_time = _format_game_time(signal.commence_time)
        if game_time:
            lines.append(f"\U0001f4c5 {game_time}")
        return "\n".join(lines)

    async def post_digest(self) -> None:
        """No-op — teasers disabled, digest is no longer needed."""
        return

    async def post_daily_recap(self) -> None:
        """Post a daily recap of yesterday's free plays to X, with card image.

        Always posts — even on zero-free-play days, an accountability footer
        with month-to-date running totals goes out. Skipping breaks the daily
        beat that builds the audience habit.
        """
        if not self._enabled:
            log.info("x_recap_skipped", reason="disabled")
            return

        since = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
        results = await self._repo.get_free_play_results_resolved_since(since)

        # Month-to-date results for the running footer
        mtd_since = _month_start_iso()
        mtd_results = await self._repo.get_free_play_results_resolved_since(mtd_since)

        text = self._format_recap(results, mtd_results)

        # Card image only makes sense when there are plays to show
        media_ids = None
        if results and self._card_gen is not None:
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
        log.info(
            "x_recap_posted",
            free_plays=len(results),
            mtd_plays=len(mtd_results),
            has_card=media_ids is not None,
        )

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
        """Format a weekly recap tweet: record + net units only.

        A full week of free plays never fits in 280 chars, so the weekly recap
        is a summary - the week's win-loss record and net risk-adjusted units -
        not a per-pick list. Pending plays are noted so an incomplete record
        doesn't read as if it's hiding unresolved bets.
        """
        header = "\U0001f4ca Weekly Free Plays"

        wins = losses = pending = 0
        week_units = 0.0
        for row in results:
            row_dict = dict(row) if not isinstance(row, dict) else row
            result = row_dict.get("result")
            if result == "won":
                wins += 1
            elif result == "lost":
                losses += 1
            elif not result:
                pending += 1
            week_units += _units_from_row(row_dict)

        lines = [header, ""]
        if wins + losses > 0:
            lines.append(f"Record: {wins}-{losses} ({_fmt_units(week_units)})")
        else:
            lines.append("No decided plays this week.")
        if pending:
            lines.append(f"{pending} still pending")

        return "\n".join(lines)

    def _format_recap(self, results: list, mtd_results: list | None = None) -> str:
        """Format a recap tweet from free play results.

        Adds per-pick units, daily unit total in the header, and a month-to-date
        line in the footer if mtd_results is provided. Always returns text even
        when results is empty (zero-play accountability post).
        """
        _RESULT_EMOJI = {"won": "\u2705", "lost": "\u274c", "push": "\u21a9\ufe0f"}

        # Aggregate yesterday's record + units while we have the rows in hand
        wins = losses = 0
        daily_units = 0.0
        play_lines: list[str] = []
        for row in results:
            row_dict = dict(row) if not isinstance(row, dict) else row
            result = row_dict.get("result")
            outcome = row_dict["outcome_name"]
            market = row_dict["market_key"]

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
                    home = details.get("home_team")
                    away = details.get("away_team")
                    if home and away and market == "totals":
                        matchup_prefix = _abbr(away) + "/" + _abbr(home) + " "
                except (json.JSONDecodeError, TypeError):
                    pass

            if market == "totals":
                direction = "O" if outcome.lower() == "over" else "U"
                pick_label = f"{matchup_prefix}{direction}{odds_str}"
            else:
                pick_label = f"{outcome}{odds_str}"

            if result:
                emoji = _RESULT_EMOJI.get(result, "\u2753")
                u = _units_from_row(row_dict)
                daily_units += u
                u_str = f" ({_fmt_units(u)})"
                play_lines.append(f"{emoji} {pick_label}{tier_badge}{u_str}")
                if result == "won":
                    wins += 1
                elif result == "lost":
                    losses += 1
            else:
                play_lines.append(f"\u23f3 {pick_label}{tier_badge} \u2014 PENDING")

        # Build the message
        out: list[str] = []
        if results:
            decided = wins + losses
            if decided > 0:
                header = f"\U0001f4ca Yesterday: {wins}-{losses} ({_fmt_units(daily_units)})"
            else:
                header = "\U0001f4ca Yesterday's Free Plays"
            out.append(header)
            out.append("")
            # Cap to 6 plays to keep tweet under 280 chars; card image has the rest
            out.extend(play_lines[:6])
            if len(play_lines) > 6:
                out.append(f"+{len(play_lines) - 6} more on card")
        else:
            out.append("\U0001f4ca No free plays yesterday \u2014 no qualifying signals fired.")

        # Month-to-date footer
        if mtd_results:
            mtd_w = mtd_l = 0
            mtd_units = 0.0
            for row in mtd_results:
                row_dict = dict(row) if not isinstance(row, dict) else row
                r = row_dict.get("result")
                if r == "won":
                    mtd_w += 1
                elif r == "lost":
                    mtd_l += 1
                mtd_units += _units_from_row(row_dict)
            out.append("")
            out.append(
                f"{_month_label()}: {mtd_w}-{mtd_l} ({_fmt_units(mtd_units)})"
            )

        return "\n".join(out)

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
