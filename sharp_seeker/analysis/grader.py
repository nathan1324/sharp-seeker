"""Auto-grading system for signal results against final game scores."""

from __future__ import annotations

import structlog

from sharp_seeker.api.odds_client import OddsClient
from sharp_seeker.config import Settings
from sharp_seeker.db.repository import Repository

log = structlog.get_logger()


class ScoreGrader:
    def __init__(
        self, settings: Settings, odds_client: OddsClient, repo: Repository
    ) -> None:
        self._settings = settings
        self._odds_client = odds_client
        self._repo = repo

    async def resolve_all(self) -> dict[str, int]:
        """Grade all unresolved signals against final scores.

        Returns counts: {"resolved": N, "skipped": N, "errors": N}
        """
        unresolved = await self._repo.get_unresolved_signals()
        if not unresolved:
            log.info("grader_no_unresolved")
            return {"resolved": 0, "skipped": 0, "errors": 0}

        # Collect sport keys from unresolved signals to fetch scores
        sport_keys: set[str] = set()
        for sig in unresolved:
            teams = await self._repo.get_event_teams(sig["event_id"])
            if teams:
                # Get sport_key from snapshots
                row = await self._repo._db.execute(
                    "SELECT sport_key FROM odds_snapshots WHERE event_id = ? LIMIT 1",
                    (sig["event_id"],),
                )
                sport_row = await row.fetchone()
                if sport_row:
                    sport_keys.add(sport_row["sport_key"])

        if not sport_keys:
            # Fall back to configured sports
            sport_keys = set(self._settings.sports)

        # Fetch scores for each sport (with daysFrom=3 to catch weekend games)
        scores_by_event: dict[str, dict] = {}
        for sport_key in sport_keys:
            try:
                games = await self._odds_client.fetch_scores(sport_key, days_from=3)
                for game in games:
                    scores_by_event[game["id"]] = game
            except Exception:
                log.exception("grader_fetch_scores_error", sport=sport_key)

        resolved = 0
        skipped = 0
        errors = 0

        for sig in unresolved:
            sig_dict = dict(sig)
            event_id = sig_dict["event_id"]
            market_key = sig_dict["market_key"]
            outcome_name = sig_dict["outcome_name"]
            signal_at = sig_dict["signal_at"]
            signal_type = sig_dict["signal_type"]

            game = scores_by_event.get(event_id)
            if not game or not game.get("scores"):
                skipped += 1
                continue

            try:
                if market_key == "h2h":
                    result = self._grade_h2h(outcome_name, game)
                elif market_key == "spreads":
                    point = await self._repo.get_reference_line(
                        event_id, market_key, outcome_name, signal_at
                    )
                    if point is None:
                        log.warning(
                            "grader_no_reference_line",
                            event_id=event_id,
                            market="spreads",
                        )
                        skipped += 1
                        continue
                    result = self._grade_spread(outcome_name, game, point)
                elif market_key == "totals":
                    point = await self._repo.get_reference_line(
                        event_id, market_key, outcome_name, signal_at
                    )
                    if point is None:
                        log.warning(
                            "grader_no_reference_line",
                            event_id=event_id,
                            market="totals",
                        )
                        skipped += 1
                        continue
                    result = self._grade_total(outcome_name, game, point)
                else:
                    log.warning("grader_unknown_market", market=market_key)
                    skipped += 1
                    continue

                await self._repo.resolve_signal(
                    event_id, signal_type, market_key, outcome_name, signal_at, result
                )
                resolved += 1
                log.info(
                    "signal_resolved",
                    event_id=event_id,
                    signal_type=signal_type,
                    market=market_key,
                    outcome=outcome_name,
                    result=result,
                )
            except Exception:
                log.exception(
                    "grader_error",
                    event_id=event_id,
                    signal_type=signal_type,
                )
                errors += 1

        log.info(
            "grader_complete",
            resolved=resolved,
            skipped=skipped,
            errors=errors,
        )
        return {"resolved": resolved, "skipped": skipped, "errors": errors}

    @staticmethod
    def _grade_h2h(outcome_name: str, game: dict) -> str:
        """Grade a moneyline bet: did the named team win?"""
        scores = {s["name"]: int(s["score"]) for s in game["scores"]}
        home = game["home_team"]
        away = game["away_team"]

        home_score = scores.get(home, 0)
        away_score = scores.get(away, 0)

        if home_score == away_score:
            return "push"

        winner = home if home_score > away_score else away
        return "won" if outcome_name == winner else "lost"

    @staticmethod
    def _grade_spread(outcome_name: str, game: dict, point: float) -> str:
        """Grade a spread bet.

        `point` is the spread from the signal (e.g., -3.5 for a favorite).
        The bet wins if team_score - opponent_score + point > 0.
        """
        scores = {s["name"]: int(s["score"]) for s in game["scores"]}
        home = game["home_team"]
        away = game["away_team"]

        home_score = scores.get(home, 0)
        away_score = scores.get(away, 0)

        if outcome_name == home:
            margin = home_score - away_score
        elif outcome_name == away:
            margin = away_score - home_score
        else:
            return "push"  # shouldn't happen

        adjusted = margin + point
        if adjusted > 0:
            return "won"
        elif adjusted < 0:
            return "lost"
        return "push"

    @staticmethod
    def _grade_total(outcome_name: str, game: dict, point: float) -> str:
        """Grade an over/under totals bet.

        `point` is the total line (e.g., 220.5).
        Over wins if combined > point, Under wins if combined < point.
        """
        scores = {s["name"]: int(s["score"]) for s in game["scores"]}
        combined = sum(scores.values())

        if combined > point:
            return "won" if outcome_name == "Over" else "lost"
        elif combined < point:
            return "won" if outcome_name == "Under" else "lost"
        return "push"
