"""Tests for the auto-grading system."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, patch

import pytest

from sharp_seeker.analysis.grader import ScoreGrader


# ── Static grading method tests ──────────────────────────────────

GAME_LAKERS_WIN = {
    "id": "game1",
    "home_team": "Los Angeles Lakers",
    "away_team": "Boston Celtics",
    "completed": True,
    "scores": [
        {"name": "Los Angeles Lakers", "score": "110"},
        {"name": "Boston Celtics", "score": "105"},
    ],
}

GAME_CELTICS_WIN = {
    "id": "game2",
    "home_team": "Los Angeles Lakers",
    "away_team": "Boston Celtics",
    "completed": True,
    "scores": [
        {"name": "Los Angeles Lakers", "score": "100"},
        {"name": "Boston Celtics", "score": "108"},
    ],
}

GAME_TIE = {
    "id": "game3",
    "home_team": "Los Angeles Lakers",
    "away_team": "Boston Celtics",
    "completed": True,
    "scores": [
        {"name": "Los Angeles Lakers", "score": "105"},
        {"name": "Boston Celtics", "score": "105"},
    ],
}


class TestGradeH2H:
    def test_team_won(self):
        result = ScoreGrader._grade_h2h("Los Angeles Lakers", GAME_LAKERS_WIN)
        assert result == "won"

    def test_team_lost(self):
        result = ScoreGrader._grade_h2h("Boston Celtics", GAME_LAKERS_WIN)
        assert result == "lost"

    def test_away_team_won(self):
        result = ScoreGrader._grade_h2h("Boston Celtics", GAME_CELTICS_WIN)
        assert result == "won"

    def test_tie_push(self):
        result = ScoreGrader._grade_h2h("Los Angeles Lakers", GAME_TIE)
        assert result == "push"


class TestGradeSpread:
    def test_covered_spread(self):
        # Lakers won by 5, spread -3.5 → margin(5) + (-3.5) = 1.5 > 0 → won
        result = ScoreGrader._grade_spread(
            "Los Angeles Lakers", GAME_LAKERS_WIN, -3.5
        )
        assert result == "won"

    def test_did_not_cover(self):
        # Lakers won by 5, spread -6.5 → margin(5) + (-6.5) = -1.5 < 0 → lost
        result = ScoreGrader._grade_spread(
            "Los Angeles Lakers", GAME_LAKERS_WIN, -6.5
        )
        assert result == "lost"

    def test_push_exact_margin(self):
        # Lakers won by 5, spread -5.0 → margin(5) + (-5.0) = 0 → push
        result = ScoreGrader._grade_spread(
            "Los Angeles Lakers", GAME_LAKERS_WIN, -5.0
        )
        assert result == "push"

    def test_underdog_covered(self):
        # Celtics lost by 5, spread +6.5 → margin(-5) + 6.5 = 1.5 > 0 → won
        result = ScoreGrader._grade_spread(
            "Boston Celtics", GAME_LAKERS_WIN, 6.5
        )
        assert result == "won"

    def test_underdog_did_not_cover(self):
        # Celtics lost by 5, spread +3.5 → margin(-5) + 3.5 = -1.5 < 0 → lost
        result = ScoreGrader._grade_spread(
            "Boston Celtics", GAME_LAKERS_WIN, 3.5
        )
        assert result == "lost"


class TestGradeTotal:
    def test_over_hit(self):
        # Combined = 215, line = 210.5 → 215 > 210.5 → Over wins
        result = ScoreGrader._grade_total("Over", GAME_LAKERS_WIN, 210.5)
        assert result == "won"

    def test_over_missed(self):
        # Combined = 215, line = 220.5 → 215 < 220.5 → Over loses
        result = ScoreGrader._grade_total("Over", GAME_LAKERS_WIN, 220.5)
        assert result == "lost"

    def test_under_hit(self):
        # Combined = 215, line = 220.5 → 215 < 220.5 → Under wins
        result = ScoreGrader._grade_total("Under", GAME_LAKERS_WIN, 220.5)
        assert result == "won"

    def test_under_missed(self):
        # Combined = 215, line = 210.5 → 215 > 210.5 → Under loses
        result = ScoreGrader._grade_total("Under", GAME_LAKERS_WIN, 210.5)
        assert result == "lost"

    def test_push_exact_total(self):
        # Combined = 215, line = 215.0 → push
        result = ScoreGrader._grade_total("Over", GAME_LAKERS_WIN, 215.0)
        assert result == "push"

    def test_push_exact_total_under(self):
        result = ScoreGrader._grade_total("Under", GAME_LAKERS_WIN, 215.0)
        assert result == "push"


# ── Integration-level tests for resolve_all ──────────────────────

class TestResolveAll:
    @pytest.fixture
    def grader(self, settings, repo):
        odds_client = AsyncMock()
        return ScoreGrader(settings, odds_client, repo)

    @pytest.mark.asyncio
    async def test_no_unresolved_signals(self, grader):
        counts = await grader.resolve_all()
        assert counts == {"resolved": 0, "skipped": 0, "errors": 0}

    @pytest.mark.asyncio
    async def test_skip_when_no_scores(self, grader, repo):
        """Signals with no matching score data should be skipped."""
        await repo.record_signal_result(
            event_id="missing_game",
            signal_type="steam_move",
            market_key="h2h",
            outcome_name="Los Angeles Lakers",
            signal_direction="up",
            signal_strength=0.8,
            signal_at="2025-01-15T20:00:00",
        )
        # odds_client returns no scores
        grader._odds_client.fetch_scores = AsyncMock(return_value=[])
        counts = await grader.resolve_all()
        assert counts["skipped"] == 1
        assert counts["resolved"] == 0

    @pytest.mark.asyncio
    async def test_resolve_h2h_signal(self, grader, repo):
        """H2H signal should be resolved when score data is available."""
        # Insert a snapshot so we can find the sport_key
        await repo.insert_snapshots([{
            "event_id": "game1",
            "sport_key": "basketball_nba",
            "home_team": "Los Angeles Lakers",
            "away_team": "Boston Celtics",
            "commence_time": "2025-01-15T00:00:00Z",
            "bookmaker_key": "pinnacle",
            "market_key": "h2h",
            "outcome_name": "Los Angeles Lakers",
            "price": -150,
            "point": None,
            "deep_link": None,
            "fetched_at": "2025-01-15T19:00:00",
        }])

        await repo.record_signal_result(
            event_id="game1",
            signal_type="steam_move",
            market_key="h2h",
            outcome_name="Los Angeles Lakers",
            signal_direction="up",
            signal_strength=0.8,
            signal_at="2025-01-15T20:00:00",
            sport_key="basketball_nba",
            is_live=True,
        )

        grader._odds_client.fetch_scores = AsyncMock(return_value=[GAME_LAKERS_WIN])

        counts = await grader.resolve_all()
        assert counts["resolved"] == 1
        assert counts["skipped"] == 0

        # Verify the signal is now resolved
        unresolved = await repo.get_unresolved_signals()
        assert len(unresolved) == 0

    @pytest.mark.asyncio
    async def test_resolve_spread_signal(self, grader, repo):
        """Spread signal should use reference line from snapshots."""
        await repo.insert_snapshots([{
            "event_id": "game1",
            "sport_key": "basketball_nba",
            "home_team": "Los Angeles Lakers",
            "away_team": "Boston Celtics",
            "commence_time": "2025-01-15T00:00:00Z",
            "bookmaker_key": "pinnacle",
            "market_key": "spreads",
            "outcome_name": "Los Angeles Lakers",
            "price": -110,
            "point": -3.5,
            "deep_link": None,
            "fetched_at": "2025-01-15T19:00:00",
        }])

        await repo.record_signal_result(
            event_id="game1",
            signal_type="steam_move",
            market_key="spreads",
            outcome_name="Los Angeles Lakers",
            signal_direction="up",
            signal_strength=0.8,
            signal_at="2025-01-15T20:00:00",
            sport_key="basketball_nba",
            is_live=True,
        )

        grader._odds_client.fetch_scores = AsyncMock(return_value=[GAME_LAKERS_WIN])

        counts = await grader.resolve_all()
        assert counts["resolved"] == 1

        # Lakers won by 5, spread -3.5 → covered → won
        unresolved = await repo.get_unresolved_signals()
        assert len(unresolved) == 0

    @pytest.mark.asyncio
    async def test_skip_incomplete_game(self, grader, repo):
        """Games not yet completed (no scores) should be skipped."""
        await repo.insert_snapshots([{
            "event_id": "game_live",
            "sport_key": "basketball_nba",
            "home_team": "Los Angeles Lakers",
            "away_team": "Boston Celtics",
            "commence_time": "2025-01-15T00:00:00Z",
            "bookmaker_key": "pinnacle",
            "market_key": "h2h",
            "outcome_name": "Los Angeles Lakers",
            "price": -150,
            "point": None,
            "deep_link": None,
            "fetched_at": "2025-01-15T19:00:00",
        }])

        await repo.record_signal_result(
            event_id="game_live",
            signal_type="steam_move",
            market_key="h2h",
            outcome_name="Los Angeles Lakers",
            signal_direction="up",
            signal_strength=0.8,
            signal_at="2025-01-15T20:00:00",
        )

        # fetch_scores only returns completed games, so this game won't appear
        grader._odds_client.fetch_scores = AsyncMock(return_value=[])

        counts = await grader.resolve_all()
        assert counts["skipped"] == 1
        assert counts["resolved"] == 0

        # Signal should still be unresolved
        unresolved = await repo.get_unresolved_signals()
        assert len(unresolved) == 1

    @pytest.mark.asyncio
    async def test_resolve_spread_uses_details_point(self, grader, repo):
        """Grader should use the details_json point (US book) over Pinnacle's.

        Reproduces the NHL bug: Pinnacle has Buffalo Sabres at -1.5, but the
        signal recommends betting at FanDuel +1.5.  Buffalo wins outright,
        so the +1.5 bet should grade as "won" (not "lost" from Pinnacle's -1.5).
        """
        game_nhl = {
            "id": "nhl_game1",
            "home_team": "New Jersey Devils",
            "away_team": "Buffalo Sabres",
            "completed": True,
            "scores": [
                {"name": "New Jersey Devils", "score": "3"},
                {"name": "Buffalo Sabres", "score": "4"},
            ],
        }

        # Insert Pinnacle snapshot with -1.5 (wrong line for the recommended bet)
        await repo.insert_snapshots([{
            "event_id": "nhl_game1",
            "sport_key": "icehockey_nhl",
            "home_team": "New Jersey Devils",
            "away_team": "Buffalo Sabres",
            "commence_time": "2025-02-25T00:00:00Z",
            "bookmaker_key": "pinnacle",
            "market_key": "spreads",
            "outcome_name": "Buffalo Sabres",
            "price": 229,
            "point": -1.5,
            "deep_link": None,
            "fetched_at": "2025-02-25T13:00:00",
        }])

        # Signal with details_json containing the US book's +1.5 line
        details = {
            "us_book": "fanduel",
            "us_value": 1.5,
            "pinnacle_value": -1.5,
            "delta": 3.0,
            "value_books": [
                {
                    "bookmaker": "fanduel",
                    "price": -280.0,
                    "point": 1.5,
                    "deep_link": "https://example.com",
                }
            ],
        }
        await repo.record_signal_result(
            event_id="nhl_game1",
            signal_type="pinnacle_divergence",
            market_key="spreads",
            outcome_name="Buffalo Sabres",
            signal_direction="up",
            signal_strength=1.0,
            signal_at="2025-02-25T13:11:49",
            details_json=json.dumps(details),
            sport_key="icehockey_nhl",
            is_live=True,
        )

        grader._odds_client.fetch_scores = AsyncMock(return_value=[game_nhl])

        counts = await grader.resolve_all()
        assert counts["resolved"] == 1

        # Verify graded as "won" — uses FanDuel +1.5, not Pinnacle -1.5
        unresolved = await repo.get_unresolved_signals()
        assert len(unresolved) == 0

        # Double-check: directly verify the result stored in DB
        cursor = await repo._db.execute(
            "SELECT result FROM signal_results WHERE event_id = ?",
            ("nhl_game1",),
        )
        row = await cursor.fetchone()
        assert row["result"] == "won"
