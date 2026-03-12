"""Tests for daily results card image generator."""

from __future__ import annotations

import json
import os
from unittest.mock import AsyncMock, patch

import pytest

from sharp_seeker.analysis.card_generator import CardGenerator, CardStats, compute_risk


# ── Unit calculation tests ───────────────────────────────────────────────────


def test_compute_risk_minus_odds():
    """At -110: risk 1.10u to win 1u."""
    assert compute_risk(-110) == pytest.approx(1.10)


def test_compute_risk_plus_odds():
    """At +150: risk 0.667u to win 1u."""
    assert compute_risk(150) == pytest.approx(100 / 150)


def test_compute_risk_heavy_favorite():
    """At -200: risk 2.0u to win 1u."""
    assert compute_risk(-200) == pytest.approx(2.0)


def test_compute_risk_even_money():
    """At +100: risk 1.0u to win 1u."""
    assert compute_risk(100) == pytest.approx(1.0)


# ── Unit profit tests with _tally ────────────────────────────────────────────


def _make_row(result: str, price: float) -> dict:
    """Create a dict mimicking an aiosqlite.Row for tally tests."""
    details = json.dumps({"value_books": [{"bookmaker": "fanduel", "price": price}]})
    return {
        "event_id": "e1",
        "market_key": "h2h",
        "outcome_name": "Lakers",
        "sent_at": "2026-01-15T12:00:00+00:00",
        "details_json": details,
        "result": result,
        "signal_strength": 0.8,
    }


def test_compute_units_minus_odds_win():
    """-110 win = +1.0u profit."""
    gen = CardGenerator.__new__(CardGenerator)
    w, l, units = gen._tally([_make_row("won", -110)])
    assert w == 1
    assert l == 0
    assert units == pytest.approx(1.0)


def test_compute_units_minus_odds_loss():
    """-110 loss = -1.1u."""
    gen = CardGenerator.__new__(CardGenerator)
    w, l, units = gen._tally([_make_row("lost", -110)])
    assert w == 0
    assert l == 1
    assert units == pytest.approx(-1.1)


def test_compute_units_plus_odds_win():
    """+150 win = +1.0u."""
    gen = CardGenerator.__new__(CardGenerator)
    w, l, units = gen._tally([_make_row("won", 150)])
    assert w == 1
    assert l == 0
    assert units == pytest.approx(1.0)


def test_compute_units_plus_odds_loss():
    """+150 loss = -0.667u."""
    gen = CardGenerator.__new__(CardGenerator)
    w, l, units = gen._tally([_make_row("lost", 150)])
    assert w == 0
    assert l == 1
    assert units == pytest.approx(-100 / 150)


def test_compute_units_push():
    """Push = 0u."""
    gen = CardGenerator.__new__(CardGenerator)
    w, l, units = gen._tally([_make_row("push", -110)])
    assert w == 0
    assert l == 0
    assert units == pytest.approx(0.0)


def test_compute_units_mixed():
    """Mixed results: 2 wins at -110, 1 loss at +150."""
    gen = CardGenerator.__new__(CardGenerator)
    rows = [
        _make_row("won", -110),
        _make_row("won", -110),
        _make_row("lost", 150),
    ]
    w, l, units = gen._tally(rows)
    assert w == 2
    assert l == 1
    # +1.0 + 1.0 - 0.667 = 1.333
    assert units == pytest.approx(2.0 - 100 / 150)


# ── Streak tests ─────────────────────────────────────────────────────────────


def _make_streak_row(result: str, sent_at: str) -> dict:
    return {
        "result": result,
        "sent_at": sent_at,
        "details_json": None,
    }


def test_compute_streak_all_wins():
    gen = CardGenerator.__new__(CardGenerator)
    rows = [
        _make_streak_row("won", "2026-01-01T12:00:00"),
        _make_streak_row("won", "2026-01-02T12:00:00"),
        _make_streak_row("won", "2026-01-03T12:00:00"),
    ]
    count, stype = gen._compute_streak(rows)
    assert count == 3
    assert stype == "W"


def test_compute_streak_broken():
    gen = CardGenerator.__new__(CardGenerator)
    rows = [
        _make_streak_row("lost", "2026-01-01T12:00:00"),
        _make_streak_row("won", "2026-01-02T12:00:00"),
        _make_streak_row("won", "2026-01-03T12:00:00"),
    ]
    count, stype = gen._compute_streak(rows)
    assert count == 2
    assert stype == "W"


def test_compute_streak_pushes_skipped():
    gen = CardGenerator.__new__(CardGenerator)
    rows = [
        _make_streak_row("won", "2026-01-01T12:00:00"),
        _make_streak_row("push", "2026-01-02T12:00:00"),
        _make_streak_row("won", "2026-01-03T12:00:00"),
    ]
    count, stype = gen._compute_streak(rows)
    assert count == 2
    assert stype == "W"


def test_compute_streak_loss_streak():
    gen = CardGenerator.__new__(CardGenerator)
    rows = [
        _make_streak_row("lost", "2026-01-01T12:00:00"),
        _make_streak_row("lost", "2026-01-02T12:00:00"),
    ]
    count, stype = gen._compute_streak(rows)
    assert count == 2
    assert stype == "L"


def test_compute_streak_empty():
    gen = CardGenerator.__new__(CardGenerator)
    count, stype = gen._compute_streak([])
    assert count == 0
    assert stype == "W"


# ── Card rendering tests ────────────────────────────────────────────────────


def test_render_card_square():
    """Render a 1080x1080 card without errors."""
    gen = CardGenerator.__new__(CardGenerator)
    stats = CardStats(
        yesterday_w=3, yesterday_l=1, yesterday_units=1.8,
        month_w=24, month_l=14, month_units=8.2,
        ytd_w=50, ytd_l=30, ytd_units=12.4,
        streak_count=10, streak_type="W",
        date_str="March 11, 2026", month_name="March",
    )
    img = gen._render_card((1080, 1080), stats)
    assert img.size == (1080, 1080)


def test_render_card_story():
    """Render a 1080x1920 story card without errors."""
    gen = CardGenerator.__new__(CardGenerator)
    stats = CardStats(
        yesterday_w=1, yesterday_l=2, yesterday_units=-1.3,
        month_w=5, month_l=8, month_units=-3.1,
        ytd_w=10, ytd_l=15, ytd_units=-5.5,
        streak_count=2, streak_type="L",
        date_str="March 11, 2026", month_name="March",
    )
    img = gen._render_card((1080, 1920), stats)
    assert img.size == (1080, 1920)


# ── Integration tests ────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_no_results_returns_empty(settings, repo):
    """When there are no free plays, generate_daily_cards returns empty list."""
    gen = CardGenerator(settings, repo)
    paths = await gen.generate_daily_cards()
    assert paths == []


@pytest.mark.asyncio
async def test_generates_two_images(settings, repo, tmp_path):
    """With mock stats, two PNG files should be created."""
    settings.card_output_dir = str(tmp_path)
    gen = CardGenerator(settings, repo)

    mock_stats = CardStats(
        yesterday_w=3, yesterday_l=1, yesterday_units=1.8,
        month_w=24, month_l=14, month_units=8.2,
        ytd_w=50, ytd_l=30, ytd_units=12.4,
        streak_count=5, streak_type="W",
        date_str="March 11, 2026", month_name="March",
    )

    with patch.object(gen, "_get_stats", new_callable=AsyncMock, return_value=mock_stats):
        paths = await gen.generate_daily_cards()

    assert len(paths) == 2
    for p in paths:
        assert os.path.exists(p)
        assert p.endswith(".png")

    # Verify one is square and one is story
    names = [os.path.basename(p) for p in paths]
    assert any("1080x1080" in n for n in names)
    assert any("1080x1920" in n for n in names)
