"""Tests for report generation (stats queries only, no Discord calls)."""

from __future__ import annotations

import csv
import io
import json
from unittest.mock import MagicMock, patch

import pytest

from sharp_seeker.analysis.performance import PerformanceTracker
from sharp_seeker.analysis.reports import ReportGenerator
from sharp_seeker.engine.base import Signal, SignalType


def _signal(
    event_id: str,
    signal_type: SignalType,
    market_key: str = "spreads",
) -> Signal:
    return Signal(
        signal_type=signal_type,
        event_id=event_id,
        sport_key="basketball_nba",
        home_team="Lakers",
        away_team="Celtics",
        market_key=market_key,
        outcome_name="Lakers",
        strength=0.7,
        description="test",
        details={"direction": "down"},
    )


@pytest.mark.asyncio
async def test_stats_multiple_types(settings, repo):
    """Stats should be grouped by signal type."""
    tracker = PerformanceTracker(repo)

    # Record signals of different types
    await tracker.record_signals(
        [_signal("e1", SignalType.STEAM_MOVE)], "2025-01-15T12:00:00+00:00"
    )
    await tracker.record_signals(
        [_signal("e2", SignalType.RAPID_CHANGE)], "2025-01-15T12:01:00+00:00"
    )
    await tracker.record_signals(
        [_signal("e3", SignalType.STEAM_MOVE)], "2025-01-15T12:02:00+00:00"
    )

    # Resolve all
    for row in await repo.get_unresolved_signals():
        r = dict(row)
        await repo.resolve_signal(
            r["event_id"], r["signal_type"], r["market_key"],
            r["outcome_name"], r["signal_at"], "won",
        )

    stats = await tracker.get_stats()
    assert stats["steam_move"]["won"] == 2
    assert stats["rapid_change"]["won"] == 1


@pytest.mark.asyncio
async def test_signal_count_since(settings, repo):
    """Signal count since a timestamp should work correctly."""
    tracker = PerformanceTracker(repo)

    await tracker.record_signals(
        [_signal("e1", SignalType.STEAM_MOVE)], "2025-01-15T12:00:00+00:00"
    )
    await tracker.record_signals(
        [_signal("e2", SignalType.STEAM_MOVE)], "2025-01-16T12:00:00+00:00"
    )

    count = await repo.get_signal_count_since("2025-01-16T00:00:00+00:00")
    assert count == 1

    count_all = await repo.get_signal_count_since("2025-01-01T00:00:00+00:00")
    assert count_all == 2


@pytest.mark.asyncio
async def test_performance_stats_by_market(settings, repo):
    """Stats should be grouped by market_key."""
    tracker = PerformanceTracker(repo)

    # Record signals across different markets
    await tracker.record_signals(
        [_signal("e1", SignalType.STEAM_MOVE, market_key="h2h")],
        "2025-01-15T12:00:00+00:00",
    )
    await tracker.record_signals(
        [_signal("e2", SignalType.STEAM_MOVE, market_key="h2h")],
        "2025-01-15T12:01:00+00:00",
    )
    await tracker.record_signals(
        [_signal("e3", SignalType.STEAM_MOVE, market_key="spreads")],
        "2025-01-15T12:02:00+00:00",
    )
    await tracker.record_signals(
        [_signal("e4", SignalType.RAPID_CHANGE, market_key="totals")],
        "2025-01-15T12:03:00+00:00",
    )

    # Resolve with mixed results
    unresolved = await repo.get_unresolved_signals()
    results_map = {"e1": "won", "e2": "lost", "e3": "won", "e4": "push"}
    for row in unresolved:
        r = dict(row)
        await repo.resolve_signal(
            r["event_id"], r["signal_type"], r["market_key"],
            r["outcome_name"], r["signal_at"], results_map[r["event_id"]],
        )

    # All markets
    stats = await repo.get_performance_stats_by_market()
    assert stats["h2h"]["won"] == 1
    assert stats["h2h"]["lost"] == 1
    assert stats["h2h"]["total"] == 2
    assert stats["spreads"]["won"] == 1
    assert stats["spreads"]["total"] == 1
    assert stats["totals"]["push"] == 1

    # Filtered by signal_type
    stats_steam = await repo.get_performance_stats_by_market(
        signal_type="steam_move"
    )
    assert "h2h" in stats_steam
    assert "spreads" in stats_steam
    assert "totals" not in stats_steam

    # Filtered by since
    stats_recent = await repo.get_performance_stats_by_market(
        since="2025-01-15T12:02:00+00:00"
    )
    assert "h2h" not in stats_recent
    assert "spreads" in stats_recent


# ── CSV generation + attachment tests ──────────────────────────


async def _seed_resolved_signal(repo, event_id="e1", signal_type="steam_move",
                                 sport_key="basketball_nba", result="won",
                                 details_json=None):
    """Insert a resolved signal and an odds snapshot for team lookup."""
    await repo.record_signal_result(
        event_id=event_id,
        signal_type=signal_type,
        market_key="spreads",
        outcome_name="Lakers",
        signal_direction="up",
        signal_strength=0.8,
        signal_at="2025-01-15T12:00:00+00:00",
        details_json=details_json,
        sport_key=sport_key,
    )
    await repo.resolve_signal(
        event_id, signal_type, "spreads", "Lakers",
        "2025-01-15T12:00:00+00:00", result,
    )
    # Insert a snapshot so get_event_teams returns data
    await repo._db.execute(
        """INSERT INTO odds_snapshots
           (event_id, sport_key, home_team, away_team, commence_time,
            bookmaker_key, market_key, outcome_name, price, fetched_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (event_id, sport_key, "Lakers", "Celtics", "2025-01-15T18:00:00+00:00",
         "fanduel", "spreads", "Lakers", -110, "2025-01-15T12:00:00+00:00"),
    )
    await repo._db.commit()


@pytest.mark.asyncio
async def test_build_results_csv_content(settings, repo):
    """CSV should have correct headers and data rows."""
    details = json.dumps({"value_books": [{"bookmaker": "fanduel", "point": -3.5, "price": -110}]})
    await _seed_resolved_signal(repo, details_json=details)

    gen = ReportGenerator(settings, repo)
    csv_bytes = await gen._build_results_csv("2025-01-01T00:00:00+00:00")

    assert csv_bytes is not None
    reader = csv.reader(io.StringIO(csv_bytes.decode("utf-8")))
    rows = list(reader)

    # Header row
    assert rows[0] == [
        "result", "sport", "matchup", "signal_type", "market",
        "outcome", "book", "point", "price", "strength", "signal_at",
    ]
    # Data row
    assert rows[1][0] == "WON"
    assert rows[1][1] == "NBA"
    assert rows[1][2] == "Celtics vs Lakers"
    assert rows[1][3] == "steam_move"
    assert rows[1][6] == "fanduel"
    assert rows[1][7] == "-3.5"
    assert rows[1][8] == "-110"


@pytest.mark.asyncio
async def test_build_results_csv_empty(settings, repo):
    """Should return None when there are no resolved signals."""
    gen = ReportGenerator(settings, repo)
    result = await gen._build_results_csv("2025-01-01T00:00:00+00:00")
    assert result is None


@pytest.mark.asyncio
async def test_send_webhook_with_file():
    """add_file should be called when file_content is provided."""
    with patch("sharp_seeker.analysis.reports.DiscordWebhook") as MockWebhook:
        mock_instance = MagicMock()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_instance.execute.return_value = mock_resp
        MockWebhook.return_value = mock_instance

        ReportGenerator._send_webhook(
            "https://example.com/webhook",
            MagicMock(),
            "test",
            file_content=b"csv,data",
            filename="test.csv",
        )

        mock_instance.add_file.assert_called_once_with(file=b"csv,data", filename="test.csv")


@pytest.mark.asyncio
async def test_send_webhook_without_file():
    """add_file should NOT be called when no file_content."""
    with patch("sharp_seeker.analysis.reports.DiscordWebhook") as MockWebhook:
        mock_instance = MagicMock()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_instance.execute.return_value = mock_resp
        MockWebhook.return_value = mock_instance

        ReportGenerator._send_webhook(
            "https://example.com/webhook",
            MagicMock(),
            "test",
        )

        mock_instance.add_file.assert_not_called()


@pytest.mark.asyncio
async def test_per_type_report_attaches_csv(settings, repo):
    """Per-type report should call _send_webhook with CSV file content."""
    await _seed_resolved_signal(repo)

    gen = ReportGenerator(settings, repo)

    calls = []

    def capture_send(*args, **kwargs):
        calls.append((args, kwargs))

    with patch.object(ReportGenerator, "_send_webhook", staticmethod(capture_send)):
        await gen._send_per_type_reports("Daily", "2025-01-01T00:00:00+00:00")

    assert len(calls) >= 1
    # First call should have file_content and filename
    _, kw = calls[0]
    assert kw.get("file_content") is not None
    assert kw.get("filename", "").endswith(".csv")
