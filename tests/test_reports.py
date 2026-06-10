"""Tests for report generation (stats queries only, no Discord calls)."""

from __future__ import annotations

import csv
import io
import json
from datetime import datetime, timedelta, timezone
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


@pytest.mark.asyncio
async def test_window_by_resolved_at_captures_early_signals(settings, repo):
    """A signal that fired long before the window but was graded inside it must
    appear when windowing by resolved_at — the daily-recap bug fix.

    Recaps run shortly after grading; a play that fired 3 days ahead of the game
    has signal_at far outside the 24h window but resolved_at inside it.
    """
    tracker = PerformanceTracker(repo)

    fired_at = (datetime.now(timezone.utc) - timedelta(days=3)).isoformat()
    await tracker.record_signals([_signal("e_early", SignalType.STEAM_MOVE)], fired_at)

    # Grade it now (resolve_signal stamps resolved_at = now).
    row = dict((await repo.get_unresolved_signals())[0])
    await repo.resolve_signal(
        row["event_id"], row["signal_type"], row["market_key"],
        row["outcome_name"], row["signal_at"], "won",
    )

    since = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()

    # Old behavior: windowing by fire time drops the early signal.
    by_signal = await repo.get_performance_stats(since, window_by="signal_at")
    assert "steam_move" not in by_signal

    # Fixed behavior: windowing by grading time captures it.
    by_resolved = await repo.get_performance_stats(since, window_by="resolved_at")
    assert by_resolved["steam_move"]["won"] == 1

    # And the row-returning + by-market queries agree.
    resolved_rows = await repo.get_resolved_signals_since(since, window_by="resolved_at")
    assert len(resolved_rows) == 1
    by_market = await repo.get_performance_stats_by_market(
        since, window_by="resolved_at"
    )
    assert by_market["spreads"]["won"] == 1


@pytest.mark.asyncio
async def test_sent_only_counts_raw_pd_sends_not_suppressed(repo):
    """sent_only must mean "published to Discord" (a sent_alerts row exists),
    NOT qualifier_count>0. A raw-PD MLB send (qualifier_count=0 but recorded in
    sent_alerts) belongs in the combined recap; a truly-suppressed 0-qualifier
    signal (never recorded) must not. This is the missing-MLB-signals fix.
    """
    # Raw-PD MLB send: qualifier_count=0 but it WAS sent to the MLB channel.
    await _seed_resolved_signal(
        repo, event_id="mlb_raw", signal_type="pinnacle_divergence",
        sport_key="baseball_mlb", result="won",
        details_json='{"qualifier_count": 0, "value_books": [{"price": -110}]}',
        sent=True,
    )
    # Truly suppressed: 0 qualifiers, no raw channel — never reached Discord.
    await _seed_resolved_signal(
        repo, event_id="nba_suppressed", signal_type="pinnacle_divergence",
        sport_key="basketball_nba", result="won",
        details_json='{"qualifier_count": 0, "value_books": [{"price": -110}]}',
        sent=False,
    )

    since = "2025-01-01T00:00:00+00:00"
    stats = await repo.get_performance_stats(since, sent_only=True)

    # The raw-PD send counts; the suppressed signal does not -> exactly 1 win.
    assert stats.get("pinnacle_divergence", {}).get("won") == 1
    rows = await repo.get_resolved_signals_since(since, sent_only=True)
    assert [dict(r)["event_id"] for r in rows] == ["mlb_raw"]


# ── CSV generation + attachment tests ──────────────────────────


async def _seed_resolved_signal(repo, event_id="e1", signal_type="steam_move",
                                 sport_key="basketball_nba", result="won",
                                 details_json=None, sent=True):
    """Insert a resolved signal and an odds snapshot for team lookup.

    When sent=True (default) also records a sent_alerts row, since recaps now
    define "sent" as "a sent_alerts row exists" rather than qualifier_count>0.
    Pass sent=False to model a truly-suppressed signal that never reached
    Discord.
    """
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
    if sent:
        await repo.record_alert(
            event_id=event_id, alert_type=signal_type,
            market_key="spreads", outcome_name="Lakers",
            details_json=details_json,
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
        "outcome", "book", "point", "price", "strength", "units", "signal_at",
    ]
    # Data row
    assert rows[1][0] == "WON"
    assert rows[1][1] == "NBA"
    assert rows[1][2] == "Celtics vs Lakers"
    assert rows[1][3] == "steam_move"
    assert rows[1][6] == "fanduel"
    assert rows[1][7] == "-3.5"
    assert rows[1][8] == "-110"
    # Units: a won bet at -110 returns +1.00u (no Elite multiplier; qualifier_count=0)
    assert rows[1][10] == "+1.00"


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


# ── Raw-PD recap routing tests ─────────────────────────────────


def _settings_with_mlb_raw_pd():
    from sharp_seeker.config import Settings
    return Settings(
        odds_api_key="test_key",
        discord_webhook_url="https://discord.com/api/webhooks/default/x",
        db_path=":memory:",
        discord_webhook_pinnacle_divergence_mlb="https://discord.com/api/webhooks/mlb_raw_pd/x",
    )


@pytest.mark.asyncio
async def test_raw_pd_mlb_channel_receives_override_report(repo):
    """An MLB PD signal with qualifier_count=0 (raw-PD bypass) should appear
    in the MLB raw-PD channel's daily recap."""
    await _seed_resolved_signal(
        repo,
        event_id="mlb1",
        signal_type="pinnacle_divergence",
        sport_key="baseball_mlb",
        result="won",
        details_json='{"qualifier_count": 0, "value_books": [{"bookmaker": "fanduel", "point": -1.5, "price": -110}]}',
    )

    gen = ReportGenerator(_settings_with_mlb_raw_pd(), repo)

    calls = []

    def capture_send(url, *args, **kwargs):
        calls.append((url, args, kwargs))

    with patch.object(ReportGenerator, "_send_webhook", staticmethod(capture_send)):
        await gen._send_override_reports("Daily", "2025-01-01T00:00:00+00:00")

    sent_urls = [c[0] for c in calls]
    assert "https://discord.com/api/webhooks/mlb_raw_pd/x" in sent_urls, (
        f"MLB raw-PD webhook did not receive a recap; got urls: {sent_urls}"
    )


@pytest.mark.asyncio
async def test_raw_pd_mlb_excluded_from_main_pd_per_type_report(repo):
    """MLB PD signals routed via raw-PD bypass must not be counted in the
    main PD per-type recap (avoid double-counting if MLB ever gets a qualified
    PD signal in the future)."""
    # Seed an MLB raw-PD signal AND a qualified NBA PD signal
    await _seed_resolved_signal(
        repo,
        event_id="mlb1",
        signal_type="pinnacle_divergence",
        sport_key="baseball_mlb",
        result="won",
        details_json='{"qualifier_count": 0}',
    )
    await _seed_resolved_signal(
        repo,
        event_id="nba1",
        signal_type="pinnacle_divergence",
        sport_key="basketball_nba",
        result="won",
        details_json='{"qualifier_count": 1, "qualifier_tags": ["Best Combo"]}',
    )

    gen = ReportGenerator(_settings_with_mlb_raw_pd(), repo)

    captured_excludes = []

    real_get_perf = repo.get_performance_stats

    async def spy_get_perf(*args, **kwargs):
        if kwargs.get("sent_only") is True:
            captured_excludes.append(kwargs.get("exclude_sports"))
        return await real_get_perf(*args, **kwargs)

    with patch.object(repo, "get_performance_stats", spy_get_perf):
        with patch.object(ReportGenerator, "_send_webhook", staticmethod(lambda *a, **k: None)):
            await gen._send_per_type_reports("Daily", "2025-01-01T00:00:00+00:00")

    # At least one of the per-type stat queries should exclude baseball_mlb
    assert any(
        ex and "baseball_mlb" in ex for ex in captured_excludes
    ), f"baseball_mlb was not excluded from any per-type stats query; captured: {captured_excludes}"


# ── Units tests ────────────────────────────────────────────────


def test_compute_units_won_at_negative_odds():
    """A won bet at -110 returns +1.0u (risk 1.10 to win 1.00)."""
    from sharp_seeker.analysis.reports import _compute_units
    assert _compute_units(-110, "won") == 1.0
    assert _compute_units(-110, "won", multiplier=2) == 2.0


def test_compute_units_lost_at_negative_odds():
    """A lost bet at -150 returns -1.5u (risked 1.50 to win 1.00)."""
    from sharp_seeker.analysis.reports import _compute_units
    assert _compute_units(-150, "lost") == pytest.approx(-1.5)


def test_compute_units_won_at_positive_odds():
    """A won bet at +150 returns +1.0u (risked 0.667 to win 1.00)."""
    from sharp_seeker.analysis.reports import _compute_units
    assert _compute_units(150, "won") == 1.0


def test_compute_units_lost_at_positive_odds():
    """A lost bet at +200 returns -0.5u (risked 0.50 to win 1.00)."""
    from sharp_seeker.analysis.reports import _compute_units
    assert _compute_units(200, "lost") == pytest.approx(-0.5)


def test_compute_units_push():
    """A push returns 0u regardless of odds or multiplier."""
    from sharp_seeker.analysis.reports import _compute_units
    assert _compute_units(-110, "push") == 0.0
    assert _compute_units(-110, "push", multiplier=2) == 0.0


def test_compute_units_missing_price():
    """No price information returns 0u (can't risk-adjust without odds)."""
    from sharp_seeker.analysis.reports import _compute_units
    assert _compute_units(None, "won") == 0.0
    assert _compute_units(None, "lost") == 0.0


def test_arbitrage_units_use_profit_pct_not_side_a_swing():
    """An arb is recorded as a single side-A row, but a properly-sized arb is
    ~flat: it should count its guaranteed profit_pct, not a full +/-1u swing on
    whether side A won or lost."""
    from sharp_seeker.analysis.reports import _units_from_signal

    details = '{"profit_pct": 2.0, "side_a": {"outcome": "Over"}}'
    # Side A graded a loss — a naive 1u bet would be about -1.1u.
    arb_lost = {
        "signal_type": "arbitrage", "result": "lost", "details_json": details,
    }
    assert _units_from_signal(arb_lost) == 0.02
    # Side A graded a win — still just the guaranteed edge, not +1u.
    arb_won = {
        "signal_type": "arbitrage", "result": "won", "details_json": details,
    }
    assert _units_from_signal(arb_won) == 0.02
    # No profit_pct stored -> 0 impact, never a full swing.
    arb_missing = {
        "signal_type": "arbitrage", "result": "won", "details_json": "{}",
    }
    assert _units_from_signal(arb_missing) == 0.0


def test_units_from_signal_applies_elite_multiplier():
    """qualifier_count >= 2 triggers 2x sizing."""
    from sharp_seeker.analysis.reports import _units_from_signal
    elite_sig = {
        "result": "won",
        "details_json": '{"qualifier_count": 2, "value_books": [{"price": -110}]}',
    }
    assert _units_from_signal(elite_sig) == 2.0

    top_perf_sig = {
        "result": "won",
        "details_json": '{"qualifier_count": 1, "value_books": [{"price": -110}]}',
    }
    assert _units_from_signal(top_perf_sig) == 1.0


def test_fmt_units_signs_and_decimals():
    """Format produces signed value with one decimal and 'u' suffix."""
    from sharp_seeker.analysis.reports import _fmt_units
    assert _fmt_units(5.4) == "[+5.4u]"
    assert _fmt_units(-3.2) == "[-3.2u]"
    assert _fmt_units(0) == "[+0.0u]"


@pytest.mark.asyncio
async def test_per_type_report_embed_includes_units(settings, repo):
    """The Record field on a per-type report embed should include unit total."""
    # qualifier_count=1 (Top Perf) so the signal isn't filtered by sent_only=True
    details = json.dumps({
        "qualifier_count": 1,
        "value_books": [{"bookmaker": "fanduel", "point": -3.5, "price": -110}],
    })
    await _seed_resolved_signal(repo, details_json=details)

    gen = ReportGenerator(settings, repo)

    captured_embeds = []

    def capture_send(url, embed, *args, **kwargs):
        captured_embeds.append(embed)

    with patch.object(ReportGenerator, "_send_webhook", staticmethod(capture_send)):
        await gen._send_per_type_reports("Daily", "2025-01-01T00:00:00+00:00")

    assert captured_embeds, "no embeds were sent"
    record_fields = [
        f for embed in captured_embeds
        for f in embed.fields
        if f.get("name") == "Record"
    ]
    assert record_fields, "no Record field on any embed"
    # One won bet at -110 = +1.0u
    assert "[+1.0u]" in record_fields[0]["value"]
