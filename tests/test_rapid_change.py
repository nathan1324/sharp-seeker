"""Tests for the rapid change detector."""

from __future__ import annotations

import pytest

from sharp_seeker.engine.base import SignalType
from sharp_seeker.engine.rapid_change import RapidChangeDetector


def _snap(
    event_id: str,
    bookmaker: str,
    market: str,
    outcome: str,
    price: float,
    point: float | None,
    fetched_at: str,
) -> dict:
    return {
        "event_id": event_id,
        "sport_key": "americanfootball_nfl",
        "home_team": "Chiefs",
        "away_team": "Bills",
        "commence_time": "2025-01-20T00:00:00Z",
        "bookmaker_key": bookmaker,
        "market_key": market,
        "outcome_name": outcome,
        "price": price,
        "point": point,
        "deep_link": None,
        "fetched_at": fetched_at,
    }


@pytest.mark.asyncio
async def test_rapid_spread_change(settings, repo):
    """A 1-point spread move at one book should trigger."""
    event = "evt_rapid1"
    t1 = "2025-01-20T12:00:00+00:00"
    t2 = "2025-01-20T12:20:00+00:00"

    snapshots = [
        _snap(event, "pinnacle", "spreads", "Chiefs", -110, -3.0, t1),
        _snap(event, "pinnacle", "spreads", "Chiefs", -110, -4.0, t2),
    ]
    await repo.insert_snapshots(snapshots)

    detector = RapidChangeDetector(settings, repo)
    signals = await detector.detect(event, t2)

    assert len(signals) == 1
    sig = signals[0]
    assert sig.signal_type == SignalType.RAPID_CHANGE
    assert sig.details["delta"] == 1.0
    assert sig.details["bookmaker"] == "pinnacle"


@pytest.mark.asyncio
async def test_rapid_change_below_threshold(settings, repo):
    """A 0.25-point move should NOT trigger (threshold is 0.5)."""
    event = "evt_rapid2"
    t1 = "2025-01-20T12:00:00+00:00"
    t2 = "2025-01-20T12:20:00+00:00"

    snapshots = [
        _snap(event, "fanduel", "spreads", "Chiefs", -110, -3.0, t1),
        _snap(event, "fanduel", "spreads", "Chiefs", -110, -3.25, t2),
    ]
    await repo.insert_snapshots(snapshots)

    detector = RapidChangeDetector(settings, repo)
    signals = await detector.detect(event, t2)

    assert len(signals) == 0


@pytest.mark.asyncio
async def test_rapid_moneyline_change(settings, repo):
    """A 25-cent moneyline move should trigger (threshold is 20)."""
    event = "evt_rapid3"
    t1 = "2025-01-20T12:00:00+00:00"
    t2 = "2025-01-20T12:20:00+00:00"

    snapshots = [
        _snap(event, "pinnacle", "h2h", "Chiefs", -150, None, t1),
        _snap(event, "pinnacle", "h2h", "Chiefs", -175, None, t2),
    ]
    await repo.insert_snapshots(snapshots)

    detector = RapidChangeDetector(settings, repo)
    signals = await detector.detect(event, t2)

    assert len(signals) == 1
    assert signals[0].details["delta"] == 25.0


@pytest.mark.asyncio
async def test_rapid_ml_shortening_no_us_books(settings, repo):
    """Pinnacle shortening with no US books should NOT trigger (no value)."""
    event = "evt_rapid4"
    t1 = "2025-01-20T12:00:00+00:00"
    t2 = "2025-01-20T12:20:00+00:00"

    snapshots = [
        _snap(event, "pinnacle", "h2h", "Chiefs", -775, None, t1),
        _snap(event, "pinnacle", "h2h", "Chiefs", -727, None, t2),
        # Need both outcomes at t2 so the other side can be found
        _snap(event, "pinnacle", "h2h", "Bills", 550, None, t2),
    ]
    await repo.insert_snapshots(snapshots)

    detector = RapidChangeDetector(settings, repo)
    signals = await detector.detect(event, t2)

    assert len(signals) == 0


@pytest.mark.asyncio
async def test_rapid_ml_shortening_flips_to_other_side(settings, repo):
    """Pinnacle shortening Chiefs (-775 -> -727) should signal Bills
    if a US book offers better odds than Pinnacle on Bills."""
    event = "evt_rapid4b"
    t1 = "2025-01-20T12:00:00+00:00"
    t2 = "2025-01-20T12:20:00+00:00"

    snapshots = [
        # Pinnacle moves Chiefs from -775 to -727 (shortening = less favored)
        _snap(event, "pinnacle", "h2h", "Chiefs", -775, None, t1),
        _snap(event, "pinnacle", "h2h", "Chiefs", -727, None, t2),
        # Pinnacle's Bills line at t2
        _snap(event, "pinnacle", "h2h", "Bills", 500, None, t2),
        # FanDuel offers Bills at +550 (better than Pinnacle's +500)
        _snap(event, "fanduel", "h2h", "Bills", 550, None, t2),
    ]
    await repo.insert_snapshots(snapshots)

    detector = RapidChangeDetector(settings, repo)
    signals = await detector.detect(event, t2)

    assert len(signals) == 1
    sig = signals[0]
    assert sig.outcome_name == "Bills"  # flipped to other side
    assert sig.details["value_books"][0]["bookmaker"] == "fanduel"
    assert sig.details["value_books"][0]["price"] == 550


@pytest.mark.asyncio
async def test_rapid_ml_shortening_no_better_us_book(settings, repo):
    """Pinnacle shortening but US books don't beat Pinnacle on other side — no signal."""
    event = "evt_rapid4c"
    t1 = "2025-01-20T12:00:00+00:00"
    t2 = "2025-01-20T12:20:00+00:00"

    snapshots = [
        _snap(event, "pinnacle", "h2h", "Chiefs", -775, None, t1),
        _snap(event, "pinnacle", "h2h", "Chiefs", -727, None, t2),
        _snap(event, "pinnacle", "h2h", "Bills", 500, None, t2),
        # FanDuel offers Bills at +450 (WORSE than Pinnacle's +500)
        _snap(event, "fanduel", "h2h", "Bills", 450, None, t2),
    ]
    await repo.insert_snapshots(snapshots)

    detector = RapidChangeDetector(settings, repo)
    signals = await detector.detect(event, t2)

    assert len(signals) == 0


@pytest.mark.asyncio
async def test_rapid_spread_tightening_suppressed(settings, repo):
    """Pinnacle tightening a spread (toward zero) with no US books — no signal."""
    event = "evt_rapid5"
    t1 = "2025-01-20T12:00:00+00:00"
    t2 = "2025-01-20T12:20:00+00:00"

    snapshots = [
        _snap(event, "pinnacle", "spreads", "Chiefs", -110, -4.0, t1),
        _snap(event, "pinnacle", "spreads", "Chiefs", -110, -3.0, t2),
        _snap(event, "pinnacle", "spreads", "Bills", -110, 3.0, t2),
    ]
    await repo.insert_snapshots(snapshots)

    detector = RapidChangeDetector(settings, repo)
    signals = await detector.detect(event, t2)

    assert len(signals) == 0


@pytest.mark.asyncio
async def test_rapid_spread_tightening_flips_with_value(settings, repo):
    """Pinnacle tightening Chiefs spread (-4 -> -3) should signal Bills
    if a US book offers a better spread than Pinnacle on Bills."""
    event = "evt_rapid5b"
    t1 = "2025-01-20T12:00:00+00:00"
    t2 = "2025-01-20T12:20:00+00:00"

    snapshots = [
        # Pinnacle moves Chiefs from -4.0 to -3.0 (tightening = less favored)
        _snap(event, "pinnacle", "spreads", "Chiefs", -110, -4.0, t1),
        _snap(event, "pinnacle", "spreads", "Chiefs", -110, -3.0, t2),
        # Pinnacle's Bills spread at t2
        _snap(event, "pinnacle", "spreads", "Bills", -110, 3.0, t2),
        # DraftKings offers Bills at +4.0 (better than Pinnacle's +3.0)
        _snap(event, "draftkings", "spreads", "Bills", -110, 4.0, t2),
    ]
    await repo.insert_snapshots(snapshots)

    detector = RapidChangeDetector(settings, repo)
    signals = await detector.detect(event, t2)

    assert len(signals) == 1
    sig = signals[0]
    assert sig.outcome_name == "Bills"  # flipped to other side
    assert sig.details["value_books"][0]["bookmaker"] == "draftkings"
    assert sig.details["value_books"][0]["point"] == 4.0


@pytest.mark.asyncio
async def test_rapid_total_down_signals_under(settings, repo):
    """Pinnacle lowering total (147.5 -> 146.5) = sharps on Under.
    Should signal Under if a US book still has the higher total."""
    event = "evt_rapid_totals1"
    t1 = "2025-01-20T12:00:00+00:00"
    t2 = "2025-01-20T12:20:00+00:00"

    snapshots = [
        # Pinnacle lowers Over total from 147.5 to 146.5
        _snap(event, "pinnacle", "totals", "Over", -110, 147.5, t1),
        _snap(event, "pinnacle", "totals", "Over", -110, 146.5, t2),
        # Pinnacle Under at t2
        _snap(event, "pinnacle", "totals", "Under", -110, 146.5, t2),
        # DraftKings still at 147.5 (stale — Under 147.5 is easier)
        _snap(event, "draftkings", "totals", "Under", -105, 147.5, t2),
    ]
    await repo.insert_snapshots(snapshots)

    detector = RapidChangeDetector(settings, repo)
    signals = await detector.detect(event, t2)

    assert len(signals) == 1
    sig = signals[0]
    assert sig.outcome_name == "Under"  # sharps on Under
    assert sig.details["value_books"][0]["bookmaker"] == "draftkings"
    assert sig.details["value_books"][0]["point"] == 147.5


@pytest.mark.asyncio
async def test_rapid_total_up_signals_over(settings, repo):
    """Pinnacle raising total (147.5 -> 148.5) = sharps on Over.
    Should signal Over (steepening) with stale US books at old lower total."""
    event = "evt_rapid_totals2"
    t1 = "2025-01-20T12:00:00+00:00"
    t2 = "2025-01-20T12:20:00+00:00"

    snapshots = [
        # Pinnacle raises Over total from 147.5 to 148.5
        _snap(event, "pinnacle", "totals", "Over", -110, 147.5, t1),
        _snap(event, "pinnacle", "totals", "Over", -110, 148.5, t2),
        # FanDuel still at 147.5 (stale — Over 147.5 is easier to clear)
        _snap(event, "fanduel", "totals", "Over", -110, 147.5, t2),
    ]
    await repo.insert_snapshots(snapshots)

    detector = RapidChangeDetector(settings, repo)
    signals = await detector.detect(event, t2)

    assert len(signals) == 1
    sig = signals[0]
    assert sig.outcome_name == "Over"  # sharps on Over
    assert sig.details["value_books"][0]["bookmaker"] == "fanduel"
    assert sig.details["value_books"][0]["point"] == 147.5


@pytest.mark.asyncio
async def test_rapid_steepening_finds_stale_books(settings, repo):
    """Pinnacle steepening should signal same side with stale US books."""
    event = "evt_rapid6"
    t1 = "2025-01-20T12:00:00+00:00"
    t2 = "2025-01-20T12:20:00+00:00"

    snapshots = [
        # Pinnacle steepens Chiefs from -3.0 to -4.0 (more favored)
        _snap(event, "pinnacle", "spreads", "Chiefs", -110, -3.0, t1),
        _snap(event, "pinnacle", "spreads", "Chiefs", -110, -4.0, t2),
        # FanDuel still at -3.0 (stale — closer to old line)
        _snap(event, "fanduel", "spreads", "Chiefs", -110, -3.0, t2),
    ]
    await repo.insert_snapshots(snapshots)

    detector = RapidChangeDetector(settings, repo)
    signals = await detector.detect(event, t2)

    assert len(signals) == 1
    sig = signals[0]
    assert sig.outcome_name == "Chiefs"  # same side
    assert sig.details["value_books"][0]["bookmaker"] == "fanduel"
    assert sig.details["value_books"][0]["point"] == -3.0


@pytest.mark.asyncio
async def test_rapid_hold_in_details(settings, repo):
    """Rapid change should include us_hold when value book has both sides."""
    event = "evt_rapid_hold1"
    t1 = "2025-01-20T12:00:00+00:00"
    t2 = "2025-01-20T12:20:00+00:00"

    snapshots = [
        # Pinnacle steepens Chiefs from -3.0 to -4.0
        _snap(event, "pinnacle", "spreads", "Chiefs", -110, -3.0, t1),
        _snap(event, "pinnacle", "spreads", "Chiefs", -110, -4.0, t2),
        # FanDuel stale at -3.0 with both sides for hold calculation
        _snap(event, "fanduel", "spreads", "Chiefs", -105, -3.0, t2),
        _snap(event, "fanduel", "spreads", "Bills", -105, 3.0, t2),
    ]
    await repo.insert_snapshots(snapshots)

    detector = RapidChangeDetector(settings, repo)
    signals = await detector.detect(event, t2)

    assert len(signals) == 1
    sig = signals[0]
    assert sig.details["us_hold"] is not None
    # -105/-105 hold ≈ 0.0244
    assert sig.details["us_hold"] < 0.03


@pytest.mark.asyncio
async def test_rapid_hold_none_no_value_books(settings, repo):
    """Hold should be None when there are no value books."""
    event = "evt_rapid_hold2"
    t1 = "2025-01-20T12:00:00+00:00"
    t2 = "2025-01-20T12:20:00+00:00"

    snapshots = [
        # Pinnacle steepens but no stale US books
        _snap(event, "pinnacle", "spreads", "Chiefs", -110, -3.0, t1),
        _snap(event, "pinnacle", "spreads", "Chiefs", -110, -4.0, t2),
    ]
    await repo.insert_snapshots(snapshots)

    detector = RapidChangeDetector(settings, repo)
    signals = await detector.detect(event, t2)

    assert len(signals) == 1
    assert signals[0].details["us_hold"] is None
