"""Tests for the Pinnacle divergence detector."""

from __future__ import annotations

import pytest

from sharp_seeker.engine.base import SignalType
from sharp_seeker.engine.pinnacle_divergence import PinnacleDivergenceDetector


def _snap(
    event_id: str,
    bookmaker: str,
    market: str,
    outcome: str,
    price: float,
    point: float | None,
    fetched_at: str,
    sport_key: str = "basketball_nba",
) -> dict:
    return {
        "event_id": event_id,
        "sport_key": sport_key,
        "home_team": "Lakers",
        "away_team": "Celtics",
        "commence_time": "2025-01-15T00:00:00Z",
        "bookmaker_key": bookmaker,
        "market_key": market,
        "outcome_name": outcome,
        "price": price,
        "point": point,
        "deep_link": None,
        "fetched_at": fetched_at,
    }


@pytest.mark.asyncio
async def test_pinnacle_divergence_spread_value(settings, repo):
    """US book with better spread than Pinnacle should trigger."""
    event = "evt_pin1"
    t = "2025-01-15T12:00:00+00:00"

    # DK has -1.5 (better for bettor) vs Pinnacle -3.0 — value at DK
    snapshots = [
        _snap(event, "pinnacle", "spreads", "Lakers", -110, -3.0, t),
        _snap(event, "draftkings", "spreads", "Lakers", -110, -1.5, t),  # 1.5 better
        _snap(event, "fanduel", "spreads", "Lakers", -110, -3.0, t),    # same as pin
    ]
    await repo.insert_snapshots(snapshots)

    detector = PinnacleDivergenceDetector(settings, repo)
    signals = await detector.detect(event, t)

    assert len(signals) == 1
    sig = signals[0]
    assert sig.signal_type == SignalType.PINNACLE_DIVERGENCE
    assert sig.details["us_book"] == "draftkings"
    assert sig.details["delta"] == 1.5


@pytest.mark.asyncio
async def test_pinnacle_divergence_no_signal_when_pinnacle_better(settings, repo):
    """US book with worse spread than Pinnacle should NOT trigger."""
    event = "evt_pin1b"
    t = "2025-01-15T12:00:00+00:00"

    # DK has -4.5 (worse for bettor) vs Pinnacle -3.0 — no value at DK
    snapshots = [
        _snap(event, "pinnacle", "spreads", "Lakers", -110, -3.0, t),
        _snap(event, "draftkings", "spreads", "Lakers", -110, -4.5, t),  # 1.5 worse
    ]
    await repo.insert_snapshots(snapshots)

    detector = PinnacleDivergenceDetector(settings, repo)
    signals = await detector.detect(event, t)

    assert len(signals) == 0


@pytest.mark.asyncio
async def test_pinnacle_divergence_moneyline_value(settings, repo):
    """US book with better ML odds than Pinnacle should trigger.

    BetMGM -110 implied = 110/210 ≈ 0.5238
    Pinnacle -150 implied = 150/250 = 0.6000
    Delta ≈ 0.0762 (7.6%), well above 3% threshold.
    """
    event = "evt_pin2"
    t = "2025-01-15T12:00:00+00:00"

    snapshots = [
        _snap(event, "pinnacle", "h2h", "Lakers", -150, None, t),
        _snap(event, "betmgm", "h2h", "Lakers", -110, None, t),
    ]
    await repo.insert_snapshots(snapshots)

    detector = PinnacleDivergenceDetector(settings, repo)
    signals = await detector.detect(event, t)

    assert len(signals) == 1
    sig = signals[0]
    # Delta is now in implied probability units
    assert abs(sig.details["delta"] - 0.0762) < 0.001
    assert "us_implied_prob" in sig.details
    assert "pinnacle_implied_prob" in sig.details


@pytest.mark.asyncio
async def test_pinnacle_divergence_ml_cross_zero_no_fire(settings, repo):
    """Cross-zero case: +100 vs -104 is only ~1% edge — should NOT fire.

    +100 implied = 100/200 = 0.5000
    -104 implied = 104/204 ≈ 0.5098
    Delta ≈ 0.0098 (0.98%), below 3% threshold.
    """
    event = "evt_pin_cross"
    t = "2025-01-15T12:00:00+00:00"

    snapshots = [
        _snap(event, "pinnacle", "h2h", "Lakers", -104, None, t),
        _snap(event, "betmgm", "h2h", "Lakers", 100, None, t),
    ]
    await repo.insert_snapshots(snapshots)

    detector = PinnacleDivergenceDetector(settings, repo)
    signals = await detector.detect(event, t)

    assert len(signals) == 0


@pytest.mark.asyncio
async def test_pinnacle_divergence_ml_large_gap(settings, repo):
    """Real divergence: +200 vs -200 should fire.

    +200 implied = 100/300 ≈ 0.3333
    -200 implied = 200/300 ≈ 0.6667
    Delta ≈ 0.3333 (33.3%), way above 3% threshold.
    """
    event = "evt_pin_large"
    t = "2025-01-15T12:00:00+00:00"

    # US book has +200 (better for bettor) vs Pinnacle -200
    snapshots = [
        _snap(event, "pinnacle", "h2h", "Lakers", -200, None, t),
        _snap(event, "fanduel", "h2h", "Lakers", 200, None, t),
    ]
    await repo.insert_snapshots(snapshots)

    detector = PinnacleDivergenceDetector(settings, repo)
    signals = await detector.detect(event, t)

    assert len(signals) == 1
    sig = signals[0]
    assert abs(sig.details["delta"] - 0.3333) < 0.001


@pytest.mark.asyncio
async def test_no_divergence_below_threshold(settings, repo):
    """Spread diff of 0.5 should NOT trigger (threshold is 1.0)."""
    event = "evt_pin3"
    t = "2025-01-15T12:00:00+00:00"

    snapshots = [
        _snap(event, "pinnacle", "spreads", "Lakers", -110, -3.0, t),
        _snap(event, "draftkings", "spreads", "Lakers", -110, -3.5, t),
    ]
    await repo.insert_snapshots(snapshots)

    detector = PinnacleDivergenceDetector(settings, repo)
    signals = await detector.detect(event, t)

    assert len(signals) == 0


@pytest.mark.asyncio
async def test_excluded_book_skipped(settings, repo):
    """Books in pd_excluded_books should not generate PD signals."""
    event = "evt_pin_excl"
    t = "2025-01-15T12:00:00+00:00"

    # BetMGM has better ML odds than Pinnacle — would fire without exclusion
    snapshots = [
        _snap(event, "pinnacle", "h2h", "Lakers", -150, None, t),
        _snap(event, "betmgm", "h2h", "Lakers", -110, None, t),
        _snap(event, "draftkings", "h2h", "Lakers", -150, None, t),  # same as pin, no signal
    ]
    await repo.insert_snapshots(snapshots)

    settings.pd_excluded_books = ["betmgm"]
    detector = PinnacleDivergenceDetector(settings, repo)
    signals = await detector.detect(event, t)

    assert len(signals) == 0


@pytest.mark.asyncio
async def test_excluded_book_does_not_affect_others(settings, repo):
    """Excluding one book should not suppress signals from other books."""
    event = "evt_pin_excl2"
    t = "2025-01-15T12:00:00+00:00"

    snapshots = [
        _snap(event, "pinnacle", "h2h", "Lakers", -150, None, t),
        _snap(event, "betmgm", "h2h", "Lakers", -110, None, t),
        _snap(event, "draftkings", "h2h", "Lakers", -110, None, t),  # also diverges
    ]
    await repo.insert_snapshots(snapshots)

    settings.pd_excluded_books = ["betmgm"]
    detector = PinnacleDivergenceDetector(settings, repo)
    signals = await detector.detect(event, t)

    # Only DraftKings should fire, not BetMGM
    assert len(signals) == 1
    assert signals[0].details["us_book"] == "draftkings"


@pytest.mark.asyncio
async def test_no_signal_without_pinnacle(settings, repo):
    """No Pinnacle data means no divergence signals."""
    event = "evt_pin4"
    t = "2025-01-15T12:00:00+00:00"

    snapshots = [
        _snap(event, "draftkings", "spreads", "Lakers", -110, -4.5, t),
        _snap(event, "fanduel", "spreads", "Lakers", -110, -3.0, t),
    ]
    await repo.insert_snapshots(snapshots)

    detector = PinnacleDivergenceDetector(settings, repo)
    signals = await detector.detect(event, t)

    assert len(signals) == 0


@pytest.mark.asyncio
async def test_sport_ml_prob_override(settings, repo):
    """Sport-specific ML threshold should override global.

    BetMGM -140 implied = 140/240 ≈ 0.5833
    Pinnacle -155 implied = 155/255 ≈ 0.6078
    Delta ≈ 0.0245 (2.45%) — below global 3% but above NHL 1.5% override.
    """
    event = "evt_nhl_override"
    t = "2025-01-15T12:00:00+00:00"

    snapshots = [
        _snap(event, "pinnacle", "h2h", "Rangers", -155, None, t, sport_key="icehockey_nhl"),
        _snap(event, "betmgm", "h2h", "Rangers", -140, None, t, sport_key="icehockey_nhl"),
    ]
    await repo.insert_snapshots(snapshots)

    # Without override — global 3% threshold, 2.45% delta should NOT fire
    settings.pd_sport_ml_prob_overrides = {}
    detector = PinnacleDivergenceDetector(settings, repo)
    signals = await detector.detect(event, t)
    assert len(signals) == 0

    # With NHL override at 1.5% — same delta SHOULD fire
    settings.pd_sport_ml_prob_overrides = {"icehockey_nhl": 0.015}
    detector = PinnacleDivergenceDetector(settings, repo)
    signals = await detector.detect(event, t)
    assert len(signals) == 1
    assert signals[0].details["us_book"] == "betmgm"

    # Strength uses sport threshold (0.015) so scores reflect the sport's scale.
    # delta 0.0245 / (0.015 * 3) ≈ 0.544
    assert 0.50 < signals[0].strength < 0.60


@pytest.mark.asyncio
async def test_totals_over_divergence(settings, repo):
    """US book with lower Over total than Pinnacle should trigger with sport override.

    DK Over 5.5 vs Pinnacle Over 6.0 — easier to go over at DK (0.5 delta).
    NHL sport override lowers threshold to 0.5.
    """
    event = "evt_totals1"
    t = "2025-01-15T12:00:00+00:00"

    snapshots = [
        _snap(event, "pinnacle", "totals", "Over", -110, 6.0, t, sport_key="icehockey_nhl"),
        _snap(event, "draftkings", "totals", "Over", -110, 5.5, t, sport_key="icehockey_nhl"),
    ]
    await repo.insert_snapshots(snapshots)

    settings.pd_sport_totals_overrides = {"icehockey_nhl": 0.5}
    detector = PinnacleDivergenceDetector(settings, repo)
    signals = await detector.detect(event, t)

    assert len(signals) == 1
    assert signals[0].details["us_book"] == "draftkings"
    assert signals[0].details["delta"] == 0.5
    assert signals[0].market_key == "totals"


@pytest.mark.asyncio
async def test_totals_under_divergence(settings, repo):
    """US book with higher Under total than Pinnacle should trigger with sport override.

    FanDuel Under 6.5 vs Pinnacle Under 6.0 — easier to stay under at FD (0.5 delta).
    """
    event = "evt_totals2"
    t = "2025-01-15T12:00:00+00:00"

    snapshots = [
        _snap(event, "pinnacle", "totals", "Under", -110, 6.0, t, sport_key="icehockey_nhl"),
        _snap(event, "fanduel", "totals", "Under", -110, 6.5, t, sport_key="icehockey_nhl"),
    ]
    await repo.insert_snapshots(snapshots)

    settings.pd_sport_totals_overrides = {"icehockey_nhl": 0.5}
    detector = PinnacleDivergenceDetector(settings, repo)
    signals = await detector.detect(event, t)

    assert len(signals) == 1
    assert signals[0].details["us_book"] == "fanduel"
    assert signals[0].details["delta"] == 0.5


@pytest.mark.asyncio
async def test_totals_no_fire_below_threshold(settings, repo):
    """Totals divergence below threshold should NOT fire."""
    event = "evt_totals3"
    t = "2025-01-15T12:00:00+00:00"

    # Same total — no divergence
    snapshots = [
        _snap(event, "pinnacle", "totals", "Over", -110, 6.0, t),
        _snap(event, "draftkings", "totals", "Over", -110, 6.0, t),
    ]
    await repo.insert_snapshots(snapshots)

    detector = PinnacleDivergenceDetector(settings, repo)
    signals = await detector.detect(event, t)

    assert len(signals) == 0


@pytest.mark.asyncio
async def test_totals_no_fire_without_sport_override(settings, repo):
    """NHL totals at 0.5 delta should NOT fire without sport override.

    Global totals threshold is 1.0 — a 0.5-point divergence is below it.
    """
    event = "evt_totals4"
    t = "2025-01-15T12:00:00+00:00"

    snapshots = [
        _snap(event, "pinnacle", "totals", "Over", -110, 6.0, t, sport_key="icehockey_nhl"),
        _snap(event, "draftkings", "totals", "Over", -110, 5.5, t, sport_key="icehockey_nhl"),
    ]
    await repo.insert_snapshots(snapshots)

    # No sport override — global 1.0 threshold applies
    settings.pd_sport_totals_overrides = {}
    detector = PinnacleDivergenceDetector(settings, repo)
    signals = await detector.detect(event, t)

    assert len(signals) == 0


@pytest.mark.asyncio
async def test_totals_sport_override_fires(settings, repo):
    """NHL totals at 0.5 delta SHOULD fire with sport override at 0.5.

    Verifies sport override lowers the threshold for NHL only.
    """
    event = "evt_totals5"
    t = "2025-01-15T12:00:00+00:00"

    snapshots = [
        _snap(event, "pinnacle", "totals", "Over", -110, 6.0, t, sport_key="icehockey_nhl"),
        _snap(event, "draftkings", "totals", "Over", -110, 5.5, t, sport_key="icehockey_nhl"),
    ]
    await repo.insert_snapshots(snapshots)

    # With NHL override at 0.5 — should fire
    settings.pd_sport_totals_overrides = {"icehockey_nhl": 0.5}
    detector = PinnacleDivergenceDetector(settings, repo)
    signals = await detector.detect(event, t)

    assert len(signals) == 1
    assert signals[0].details["us_book"] == "draftkings"

    # Strength uses sport threshold (0.5) so scores reflect the sport's scale.
    # delta 0.5 / (0.5 * 3) ≈ 0.33
    assert 0.30 < signals[0].strength < 0.40


@pytest.mark.asyncio
async def test_spread_sport_override_fires(settings, repo):
    """NBA spread at 0.5 delta should fire with sport override at 0.5.

    Global spread threshold is 1.0 — without override this wouldn't fire.
    """
    event = "evt_spread_override"
    t = "2025-01-15T12:00:00+00:00"

    # DK has -2.5 vs Pinnacle -3.0 — 0.5 better for bettor at DK
    snapshots = [
        _snap(event, "pinnacle", "spreads", "Lakers", -110, -3.0, t),
        _snap(event, "draftkings", "spreads", "Lakers", -110, -2.5, t),
    ]
    await repo.insert_snapshots(snapshots)

    # Without override — global 1.0 threshold, 0.5 delta should NOT fire
    settings.pd_sport_spread_overrides = {}
    detector = PinnacleDivergenceDetector(settings, repo)
    signals = await detector.detect(event, t)
    assert len(signals) == 0

    # With NBA override at 0.5 — should fire
    settings.pd_sport_spread_overrides = {"basketball_nba": 0.5}
    detector = PinnacleDivergenceDetector(settings, repo)
    signals = await detector.detect(event, t)
    assert len(signals) == 1
    assert signals[0].details["us_book"] == "draftkings"
    assert signals[0].details["delta"] == 0.5

    # Strength uses sport threshold (0.5) so scores reflect the sport's scale.
    # delta 0.5 / (0.5 * 3) ≈ 0.33 (no hold boost — only one side in snapshot)
    assert 0.30 < signals[0].strength < 0.40


# ── Hold (vig) tests ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_hold_in_signal_details(settings, repo):
    """Hold should be computed when both sides of the market are available."""
    event = "evt_hold1"
    t = "2025-01-15T12:00:00+00:00"

    # Both sides of NHL totals for DK and Pinnacle
    snapshots = [
        _snap(event, "pinnacle", "totals", "Over", -110, 6.0, t, sport_key="icehockey_nhl"),
        _snap(event, "pinnacle", "totals", "Under", -110, 6.0, t, sport_key="icehockey_nhl"),
        _snap(event, "draftkings", "totals", "Over", -110, 5.5, t, sport_key="icehockey_nhl"),
        _snap(event, "draftkings", "totals", "Under", -110, 5.5, t, sport_key="icehockey_nhl"),
    ]
    await repo.insert_snapshots(snapshots)

    settings.pd_sport_totals_overrides = {"icehockey_nhl": 0.5}
    detector = PinnacleDivergenceDetector(settings, repo)
    signals = await detector.detect(event, t)

    assert len(signals) == 1
    # Both sides at -110: implied = 110/210 ≈ 0.5238 each
    # hold = 0.5238 + 0.5238 - 1.0 ≈ 0.0476
    assert signals[0].details["us_hold"] is not None
    assert abs(signals[0].details["us_hold"] - 0.0476) < 0.001
    assert signals[0].details["pinnacle_hold"] is not None
    assert abs(signals[0].details["pinnacle_hold"] - 0.0476) < 0.001


@pytest.mark.asyncio
async def test_hold_tracked_but_no_strength_boost(settings, repo):
    """Hold is tracked for analytics but does NOT affect strength."""
    event = "evt_hold_sharp"
    t = "2025-01-15T12:00:00+00:00"

    # Sharp pricing: -105/-105 → hold = 2.44%
    snapshots = [
        _snap(event, "pinnacle", "totals", "Over", -110, 6.0, t, sport_key="icehockey_nhl"),
        _snap(event, "pinnacle", "totals", "Under", -110, 6.0, t, sport_key="icehockey_nhl"),
        _snap(event, "draftkings", "totals", "Over", -105, 5.5, t, sport_key="icehockey_nhl"),
        _snap(event, "draftkings", "totals", "Under", -105, 5.5, t, sport_key="icehockey_nhl"),
    ]
    await repo.insert_snapshots(snapshots)

    settings.pd_sport_totals_overrides = {"icehockey_nhl": 0.5}
    detector = PinnacleDivergenceDetector(settings, repo)
    signals = await detector.detect(event, t)

    assert len(signals) == 1
    # Hold is tracked but doesn't boost strength
    assert signals[0].details["hold_boost"] == 0.0
    assert signals[0].details["us_hold"] is not None
    assert signals[0].details["us_hold"] < 0.045  # sharp hold
    # Strength is pure base: 0.5 / (0.5 * 3) ≈ 0.33
    assert 0.30 < signals[0].strength < 0.40


@pytest.mark.asyncio
async def test_no_hold_when_other_side_missing(settings, repo):
    """Hold should be None when only one side of the market is in the snapshot."""
    event = "evt_no_hold"
    t = "2025-01-15T12:00:00+00:00"

    # Only Over side — no Under
    snapshots = [
        _snap(event, "pinnacle", "totals", "Over", -110, 6.0, t, sport_key="icehockey_nhl"),
        _snap(event, "draftkings", "totals", "Over", -110, 5.5, t, sport_key="icehockey_nhl"),
    ]
    await repo.insert_snapshots(snapshots)

    settings.pd_sport_totals_overrides = {"icehockey_nhl": 0.5}
    detector = PinnacleDivergenceDetector(settings, repo)
    signals = await detector.detect(event, t)

    assert len(signals) == 1
    assert signals[0].details["us_hold"] is None
    assert signals[0].details["hold_boost"] == 0.0


# ── Sharp-line direction annotation (measurement only) ────────────


@pytest.mark.asyncio
async def test_h2h_direction_toward_when_pinnacle_shortens(settings, repo):
    """When Pinnacle shortened the flagged side over the window → 'toward'."""
    event = "evt_dir_toward"
    t1 = "2025-01-15T12:00:00+00:00"
    t2 = "2025-01-15T12:20:00+00:00"  # within 30-min window

    snapshots = [
        # Pinnacle Lakers shortens: -120 (0.5455) → -150 (0.60) = more likely
        _snap(event, "pinnacle", "h2h", "Lakers", -120, None, t1),
        _snap(event, "pinnacle", "h2h", "Lakers", -150, None, t2),
        _snap(event, "betmgm", "h2h", "Lakers", -110, None, t2),  # value at t2 → fires
    ]
    await repo.insert_snapshots(snapshots)

    detector = PinnacleDivergenceDetector(settings, repo)
    signals = await detector.detect(event, t2)

    assert len(signals) == 1
    assert signals[0].details["pinnacle_recent_direction"] == "toward"
    assert signals[0].details["pinnacle_recent_delta"] > 0


@pytest.mark.asyncio
async def test_h2h_direction_against_when_pinnacle_lengthens(settings, repo):
    """When Pinnacle lengthened the flagged side over the window → 'against'."""
    event = "evt_dir_against"
    t1 = "2025-01-15T12:00:00+00:00"
    t2 = "2025-01-15T12:20:00+00:00"

    snapshots = [
        # Pinnacle Lakers lengthens: -180 (0.6429) → -150 (0.60) = less likely
        _snap(event, "pinnacle", "h2h", "Lakers", -180, None, t1),
        _snap(event, "pinnacle", "h2h", "Lakers", -150, None, t2),
        _snap(event, "betmgm", "h2h", "Lakers", -110, None, t2),  # value at t2 → fires
    ]
    await repo.insert_snapshots(snapshots)

    detector = PinnacleDivergenceDetector(settings, repo)
    signals = await detector.detect(event, t2)

    assert len(signals) == 1
    assert signals[0].details["pinnacle_recent_direction"] == "against"
    assert signals[0].details["pinnacle_recent_delta"] < 0


@pytest.mark.asyncio
async def test_direction_unknown_with_single_snapshot(settings, repo):
    """One snapshot in the window → not enough history → 'unknown'."""
    event = "evt_dir_unknown"
    t = "2025-01-15T12:00:00+00:00"

    snapshots = [
        _snap(event, "pinnacle", "h2h", "Lakers", -150, None, t),
        _snap(event, "betmgm", "h2h", "Lakers", -110, None, t),
    ]
    await repo.insert_snapshots(snapshots)

    detector = PinnacleDivergenceDetector(settings, repo)
    signals = await detector.detect(event, t)

    assert len(signals) == 1
    assert signals[0].details["pinnacle_recent_direction"] == "unknown"


@pytest.mark.asyncio
async def test_spread_direction_toward_when_point_drops(settings, repo):
    """Spread: Pinnacle point lowering for the side → 'toward'."""
    event = "evt_dir_spread_toward"
    t1 = "2025-01-15T12:00:00+00:00"
    t2 = "2025-01-15T12:20:00+00:00"

    snapshots = [
        # Pinnacle Lakers point drops -2.0 → -3.0 (more favored / backed)
        _snap(event, "pinnacle", "spreads", "Lakers", -110, -2.0, t1),
        _snap(event, "pinnacle", "spreads", "Lakers", -110, -3.0, t2),
        _snap(event, "draftkings", "spreads", "Lakers", -110, -1.5, t2),  # value → fires
    ]
    await repo.insert_snapshots(snapshots)

    detector = PinnacleDivergenceDetector(settings, repo)
    signals = await detector.detect(event, t2)

    assert len(signals) == 1
    assert signals[0].details["pinnacle_recent_direction"] == "toward"
    assert signals[0].details["pinnacle_recent_delta"] < 0


@pytest.mark.asyncio
async def test_spread_direction_against_when_point_rises(settings, repo):
    """Spread: Pinnacle point rising for the side (more points = faded) → 'against'."""
    event = "evt_dir_spread_against"
    t1 = "2025-01-15T12:00:00+00:00"
    t2 = "2025-01-15T12:20:00+00:00"

    snapshots = [
        # Pinnacle Lakers point rises -4.0 → -3.0 (less favored / faded)
        _snap(event, "pinnacle", "spreads", "Lakers", -110, -4.0, t1),
        _snap(event, "pinnacle", "spreads", "Lakers", -110, -3.0, t2),
        _snap(event, "draftkings", "spreads", "Lakers", -110, -1.5, t2),  # value → fires
    ]
    await repo.insert_snapshots(snapshots)

    detector = PinnacleDivergenceDetector(settings, repo)
    signals = await detector.detect(event, t2)

    assert len(signals) == 1
    assert signals[0].details["pinnacle_recent_direction"] == "against"
    assert signals[0].details["pinnacle_recent_delta"] > 0


@pytest.mark.asyncio
async def test_hold_works_for_h2h(settings, repo):
    """Hold should be computed for moneyline (h2h) markets too."""
    event = "evt_hold_h2h"
    t = "2025-01-15T12:00:00+00:00"

    # Both sides of h2h at FanDuel
    snapshots = [
        _snap(event, "pinnacle", "h2h", "Lakers", -150, None, t),
        _snap(event, "pinnacle", "h2h", "Celtics", 130, None, t),
        _snap(event, "fanduel", "h2h", "Lakers", -110, None, t),
        _snap(event, "fanduel", "h2h", "Celtics", 130, None, t),
    ]
    await repo.insert_snapshots(snapshots)

    detector = PinnacleDivergenceDetector(settings, repo)
    signals = await detector.detect(event, t)

    assert len(signals) == 1
    assert signals[0].details["us_hold"] is not None
    assert signals[0].details["pinnacle_hold"] is not None


# ── MLB 9.0-9.5 line × negative-hold suppression ──────────────────


@pytest.mark.asyncio
async def test_mlb_totals_9_9_5_neg_hold_suppressed(settings, repo):
    """MLB totals on the 9.0-9.5 line at NEGATIVE cross-book hold are dropped.

    This is the lone losing cell in the line×hold cross-tab (49% WR, -9.85u).
    """
    event = "evt_mlb_9_neg"
    t = "2025-01-15T12:00:00+00:00"

    # DK Over 9.0 vs Pinnacle Over 9.5 → 0.5 delta, us_val 9.0 (in band).
    # +105 both sides → cross-book hold ≈ -0.024 (negative).
    snapshots = [
        _snap(event, "pinnacle", "totals", "Over", 105, 9.5, t, sport_key="baseball_mlb"),
        _snap(event, "pinnacle", "totals", "Under", 105, 9.5, t, sport_key="baseball_mlb"),
        _snap(event, "draftkings", "totals", "Over", 105, 9.0, t, sport_key="baseball_mlb"),
        _snap(event, "draftkings", "totals", "Under", 105, 9.0, t, sport_key="baseball_mlb"),
    ]
    await repo.insert_snapshots(snapshots)

    settings.pd_sport_totals_overrides = {"baseball_mlb": 0.5}
    detector = PinnacleDivergenceDetector(settings, repo)
    signals = await detector.detect(event, t)

    assert signals == []


@pytest.mark.asyncio
async def test_mlb_totals_neg_hold_other_line_still_fires(settings, repo):
    """The suppression is scoped to 9.0-9.5 — an 8.0 line at neg hold still fires."""
    event = "evt_mlb_8_neg"
    t = "2025-01-15T12:00:00+00:00"

    # Same negative-hold setup, but on the 8.0/8.5 line (out of the dead band).
    snapshots = [
        _snap(event, "pinnacle", "totals", "Over", 105, 8.5, t, sport_key="baseball_mlb"),
        _snap(event, "pinnacle", "totals", "Under", 105, 8.5, t, sport_key="baseball_mlb"),
        _snap(event, "draftkings", "totals", "Over", 105, 8.0, t, sport_key="baseball_mlb"),
        _snap(event, "draftkings", "totals", "Under", 105, 8.0, t, sport_key="baseball_mlb"),
    ]
    await repo.insert_snapshots(snapshots)

    settings.pd_sport_totals_overrides = {"baseball_mlb": 0.5}
    detector = PinnacleDivergenceDetector(settings, repo)
    signals = await detector.detect(event, t)

    assert len(signals) == 1
    assert signals[0].details["us_value"] == 8.0
    assert signals[0].details["cross_book_hold"] < 0


@pytest.mark.asyncio
async def test_fanatics_treated_as_us_value_book(settings, repo):
    """Fanatics is a recognized US book → it fires PD like DK/FD/BetRivers."""
    event = "evt_fanatics"
    t = "2025-01-15T12:00:00+00:00"

    # Fanatics has a better total (Over 7.5 vs Pinnacle 8.0) → 0.5 delta.
    snapshots = [
        _snap(event, "pinnacle", "totals", "Over", -110, 8.0, t, sport_key="icehockey_nhl"),
        _snap(event, "fanatics", "totals", "Over", -110, 7.5, t, sport_key="icehockey_nhl"),
    ]
    await repo.insert_snapshots(snapshots)

    settings.pd_sport_totals_overrides = {"icehockey_nhl": 0.5}
    detector = PinnacleDivergenceDetector(settings, repo)
    signals = await detector.detect(event, t)

    assert len(signals) == 1
    assert signals[0].details["us_book"] == "fanatics"


@pytest.mark.asyncio
async def test_espnbet_treated_as_us_value_book(settings, repo):
    """ESPN Bet (key 'espnbet', now TheScoreBet) is a recognized US value book."""
    event = "evt_espnbet"
    t = "2025-01-15T12:00:00+00:00"

    snapshots = [
        _snap(event, "pinnacle", "totals", "Over", -110, 8.0, t, sport_key="icehockey_nhl"),
        _snap(event, "espnbet", "totals", "Over", -110, 7.5, t, sport_key="icehockey_nhl"),
    ]
    await repo.insert_snapshots(snapshots)

    settings.pd_sport_totals_overrides = {"icehockey_nhl": 0.5}
    detector = PinnacleDivergenceDetector(settings, repo)
    signals = await detector.detect(event, t)

    assert len(signals) == 1
    assert signals[0].details["us_book"] == "espnbet"


def test_new_book_display_names():
    """User-facing names: espnbet renders as TheScoreBet (per operator)."""
    from sharp_seeker.alerts.models import display_book

    assert display_book("espnbet") == "TheScoreBet"
    assert display_book("hardrockbet") == "Hard Rock Bet"
    assert display_book("fanatics") == "Fanatics"
