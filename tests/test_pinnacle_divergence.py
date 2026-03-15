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
async def test_hold_boost_sharp_pricing(settings, repo):
    """Low hold at US book (< 4.5%) should boost strength above base."""
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
    # base = 0.5 / (0.5 * 3) ≈ 0.33, hold 2.44% → sharp boost +0.08
    assert signals[0].strength > 0.40
    assert signals[0].details["hold_boost"] == 0.08


@pytest.mark.asyncio
async def test_hold_boost_average_pricing(settings, repo):
    """Below-average hold (4.5-5.0%) should get smaller boost."""
    event = "evt_hold_avg"
    t = "2025-01-15T12:00:00+00:00"

    # DK at -110/-110 → hold = 4.76% → between 4.5% and 5.0% = average boost
    snapshots = [
        _snap(event, "pinnacle", "totals", "Over", -105, 6.0, t, sport_key="icehockey_nhl"),
        _snap(event, "pinnacle", "totals", "Under", -105, 6.0, t, sport_key="icehockey_nhl"),
        _snap(event, "draftkings", "totals", "Over", -110, 5.5, t, sport_key="icehockey_nhl"),
        _snap(event, "draftkings", "totals", "Under", -110, 5.5, t, sport_key="icehockey_nhl"),
    ]
    await repo.insert_snapshots(snapshots)

    settings.pd_sport_totals_overrides = {"icehockey_nhl": 0.5}
    detector = PinnacleDivergenceDetector(settings, repo)
    signals = await detector.detect(event, t)

    assert len(signals) == 1
    # DK hold at -110/-110 = 4.76% → between 4.5% and 5.0% = average boost +0.04
    assert signals[0].details["hold_boost"] == 0.04
    # base ≈ 0.33 + 0.04 = 0.37
    assert 0.35 < signals[0].strength < 0.40


@pytest.mark.asyncio
async def test_no_hold_boost_wide_pricing(settings, repo):
    """High hold (>= 5.0%) should get no boost."""
    event = "evt_hold_wide"
    t = "2025-01-15T12:00:00+00:00"

    # Wide pricing: -115/-115 → hold ≈ 6.52%
    snapshots = [
        _snap(event, "pinnacle", "totals", "Over", -105, 6.0, t, sport_key="icehockey_nhl"),
        _snap(event, "pinnacle", "totals", "Under", -105, 6.0, t, sport_key="icehockey_nhl"),
        _snap(event, "draftkings", "totals", "Over", -115, 5.5, t, sport_key="icehockey_nhl"),
        _snap(event, "draftkings", "totals", "Under", -115, 5.5, t, sport_key="icehockey_nhl"),
    ]
    await repo.insert_snapshots(snapshots)

    settings.pd_sport_totals_overrides = {"icehockey_nhl": 0.5}
    detector = PinnacleDivergenceDetector(settings, repo)
    signals = await detector.detect(event, t)

    assert len(signals) == 1
    assert signals[0].details["hold_boost"] == 0.0
    # base ≈ 0.33, no boost
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
