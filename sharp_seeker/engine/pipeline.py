"""Detection pipeline: runs all detectors and deduplicates signals."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone

import structlog

from sharp_seeker.config import Settings
from sharp_seeker.db.repository import Repository
from sharp_seeker.engine.base import BaseDetector, Signal, SignalType
from sharp_seeker.engine.arbitrage import ArbitrageDetector
from sharp_seeker.engine.exchange_monitor import ExchangeMonitorDetector
from sharp_seeker.engine.pinnacle_divergence import PinnacleDivergenceDetector
from sharp_seeker.engine.rapid_change import RapidChangeDetector
from sharp_seeker.engine.reverse_line import ReverseLineDetector
from sharp_seeker.engine.steam_move import SteamMoveDetector

log = structlog.get_logger()


def _price_rank(price) -> float:
    """Bettor-value of an American price as a sortable number: higher = better
    payout (+150 > -110 > -130). Missing prices sort last."""
    return price if isinstance(price, (int, float)) else float("-inf")


def _best_price(sig: Signal) -> float:
    """Best (most favorable) price across a signal's value books."""
    vbs = sig.details.get("value_books") or []
    return max((_price_rank(vb.get("price")) for vb in vbs), default=float("-inf"))


def _merge_same_side_value_books(best: Signal, sigs: list[Signal]) -> None:
    """Line-shopping: consolidate every book offering the SAME side at the SAME
    point as the chosen signal into ``best.details['value_books']``, best price
    first.

    Detectors like Pinnacle Divergence emit one single-book signal per book, so
    three books on Over 11.5 arrive as three signals and dedup would otherwise
    keep just one (arbitrary) book's price. Merging them means the recommendation
    shows the best price and the rest render as shoppable alternatives — same
    bet, every book that lists it, no added variance.

    Books at a different (worse) point are a different bet and are left out. A
    no-op when only one signal is on the chosen side (e.g. Steam/Rapid emit a
    single multi-book signal per side — we must not strip their other points).
    """
    same_side = [s for s in sigs if s.outcome_name == best.outcome_name]
    if len(same_side) <= 1:
        return
    primary = best.details.get("value_books") or []
    if not primary:
        return
    best_point = primary[0].get("point")
    merged: dict[str, dict] = {}
    for s in same_side:
        for vb in s.details.get("value_books", []):
            if vb.get("point") != best_point:
                continue
            bk = vb.get("bookmaker")
            if not bk:
                continue
            cur = merged.get(bk)
            if cur is None or _price_rank(vb.get("price")) > _price_rank(cur.get("price")):
                merged[bk] = vb
    if len(merged) > 1:
        best.details["value_books"] = sorted(
            merged.values(), key=lambda vb: _price_rank(vb.get("price")), reverse=True
        )


def _pick_best_signal(sigs: list[Signal]) -> Signal:
    """From mirror-side signals, pick the most actionable one.

    Each signal type has a preferred side based on its directional context:
    - Reverse Line: follow Pinnacle's direction (pinnacle_delta > 0)
    - Steam Move: stale books are value on the side where the line moved
      AGAINST bettors (direction "down" for h2h/spreads; for totals,
      "up" favors Over and "down" favors Under)
    - Exchange Shift: the side that shortened (exchange thinks more likely)
    - Rapid Change: the side with the larger delta
    - Pinnacle Divergence: already fires only for the value side, but
      falls through to generic tiebreaker if both sides appear.
    """
    if len(sigs) == 1:
        return sigs[0]

    sig_type = sigs[0].signal_type
    market = sigs[0].market_key

    if sig_type == SignalType.REVERSE_LINE:
        # Follow Pinnacle: keep the side where Pinnacle moved favorably
        for s in sigs:
            if s.details.get("pinnacle_delta", 0) > 0:
                return s

    elif sig_type == SignalType.STEAM_MOVE:
        # Value is at stale books on the side where the line moved AGAINST
        # bettors.  h2h/spreads: "down" means the line got worse for this
        # side's bettors → stale books still on the old (better) line.
        # Totals: "up" favors Over (lower stale total easier to clear),
        # "down" favors Under (higher stale total easier to stay under).
        for s in sigs:
            direction = s.details.get("direction", "")
            if market == "totals":
                if s.outcome_name.lower() == "over" and direction == "up":
                    return s
                if s.outcome_name.lower() == "under" and direction == "down":
                    return s
            else:
                if direction == "down":
                    return s

    elif sig_type == SignalType.EXCHANGE_SHIFT:
        # Prefer the side the exchange shortened (thinks more likely)
        for s in sigs:
            if s.details.get("direction") == "shortened":
                return s

    elif sig_type == SignalType.RAPID_CHANGE:
        # Prefer the side with the larger move
        return max(sigs, key=lambda s: abs(s.details.get("delta", 0)))

    # Fallback: most value books, then highest strength, then best price for the
    # bettor. The price tiebreaker only separates otherwise-equal signals (e.g.
    # PD books at the same number), so a genuinely better line (higher strength)
    # is never traded away for a worse one — it just stops us posting -130 when
    # -106 on the same number is available.
    return max(
        sigs,
        key=lambda s: (len(s.details.get("value_books", [])), s.strength, _best_price(s)),
    )


class DetectionPipeline:
    def __init__(self, settings: Settings, repo: Repository) -> None:
        self._settings = settings
        self._repo = repo
        self._detectors: list[BaseDetector] = [
            SteamMoveDetector(settings, repo),
            RapidChangeDetector(settings, repo),
            PinnacleDivergenceDetector(settings, repo),
            ReverseLineDetector(settings, repo),
            ExchangeMonitorDetector(settings, repo),
            ArbitrageDetector(settings, repo),
        ]
        self._blocklist: frozenset[str] = frozenset(settings.signal_blocklist)

    def _get_min_strength(self, signal_type: str, market_key: str, sport_key: str) -> float:
        """Resolve min strength via tiered lookup: market > sport > type > global."""
        s = self._settings
        # 1. Market-level (most specific)
        market_override = s.signal_market_strength_overrides.get(f"{signal_type}:{market_key}")
        if market_override is not None:
            return market_override
        # 2. Sport-level
        sport_override = s.signal_sport_strength_overrides.get(f"{signal_type}:{sport_key}")
        if sport_override is not None:
            return sport_override
        # 3. Type-level
        return s.signal_strength_overrides.get(signal_type, s.min_signal_strength)

    def _is_blocklisted(self, signal_type: str, sport_key: str, market_key: str) -> bool:
        """Check if a signal matches any blocklist pattern (2-key or 3-key)."""
        two_key = f"{signal_type}:{market_key}"
        three_key = f"{signal_type}:{sport_key}:{market_key}"
        return two_key in self._blocklist or three_key in self._blocklist

    async def run(self, fetched_at: str) -> list[Signal]:
        """Run all detectors on all events from a fetch cycle, return deduplicated signals."""
        event_ids = await self._repo.get_distinct_event_ids_at(fetched_at)
        log.info("pipeline_start", event_count=len(event_ids))

        all_signals: list[Signal] = []
        for event_id in event_ids:
            for detector in self._detectors:
                try:
                    signals = await detector.detect(event_id, fetched_at)
                    all_signals.extend(signals)
                except Exception:
                    log.exception(
                        "detector_error",
                        detector=type(detector).__name__,
                        event_id=event_id,
                    )

        # Filter by minimum strength (tiered: market > sport > type > global).
        # Arbs are exempt: their strength encodes profit% (strength = profit%/10),
        # so the generic 0.5 floor would silently drop every arb under ~5% profit.
        # Arb volume is gated at the detector by arb_min_profit_pct instead.
        overrides = self._settings.signal_strength_overrides
        global_min = self._settings.min_signal_strength
        strong_signals = [
            s for s in all_signals
            if s.signal_type == SignalType.ARBITRAGE
            or s.strength > self._get_min_strength(
                s.signal_type.value, s.market_key, s.sport_key
            )
        ]
        log.info(
            "strength_filter",
            before=len(all_signals),
            after=len(strong_signals),
            min_strength=global_min,
            overrides=overrides or None,
        )

        # Filter by maximum strength cap (drop trap signals)
        max_caps = self._settings.max_signal_strength_overrides
        if max_caps:
            before_cap = len(strong_signals)
            strong_signals = [
                s for s in strong_signals
                if s.signal_type.value not in max_caps
                or s.strength < max_caps[s.signal_type.value]
            ]
            if len(strong_signals) < before_cap:
                log.info(
                    "max_strength_filter",
                    dropped=before_cap - len(strong_signals),
                    remaining=len(strong_signals),
                )

        # Filter by signal blocklist (2-key type:market or 3-key type:sport:market)
        if self._blocklist:
            before_bl = len(strong_signals)
            strong_signals = [
                s for s in strong_signals
                if not self._is_blocklisted(
                    s.signal_type.value, s.sport_key, s.market_key
                )
            ]
            if len(strong_signals) < before_bl:
                log.info(
                    "blocklist_filter",
                    dropped=before_bl - len(strong_signals),
                    remaining=len(strong_signals),
                )

        # Suppress signal types during their configured quiet hours
        # Supports sport-specific keys ("type:sport") with fallback to type-level
        quiet_map = self._settings.signal_quiet_hours
        if quiet_map:
            now_hour = datetime.now(timezone.utc).hour
            before_quiet = len(strong_signals)
            strong_signals = [
                s for s in strong_signals
                if now_hour not in quiet_map.get(
                    f"{s.signal_type.value}:{s.sport_key}",
                    quiet_map.get(s.signal_type.value, []),
                )
            ]
            if len(strong_signals) < before_quiet:
                log.info(
                    "signal_quiet_hours_filter",
                    hour_utc=now_hour,
                    dropped=before_quiet - len(strong_signals),
                )

        # Drop all live signals — in-game line moves are noisy
        now = datetime.now(timezone.utc)
        filtered_signals = []
        live_dropped = 0
        for sig in strong_signals:
            if sig.commence_time:
                try:
                    ct = datetime.fromisoformat(sig.commence_time)
                    if ct.tzinfo is None:
                        ct = ct.replace(tzinfo=timezone.utc)
                    if now >= ct:
                        live_dropped += 1
                        continue
                except (ValueError, TypeError):
                    pass
            filtered_signals.append(sig)
        if live_dropped:
            log.info(
                "live_signal_filter",
                dropped=live_dropped,
                remaining=len(filtered_signals),
            )

        # Deduplicate opposite sides of the same market.
        # e.g. "Nuggets -7.5" and "Clippers +7.5" are mirror signals — keep only
        # the most actionable side (more value books, then higher strength).
        grouped: dict[tuple[str, str, str], list[Signal]] = defaultdict(list)
        for sig in filtered_signals:
            grouped[(sig.event_id, sig.signal_type.value, sig.market_key)].append(sig)

        deduped_signals: list[Signal] = []
        for key, sigs in grouped.items():
            if len(sigs) <= 1:
                deduped_signals.extend(sigs)
            else:
                best = _pick_best_signal(sigs)
                # Line-shopping: surface every book on the chosen side at the
                # chosen number, best price first (recommendation + alternatives).
                _merge_same_side_value_books(best, sigs)
                deduped_signals.append(best)
                log.debug(
                    "market_side_dedup",
                    event_id=key[0],
                    signal_type=key[1],
                    market=key[2],
                    kept=best.outcome_name,
                    books=[vb.get("bookmaker") for vb in best.details.get("value_books", [])],
                    dropped=[s.outcome_name for s in sigs if s is not best],
                )

        # Require actionable bet: every signal must have value books
        # Arb signals are always actionable (they have side_a/side_b instead)
        actionable = [
            s for s in deduped_signals
            if s.details.get("value_books") or s.signal_type == SignalType.ARBITRAGE
        ]
        log.info(
            "value_filter",
            before=len(deduped_signals),
            after=len(actionable),
        )

        # Deduplicate against recently sent alerts
        new_signals: list[Signal] = []
        for sig in actionable:
            already_sent = await self._repo.was_alert_sent_recently(
                event_id=sig.event_id,
                alert_type=sig.signal_type.value,
                market_key=sig.market_key,
                cooldown_minutes=self._settings.alert_cooldown_minutes,
            )
            if already_sent:
                log.debug("signal_deduped", signal_type=sig.signal_type.value, event_id=sig.event_id)
                continue
            new_signals.append(sig)

        log.info(
            "pipeline_complete",
            total_signals=len(all_signals),
            after_strength=len(strong_signals),
            live_dropped=live_dropped,
            after_side_dedup=len(deduped_signals),
            after_value_filter=len(actionable),
            new_signals=len(new_signals),
        )
        return new_signals
