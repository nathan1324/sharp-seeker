"""Detection pipeline: runs all detectors and deduplicates signals."""

from __future__ import annotations

from collections import defaultdict

import structlog

from sharp_seeker.config import Settings
from sharp_seeker.db.repository import Repository
from sharp_seeker.engine.base import BaseDetector, Signal, SignalType
from sharp_seeker.engine.exchange_monitor import ExchangeMonitorDetector
from sharp_seeker.engine.pinnacle_divergence import PinnacleDivergenceDetector
from sharp_seeker.engine.rapid_change import RapidChangeDetector
from sharp_seeker.engine.reverse_line import ReverseLineDetector
from sharp_seeker.engine.steam_move import SteamMoveDetector

log = structlog.get_logger()


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

    # Fallback: most value books, then highest strength
    return max(sigs, key=lambda s: (len(s.details.get("value_books", [])), s.strength))


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
        ]

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

        # Filter by minimum strength
        min_str = self._settings.min_signal_strength
        strong_signals = [s for s in all_signals if s.strength >= min_str]
        log.info(
            "strength_filter",
            before=len(all_signals),
            after=len(strong_signals),
            min_strength=min_str,
        )

        # Deduplicate opposite sides of the same market.
        # e.g. "Nuggets -7.5" and "Clippers +7.5" are mirror signals — keep only
        # the most actionable side (more value books, then higher strength).
        grouped: dict[tuple[str, str, str], list[Signal]] = defaultdict(list)
        for sig in strong_signals:
            grouped[(sig.event_id, sig.signal_type.value, sig.market_key)].append(sig)

        deduped_signals: list[Signal] = []
        for key, sigs in grouped.items():
            if len(sigs) <= 1:
                deduped_signals.extend(sigs)
            else:
                best = _pick_best_signal(sigs)
                deduped_signals.append(best)
                log.debug(
                    "market_side_dedup",
                    event_id=key[0],
                    signal_type=key[1],
                    market=key[2],
                    kept=best.outcome_name,
                    dropped=[s.outcome_name for s in sigs if s is not best],
                )

        # Deduplicate against recently sent alerts
        new_signals: list[Signal] = []
        for sig in deduped_signals:
            already_sent = await self._repo.was_alert_sent_recently(
                event_id=sig.event_id,
                alert_type=sig.signal_type.value,
                market_key=sig.market_key,
                outcome_name=sig.outcome_name,
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
            after_side_dedup=len(deduped_signals),
            new_signals=len(new_signals),
        )
        return new_signals
