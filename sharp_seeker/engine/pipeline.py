"""Detection pipeline: runs all detectors and deduplicates signals."""

from __future__ import annotations

from collections import defaultdict

import structlog

from sharp_seeker.config import Settings
from sharp_seeker.db.repository import Repository
from sharp_seeker.engine.base import BaseDetector, Signal
from sharp_seeker.engine.exchange_monitor import ExchangeMonitorDetector
from sharp_seeker.engine.pinnacle_divergence import PinnacleDivergenceDetector
from sharp_seeker.engine.rapid_change import RapidChangeDetector
from sharp_seeker.engine.reverse_line import ReverseLineDetector
from sharp_seeker.engine.steam_move import SteamMoveDetector

log = structlog.get_logger()


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
                best = max(
                    sigs,
                    key=lambda s: (len(s.details.get("value_books", [])), s.strength),
                )
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
