"""Dump the live effective quiet-hours config and explain why a WNBA PD fired.

Answers: "should it be quiet right now, and why did a WNBA PD get through?"
Prints the polling quiet window, per-signal quiet hours, and computes the
current state for WNBA pinnacle_divergence. Read-only — no DB access.

Run on server:
    docker compose exec sharp-seeker python /app/scripts/diagnose_quiet_hours.py
"""

from datetime import datetime, timezone

from sharp_seeker.config import Settings

WNBA = "basketball_wnba"
PD = "pinnacle_divergence"


def in_polling_quiet(hour, start, end):
    if start < end:
        return start <= hour < end
    return hour >= start or hour < end


def run():
    s = Settings()
    now = datetime.now(timezone.utc)
    hour = now.hour

    print("=" * 60)
    print("  QUIET-HOURS DIAGNOSTIC")
    print("=" * 60)
    print("  Now: {iso} (UTC hour = {h})".format(iso=now.isoformat(), h=hour))
    print()

    # 1. Global polling quiet window
    start = s.quiet_hours_start
    end = s.quiet_hours_end
    polling_quiet = in_polling_quiet(hour, start, end)
    print("  [1] GLOBAL POLLING QUIET WINDOW")
    print("      quiet_hours_start = {a}  quiet_hours_end = {b}  (UTC)".format(a=start, b=end))
    print("      -> polling is currently {state} at hour {h}".format(
        state="SKIPPED (quiet)" if polling_quiet else "ACTIVE (polling runs)", h=hour))
    if not polling_quiet:
        print("      ** This is why signals can fire now: polling is ACTIVE. **")
    print()

    # 2. Per-signal quiet hours for WNBA PD
    quiet_map = s.signal_quiet_hours or {}
    sport_key = "{t}:{s}".format(t=PD, s=WNBA)
    sport_hours = quiet_map.get(sport_key)
    generic_hours = quiet_map.get(PD, [])
    effective = sport_hours if sport_hours is not None else generic_hours
    suppressed = hour in effective
    print("  [2] PER-SIGNAL QUIET HOURS (WNBA pinnacle_divergence)")
    print("      signal_quiet_hours['{k}'] = {v}".format(k=sport_key, v=sport_hours))
    print("      signal_quiet_hours['{k}'] = {v}  (generic fallback)".format(k=PD, v=generic_hours))
    print("      effective list used = {v}".format(v=effective))
    print("      -> WNBA PD is {state} by per-signal quiet hours at hour {h}".format(
        state="SUPPRESSED" if suppressed else "NOT suppressed", h=hour))
    print()

    # 3. Verdict
    print("  [3] VERDICT")
    if polling_quiet:
        print("      Polling is quiet now -> no fresh signal should have fired.")
        print("      If you just got one, it likely arrived from a poll BEFORE the")
        print("      quiet window started, or the alert is a delayed/cooldown echo.")
    else:
        print("      Polling is ACTIVE and WNBA PD has no per-signal quiet entry,")
        print("      so a WNBA PD firing now is WORKING AS CONFIGURED.")
        print("      To silence WNBA PD around this hour, either widen the polling")
        print("      window or add an entry, e.g.:")
        print("        SIGNAL_QUIET_HOURS['{k}'] = [...hours including {h}...]".format(
            k=sport_key, h=hour))
    print("=" * 60)


if __name__ == "__main__":
    run()
