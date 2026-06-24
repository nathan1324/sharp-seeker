"""LIVE probe: when a PD totals divergence is juiced worse than -115, does a
better-EV alt rung exist? (read-only on our DB; makes a few Odds API calls)

This is the prototype of the proposed feature. It does NOT touch the pipeline.
For UPCOMING games it:

  1. Bulk-fetches current totals (1 credit/sport) and finds PD-style totals
     divergences: a US book whose total beats Pinnacle by >= MIN_DELTA, where the
     value-side price is worse than -115 (the alt-lookup trigger).
  2. For each trigger, makes a targeted per-event call for `alternate_totals`
     (US book + Pinnacle, region us) -> ~2 credits/event.
  3. De-vigs Pinnacle's ladder to a fair prob at each total, then computes the
     EV of every US rung on the value side: EV = fair_prob * decimal_odds - 1.
  4. Reports the best-EV rung vs the main-line rung, so we can see whether
     switching actually IMPROVES EV (not just lowers the price).

Why EV-vs-Pinnacle and not "just find a cheaper price": moving Over 8.5(-130) to
Over 9.0(-105) can erase the entire edge if 9.0 is Pinnacle's fair number. The
juice partly pays for the cushion below fair; a rung only helps if its EV beats
the main line.

Credits: ~1/sport for discovery + ~2/triggered-event. Bounded by MAX_EVENTS.

Usage (server, during active hours so games are upcoming):
  docker compose exec sharp-seeker python /app/scripts/probe_alt_lines_for_juiced_pd.py [sport] [max_events] [trigger] [min_delta]
  (defaults: sport=baseball_mlb, max_events=20, trigger=-126, min_delta=1.0;
   pass "all" to sweep configured sports. trigger=-126 targets the only band that
   actually loses per analyze_juiced_pd_totals.py; for NBA/NHL pass min_delta 0.5.)
"""

from __future__ import annotations

import sys

import httpx

from sharp_seeker.config import Settings

SPORT_ARG = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] else "baseball_mlb"
MAX_EVENTS = int(sys.argv[2]) if len(sys.argv) > 2 and sys.argv[2] else 20

# Value-side price worse than this fires the lookup. Default -126: the 60d split
# showed -116..-125 is our BEST band (+31.69u) and must be left alone; only -126
# and worse actually loses (33 plays, -5.77u, WR below break-even).
JUICE_TRIGGER = int(sys.argv[3]) if len(sys.argv) > 3 and sys.argv[3] else -126
# US-vs-Pinnacle total gap to count as a divergence (MLB 1.0; NBA/NHL 0.5).
MIN_DELTA = float(sys.argv[4]) if len(sys.argv) > 4 and sys.argv[4] else 1.0
PIN = "pinnacle"
# PD-active US books (betmgm + williamhill_us are excluded from PD per config).
PD_US = ("draftkings", "fanduel", "betrivers")
PROBE_BOOKS = (PIN,) + PD_US


def imp(price):
    """American -> implied probability."""
    if price is None:
        return None
    if price < 0:
        return abs(price) / (abs(price) + 100.0)
    return 100.0 / (price + 100.0)


def dec(price):
    """American -> decimal odds."""
    if price is None:
        return None
    if price < 0:
        return 1.0 + 100.0 / abs(price)
    return 1.0 + price / 100.0


def better_value(side, us_point, pin_point):
    """Totals: Over is better at a LOWER number, Under at a HIGHER number."""
    if side == "Over":
        return us_point < pin_point
    return us_point > pin_point


def get(client, key, params):
    params = dict(params)
    params["apiKey"] = key
    r = client.get(params.pop("_path"), params=params)
    r.raise_for_status()
    used = r.headers.get("x-requests-used")
    remaining = r.headers.get("x-requests-remaining")
    return r.json(), used, remaining


def totals_by_book(event):
    """{book_key: {"Over": {point: price}, "Under": {point: price}}} from one
    event's `totals` + `alternate_totals` markets."""
    out = {}
    for bm in event.get("bookmakers", []):
        ladder = {"Over": {}, "Under": {}}
        for mkt in bm.get("markets", []):
            if mkt.get("key") not in ("totals", "alternate_totals"):
                continue
            for oc in mkt.get("outcomes", []):
                name = oc.get("name")
                pt = oc.get("point")
                pr = oc.get("price")
                if name in ladder and pt is not None and pr is not None:
                    ladder[name][pt] = pr
        if ladder["Over"] or ladder["Under"]:
            out[bm["key"]] = ladder
    return out


def pin_fair(pin_ladder, side, point):
    """De-vigged Pinnacle fair prob for `side` at `point` (None if not 2-way)."""
    over_p = pin_ladder.get("Over", {}).get(point)
    under_p = pin_ladder.get("Under", {}).get(point)
    io, iu = imp(over_p), imp(under_p)
    if io is None or iu is None or (io + iu) == 0:
        return None
    fair_over = io / (io + iu)
    return fair_over if side == "Over" else (1.0 - fair_over)


def find_triggers(events):
    """Find current juiced PD-style totals divergences from a bulk totals fetch.

    Returns list of dicts: event_id, teams, commence, book, side, us_point,
    us_price, pin_point.
    """
    triggers = []
    for ev in events:
        books = totals_by_book(ev)
        pin = books.get(PIN)
        if not pin:
            continue
        # Pinnacle main total = its single non-alt number; bulk has only `totals`,
        # so each side has exactly one point here.
        for side in ("Over", "Under"):
            pin_points = list(pin.get(side, {}).keys())
            if len(pin_points) != 1:
                continue
            pin_point = pin_points[0]
            for bk in PD_US:
                us = books.get(bk)
                if not us:
                    continue
                us_pts = list(us.get(side, {}).keys())
                if len(us_pts) != 1:
                    continue
                us_point = us_pts[0]
                us_price = us[side][us_point]
                if abs(us_point - pin_point) < MIN_DELTA:
                    continue
                if not better_value(side, us_point, pin_point):
                    continue
                if not (us_price < JUICE_TRIGGER):
                    continue
                triggers.append({
                    "event_id": ev["id"],
                    "teams": ev.get("away_team", "?") + " @ " + ev.get("home_team", "?"),
                    "commence": ev.get("commence_time", ""),
                    "book": bk,
                    "side": side,
                    "us_point": us_point,
                    "us_price": us_price,
                    "pin_point": pin_point,
                })
    return triggers


def evaluate(client, key, base, sport, trig):
    """Pull alt ladders for one trigger and rank US rungs by EV vs Pinnacle fair."""
    path = "/sports/" + sport + "/events/" + trig["event_id"] + "/odds"
    data, used, remaining = get(client, key, {
        "_path": path,
        "regions": "us",
        "bookmakers": PIN + "," + trig["book"],
        "markets": "totals,alternate_totals",
        "oddsFormat": "american",
    })
    books = totals_by_book(data)
    pin = books.get(PIN, {})
    us = books.get(trig["book"], {})
    side = trig["side"]

    rungs = []
    for point, price in sorted(us.get(side, {}).items()):
        fair = pin_fair(pin, side, point)
        d = dec(price)
        if fair is None or d is None:
            continue
        rungs.append({"point": point, "price": price, "fair": fair, "ev": fair * d - 1.0})

    main_ev = None
    for r in rungs:
        if r["point"] == trig["us_point"] and r["price"] == trig["us_price"]:
            main_ev = r["ev"]
            break
    if main_ev is None:
        f = pin_fair(pin, side, trig["us_point"])
        d = dec(trig["us_price"])
        main_ev = (f * d - 1.0) if (f is not None and d is not None) else None

    best = max(rungs, key=lambda r: r["ev"]) if rungs else None
    return rungs, main_ev, best, remaining


def main():
    settings = Settings()
    sports = settings.sports if SPORT_ARG == "all" else [SPORT_ARG]
    base = settings.odds_api_base_url
    key = settings.odds_api_key

    client = httpx.Client(base_url=base, timeout=30.0)
    last_remaining = None
    n_events = 0
    n_improved = 0

    try:
        for sport in sports:
            try:
                events, _, rem = get(client, key, {
                    "_path": "/sports/" + sport + "/odds",
                    "bookmakers": ",".join(PROBE_BOOKS),
                    "markets": "totals",
                    "oddsFormat": "american",
                })
            except httpx.HTTPStatusError as exc:
                print("[" + sport + "] bulk fetch failed: " + str(exc.response.status_code))
                continue
            last_remaining = rem or last_remaining
            triggers = find_triggers(events)
            print("\n=== " + sport + " ===")
            print("Juiced PD-style totals triggers (price worse than "
                  + str(JUICE_TRIGGER) + ", delta >= " + str(MIN_DELTA) + "): "
                  + str(len(triggers)))

            for trig in triggers:
                if n_events >= MAX_EVENTS:
                    print("\n[reached MAX_EVENTS=" + str(MAX_EVENTS) + " - stopping to bound credits]")
                    break
                n_events += 1
                try:
                    rungs, main_ev, best, rem = evaluate(client, key, base, sport, trig)
                except httpx.HTTPStatusError as exc:
                    print("  ! event fetch failed (" + str(exc.response.status_code) + "): " + trig["teams"])
                    continue
                last_remaining = rem or last_remaining

                hdr = ("  " + trig["teams"] + "  | " + trig["book"] + " "
                       + trig["side"] + " " + str(trig["us_point"]) + " @ "
                       + str(trig["us_price"]) + "  (Pin " + str(trig["pin_point"]) + ")")
                print(hdr)
                if main_ev is None:
                    print("    main-line EV: n/a (no Pinnacle ref at that total)")
                else:
                    print("    main-line EV: " + format(main_ev, "+.3f"))
                if not rungs:
                    print("    no US alt rungs with a Pinnacle reference")
                    continue

                # Show the EV-best rung and the cheapest rung that still beats main EV.
                if best is not None:
                    tag = ""
                    if main_ev is not None and best["ev"] > main_ev + 1e-9 \
                            and (best["point"] != trig["us_point"] or best["price"] != trig["us_price"]):
                        tag = "   <-- better than main line"
                        n_improved += 1
                    print("    best-EV rung: " + trig["side"] + " " + str(best["point"])
                          + " @ " + str(best["price"]) + "   EV " + format(best["ev"], "+.3f")
                          + "  (fair " + format(best["fair"], ".1%") + ")" + tag)

                # Cheaper-price rungs (<= -115) ranked by EV, top 3, for eyeballing.
                cheaper = sorted(
                    [r for r in rungs if r["price"] >= JUICE_TRIGGER],
                    key=lambda r: r["ev"], reverse=True,
                )[:3]
                if cheaper:
                    line = "    rungs <= -115: " + ", ".join(
                        str(r["point"]) + "@" + str(r["price"]) + "(EV" + format(r["ev"], "+.3f") + ")"
                        for r in cheaper
                    )
                    print(line)
            if n_events >= MAX_EVENTS:
                break
    finally:
        client.close()

    print("\n=== SUMMARY ===")
    print("Triggers evaluated: " + str(n_events))
    print("Where an alt rung beats the main-line EV: " + str(n_improved))
    if last_remaining is not None:
        print("API credits remaining: " + str(last_remaining))
    print("\nIf 'improved' is a meaningful share, the alt feature earns its place;")
    print("pick the decision rule (best-EV rung, or best EV among rungs <= -115).")


if __name__ == "__main__":
    main()
