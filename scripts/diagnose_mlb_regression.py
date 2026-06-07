"""Diagnose whether MLB signal performance is genuinely regressing.

Separates a real MLB signal-quality decline from normal variance, and audits
whether our recent MLB bleed-fixes are actually LIVE in the running config
(a fix can be merged in code but never take effect if the server `.env` wasn't
updated — which looks exactly like "MLB got worse right when we changed stuff").

Two concrete audits:
  - Quiet-hours fix (2026-05-31): MLB signals should NOT survive the pipeline at
    12 or 13 UTC (5-6 AM MST). signal_results holds POST-filter survivors, so any
    MLB rows at those hours mean SIGNAL_QUIET_HOURS isn't applied on the server.
  - DraftKings exclusion (2026-04-25): MLB pinnacle_divergence should have ZERO
    DraftKings value-book picks. If DK shows up, the exclusion isn't live (DK on
    MLB PD historically ran 25% WR / -86.8u).

Also breaks graded MLB signals down by week (trend), market, UTC hour and book,
and splits performance before vs after 2026-05-31 to test correlation with our
change dates.

Read-only; streams rows (server is RAM-constrained). Run on server:
    docker compose exec sharp-seeker python /app/scripts/diagnose_mlb_regression.py [days]
"""

import json
import sqlite3
import sys
from collections import defaultdict
from datetime import date as _date

try:
    from sharp_seeker.config import Settings
except Exception:  # pragma: no cover - script may run without package on path
    Settings = None

DB = "/app/data/sharp_seeker.db"
SPLIT_DATE = "2026-05-31"  # last MLB config change (quiet hours restored)
BLEED_HOURS = {"12", "13"}  # 5-6 AM MST — should be quiet for MLB after 05-31


def _mlb_exposed_hours():
    """UTC hours MLB PD now fires that the generic PD quiet list would suppress.

    The sport-specific quiet-hours key REPLACES (not merges with) the generic
    `pinnacle_divergence` list, so narrowing MLB to [12,13] re-opens every hour
    in the generic list. Returns (exposed_set, generic_set, mlb_set) as str
    hours ("03".."22"), or (set(), set(), set()) if config is unavailable.
    """
    if Settings is None:
        return set(), set(), set()
    try:
        qh = Settings().signal_quiet_hours or {}
    except Exception:
        return set(), set(), set()
    generic = {f"{h:02d}" for h in qh.get("pinnacle_divergence", [])}
    mlb = {f"{h:02d}" for h in qh.get("pinnacle_divergence:baseball_mlb", [])}
    return generic - mlb, generic, mlb


def _details(dj):
    if not dj:
        return {}
    try:
        return json.loads(dj) if isinstance(dj, str) else dj
    except (json.JSONDecodeError, TypeError):
        return {}


def _price(d):
    vb = d.get("value_books") or []
    if vb:
        return vb[0].get("price")
    return None


def _book(d):
    vb = d.get("value_books") or []
    if vb:
        return vb[0].get("bookmaker")
    return d.get("us_book")


def _pnl(result, price):
    if result == "push" or price is None:
        return 0.0
    if price < 0:
        risk = abs(price) / 100.0
    elif price > 0:
        risk = 100.0 / price
    else:
        risk = 1.0
    if result == "won":
        return 1.0
    if result == "lost":
        return -risk
    return 0.0


def _acc():
    return {"u": 0.0, "w": 0, "l": 0, "p": 0}


def _add(c, result, price):
    c["u"] += _pnl(result, price)
    if result == "won":
        c["w"] += 1
    elif result == "lost":
        c["l"] += 1
    else:
        c["p"] += 1


def _fmt(c):
    decided = c["w"] + c["l"]
    n = decided + c["p"]
    wr = f"{c['w'] / decided:.0%}" if decided else "N/A"
    sign = "+" if c["u"] >= 0 else ""
    units = c["u"]
    return f"{sign}{units:.2f}u | {wr} ({c['w']}W/{c['l']}L/{c['p']}P) n={n}"


def _week(datestr):
    try:
        y, m, d = datestr.split("-")
        iso = _date(int(y), int(m), int(d)).isocalendar()
        year, wk = iso[0], iso[1]
        return f"{year}-W{wk:02d}"
    except Exception:
        return "unknown"


def run():
    days = None
    if len(sys.argv) > 1:
        try:
            days = int(sys.argv[1])
        except ValueError:
            pass

    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row

    where = "sport_key = 'baseball_mlb' AND result IN ('won','lost','push')"
    params = []
    if days:
        where += " AND signal_at >= datetime('now', ?)"
        params.append(f"-{days} days")

    query = (
        "SELECT signal_type, market_key, result, signal_at, details_json "
        "FROM signal_results WHERE " + where + " ORDER BY signal_at ASC"
    )

    overall = _acc()
    before = _acc()
    after = _acc()
    by_week = defaultdict(_acc)
    by_market = defaultdict(_acc)
    by_hour = defaultdict(_acc)
    by_book = defaultdict(_acc)
    bleed = _acc()       # MLB rows at 12/13 UTC — should be empty post 05-31
    pd_dk = _acc()       # MLB PD rows with DraftKings book — should be empty
    exposed_hours, generic_q, mlb_q = _mlb_exposed_hours()
    exposed_pd = _acc()  # MLB PD at hours other PD sports suppress (override gap)
    n = 0

    for r in conn.execute(query, params):  # cursor iteration streams, no fetchall
        d = _details(r["details_json"])
        price = _price(d)
        result = r["result"]
        sa = r["signal_at"] or ""
        date = sa[:10]
        hour = sa[11:13] if len(sa) >= 13 else "??"
        book = (_book(d) or "?").lower()
        n += 1

        _add(overall, result, price)
        _add(by_week[_week(date)], result, price)
        _add(by_market[r["market_key"]], result, price)
        _add(by_hour[hour], result, price)
        _add(by_book[book], result, price)
        _add(before if date < SPLIT_DATE else after, result, price)
        is_pd = r["signal_type"] == "pinnacle_divergence"
        # Quiet-hours fix is PD-specific: only pinnacle_divergence:baseball_mlb
        # is quiet at 12/13. Steam/Rapid legitimately fire there, so scope the
        # audit to PD or it false-alarms on other signal types.
        if is_pd and hour in BLEED_HOURS:
            _add(bleed, result, price)
        if is_pd and book == "draftkings":
            _add(pd_dk, result, price)
        if is_pd and hour in exposed_hours:
            _add(exposed_pd, result, price)

    win = f"last {days} days" if days else "all time"
    print(f"=== MLB SIGNAL REGRESSION DIAGNOSTIC ({win}, {n} graded) ===\n")
    if n == 0:
        print("No graded MLB signals in window.")
        return

    print(f"Overall: {_fmt(overall)}\n")

    print(f"=== BEFORE vs AFTER {SPLIT_DATE} (last MLB config change) ===")
    print(f"  before: {_fmt(before)}")
    print(f"  after : {_fmt(after)}\n")

    print("=== BY WEEK (trend — is it actually declining?) ===")
    for k in sorted(by_week):
        print(f"  {k}: {_fmt(by_week[k])}")

    print("\n=== BY MARKET (judge each market on its own — totals != spreads) ===")
    for k in sorted(by_market, key=lambda x: by_market[x]["u"]):
        print(f"  {k}: {_fmt(by_market[k])}")

    print("\n=== BY UTC HOUR (12/13 = 5-6 AM MST, should be quiet post 05-31) ===")
    for k in sorted(by_hour):
        flag = "  <-- BLEED HOUR" if k in BLEED_HOURS else ""
        print(f"  {k}: {_fmt(by_hour[k])}{flag}")

    print("\n=== BY BOOK (DraftKings should be absent from MLB PD) ===")
    for k in sorted(by_book, key=lambda x: by_book[x]["u"]):
        print(f"  {k}: {_fmt(by_book[k])}")

    print("\n=== AUDIT: ARE OUR MLB FIXES ACTUALLY LIVE? ===")
    bleed_n = bleed["w"] + bleed["l"] + bleed["p"]
    if bleed_n == 0:
        print("  [OK]   Quiet hours: no MLB PD signals at 12/13 UTC. Fix is live.")
    else:
        print(f"  [WARN] Quiet hours NOT live: {bleed_n} MLB PD signals survived at "
              f"12/13 UTC -> {_fmt(bleed)}")
        print("         SIGNAL_QUIET_HOURS['pinnacle_divergence:baseball_mlb'] is "
              "likely [] or missing in the server .env.")

    dk_n = pd_dk["w"] + pd_dk["l"] + pd_dk["p"]
    if dk_n == 0:
        print("  [OK]   DraftKings exclusion: no DK picks on MLB PD. Fix is live.")
    else:
        print(f"  [WARN] DK exclusion NOT live: {dk_n} MLB PD signals picked "
              f"DraftKings -> {_fmt(pd_dk)}")
        print("         PD_SPORT_EXCLUDED_BOOKS likely missing "
              "{'baseball_mlb': ['draftkings']} in the server .env.")

    print("\n=== AUDIT: override gap from narrowing MLB quiet hours to [12,13] ===")
    if not generic_q:
        print("  (config unavailable — run inside the container to load Settings)")
    else:
        exp = ",".join(sorted(exposed_hours)) or "(none)"
        print(f"  generic PD quiet: {sorted(generic_q)} | MLB override: {sorted(mlb_q)}")
        print(f"  MLB PD now FIRES at hours other PD sports suppress: [{exp}]")
        exp_n = exposed_pd["w"] + exposed_pd["l"] + exposed_pd["p"]
        if exp_n == 0:
            print("  No MLB PD signals landed in those hours -> not the cause.")
        else:
            exp_dec = exposed_pd["w"] + exposed_pd["l"]
            print(f"  MLB PD perf in those exposed hours: {_fmt(exposed_pd)}")
            if exp_dec < 30:
                print(f"  -> inconclusive: only n={exp_dec} decided across "
                      f"{len(exposed_hours)} hours (~{exp_dec // max(len(exposed_hours), 1)}/hr); "
                      "too small to act on. Re-check on a wider window.")
            elif exposed_pd["u"] < 0:
                print("  -> NEGATIVE: narrowing to [12,13] re-opened bleed hours. "
                      "Consider restoring suppression for the losing ones.")
            else:
                print("  -> Not bleeding; the exposed hours are net positive.")


if __name__ == "__main__":
    run()
