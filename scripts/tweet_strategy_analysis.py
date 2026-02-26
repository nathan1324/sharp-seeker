"""Analyze signal data to find the optimal X tweet strategy.

Answers:
  1. What strength threshold should tweets use? (strength brackets)
  2. Which market types win most? (best for free plays)
  3. Which signal_type × market combos are strongest?
  4. How are free plays actually performing?
  5. Which hours have the best win rate? (teaser window tuning)
  6. Live vs pre-game performance
  7. Recommended tweet filters based on all of the above

Run on server:
  docker compose exec sharp-seeker python /app/scripts/tweet_strategy_analysis.py
"""

import asyncio
import json

from sharp_seeker.config import Settings
from sharp_seeker.db.migrations import init_db
from sharp_seeker.db.repository import Repository


def _pct(won, total):
    return f"{won / total * 100:.1f}%" if total else "—"


def _record(b):
    decided = b["won"] + b["lost"]
    return f"{b['won']}W-{b['lost']}L-{b['push']}P ({_pct(b['won'], decided)})"


def _bucket(sigs):
    b = {"won": 0, "lost": 0, "push": 0}
    for s in sigs:
        r = s["result"]
        if r in b:
            b[r] += 1
    return b


def _sport_label(sport_key):
    parts = sport_key.split("_", 1)
    return parts[-1].upper() if len(parts) > 1 else sport_key.upper()


STRENGTH_BRACKETS = [
    (0.50, 0.60, "0.50–0.59"),
    (0.60, 0.70, "0.60–0.69"),
    (0.70, 0.80, "0.70–0.79"),
    (0.80, 0.90, "0.80–0.89"),
    (0.90, 1.01, "0.90–1.00"),
]

MARKET_LABELS = {"spreads": "Spreads", "totals": "Totals", "h2h": "Moneyline"}


async def main():
    s = Settings()
    db = await init_db(s.db_path)
    repo = Repository(db)

    # ── Pull all resolved signals ──
    rows = await repo.get_resolved_signals_since("2000-01-01T00:00:00+00:00")
    if not rows:
        print("No resolved signals found.")
        return

    signals = []
    for row in rows:
        d = dict(row)
        signal_at = d.get("signal_at", "")
        hour_utc = int(signal_at[11:13]) if len(signal_at) >= 13 else -1
        signals.append({
            "result": d["result"].lower(),
            "signal_type": d["signal_type"],
            "market": d["market_key"],
            "sport": _sport_label(d.get("sport_key", "") or ""),
            "strength": d.get("signal_strength", 0.0),
            "hour_utc": hour_utc,
            "is_live": d.get("is_live"),
            "event_id": d["event_id"],
        })

    # ── Pull free play results ──
    fp_rows = await repo.get_free_play_results_since("2000-01-01T00:00:00+00:00")
    free_plays = []
    for row in fp_rows:
        d = dict(row)
        free_plays.append({
            "result": (d.get("result") or "pending").lower(),
            "outcome": d["outcome_name"],
            "market": d["market_key"],
            "strength": d.get("signal_strength", 0.0),
            "sent_at": d.get("sent_at", ""),
        })

    await db.close()

    # Only Pinnacle Divergence signals are tweeted — filter for relevance
    pd_signals = [s for s in signals if s["signal_type"] == "pinnacle_divergence"]

    print("=" * 65)
    print("TWEET STRATEGY ANALYSIS")
    print("=" * 65)
    print(f"Total resolved signals: {len(signals)}")
    print(f"Pinnacle Divergence (tweetable): {len(pd_signals)}")
    print(f"Free plays posted: {len(free_plays)}")
    print()

    # ── 1. Win rate by strength bracket (all signals + PD only) ──
    print("=" * 65)
    print("1. WIN RATE BY STRENGTH BRACKET")
    print("   → Should tweets require a higher strength floor?")
    print("-" * 65)
    print(f"  {'Bracket':<12} {'All Signals':<28} {'Pinnacle Div Only'}")
    print(f"  {'-------':<12} {'-----------':<28} {'------------------'}")
    for lo, hi, label in STRENGTH_BRACKETS:
        all_b = _bucket([s for s in signals if lo <= s["strength"] < hi])
        pd_b = _bucket([s for s in pd_signals if lo <= s["strength"] < hi])
        all_n = all_b["won"] + all_b["lost"] + all_b["push"]
        pd_n = pd_b["won"] + pd_b["lost"] + pd_b["push"]
        all_str = f"{_record(all_b)} n={all_n}" if all_n else "—"
        pd_str = f"{_record(pd_b)} n={pd_n}" if pd_n else "—"
        print(f"  {label:<12} {all_str:<28} {pd_str}")
    print()

    # ── 2. Win rate by market type (PD only) ──
    print("=" * 65)
    print("2. WIN RATE BY MARKET TYPE (Pinnacle Divergence)")
    print("   → Which markets should free plays feature?")
    print("-" * 65)
    for mkt in sorted(set(s["market"] for s in pd_signals)):
        b = _bucket([s for s in pd_signals if s["market"] == mkt])
        n = b["won"] + b["lost"] + b["push"]
        label = MARKET_LABELS.get(mkt, mkt)
        print(f"  {label:<12} {_record(b)}  (n={n})")
    print()

    # ── 3. Signal type × market (all signals) ──
    print("=" * 65)
    print("3. SIGNAL TYPE × MARKET (best combos)")
    print("   → Which detector+market combos are most profitable?")
    print("-" * 65)
    combos = {}
    for s in signals:
        key = (s["signal_type"], s["market"])
        combos.setdefault(key, []).append(s)

    # Sort by win rate descending
    combo_stats = []
    for (st, mkt), sigs in combos.items():
        b = _bucket(sigs)
        decided = b["won"] + b["lost"]
        wr = b["won"] / decided if decided else 0
        n = b["won"] + b["lost"] + b["push"]
        combo_stats.append((st, mkt, b, wr, n))

    combo_stats.sort(key=lambda x: (-x[3], -x[4]))
    for st, mkt, b, wr, n in combo_stats:
        label = MARKET_LABELS.get(mkt, mkt)
        print(f"  {st:<25} {label:<12} {_record(b)}  (n={n})")
    print()

    # ── 4. Free play track record ──
    print("=" * 65)
    print("4. FREE PLAY TRACK RECORD")
    print("   → How are the publicly visible picks doing?")
    print("-" * 65)
    resolved_fp = [f for f in free_plays if f["result"] in ("won", "lost", "push")]
    pending_fp = [f for f in free_plays if f["result"] == "pending"]
    if resolved_fp:
        b = _bucket(resolved_fp)
        print(f"  Resolved: {_record(b)}  (n={len(resolved_fp)})")
    else:
        print("  No resolved free plays yet.")
    if pending_fp:
        print(f"  Pending:  {len(pending_fp)}")

    # Free plays by market
    if resolved_fp:
        print()
        print("  By market:")
        for mkt in sorted(set(f["market"] for f in resolved_fp)):
            b = _bucket([f for f in resolved_fp if f["market"] == mkt])
            n = b["won"] + b["lost"] + b["push"]
            label = MARKET_LABELS.get(mkt, mkt)
            print(f"    {label:<12} {_record(b)}  (n={n})")
    print()

    # ── 5. Win rate by hour (PD only, for teaser window tuning) ──
    print("=" * 65)
    print("5. WIN RATE BY HOUR — Pinnacle Divergence")
    print("   → Which hours should teaser windows target?")
    print("-" * 65)
    print(f"  {'UTC':<7} {'MST':<7} {'Record':<24} {'n'}")
    print(f"  {'---':<7} {'---':<7} {'------':<24} {'-'}")
    hours = sorted(set(s["hour_utc"] for s in pd_signals if s["hour_utc"] >= 0))
    for h in hours:
        mst = (h - 7) % 24
        sigs = [s for s in pd_signals if s["hour_utc"] == h]
        b = _bucket(sigs)
        n = b["won"] + b["lost"] + b["push"]
        decided = b["won"] + b["lost"]
        wr = b["won"] / decided * 100 if decided else 0
        bar = "█" * int(wr / 5) if decided >= 3 else ""
        print(f"  {h:02d}:00   {mst:02d}:00   {_record(b):<24} n={n}  {bar}")
    print()

    # ── 6. Live vs pre-game (PD only) ──
    print("=" * 65)
    print("6. LIVE vs PRE-GAME (Pinnacle Divergence)")
    print("   → Should tweets filter out live signals?")
    print("-" * 65)
    pre = [s for s in pd_signals if s["is_live"] == 0]
    live = [s for s in pd_signals if s["is_live"] == 1]
    unknown = [s for s in pd_signals if s["is_live"] is None]
    if pre:
        b = _bucket(pre)
        print(f"  Pre-game: {_record(b)}  (n={b['won']+b['lost']+b['push']})")
    if live:
        b = _bucket(live)
        print(f"  Live:     {_record(b)}  (n={b['won']+b['lost']+b['push']})")
    if unknown:
        b = _bucket(unknown)
        print(f"  Unknown:  {_record(b)}  (n={b['won']+b['lost']+b['push']})")
    print()

    # ── 7. Win rate by sport (PD only) ──
    print("=" * 65)
    print("7. WIN RATE BY SPORT (Pinnacle Divergence)")
    print("   → Which sports are best for public tweets?")
    print("-" * 65)
    sports = sorted(set(s["sport"] for s in pd_signals))
    for sp in sports:
        b = _bucket([s for s in pd_signals if s["sport"] == sp])
        n = b["won"] + b["lost"] + b["push"]
        print(f"  {sp:<12} {_record(b)}  (n={n})")
    print()

    # ── 8. Strength × market (PD only) — best free play combos ──
    print("=" * 65)
    print("8. STRENGTH × MARKET (Pinnacle Divergence)")
    print("   → Best combos for free play selection")
    print("-" * 65)
    for lo, hi, bracket_label in STRENGTH_BRACKETS:
        bracket_sigs = [s for s in pd_signals if lo <= s["strength"] < hi]
        if not bracket_sigs:
            continue
        print(f"\n  {bracket_label}:")
        for mkt in sorted(set(s["market"] for s in bracket_sigs)):
            b = _bucket([s for s in bracket_sigs if s["market"] == mkt])
            n = b["won"] + b["lost"] + b["push"]
            label = MARKET_LABELS.get(mkt, mkt)
            print(f"    {label:<12} {_record(b)}  (n={n})")
    print()

    # ── 9. Recommendations summary ──
    print("=" * 65)
    print("RECOMMENDATIONS (based on data above)")
    print("=" * 65)

    # Best strength bracket for PD
    best_bracket = None
    best_wr = 0
    for lo, hi, label in STRENGTH_BRACKETS:
        b = _bucket([s for s in pd_signals if lo <= s["strength"] < hi])
        decided = b["won"] + b["lost"]
        if decided >= 5:
            wr = b["won"] / decided
            if wr > best_wr:
                best_wr = wr
                best_bracket = (lo, label, wr, decided)

    if best_bracket:
        print(f"  • Best PD strength bracket: {best_bracket[1]} "
              f"({best_bracket[2]:.0%} win rate, {best_bracket[3]} decided)")
        print(f"    → Consider X_TWEET_MIN_STRENGTH={best_bracket[0]:.2f} "
              f"(if implemented)")
    print()

    # Best market for PD
    best_mkt = None
    best_mkt_wr = 0
    for mkt in set(s["market"] for s in pd_signals):
        b = _bucket([s for s in pd_signals if s["market"] == mkt])
        decided = b["won"] + b["lost"]
        if decided >= 5:
            wr = b["won"] / decided
            if wr > best_mkt_wr:
                best_mkt_wr = wr
                best_mkt = (mkt, wr, decided)

    if best_mkt:
        label = MARKET_LABELS.get(best_mkt[0], best_mkt[0])
        print(f"  • Best PD market type: {label} "
              f"({best_mkt[1]:.0%} win rate, {best_mkt[2]} decided)")
        print(f"    → Prioritize {label.lower()} for free play tweets")
    print()

    # Best hours for PD (>=55% win rate with n>=5)
    good_hours = []
    for h in hours:
        sigs = [s for s in pd_signals if s["hour_utc"] == h]
        b = _bucket(sigs)
        decided = b["won"] + b["lost"]
        if decided >= 5:
            wr = b["won"] / decided
            if wr >= 0.55:
                mst = (h - 7) % 24
                good_hours.append((h, mst, wr, decided))

    if good_hours:
        utc_list = [h[0] for h in good_hours]
        print(f"  • Best PD hours (≥55% WR, n≥5): {utc_list}")
        for h, mst, wr, n in good_hours:
            print(f"    {h:02d}:00 UTC ({mst:02d}:00 MST) — {wr:.0%} over {n} decided")
        print(f"    → Consider X_TEASER_HOURS={json.dumps(utc_list)}")
    print()

    # Live vs pre-game recommendation
    if pre and live:
        pre_b = _bucket(pre)
        live_b = _bucket(live)
        pre_decided = pre_b["won"] + pre_b["lost"]
        live_decided = live_b["won"] + live_b["lost"]
        if pre_decided >= 5 and live_decided >= 5:
            pre_wr = pre_b["won"] / pre_decided
            live_wr = live_b["won"] / live_decided
            if pre_wr > live_wr + 0.05:
                print(f"  • Pre-game ({pre_wr:.0%}) outperforms live ({live_wr:.0%})")
                print("    → Consider filtering out live signals from tweets")
            elif live_wr > pre_wr + 0.05:
                print(f"  • Live ({live_wr:.0%}) outperforms pre-game ({pre_wr:.0%})")
                print("    → Live signals are fine to tweet")
            else:
                print(f"  • Pre-game ({pre_wr:.0%}) ≈ Live ({live_wr:.0%})")
                print("    → No need to filter by live/pre-game")
    print()


if __name__ == "__main__":
    asyncio.run(main())
