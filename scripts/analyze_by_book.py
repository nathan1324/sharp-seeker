"""Analyze signal win rates by recommended bookmaker."""

import asyncio
import json
from collections import defaultdict

from sharp_seeker.config import Settings
from sharp_seeker.db.migrations import init_db
from sharp_seeker.db.repository import Repository


def _pct(won, total):
    return f"{won / total * 100:.1f}%" if total else "N/A"


def _record(b):
    decisive = b["won"] + b["lost"]
    return f"{b['won']}W-{b['lost']}L-{b['push']}P ({_pct(b['won'], decisive)})"


def _extract_book(details_json):
    """Extract the recommended bookmaker from details_json."""
    if not details_json:
        return None
    try:
        details = json.loads(details_json) if isinstance(details_json, str) else details_json
        # Try us_book first (pinnacle_divergence style)
        book = details.get("us_book")
        if book:
            return book
        # Fall back to value_books[0].bookmaker
        value_books = details.get("value_books", [])
        if value_books and value_books[0].get("bookmaker"):
            return value_books[0]["bookmaker"]
    except (json.JSONDecodeError, TypeError, ValueError):
        pass
    return None


async def main():
    s = Settings()
    db = await init_db(s.db_path)
    repo = Repository(db)

    cursor = await db.execute("""
        SELECT * FROM signal_results
        WHERE result IS NOT NULL
        ORDER BY signal_at
    """)
    rows = await cursor.fetchall()
    await db.close()

    if not rows:
        print("No resolved signals found.")
        return

    # Group by bookmaker
    by_book = defaultdict(lambda: {"won": 0, "lost": 0, "push": 0, "total": 0})
    by_book_type = defaultdict(lambda: defaultdict(lambda: {"won": 0, "lost": 0, "push": 0, "total": 0}))
    by_book_market = defaultdict(lambda: defaultdict(lambda: {"won": 0, "lost": 0, "push": 0, "total": 0}))
    no_book = {"won": 0, "lost": 0, "push": 0, "total": 0}

    for row in rows:
        d = dict(row)
        result = d["result"].lower()
        signal_type = d["signal_type"]
        market = d["market_key"]
        book = _extract_book(d.get("details_json"))

        if not book:
            if result in no_book:
                no_book[result] += 1
            no_book["total"] += 1
            continue

        book = book.lower()
        for bucket in [by_book[book], by_book_type[book][signal_type], by_book_market[book][market]]:
            if result in bucket:
                bucket[result] += 1
            bucket["total"] += 1

    # ── Overall by book ──
    print("=" * 60)
    print("WIN RATE BY BOOKMAKER")
    print("=" * 60)
    for book in sorted(by_book, key=lambda b: by_book[b]["total"], reverse=True):
        b = by_book[book]
        print(f"  {book:15s} {_record(b):30s} (n={b['total']})")
    if no_book["total"]:
        print(f"  {'(no book)':15s} {_record(no_book):30s} (n={no_book['total']})")
    print()

    # ── By book × signal type ──
    print("=" * 60)
    print("BY BOOKMAKER × SIGNAL TYPE")
    print("=" * 60)
    for book in sorted(by_book, key=lambda b: by_book[b]["total"], reverse=True):
        print(f"\n  {book}:")
        for sig_type in sorted(by_book_type[book], key=lambda t: by_book_type[book][t]["total"], reverse=True):
            b = by_book_type[book][sig_type]
            print(f"    {sig_type:25s} {_record(b):30s} (n={b['total']})")
    print()

    # ── By book × market ──
    print("=" * 60)
    print("BY BOOKMAKER × MARKET")
    print("=" * 60)
    for book in sorted(by_book, key=lambda b: by_book[b]["total"], reverse=True):
        print(f"\n  {book}:")
        for market in sorted(by_book_market[book]):
            b = by_book_market[book][market]
            print(f"    {market:15s} {_record(b):30s} (n={b['total']})")
    print()


if __name__ == "__main__":
    asyncio.run(main())
