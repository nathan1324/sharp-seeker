"""Check what's actually in signal_results.details_json for MLB signals."""

import json
import sqlite3

DB = "/app/data/sharp_seeker.db"


def run():
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row

    rows = conn.execute("""
        SELECT signal_type, market_key, outcome_name, result,
               signal_strength, details_json, signal_at
        FROM signal_results
        WHERE sport_key = 'baseball_mlb'
          AND result IN ('won', 'lost', 'push')
        ORDER BY signal_at DESC
        LIMIT 20
    """).fetchall()

    for r in rows:
        details = {}
        if r["details_json"]:
            try:
                details = json.loads(r["details_json"])
            except (json.JSONDecodeError, TypeError):
                details = {"PARSE_ERROR": True}

        vb = details.get("value_books", [])
        price = vb[0].get("price") if vb else None

        print(f"{r['signal_type']:25s} {r['market_key']:8s} "
              f"{r['outcome_name']:10s} {r['result']:5s} "
              f"str={r['signal_strength']:.2f} "
              f"price={price} "
              f"vb_count={len(vb)} "
              f"has_details={'Y' if r['details_json'] else 'N'} "
              f"details_len={len(r['details_json'] or '')}")

    # Also check if any have NULL details_json
    null_count = conn.execute("""
        SELECT COUNT(*) FROM signal_results
        WHERE sport_key = 'baseball_mlb'
          AND result IN ('won', 'lost', 'push')
          AND details_json IS NULL
    """).fetchone()[0]

    empty_vb = conn.execute("""
        SELECT COUNT(*) FROM signal_results
        WHERE sport_key = 'baseball_mlb'
          AND result IN ('won', 'lost', 'push')
          AND details_json IS NOT NULL
          AND details_json NOT LIKE '%"price"%'
    """).fetchone()[0]

    print(f"\nNULL details_json: {null_count}")
    print(f"No price in details: {empty_vb}")

    conn.close()


if __name__ == "__main__":
    run()
