#!/usr/bin/env python3
"""
scripts/ingest_all.py

Seed or refresh the database manually without starting the web server.
Run this once before first use, or any time you want an immediate update.

Usage (from the project root):
    python scripts/ingest_all.py
"""

import os
import sys

# Make sure the project root is on the path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import date, datetime, timedelta

from app.db import get_db, init_db
from app.services import boxscores as box_svc
from app.services import games as games_svc
from app.services import injuries as inj_svc


def main() -> None:
    print("Initializing database…")
    init_db()

    yesterday = date.today() - timedelta(days=1)
    tomorrow  = date.today() + timedelta(days=1)

    # ── Scoreboard ────────────────────────────────────────────────────
    print(f"\n[1/4] Fetching scoreboard for yesterday ({yesterday})…")
    n = games_svc.fetch_and_store_games(yesterday)
    print(f"      {n} game(s) stored")

    print(f"\n[2/4] Fetching scoreboard for tomorrow ({tomorrow})…")
    n = games_svc.fetch_and_store_games(tomorrow)
    print(f"      {n} game(s) stored")

    # ── Box scores ────────────────────────────────────────────────────
    print(f"\n[3/4] Fetching box scores for yesterday's final games…")
    conn = get_db()
    final_games = conn.execute(
        "SELECT game_id FROM games WHERE game_date = ? AND status_text LIKE '%Final%'",
        (yesterday.isoformat(),),
    ).fetchall()
    conn.close()

    if final_games:
        for row in final_games:
            print(f"      game {row['game_id']}")
            box_svc.fetch_and_store_boxscore(row["game_id"])
    else:
        print("      No final games found for yesterday")

    # ── Injury report ─────────────────────────────────────────────────
    print("\n[4/4] Fetching injury report…")
    n = inj_svc.fetch_and_store_injuries()
    print(f"      {n} record(s) stored")

    # ── Timestamp ─────────────────────────────────────────────────────
    conn = get_db()
    conn.execute(
        "INSERT OR REPLACE INTO meta (key, value) VALUES ('last_updated', ?)",
        (datetime.now().strftime("%Y-%m-%d %H:%M:%S"),),
    )
    conn.commit()
    conn.close()

    print("\nDone! Start the server with: uvicorn app.main:app --reload")


if __name__ == "__main__":
    main()
