#!/usr/bin/env python3
"""
scripts/ingest_all.py

Seed or refresh the database manually without starting the web server.
All dates are based on Eastern Time (America/New_York).

Usage (from the project root):
    python scripts/ingest_all.py
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, timedelta

import pytz

from app.db import get_db, init_db
from app.services import boxscores as box_svc
from app.services import games as games_svc
from app.services import injuries as inj_svc

ET = pytz.timezone("America/New_York")


def main() -> None:
    now_et    = datetime.now(ET)
    today     = now_et.date()
    yesterday = today - timedelta(days=1)
    tomorrow  = today + timedelta(days=1)

    print(f"Eastern Time now: {now_et.strftime('%Y-%m-%d %H:%M %Z')}")
    print(f"Dates → yesterday={yesterday}  today={today}  tomorrow={tomorrow}")
    print()

    print("Initializing database…")
    init_db()

    # ── Scoreboard ────────────────────────────────────────────────────
    for label, game_date in [("yesterday", yesterday), ("today", today), ("tomorrow", tomorrow)]:
        step = {"yesterday": "1", "today": "2", "tomorrow": "3"}[label]
        print(f"\n[{step}/5] Fetching scoreboard for {label} ({game_date})…")
        n = games_svc.fetch_and_store_games(game_date)
        print(f"      → {n} game(s) stored")
        if n == 0 and label != "tomorrow":
            print(f"      ⚠  Zero games returned for {label}.")
            print("         Possible causes: no NBA games this date, nba_api timeout, or rate limit.")

    # ── Box scores ────────────────────────────────────────────────────
    print("\n[4/5] Fetching box scores for Final games (yesterday + today)…")
    conn = get_db()
    final_games = conn.execute(
        """
        SELECT game_id, game_date, home_abbr, visitor_abbr, status_text, game_status_id
        FROM   games
        WHERE  game_date IN (?, ?) AND game_status_id = 3
        """,
        (yesterday.isoformat(), today.isoformat()),
    ).fetchall()
    all_recent = conn.execute(
        """
        SELECT game_id, game_date, home_abbr, visitor_abbr, status_text, game_status_id
        FROM   games
        WHERE  game_date IN (?, ?)
        """,
        (yesterday.isoformat(), today.isoformat()),
    ).fetchall()
    conn.close()

    status_labels = {1: "Pre-game", 2: "Live", 3: "Final"}

    if all_recent and not final_games:
        live  = [r for r in all_recent if r["game_status_id"] == 2]
        pre   = [r for r in all_recent if r["game_status_id"] == 1]
        if live:
            print(f"      ℹ  {len(live)} game(s) are LIVE — box scores will update after they finish.")
        if pre:
            print(f"      ℹ  {len(pre)} game(s) not yet started.")
        print("      Status in DB:")
        for row in all_recent:
            label = status_labels.get(row["game_status_id"], "?")
            print(f"           [{row['game_date']}] {row['visitor_abbr']} @ {row['home_abbr']}: "
                  f"'{row['status_text']}' ({label})")
    elif final_games:
        for row in final_games:
            print(f"      → [{row['game_date']}] game {row['game_id']}  "
                  f"({row['visitor_abbr']} @ {row['home_abbr']})")
            box_svc.fetch_and_store_boxscore(row["game_id"])
    else:
        print("      No recent games found at all.")

    # ── Injury report ─────────────────────────────────────────────────
    print("\n[5/5] Fetching injury report…")
    n = inj_svc.fetch_and_store_injuries()
    print(f"      → {n} record(s) stored")
    if n == 0:
        print("      ⚠  Zero injury records returned. Check logs/ for details.")

    # ── Timestamp ─────────────────────────────────────────────────────
    conn = get_db()
    conn.execute(
        "INSERT OR REPLACE INTO meta (key, value) VALUES ('last_updated', ?)",
        (datetime.now(ET).strftime("%Y-%m-%d %H:%M:%S %Z"),),
    )
    conn.commit()

    # ── Final DB summary ──────────────────────────────────────────────
    print("\n── DB summary ──────────────────────────────────────────")
    for table in ("games", "team_boxscores", "player_boxscores", "injury_reports"):
        n = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"   {table:<22} {n:>5} row(s)")

    for label, d in [("yesterday", yesterday), ("today", today), ("tomorrow", tomorrow)]:
        n = conn.execute(
            "SELECT COUNT(*) FROM games WHERE game_date = ?", (d.isoformat(),)
        ).fetchone()[0]
        print(f"   games ({label:<10})   {n:>5} row(s)")

    conn.close()

    print("\nDone!")
    print("→ Start the server with:  uvicorn app.main:app --reload")
    print("→ Check DB state at:      http://localhost:8000/status")


if __name__ == "__main__":
    main()
