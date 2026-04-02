"""
Periodic refresh scheduler using APScheduler.

All dates use Eastern Time (America/New_York) to match the NBA schedule.
Jobs run in the background while the FastAPI server is live.
The scheduler is started/stopped via the FastAPI lifespan hook in main.py.

Two jobs:
  refresh_all        — every 60 min: scoreboard + final box scores + injuries
  refresh_live_games — every 60 sec: scoreboard + box scores for any live game
"""

from datetime import datetime, timedelta

import pytz
from apscheduler.schedulers.background import BackgroundScheduler

from app.db import get_db
from app.logger import logger
from app.services import boxscores as box_svc
from app.services import games as games_svc
from app.services import injuries as inj_svc
from app.services import trailing_stats as trail_svc

ET = pytz.timezone("America/New_York")

_scheduler = BackgroundScheduler(timezone=ET)


def _et_dates():
    """Return (yesterday, today, tomorrow) as date objects in Eastern Time."""
    today = datetime.now(ET).date()
    return today - timedelta(days=1), today, today + timedelta(days=1)


def _get_live_game_ids() -> list[str]:
    """Return game_ids currently marked as live (status_id = 2) in the DB."""
    conn = get_db()
    rows = conn.execute(
        "SELECT game_id FROM games WHERE game_status_id = 2"
    ).fetchall()
    conn.close()
    return [r["game_id"] for r in rows]


# ── Public interface ──────────────────────────────────────────────────────────

def start() -> None:
    """Register all jobs and start the scheduler."""
    # Full hourly refresh (scoreboard + final box scores + injuries)
    _scheduler.add_job(
        refresh_all, "interval", hours=1, id="refresh_all",
        next_run_time=datetime.now(ET),  # run immediately on startup
    )
    # Fast live-game refresh every 60 seconds — no-ops when nothing is live
    _scheduler.add_job(
        refresh_live_games, "interval", seconds=60, id="refresh_live",
    )
    _scheduler.start()
    logger.info(
        "Scheduler started — full refresh every 60 min, live refresh every 60 sec (ET)"
    )


def stop() -> None:
    """Gracefully shut down the scheduler."""
    _scheduler.shutdown(wait=False)
    logger.info("Scheduler stopped")


# ── Jobs ──────────────────────────────────────────────────────────────────────

def refresh_live_games() -> None:
    """
    Fast refresh for in-progress games.
    Runs every 60 seconds; exits immediately when nothing is live.
    Steps:
      1. Check the DB for any game_status_id = 2 (live) games.
      2. Re-fetch today's scoreboard so scores/period/status are current.
      3. Fetch box scores (team + player) for every live game.
      4. Update the last_live_refresh meta timestamp.
    """
    live_ids = _get_live_game_ids()
    if not live_ids:
        return  # nothing live — skip quietly

    now_str = datetime.now(ET).strftime("%H:%M:%S %Z")
    logger.info("[live] %s — refreshing %d live game(s): %s",
                now_str, len(live_ids), ", ".join(live_ids))

    # 1. Refresh scoreboard so scores + period data are up to date
    _, today, _ = _et_dates()
    try:
        games_svc.fetch_and_store_games(today)
    except Exception:
        logger.exception("[live] scoreboard fetch failed")

    # 2. Refresh box scores (team + player) + per-period stats for each live game
    success = fail = 0
    for gid in live_ids:
        try:
            ok = box_svc.fetch_and_store_boxscore(gid)
            if ok:
                success += 1
            else:
                fail += 1
                logger.warning("[live] box score returned no data for game %s", gid)
        except Exception:
            fail += 1
            logger.exception("[live] box score fetch failed for game %s", gid)

        try:
            trail_svc.fetch_and_store_period_stats(gid)
        except Exception:
            logger.exception("[live] period stats fetch failed for game %s", gid)

        try:
            trail_svc.fetch_and_store_stints(gid)
        except Exception:
            logger.exception("[live] stints fetch failed for game %s", gid)

    logger.info("[live] box scores: %d ok / %d failed", success, fail)

    # 3. Update live-refresh timestamp
    conn = get_db()
    conn.execute(
        "INSERT OR REPLACE INTO meta (key, value) VALUES ('last_live_refresh', ?)",
        (datetime.now(ET).strftime("%Y-%m-%d %H:%M:%S %Z"),),
    )
    conn.commit()
    conn.close()


def refresh_all() -> None:
    """
    Master hourly refresh: scoreboard (3 days) + final box scores + injuries.
    Each step is individually guarded so one failure cannot abort the rest.
    """
    logger.info("=" * 50)
    logger.info("[full] Refresh started  [ET: %s]",
                datetime.now(ET).strftime("%Y-%m-%d %H:%M %Z"))

    yesterday, today, tomorrow = _et_dates()

    # 1. Scoreboard for yesterday, today, and tomorrow
    try:
        n_yesterday = games_svc.fetch_and_store_games(yesterday)
        n_today     = games_svc.fetch_and_store_games(today)
        n_tomorrow  = games_svc.fetch_and_store_games(tomorrow)
        logger.info(
            "[full] Scoreboard: %d yesterday / %d today / %d tomorrow",
            n_yesterday, n_today, n_tomorrow,
        )
    except Exception:
        logger.exception("[full] scoreboard fetch failed")

    # 2. Box scores for Final games on yesterday and today
    try:
        conn = get_db()
        final_games = conn.execute(
            """SELECT game_id, game_date FROM games
               WHERE  game_date IN (?, ?) AND game_status_id = 3""",
            (yesterday.isoformat(), today.isoformat()),
        ).fetchall()
        conn.close()

        logger.info("[full] Box scores: %d final game(s) to process", len(final_games))
        for row in final_games:
            try:
                box_svc.fetch_and_store_boxscore(row["game_id"])
            except Exception:
                logger.exception("[full] box score failed for game %s", row["game_id"])
            try:
                trail_svc.fetch_and_store_period_stats(row["game_id"])
            except Exception:
                logger.exception("[full] period stats failed for game %s", row["game_id"])
            try:
                trail_svc.fetch_and_store_stints(row["game_id"])
            except Exception:
                logger.exception("[full] stints failed for game %s", row["game_id"])
    except Exception:
        logger.exception("[full] final-game query failed")

    # 3. Official injury report
    try:
        n_injuries = inj_svc.fetch_and_store_injuries()
        logger.info("[full] Injuries: %d record(s) stored", n_injuries)
    except Exception:
        logger.exception("[full] injury fetch failed")

    # 4. Record timestamp
    try:
        _set_last_updated()
    except Exception:
        logger.exception("[full] could not write last_updated")

    logger.info("[full] Refresh complete")
    logger.info("=" * 50)


def _set_last_updated() -> None:
    conn = get_db()
    conn.execute(
        "INSERT OR REPLACE INTO meta (key, value) VALUES ('last_updated', ?)",
        (datetime.now(ET).strftime("%Y-%m-%d %H:%M:%S %Z"),),
    )
    conn.commit()
    conn.close()
