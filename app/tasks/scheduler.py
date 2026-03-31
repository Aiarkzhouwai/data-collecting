"""
Periodic refresh scheduler using APScheduler.

Jobs run in the background while the FastAPI server is live.
The scheduler is started/stopped via the FastAPI lifespan hook in main.py.
"""

from datetime import date, datetime, timedelta

from apscheduler.schedulers.background import BackgroundScheduler

from app.db import get_db
from app.logger import logger
from app.services import boxscores as box_svc
from app.services import games as games_svc
from app.services import injuries as inj_svc

_scheduler = BackgroundScheduler(timezone="America/New_York")


# ── Public interface ──────────────────────────────────────────────────────────

def start() -> None:
    """Register all jobs and start the scheduler."""
    _scheduler.add_job(refresh_all, "interval", hours=1, id="refresh_all",
                       next_run_time=datetime.now())  # also run immediately on startup
    _scheduler.start()
    logger.info("Scheduler started — data will refresh every hour")


def stop() -> None:
    """Gracefully shut down the scheduler."""
    _scheduler.shutdown(wait=False)
    logger.info("Scheduler stopped")


# ── Jobs ──────────────────────────────────────────────────────────────────────

def refresh_all() -> None:
    """Master refresh job: games → box scores → injuries → timestamp."""
    logger.info("=" * 50)
    logger.info("Refresh started")

    yesterday = date.today() - timedelta(days=1)
    tomorrow = date.today() + timedelta(days=1)

    # 1. Scoreboard for yesterday (final scores) and tomorrow (schedule)
    games_svc.fetch_and_store_games(yesterday)
    games_svc.fetch_and_store_games(tomorrow)

    # 2. Box scores for yesterday's finished games
    conn = get_db()
    final_games = conn.execute(
        "SELECT game_id FROM games WHERE game_date = ? AND status_text LIKE '%Final%'",
        (yesterday.isoformat(),),
    ).fetchall()
    conn.close()

    for row in final_games:
        box_svc.fetch_and_store_boxscore(row["game_id"])

    # 3. Official injury report
    inj_svc.fetch_and_store_injuries()

    # 4. Record timestamp
    _set_last_updated()

    logger.info("Refresh complete")
    logger.info("=" * 50)


def _set_last_updated() -> None:
    conn = get_db()
    conn.execute(
        "INSERT OR REPLACE INTO meta (key, value) VALUES ('last_updated', ?)",
        (datetime.now().strftime("%Y-%m-%d %H:%M:%S"),),
    )
    conn.commit()
    conn.close()
