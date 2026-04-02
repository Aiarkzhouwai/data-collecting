"""
Fetch daily scoreboard data from nba_api and store it in the games table.

Uses ScoreboardV3 (recommended for the 2025-26 season and forward).
ScoreboardV2 is deprecated for current-season data and returns stale pre-game
times instead of live scores.

game_status_id values:
  1 = Pre-game   (score columns are NULL)
  2 = Live       (score columns contain the current score)
  3 = Final      (score columns contain the final score)
"""

import json
import time
from datetime import date

from nba_api.stats.endpoints import scoreboardv3
from nba_api.stats.static import teams as nba_teams_static

from app.db import get_db
from app.logger import logger

# Static lookup: team_id (int) → team info dict
_TEAMS: dict[int, dict] = {t["id"]: t for t in nba_teams_static.get_teams()}


def fetch_and_store_games(game_date: date) -> int:
    """
    Fetch the scoreboard for game_date and upsert each game into the DB.
    Returns the number of games stored.
    """
    date_str = game_date.strftime("%m/%d/%Y")
    logger.info("Fetching scoreboard (V3) for %s", date_str)

    try:
        board = scoreboardv3.ScoreboardV3(game_date=date_str, league_id="00", timeout=30)
        time.sleep(0.6)
        raw   = json.loads(board.get_json())
        games = raw["scoreboard"]["games"]
    except Exception as exc:
        logger.error("Failed to fetch scoreboard for %s: %s", date_str, exc, exc_info=True)
        return 0

    if not games:
        logger.info("No games found for %s", date_str)
        return 0

    conn  = get_db()
    count = 0

    for game in games:
        gid         = game["gameId"]
        status_id   = game["gameStatus"]              # 1=pre, 2=live, 3=final
        status_text = game["gameStatusText"].strip()

        home = game["homeTeam"]
        away = game["awayTeam"]

        home_tid  = home["teamId"]
        away_tid  = away["teamId"]
        home_abbr = home["teamTricode"]
        away_abbr = away["teamTricode"]

        # Scores are meaningful only once the game has started (status ≥ 2)
        home_pts = home["score"] if status_id >= 2 else None
        away_pts = away["score"] if status_id >= 2 else None

        home_info = _TEAMS.get(home_tid, {})
        away_info = _TEAMS.get(away_tid, {})
        home_name = (home_info.get("full_name")
                     or f"{home['teamCity']} {home['teamName']}")
        away_name = (away_info.get("full_name")
                     or f"{away['teamCity']} {away['teamName']}")

        try:
            conn.execute(
                """
                INSERT INTO games
                    (game_id, game_date, game_status_id, status_text,
                     home_team_id,    home_abbr,    home_name,    home_pts,
                     visitor_team_id, visitor_abbr, visitor_name, visitor_pts,
                     updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
                ON CONFLICT(game_id) DO UPDATE SET
                    game_status_id  = excluded.game_status_id,
                    status_text     = excluded.status_text,
                    home_pts        = excluded.home_pts,
                    visitor_pts     = excluded.visitor_pts,
                    updated_at      = excluded.updated_at
                """,
                (
                    gid, game_date.isoformat(), status_id, status_text,
                    home_tid, home_abbr, home_name, home_pts,
                    away_tid, away_abbr, away_name, away_pts,
                ),
            )
            count += 1

            # Store per-period scores for quarter-by-quarter analysis
            for team_id, team_data in [(home_tid, home), (away_tid, away)]:
                for p in team_data.get("periods") or []:
                    period_score = p.get("score")
                    if period_score is not None:
                        conn.execute(
                            """
                            INSERT INTO game_periods (game_id, team_id, period, score)
                            VALUES (?, ?, ?, ?)
                            ON CONFLICT(game_id, team_id, period) DO UPDATE SET
                                score = excluded.score
                            """,
                            (gid, team_id, p["period"], period_score),
                        )
        except Exception as exc:
            logger.warning("Could not store game %s: %s", gid, exc)

    conn.commit()
    conn.close()

    # Log a summary of what we stored
    status_summary = ", ".join(
        f"{g['awayTeam']['teamTricode']}@{g['homeTeam']['teamTricode']} "
        f"({g['gameStatusText'].strip()})"
        for g in games[:4]
    )
    if len(games) > 4:
        status_summary += f" +{len(games) - 4} more"
    logger.info("Stored %d game(s) for %s: %s", count, date_str, status_summary)
    return count
