"""
Fetch daily scoreboard data from nba_api and store it in the games table.
Covers both completed games (with scores) and scheduled games (score = NULL).
"""

import time
from datetime import date

from nba_api.stats.endpoints import scoreboardv2
from nba_api.stats.static import teams as nba_teams_static

from app.db import get_db
from app.logger import logger

# Build a static lookup: team_id (int) -> team info dict
_TEAMS: dict[int, dict] = {t["id"]: t for t in nba_teams_static.get_teams()}


def _to_rows(result_set) -> list[dict]:
    """Convert an nba_api result set to a list of plain dicts."""
    d = result_set.get_dict()
    headers = d["headers"]
    return [dict(zip(headers, row)) for row in d["data"]]


def fetch_and_store_games(game_date: date) -> int:
    """
    Fetch the scoreboard for game_date and upsert each game into the DB.
    Returns the number of games stored.
    """
    date_str = game_date.strftime("%m/%d/%Y")
    logger.info("Fetching scoreboard for %s", date_str)

    try:
        board = scoreboardv2.ScoreboardV2(game_date=date_str, timeout=30)
        time.sleep(0.6)  # stay within nba_api rate limit
        game_headers = _to_rows(board.game_header)
        line_scores = _to_rows(board.line_score)
    except Exception as exc:
        logger.error("Failed to fetch scoreboard for %s: %s", date_str, exc, exc_info=True)
        return 0

    # Index line scores by (game_id, team_id) for fast lookup
    lines: dict[tuple, dict] = {}
    for line in line_scores:
        lines[(line["GAME_ID"], line["TEAM_ID"])] = line

    conn = get_db()
    count = 0
    for game in game_headers:
        gid = game["GAME_ID"]
        home_tid = game.get("HOME_TEAM_ID")
        visitor_tid = game.get("VISITOR_TEAM_ID")

        home_line = lines.get((gid, home_tid), {})
        visitor_line = lines.get((gid, visitor_tid), {})

        home_team = _TEAMS.get(home_tid, {})
        visitor_team = _TEAMS.get(visitor_tid, {})

        try:
            conn.execute(
                """
                INSERT INTO games
                    (game_id, game_date, status_text,
                     home_team_id, home_abbr, home_name, home_pts,
                     visitor_team_id, visitor_abbr, visitor_name, visitor_pts,
                     updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
                ON CONFLICT(game_id) DO UPDATE SET
                    status_text     = excluded.status_text,
                    home_pts        = excluded.home_pts,
                    visitor_pts     = excluded.visitor_pts,
                    updated_at      = excluded.updated_at
                """,
                (
                    gid,
                    game_date.isoformat(),
                    (game.get("GAME_STATUS_TEXT") or "").strip(),
                    home_tid,
                    home_line.get("TEAM_ABBREVIATION") or home_team.get("abbreviation"),
                    home_team.get("full_name") or home_line.get("TEAM_CITY_NAME"),
                    home_line.get("PTS"),
                    visitor_tid,
                    visitor_line.get("TEAM_ABBREVIATION") or visitor_team.get("abbreviation"),
                    visitor_team.get("full_name") or visitor_line.get("TEAM_CITY_NAME"),
                    visitor_line.get("PTS"),
                ),
            )
            count += 1
        except Exception as exc:
            logger.warning("Could not store game %s: %s", gid, exc)

    conn.commit()
    conn.close()
    logger.info("Stored %d game(s) for %s", count, date_str)
    return count
