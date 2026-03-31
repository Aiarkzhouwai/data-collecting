"""
Fetch post-game box scores from nba_api and store them in
team_boxscores and player_boxscores.

Call fetch_and_store_boxscore(game_id) once per completed game.
It is safe to call it multiple times — rows are upserted, not duplicated.
"""

import time

from nba_api.stats.endpoints import boxscoretraditionalv2

from app.db import get_db
from app.logger import logger


def _to_rows(result_set) -> list[dict]:
    """Convert an nba_api result set object to a list of plain dicts."""
    d = result_set.get_dict()
    return [dict(zip(d["headers"], row)) for row in d["data"]]


def fetch_and_store_boxscore(game_id: str) -> bool:
    """
    Fetch and store team + player box scores for one completed game.
    Returns True on success, False on failure.
    """
    logger.info("  Fetching box score: game %s", game_id)

    try:
        box = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=game_id, timeout=30)
        time.sleep(0.6)  # stay within nba_api rate limit
        team_rows   = _to_rows(box.team_stats)
        player_rows = _to_rows(box.player_stats)
    except Exception as exc:
        logger.error("  Failed to fetch box score for %s: %s", game_id, exc, exc_info=True)
        return False

    if not team_rows:
        logger.warning("  Box score for %s returned no team data — skipping", game_id)
        return False

    conn = get_db()
    team_stored = player_stored = 0

    # ── Team box scores ───────────────────────────────────────────────────────
    for row in team_rows:
        try:
            conn.execute(
                """
                INSERT INTO team_boxscores
                    (game_id, team_id, team_abbr,
                     pts, reb, ast, stl, blk,
                     fgm, fga, fg_pct,
                     fg3m, fg3a, fg3_pct,
                     ftm, fta, ft_pct,
                     updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
                ON CONFLICT(game_id, team_id) DO UPDATE SET
                    pts      = excluded.pts,
                    reb      = excluded.reb,
                    ast      = excluded.ast,
                    stl      = excluded.stl,
                    blk      = excluded.blk,
                    fgm      = excluded.fgm,
                    fga      = excluded.fga,
                    fg_pct   = excluded.fg_pct,
                    fg3m     = excluded.fg3m,
                    fg3a     = excluded.fg3a,
                    fg3_pct  = excluded.fg3_pct,
                    ftm      = excluded.ftm,
                    fta      = excluded.fta,
                    ft_pct   = excluded.ft_pct,
                    updated_at = excluded.updated_at
                """,
                (
                    row["GAME_ID"], row["TEAM_ID"], row["TEAM_ABBREVIATION"],
                    row.get("PTS"), row.get("REB"), row.get("AST"),
                    row.get("STL"), row.get("BLK"),
                    row.get("FGM"), row.get("FGA"), row.get("FG_PCT"),
                    row.get("FG3M"), row.get("FG3A"), row.get("FG3_PCT"),
                    row.get("FTM"), row.get("FTA"), row.get("FT_PCT"),
                ),
            )
            team_stored += 1
        except Exception as exc:
            logger.warning("  Could not store team row for %s / %s: %s",
                           game_id, row.get("TEAM_ABBREVIATION"), exc)

    # ── Player box scores ─────────────────────────────────────────────────────
    for row in player_rows:
        try:
            conn.execute(
                """
                INSERT INTO player_boxscores
                    (game_id, team_id, player_id, player_name, start_position, minutes,
                     pts, reb, ast, stl, blk, turnovers,
                     fgm, fga, fg_pct,
                     fg3m, fg3a, fg3_pct,
                     ftm, fta, ft_pct,
                     plus_minus, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
                ON CONFLICT(game_id, player_id) DO UPDATE SET
                    pts        = excluded.pts,
                    reb        = excluded.reb,
                    ast        = excluded.ast,
                    stl        = excluded.stl,
                    blk        = excluded.blk,
                    turnovers  = excluded.turnovers,
                    fgm        = excluded.fgm,
                    fga        = excluded.fga,
                    fg_pct     = excluded.fg_pct,
                    fg3m       = excluded.fg3m,
                    fg3a       = excluded.fg3a,
                    fg3_pct    = excluded.fg3_pct,
                    ftm        = excluded.ftm,
                    fta        = excluded.fta,
                    ft_pct     = excluded.ft_pct,
                    plus_minus = excluded.plus_minus,
                    updated_at = excluded.updated_at
                """,
                (
                    row["GAME_ID"], row["TEAM_ID"], row["PLAYER_ID"],
                    row.get("PLAYER_NAME"), row.get("START_POSITION"), row.get("MIN"),
                    row.get("PTS"), row.get("REB"), row.get("AST"),
                    row.get("STL"), row.get("BLK"), row.get("TO"),
                    row.get("FGM"), row.get("FGA"), row.get("FG_PCT"),
                    row.get("FG3M"), row.get("FG3A"), row.get("FG3_PCT"),
                    row.get("FTM"), row.get("FTA"), row.get("FT_PCT"),
                    row.get("PLUS_MINUS"),
                ),
            )
            player_stored += 1
        except Exception as exc:
            logger.warning("  Could not store player row %s / %s: %s",
                           game_id, row.get("PLAYER_NAME"), exc)

    conn.commit()
    conn.close()

    # Log a quick summary: team names + top scorer
    team_names = " vs ".join(r.get("TEAM_ABBREVIATION", "?") for r in team_rows)
    top = max(
        (r for r in player_rows if r.get("PTS") is not None),
        key=lambda r: r.get("PTS", 0),
        default=None,
    )
    top_str = f" | top scorer: {top['PLAYER_NAME']} {top['PTS']}pts" if top else ""
    logger.info(
        "  Stored %s — %d teams, %d players%s",
        team_names, team_stored, player_stored, top_str,
    )
    return True
