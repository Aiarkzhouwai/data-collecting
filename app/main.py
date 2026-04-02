"""
app/main.py — FastAPI application entry point.

All dates are anchored to Eastern Time (America/New_York) so that
"today", "yesterday", and "tomorrow" match the NBA schedule perspective
regardless of the server's local timezone.

Run from the project root:
    uvicorn app.main:app --reload
"""

import json
import os
import time
from contextlib import asynccontextmanager
from datetime import timedelta

import pytz
from datetime import datetime

from fastapi import BackgroundTasks, FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.db import get_db, init_db
from app.logger import logger
from app.services import logos as logos_svc
from app.services import espn_players as espn_svc
from app.services import trailing_stats as trail_svc
from app.tasks import scheduler

# ── Paths ─────────────────────────────────────────────────────────────────────

_HERE = os.path.dirname(os.path.abspath(__file__))
templates = Jinja2Templates(directory=os.path.join(_HERE, "templates"))

# ── Timezone ──────────────────────────────────────────────────────────────────

ET = pytz.timezone("America/New_York")


def _et_dates():
    """Return (yesterday, today, tomorrow) as date objects in Eastern Time."""
    today_et = datetime.now(ET).date()
    return (
        today_et - timedelta(days=1),
        today_et,
        today_et + timedelta(days=1),
    )


def _game_rows(conn, game_date, *, with_boxscore: bool = False):
    """
    Query games for a given date and attach logo URLs.
    If with_boxscore=True, LEFT JOIN team_boxscores for REB/AST.
    """
    if with_boxscore:
        sql = """
            SELECT g.*,
                   vb.reb AS visitor_reb, vb.ast AS visitor_ast,
                   hb.reb AS home_reb,    hb.ast AS home_ast
            FROM   games g
            LEFT JOIN team_boxscores vb
                ON vb.game_id = g.game_id AND vb.team_id = g.visitor_team_id
            LEFT JOIN team_boxscores hb
                ON hb.game_id = g.game_id AND hb.team_id = g.home_team_id
            WHERE  g.game_date = ?
            ORDER  BY g.game_id
        """
    else:
        sql = "SELECT * FROM games WHERE game_date = ? ORDER BY game_id"

    rows = []
    for r in conn.execute(sql, (game_date.isoformat(),)).fetchall():
        row = dict(r)
        row["visitor_logo"] = logos_svc.logo_url(row["visitor_abbr"])
        row["home_logo"]    = logos_svc.logo_url(row["home_abbr"])
        rows.append(row)
    return rows


# ── App lifecycle ─────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("NBA Dashboard starting up")
    init_db()
    logos_svc.ensure_all_logos()
    scheduler.start()
    yield
    scheduler.stop()
    logger.info("NBA Dashboard shut down")


app = FastAPI(title="NBA Dashboard", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=os.path.join(_HERE, "static")), name="static")

# ── Routes ────────────────────────────────────────────────────────────────────

@app.get("/")
async def homepage(request: Request):
    yesterday, today, tomorrow = _et_dates()

    conn = get_db()

    yesterday_games = _game_rows(conn, yesterday, with_boxscore=True)
    today_games     = _game_rows(conn, today,     with_boxscore=True)
    tomorrow_games  = _game_rows(conn, tomorrow)

    # Latest injury report — limit to 50 rows for the homepage summary
    injuries = [
        dict(r) for r in conn.execute(
            """
            SELECT * FROM injury_reports
            WHERE  report_date = (SELECT MAX(report_date) FROM injury_reports)
            ORDER  BY team_abbr, player_last
            LIMIT  50
            """,
        ).fetchall()
    ]

    meta = conn.execute(
        "SELECT value FROM meta WHERE key = 'last_updated'"
    ).fetchone()
    last_updated = meta["value"] if meta else None

    conn.close()

    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={
            "yesterday":       yesterday,
            "today":           today,
            "tomorrow":        tomorrow,
            "yesterday_games": yesterday_games,
            "today_games":     today_games,
            "tomorrow_games":  tomorrow_games,
            "injuries":        injuries,
            "last_updated":    last_updated,
        },
    )


@app.get("/games/yesterday")
async def games_yesterday(request: Request):
    yesterday, _, _ = _et_dates()
    conn = get_db()

    games = [
        dict(r) for r in conn.execute(
            "SELECT * FROM games WHERE game_date = ? ORDER BY game_id",
            (yesterday.isoformat(),),
        ).fetchall()
    ]

    game_details = []
    for game in games:
        gid = game["game_id"]

        team_stats = {
            r["team_id"]: dict(r)
            for r in conn.execute(
                "SELECT * FROM team_boxscores WHERE game_id = ?", (gid,)
            ).fetchall()
        }

        all_players = [
            dict(r) for r in conn.execute(
                """SELECT * FROM player_boxscores
                   WHERE game_id = ?
                   ORDER BY team_id,
                            CASE WHEN start_position != '' THEN 0 ELSE 1 END,
                            pts DESC NULLS LAST""",
                (gid,),
            ).fetchall()
        ]

        game_details.append({
            "game":            game,
            "visitor_stats":   team_stats.get(game["visitor_team_id"]),
            "home_stats":      team_stats.get(game["home_team_id"]),
            "visitor_players": [p for p in all_players if p["team_id"] == game["visitor_team_id"]],
            "home_players":    [p for p in all_players if p["team_id"] == game["home_team_id"]],
        })

    meta = conn.execute("SELECT value FROM meta WHERE key = 'last_updated'").fetchone()
    last_updated = meta["value"] if meta else None
    conn.close()

    return templates.TemplateResponse(
        request=request,
        name="games_yesterday.html",
        context={
            "yesterday":    yesterday,
            "game_details": game_details,
            "last_updated": last_updated,
        },
    )


@app.get("/injuries/tomorrow")
async def injuries_tomorrow(request: Request):
    _, _, tomorrow = _et_dates()

    conn = get_db()

    tomorrow_games = [
        dict(r) for r in conn.execute(
            "SELECT * FROM games WHERE game_date = ? ORDER BY game_id",
            (tomorrow.isoformat(),),
        ).fetchall()
    ]

    all_injuries = [
        dict(r) for r in conn.execute(
            """
            SELECT * FROM injury_reports
            WHERE  report_date = (SELECT MAX(report_date) FROM injury_reports)
            ORDER  BY team_abbr, player_last
            """,
        ).fetchall()
    ]

    meta = {
        r["key"]: r["value"]
        for r in conn.execute("SELECT key, value FROM meta").fetchall()
    }
    conn.close()

    _STATUS_ORDER = {
        "Out": 0, "Suspension": 0,
        "Doubtful": 1,
        "Questionable": 2, "Day-To-Day": 2,
        "Probable": 3,
    }

    injuries_by_team: dict[str, list] = {}
    for inj in all_injuries:
        injuries_by_team.setdefault(inj["team_abbr"] or "", []).append(inj)

    def _sorted(lst):
        return sorted(lst, key=lambda x: _STATUS_ORDER.get(x["player_status"] or "", 99))

    matched_abbrs: set[str] = set()
    matchups = []
    for game in tomorrow_games:
        vis  = game["visitor_abbr"] or ""
        home = game["home_abbr"]    or ""
        matched_abbrs.update([vis, home])

        vis_list  = _sorted(injuries_by_team.get(vis,  []))
        home_list = _sorted(injuries_by_team.get(home, []))

        game_time = next(
            (i["game_time"] for i in vis_list + home_list if i.get("game_time")),
            game.get("status_text") or "",
        )

        matchups.append({
            "game":             game,
            "game_time":        game_time,
            "visitor_injuries": vis_list,
            "home_injuries":    home_list,
        })

    unmatched = _sorted([
        i for i in all_injuries if (i["team_abbr"] or "") not in matched_abbrs
    ])

    return templates.TemplateResponse(
        request=request,
        name="injuries_tomorrow.html",
        context={
            "tomorrow":     tomorrow,
            "matchups":     matchups,
            "unmatched":    unmatched,
            "report_date":  meta.get("injury_report_date"),
            "report_time":  meta.get("injury_report_time"),
            "last_updated": meta.get("last_updated"),
        },
    )


@app.get("/games/{game_id}")
async def game_detail(game_id: str, request: Request):
    """Per-game detail page: score, quarter breakdown, team stats, starters/bench analysis."""
    conn = get_db()

    game = conn.execute("SELECT * FROM games WHERE game_id = ?", (game_id,)).fetchone()
    if not game:
        conn.close()
        return templates.TemplateResponse(
            request=request, name="404.html",
            context={"message": f"Game {game_id} not found.", "last_updated": None},
            status_code=404,
        )
    game = dict(game)
    game["visitor_logo"] = logos_svc.logo_url(game["visitor_abbr"])
    game["home_logo"]    = logos_svc.logo_url(game["home_abbr"])

    # ── Team box scores ───────────────────────────────────────────────
    team_stats = {
        r["team_id"]: dict(r)
        for r in conn.execute(
            "SELECT * FROM team_boxscores WHERE game_id = ?", (game_id,)
        ).fetchall()
    }

    # ── Period scores (quarter-by-quarter) ───────────────────────────
    periods_raw = conn.execute(
        "SELECT team_id, period, score FROM game_periods WHERE game_id = ? ORDER BY period",
        (game_id,),
    ).fetchall()
    periods_by_team: dict[int, dict[int, int]] = {}
    max_period = 0
    for r in periods_raw:
        periods_by_team.setdefault(r["team_id"], {})[r["period"]] = r["score"]
        if r["period"] > max_period:
            max_period = r["period"]

    # Build a period table: list of {period, visitor_score, home_score, visitor_lead}
    period_table = []
    vis_tid  = game["visitor_team_id"]
    home_tid = game["home_team_id"]
    vis_periods  = periods_by_team.get(vis_tid,  {})
    home_periods = periods_by_team.get(home_tid, {})

    vis_cumul = home_cumul = 0
    for p in range(1, max_period + 1):
        vs = vis_periods.get(p)
        hs = home_periods.get(p)
        if vs is not None:
            vis_cumul += vs
        if hs is not None:
            home_cumul += hs
        period_table.append({
            "period":        p,
            "label":         f"Q{p}" if p <= 4 else f"OT{p - 4}",
            "visitor_score": vs,
            "home_score":    hs,
            "visitor_lead":  vis_cumul > home_cumul if (vs is not None and hs is not None) else None,
        })

    # ── Player box scores ─────────────────────────────────────────────
    all_players = [
        dict(r) for r in conn.execute(
            """SELECT * FROM player_boxscores
               WHERE game_id = ?
               ORDER BY team_id,
                        CASE WHEN start_position != '' THEN 0 ELSE 1 END,
                        pts DESC NULLS LAST""",
            (game_id,),
        ).fetchall()
    ]
    visitor_players = [p for p in all_players if p["team_id"] == vis_tid]
    home_players    = [p for p in all_players if p["team_id"] == home_tid]

    # ── Starters-vs-bench analysis ────────────────────────────────────
    def _rotation_split(players):
        starters = [p for p in players if p.get("start_position") and p["start_position"] != ""]
        bench    = [p for p in players if not p.get("start_position") or p["start_position"] == ""]
        return starters, bench

    def _aggregate(players):
        if not players:
            return None
        total_fga = sum(p.get("fga") or 0 for p in players)
        total_fg3a = sum(p.get("fg3a") or 0 for p in players)
        return {
            "count": len(players),
            "pts":   sum(p.get("pts") or 0 for p in players),
            "reb":   sum(p.get("reb") or 0 for p in players),
            "ast":   sum(p.get("ast") or 0 for p in players),
            "stl":   sum(p.get("stl") or 0 for p in players),
            "blk":   sum(p.get("blk") or 0 for p in players),
            "to":    sum(p.get("turnovers") or 0 for p in players),
            "fgm":   sum(p.get("fgm") or 0 for p in players),
            "fga":   total_fga,
            "fg_pct": round(sum(p.get("fgm") or 0 for p in players) / total_fga * 100, 1) if total_fga else 0,
            "fg3m":  sum(p.get("fg3m") or 0 for p in players),
            "fg3a":  total_fg3a,
            "fg3_pct": round(sum(p.get("fg3m") or 0 for p in players) / total_fg3a * 100, 1) if total_fg3a else 0,
            "pm":    sum(p.get("plus_minus") or 0 for p in players),
        }

    vis_starters, vis_bench   = _rotation_split(visitor_players)
    home_starters, home_bench = _rotation_split(home_players)

    analysis = {
        "visitor": {
            "abbr":            game["visitor_abbr"],
            "starters":        _aggregate(vis_starters),
            "bench":           _aggregate(vis_bench),
            "starter_players": vis_starters,
            "bench_players":   vis_bench,
        },
        "home": {
            "abbr":            game["home_abbr"],
            "starters":        _aggregate(home_starters),
            "bench":           _aggregate(home_bench),
            "starter_players": home_starters,
            "bench_players":   home_bench,
        },
    }

    # ── Trailing period analysis ──────────────────────────────────────
    # Determine which team was trailing after each quarter cumulatively
    trail_analysis = []
    if period_table:
        vis_running = home_running = 0
        for row in period_table:
            vs = row["visitor_score"] or 0
            hs = row["home_score"] or 0
            vis_running  += vs
            home_running += hs
            if row["visitor_score"] is not None and row["home_score"] is not None:
                trail_analysis.append({
                    "label":          row["label"],
                    "visitor_pts_q":  vs,
                    "home_pts_q":     hs,
                    "visitor_cumul":  vis_running,
                    "home_cumul":     home_running,
                    "trailing":       "visitor" if vis_running < home_running
                                      else "home" if home_running < vis_running
                                      else "tied",
                    "margin":         abs(vis_running - home_running),
                })

    meta = conn.execute("SELECT value FROM meta WHERE key = 'last_updated'").fetchone()
    last_updated = meta["value"] if meta else None
    conn.close()

    rotation_stats = trail_svc.get_rotation_stats(game_id)
    stints = trail_svc.get_stints(game_id)

    return templates.TemplateResponse(
        request=request,
        name="game_detail.html",
        context={
            "game":            game,
            "visitor_stats":   team_stats.get(vis_tid),
            "home_stats":      team_stats.get(home_tid),
            "period_table":    period_table,
            "visitor_players": visitor_players,
            "home_players":    home_players,
            "analysis":        analysis,
            "trail_analysis":  trail_analysis,
            "rotation_stats":  rotation_stats,
            "stints":          stints,
            "last_updated":    last_updated,
        },
    )


@app.get("/api/game/{game_id}/stints")
async def api_game_stints(game_id: str):
    """JSON endpoint: per-player rotation stints for live-game polling."""
    result = trail_svc.get_stints(game_id)
    return result or {"has_data": False, "game_status_id": None, "stints_updated_at": None}


@app.post("/api/game/{game_id}/stints/refresh")
async def api_stints_refresh(game_id: str, background_tasks: BackgroundTasks):
    """
    Trigger an immediate recomputation of stints for one game.
    Runs in the background; poll GET /api/game/{game_id}/stints for results.
    """
    conn = get_db()
    exists = conn.execute(
        "SELECT 1 FROM games WHERE game_id = ?", (game_id,)
    ).fetchone()
    conn.close()
    if not exists:
        from fastapi.responses import JSONResponse as _JR
        return _JR({"error": "game not found"}, status_code=404)

    def _run():
        try:
            trail_svc.fetch_and_store_stints(game_id)
        except Exception:
            logger.exception("stints/refresh background task failed for %s", game_id)

    background_tasks.add_task(_run)
    return {"status": "refresh started", "game_id": game_id}


@app.get("/api/game/{game_id}")
async def api_game(game_id: str):
    """JSON endpoint: live score + full player stats for a single game (real-time polling)."""
    conn = get_db()

    game = conn.execute(
        """SELECT game_id, game_status_id, status_text, visitor_pts, home_pts,
                  visitor_team_id, home_team_id, visitor_abbr, home_abbr,
                  updated_at
           FROM games WHERE game_id = ?""",
        (game_id,),
    ).fetchone()
    if not game:
        conn.close()
        logger.warning("/api/game/%s — not found", game_id)
        return JSONResponse({"error": "not found"}, status_code=404)
    game = dict(game)

    players = [
        dict(r) for r in conn.execute(
            """SELECT player_id, player_name, team_id, start_position, minutes,
                      pts, reb, ast, stl, blk, turnovers,
                      fgm, fga, fg3m, fg3a, ftm, fta, plus_minus,
                      updated_at
               FROM player_boxscores
               WHERE game_id = ?
               ORDER BY team_id,
                        CASE WHEN start_position != '' THEN 0 ELSE 1 END,
                        pts DESC NULLS LAST""",
            (game_id,),
        ).fetchall()
    ]

    team_stats = {
        r["team_id"]: dict(r)
        for r in conn.execute(
            "SELECT * FROM team_boxscores WHERE game_id = ?", (game_id,)
        ).fetchall()
    }

    meta = conn.execute(
        "SELECT value FROM meta WHERE key = 'last_live_refresh'"
    ).fetchone()
    last_live_refresh = meta["value"] if meta else None

    conn.close()

    rotation = trail_svc.get_rotation_stats(game_id)

    logger.info(
        "/api/game/%s — status=%s vis=%s home=%s players=%d db_updated=%s live_refresh=%s",
        game_id, game.get("game_status_id"), game.get("visitor_pts"),
        game.get("home_pts"), len(players),
        game.get("updated_at"), last_live_refresh,
    )

    return {
        "game": game,
        "players": players,
        "team_stats": team_stats,
        "rotation_stats": rotation,
        "db_updated_at": game.get("updated_at"),
        "last_live_refresh": last_live_refresh,
    }


@app.get("/api/live")
async def api_live():
    """JSON endpoint: all currently live games with current scores, period, and team stats."""
    conn = get_db()
    rows = conn.execute(
        """SELECT g.game_id, g.game_date, g.game_status_id, g.status_text,
                  g.home_team_id, g.home_abbr, g.home_name, g.home_pts,
                  g.visitor_team_id, g.visitor_abbr, g.visitor_name, g.visitor_pts,
                  g.updated_at,
                  vb.reb AS visitor_reb, vb.ast AS visitor_ast,
                  hb.reb AS home_reb,   hb.ast AS home_ast
           FROM   games g
           LEFT JOIN team_boxscores vb
               ON vb.game_id = g.game_id AND vb.team_id = g.visitor_team_id
           LEFT JOIN team_boxscores hb
               ON hb.game_id = g.game_id AND hb.team_id = g.home_team_id
           WHERE  g.game_status_id = 2
           ORDER  BY g.game_id"""
    ).fetchall()

    meta = conn.execute(
        "SELECT value FROM meta WHERE key = 'last_live_refresh'"
    ).fetchone()
    last_live_refresh = meta["value"] if meta else None

    conn.close()

    live_games = [dict(r) for r in rows]
    logger.info(
        "/api/live — %d live game(s) | last_live_refresh=%s",
        len(live_games), last_live_refresh,
    )

    return {
        "live_games": live_games,
        "count": len(live_games),
        "last_live_refresh": last_live_refresh,
    }


@app.get("/players/{player_id}")
async def player_detail(player_id: int, request: Request):
    """
    Player profile page.

    Looks the player up in our player_boxscores table (to get the name),
    then fetches a full profile from ESPN (bio + season averages + career
    stats table) using requests + BeautifulSoup.
    """
    conn = get_db()

    # Find the most recent boxscore row for this player so we have a name
    # and the game-level stats they put up.
    player_row = conn.execute(
        """SELECT pb.*, g.game_date, g.home_abbr, g.visitor_abbr,
                  g.home_pts, g.visitor_pts, g.game_status_id, g.status_text
           FROM   player_boxscores pb
           JOIN   games g ON g.game_id = pb.game_id
           WHERE  pb.player_id = ?
           ORDER  BY g.game_date DESC
           LIMIT  1""",
        (player_id,),
    ).fetchone()

    if not player_row:
        conn.close()
        logger.warning("/players/%d — player not found in DB", player_id)
        return templates.TemplateResponse(
            request=request,
            name="404.html",
            context={"message": f"Player {player_id} not found.", "last_updated": None},
            status_code=404,
        )

    player_row = dict(player_row)

    # Recent game log (last 10 games) for the sparkline table
    recent_games = [
        dict(r) for r in conn.execute(
            """SELECT pb.pts, pb.reb, pb.ast, pb.stl, pb.blk, pb.turnovers,
                      pb.fgm, pb.fga, pb.fg3m, pb.fg3a, pb.ftm, pb.fta,
                      pb.plus_minus, pb.minutes,
                      g.game_date, g.home_abbr, g.visitor_abbr,
                      pb.team_id, g.home_team_id, g.visitor_team_id
               FROM   player_boxscores pb
               JOIN   games g ON g.game_id = pb.game_id
               WHERE  pb.player_id = ?
               ORDER  BY g.game_date DESC
               LIMIT  10""",
            (player_id,),
        ).fetchall()
    ]

    meta = conn.execute("SELECT value FROM meta WHERE key = 'last_updated'").fetchone()
    last_updated = meta["value"] if meta else None
    conn.close()

    player_name = player_row.get("player_name", "Unknown Player")

    # Fetch ESPN profile (bio + stats); may return None if unreachable
    logger.info("/players/%d — fetching ESPN profile for '%s'", player_id, player_name)
    profile = espn_svc.get_player_profile(player_id, player_name)

    return templates.TemplateResponse(
        request=request,
        name="player_detail.html",
        context={
            "player_id":    player_id,
            "player_name":  player_name,
            "player_row":   player_row,
            "recent_games": recent_games,
            "profile":      profile,      # None if ESPN unreachable
            "last_updated": last_updated,
        },
    )


@app.get("/players/search")
async def player_search(request: Request, q: str = ""):
    """
    Name-based player lookup — used by injury pages that have no player_id.
    Tries our DB first; falls back to ESPN name search.
    """
    if not q:
        return templates.TemplateResponse(
            request=request, name="404.html",
            context={"message": "No player name provided.", "last_updated": None},
            status_code=404,
        )

    player_name = q.replace("+", " ").strip()
    conn = get_db()

    # Try exact name match in our DB first
    row = conn.execute(
        """SELECT player_id FROM player_boxscores
           WHERE player_name = ?
           LIMIT 1""",
        (player_name,),
    ).fetchone()
    conn.close()

    if row:
        # Redirect to the canonical player page
        from fastapi.responses import RedirectResponse
        return RedirectResponse(url=f"/players/{row['player_id']}", status_code=302)

    # No DB match — render the player detail page via ESPN name lookup
    logger.info("/players/search?q=%s — no DB hit, fetching ESPN by name", player_name)
    profile = espn_svc.get_player_profile_by_name(player_name)

    return templates.TemplateResponse(
        request=request,
        name="player_detail.html",
        context={
            "player_id":    None,
            "player_name":  player_name,
            "player_row":   None,
            "recent_games": [],
            "profile":      profile,
            "last_updated": None,
        },
    )


@app.get("/refresh")
async def manual_refresh(background_tasks: BackgroundTasks):
    """Trigger a full data refresh outside the normal schedule."""
    background_tasks.add_task(scheduler.refresh_all)
    return {"status": "Refresh started — check back in a minute."}


@app.get("/status")
async def status():
    """Return DB row counts and meta values — useful for diagnosing empty pages."""
    conn = get_db()

    def count(table: str, where: str = "") -> int:
        sql = f"SELECT COUNT(*) FROM {table}"
        if where:
            sql += f" WHERE {where}"
        return conn.execute(sql).fetchone()[0]

    yesterday, today, tomorrow = _et_dates()

    meta = {r["key"]: r["value"] for r in conn.execute("SELECT key, value FROM meta").fetchall()}
    result = {
        "db_counts": {
            "games_total":      count("games"),
            "games_yesterday":  count("games", f"game_date = '{yesterday}'"),
            "games_today":      count("games", f"game_date = '{today}'"),
            "games_tomorrow":   count("games", f"game_date = '{tomorrow}'"),
            "team_boxscores":   count("team_boxscores"),
            "player_boxscores": count("player_boxscores"),
            "injury_reports":   count("injury_reports"),
        },
        "meta":          meta,
        "et_dates":      {"yesterday": str(yesterday), "today": str(today), "tomorrow": str(tomorrow)},
        "et_now":        datetime.now(ET).strftime("%Y-%m-%d %H:%M:%S %Z"),
    }
    conn.close()
    return result
