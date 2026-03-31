"""
app/main.py — FastAPI application entry point.

Run from the project root:
    uvicorn app.main:app --reload
"""

import os
from contextlib import asynccontextmanager
from datetime import date, timedelta

from fastapi import BackgroundTasks, FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.db import get_db, init_db
from app.logger import logger
from app.services import logos as logos_svc
from app.tasks import scheduler

# ── Paths ─────────────────────────────────────────────────────────────────────

_HERE = os.path.dirname(os.path.abspath(__file__))
templates = Jinja2Templates(directory=os.path.join(_HERE, "templates"))

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
    yesterday = date.today() - timedelta(days=1)
    tomorrow  = date.today() + timedelta(days=1)

    conn = get_db()

    yesterday_games = []
    for r in conn.execute(
        """
        SELECT g.*,
               vb.reb AS visitor_reb, vb.ast AS visitor_ast,
               hb.reb AS home_reb,    hb.ast AS home_ast
        FROM   games g
        LEFT JOIN team_boxscores vb ON vb.game_id = g.game_id AND vb.team_id = g.visitor_team_id
        LEFT JOIN team_boxscores hb ON hb.game_id = g.game_id AND hb.team_id = g.home_team_id
        WHERE  g.game_date = ?
        ORDER  BY g.game_id
        """,
        (yesterday.isoformat(),),
    ).fetchall():
        row = dict(r)
        row["visitor_logo"] = logos_svc.logo_url(row["visitor_abbr"])
        row["home_logo"]    = logos_svc.logo_url(row["home_abbr"])
        yesterday_games.append(row)

    tomorrow_games = []
    for r in conn.execute(
        "SELECT * FROM games WHERE game_date = ? ORDER BY game_id",
        (tomorrow.isoformat(),),
    ).fetchall():
        row = dict(r)
        row["visitor_logo"] = logos_svc.logo_url(row["visitor_abbr"])
        row["home_logo"]    = logos_svc.logo_url(row["home_abbr"])
        tomorrow_games.append(row)

    # Latest injury report (most recent report_date in the table)
    injuries = [
        dict(r) for r in conn.execute(
            """
            SELECT * FROM injury_reports
            WHERE  report_date = (SELECT MAX(report_date) FROM injury_reports)
            ORDER  BY team_abbr, player_last
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
            "tomorrow":        tomorrow,
            "yesterday_games": yesterday_games,
            "tomorrow_games":  tomorrow_games,
            "injuries":        injuries,
            "last_updated":    last_updated,
        },
    )


@app.get("/games/yesterday")
async def games_yesterday(request: Request):
    yesterday = date.today() - timedelta(days=1)
    conn = get_db()

    games = [
        dict(r) for r in conn.execute(
            "SELECT * FROM games WHERE game_date = ? ORDER BY game_id",
            (yesterday.isoformat(),),
        ).fetchall()
    ]

    # For each game attach team stats and player stats (split by team)
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
    tomorrow = date.today() + timedelta(days=1)

    conn = get_db()

    # Tomorrow's scheduled games — used to pair teams into matchups
    tomorrow_games = [
        dict(r) for r in conn.execute(
            "SELECT * FROM games WHERE game_date = ? ORDER BY game_id",
            (tomorrow.isoformat(),),
        ).fetchall()
    ]

    # All injury records for tomorrow from the most-recently issued report
    all_injuries = [
        dict(r) for r in conn.execute(
            """
            SELECT * FROM injury_reports
            WHERE  game_date  = ?
              AND  report_date = (SELECT MAX(report_date) FROM injury_reports)
            ORDER  BY team_abbr, player_last
            """,
            (tomorrow.isoformat(),),
        ).fetchall()
    ]

    # Grab report timestamp and last_updated from meta
    meta = {
        r["key"]: r["value"]
        for r in conn.execute("SELECT key, value FROM meta").fetchall()
    }
    conn.close()

    # ── Group injuries by team abbreviation ───────────────────────────────────
    _STATUS_ORDER = {"Out": 0, "Doubtful": 1, "Questionable": 2, "Probable": 3}

    injuries_by_team: dict[str, list] = {}
    for inj in all_injuries:
        injuries_by_team.setdefault(inj["team_abbr"] or "", []).append(inj)

    def _sorted(lst):
        return sorted(lst, key=lambda x: _STATUS_ORDER.get(x["player_status"] or "", 99))

    # ── Build one matchup dict per game ───────────────────────────────────────
    matched_abbrs: set[str] = set()
    matchups = []
    for game in tomorrow_games:
        vis  = game["visitor_abbr"] or ""
        home = game["home_abbr"]    or ""
        matched_abbrs.update([vis, home])

        vis_list  = _sorted(injuries_by_team.get(vis,  []))
        home_list = _sorted(injuries_by_team.get(home, []))

        # Prefer game_time from the injury entries; fall back to scoreboard status
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

    # Injuries for teams whose games aren't in our DB yet
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


@app.get("/refresh")
async def manual_refresh(background_tasks: BackgroundTasks):
    """Trigger a full data refresh outside the normal schedule."""
    background_tasks.add_task(scheduler.refresh_all)
    return {"status": "Refresh started — check back in a minute."}
