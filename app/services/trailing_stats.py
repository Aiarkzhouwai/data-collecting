"""
Per-period player box scores + rotation / trailing-period analysis.

Data source: PlayByPlayV3 (BoxScoreTraditionalV3 ignores start_period/end_period
and returns full-game stats, so it cannot be used for per-period breakdowns).

A "trailing period" is any quarter a team entered while behind on the
cumulative scoreboard.  Q1 is never a trailing period (both teams start 0–0).
For live games, the current quarter counts as trailing if the team is
currently behind.

Per-period stats tracked from PBP: PTS, REB, FGM, FGA, FG3M, FG3A, FTM, FTA, TO.
(AST/STL/BLK are not reliably extractable from PBP events without description
parsing, so they are stored as 0.)
"""

import re
import time
from typing import Optional

from nba_api.stats.endpoints import playbyplayv3

from app.db import get_db
from app.logger import logger

# ── Playoff-status cache ───────────────────────────────────────────────────────
_playoff_cache: Optional[tuple] = None  # (timestamp_float, {team_id: status_str})
_PLAYOFF_CACHE_TTL = 3_600              # refresh standings at most once per hour

# Short-lived PBP cache — avoids fetching PlayByPlayV3 twice in the same scheduler run
_pbp_cache: dict = {}   # game_id -> (timestamp, df, starts_dict, player_names_dict)
_PBP_CACHE_TTL = 30     # seconds


# ── Fetch + Store ──────────────────────────────────────────────────────────────

def fetch_and_store_period_stats(game_id: str) -> bool:
    """
    Fetch PlayByPlayV3, parse per-period per-player stats, and upsert into
    player_period_boxscores.  Clears any existing rows for this game first.

    Returns True if at least one row was stored.
    """
    logger.info("  Fetching PBP for period stats: game %s", game_id)
    try:
        df, starts, _, _pt = _fetch_pbp(game_id)
    except Exception as exc:
        logger.error("  PlayByPlayV3 failed for game %s: %s", game_id, exc)
        return False

    if df.empty:
        logger.warning("  PlayByPlayV3 returned empty data for game %s", game_id)
        return False

    # ── Parse play-by-play into per-period per-player aggregates ──────────────
    # period_stats[period][player_id] = {stats dict}
    period_stats: dict[int, dict[int, dict]] = {}

    for _, row in df.iterrows():
        try:
            period  = int(row.get("period")   or 0)
            pid     = int(row.get("personId") or 0)
            team_id = int(row.get("teamId")   or 0)
        except (TypeError, ValueError):
            continue

        if not period or not pid or not team_id:
            continue

        action = str(row.get("actionType") or "").strip()

        if period not in period_stats:
            period_stats[period] = {}
        if pid not in period_stats[period]:
            period_stats[period][pid] = {
                "player_name": str(row.get("playerName") or "").strip(),
                "team_id": team_id,
                "pts": 0, "reb": 0,
                "fgm": 0, "fga": 0, "fg3m": 0, "fg3a": 0,
                "ftm": 0, "fta": 0, "turnovers": 0,
            }

        s = period_stats[period][pid]

        if action == "Made Shot":
            val = int(row.get("shotValue") or 2)
            s["pts"] += val
            s["fgm"] += 1
            s["fga"] += 1
            if val == 3:
                s["fg3m"] += 1
                s["fg3a"] += 1

        elif action == "Missed Shot":
            s["fga"] += 1
            if int(row.get("shotValue") or 0) == 3:
                s["fg3a"] += 1

        elif action == "Free Throw":
            s["fta"] += 1
            # scoreHome is non-empty only on made FTs (score was updated)
            sh = str(row.get("scoreHome") or "").strip()
            if sh and sh.lstrip("-").isdigit():
                s["pts"] += 1
                s["ftm"] += 1

        elif action == "Rebound":
            s["reb"] += 1

        elif action == "Turnover":
            s["turnovers"] += 1

    if not period_stats:
        logger.warning("  No parseable PBP events for game %s", game_id)
        return False

    # ── Store (clearing stale data first) ─────────────────────────────────────
    conn = get_db()
    conn.execute("DELETE FROM player_period_boxscores WHERE game_id = ?", (game_id,))

    stored = 0
    for period, players in period_stats.items():
        for pid, st in players.items():
            try:
                conn.execute(
                    """
                    INSERT INTO player_period_boxscores
                        (game_id, player_id, player_name, team_id, period,
                         start_position,
                         pts, reb, fgm, fga, fg3m, fg3a, ftm, fta, turnovers,
                         plus_minus, updated_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0,datetime('now'))
                    ON CONFLICT(game_id, player_id, period) DO UPDATE SET
                        player_name = excluded.player_name,
                        pts         = excluded.pts,
                        reb         = excluded.reb,
                        fgm         = excluded.fgm,
                        fga         = excluded.fga,
                        fg3m        = excluded.fg3m,
                        fg3a        = excluded.fg3a,
                        ftm         = excluded.ftm,
                        fta         = excluded.fta,
                        turnovers   = excluded.turnovers,
                        updated_at  = excluded.updated_at
                    """,
                    (
                        game_id, pid, st["player_name"], st["team_id"], period,
                        starts.get(pid, ""),
                        st["pts"], st["reb"],
                        st["fgm"], st["fga"], st["fg3m"], st["fg3a"],
                        st["ftm"], st["fta"], st["turnovers"],
                    ),
                )
                stored += 1
            except Exception as exc:
                logger.warning(
                    "  PBP period row error P%d %s/pid=%s: %s",
                    period, game_id, pid, exc,
                )

    conn.commit()
    conn.close()
    logger.info("  PBP period stats: %d player-period rows stored for game %s", stored, game_id)
    return stored > 0


# ── Analysis ───────────────────────────────────────────────────────────────────

def get_trailing_stats(game_id: str) -> Optional[dict]:
    """
    Compute starters-vs-bench stats during trailing periods for both teams.

    "Trailing periods" = quarters the team entered while behind on the
    cumulative scoreboard.  Q1 is never a trailing period.

    Returns dict with keys "visitor", "home", "has_data".
    Returns None if the game is not in the DB.
    """
    conn = get_db()

    game = conn.execute(
        "SELECT * FROM games WHERE game_id = ?", (game_id,)
    ).fetchone()
    if not game:
        conn.close()
        return None
    game = dict(game)

    vis_tid  = game["visitor_team_id"]
    home_tid = game["home_team_id"]

    # ── Cumulative team score per period ──────────────────────────────────────
    period_rows = conn.execute(
        "SELECT team_id, period, score FROM game_periods WHERE game_id = ? ORDER BY period",
        (game_id,),
    ).fetchall()

    cumul: dict[int, dict[int, int]] = {}  # cumul[team_id][period] = running total
    for r in period_rows:
        tid, p, sc = r["team_id"], r["period"], r["score"] or 0
        if tid not in cumul:
            cumul[tid] = {}
        cumul[tid][p] = cumul[tid].get(p - 1, 0) + sc

    max_period = max((r["period"] for r in period_rows), default=0)

    # ── Identify trailing periods for each team ───────────────────────────────
    def _trailing_periods(my_tid: int, opp_tid: int) -> list[int]:
        """Quarters this team entered while behind (Q1 excluded — starts 0-0)."""
        my_c  = cumul.get(my_tid,  {})
        opp_c = cumul.get(opp_tid, {})
        trailing = []
        for p in range(2, max_period + 2):
            if my_c.get(p - 1, 0) < opp_c.get(p - 1, 0):
                trailing.append(p)
        # Live: add current period if team is presently behind
        if game.get("game_status_id") == 2:
            vis_pts  = game.get("visitor_pts") or 0
            home_pts = game.get("home_pts")    or 0
            current  = max_period + 1
            if my_tid == vis_tid  and vis_pts  < home_pts and current not in trailing:
                trailing.append(current)
            elif my_tid == home_tid and home_pts < vis_pts  and current not in trailing:
                trailing.append(current)
        return sorted(trailing)

    vis_trailing  = _trailing_periods(vis_tid,  home_tid)
    home_trailing = _trailing_periods(home_tid, vis_tid)

    # ── Check for per-period data ─────────────────────────────────────────────
    period_count = conn.execute(
        "SELECT COUNT(*) FROM player_period_boxscores WHERE game_id = ?",
        (game_id,),
    ).fetchone()[0]
    has_data = period_count > 0

    def _period_label(p: int) -> str:
        return f"Q{p}" if p <= 4 else f"OT{p - 4}"

    def _build_side(team_id: int, abbr: str, trailing: list[int]) -> dict:
        base = {
            "abbr":             abbr,
            "trailing_periods": trailing,
            "trailing_labels":  [_period_label(p) for p in trailing],
            "starters":         None,
            "bench":            None,
            "starter_players":  [],
            "bench_players":    [],
        }
        if not trailing or not has_data:
            return base

        placeholders = ",".join("?" * len(trailing))
        rows = conn.execute(
            f"""
            SELECT ppb.player_id, ppb.player_name, ppb.team_id,
                   ppb.pts, ppb.reb, ppb.fgm, ppb.fga,
                   ppb.fg3m, ppb.fg3a, ppb.ftm, ppb.fta, ppb.turnovers,
                   ppb.start_position
            FROM   player_period_boxscores ppb
            WHERE  ppb.game_id = ?
              AND  ppb.team_id = ?
              AND  ppb.period  IN ({placeholders})
            ORDER  BY ppb.player_id, ppb.period
            """,
            [game_id, team_id] + trailing,
        ).fetchall()

        # Aggregate each player across trailing periods
        agg: dict[int, dict] = {}
        for r in rows:
            pid = r["player_id"]
            if pid not in agg:
                agg[pid] = {
                    "player_id":     pid,
                    "player_name":   r["player_name"],
                    "start_position": r["start_position"] or "",
                    "pts": 0, "reb": 0,
                    "fgm": 0, "fga": 0, "fg3m": 0, "fg3a": 0,
                    "ftm": 0, "fta": 0, "turnovers": 0,
                }
            a = agg[pid]
            for k in ("pts", "reb", "fgm", "fga", "fg3m", "fg3a",
                      "ftm", "fta", "turnovers"):
                a[k] += r[k] or 0

        players   = list(agg.values())
        starters  = [p for p in players if p["start_position"] not in ("", None)]
        bench     = [p for p in players if p["start_position"] in ("", None)]

        def _agg_group(group: list[dict]) -> Optional[dict]:
            if not group:
                return None
            fga  = sum(p["fga"]  for p in group)
            fg3a = sum(p["fg3a"] for p in group)
            fta  = sum(p["fta"]  for p in group)
            return {
                "count":   len(group),
                "pts":     sum(p["pts"]       for p in group),
                "reb":     sum(p["reb"]       for p in group),
                "to":      sum(p["turnovers"] for p in group),
                "fgm":     sum(p["fgm"]       for p in group),
                "fga":     fga,
                "fg_pct":  round(sum(p["fgm"]  for p in group) / fga  * 100, 1) if fga  else 0,
                "fg3m":    sum(p["fg3m"]       for p in group),
                "fg3a":    fg3a,
                "fg3_pct": round(sum(p["fg3m"] for p in group) / fg3a * 100, 1) if fg3a else 0,
                "ftm":     sum(p["ftm"]        for p in group),
                "fta":     fta,
                "ft_pct":  round(sum(p["ftm"]  for p in group) / fta  * 100, 1) if fta  else 0,
            }

        base["starters"]        = _agg_group(starters)
        base["bench"]           = _agg_group(bench)
        base["starter_players"] = sorted(starters, key=lambda p: p["pts"], reverse=True)
        base["bench_players"]   = sorted(bench,    key=lambda p: p["pts"], reverse=True)
        return base

    vis_side  = _build_side(vis_tid,  game["visitor_abbr"], vis_trailing)
    home_side = _build_side(home_tid, game["home_abbr"],    home_trailing)

    conn.close()

    return {
        "visitor":  vis_side,
        "home":     home_side,
        "has_data": has_data,
    }


# ── Playoff / Play-In status ───────────────────────────────────────────────────

def get_playoff_status() -> dict:
    """
    Return {team_id (int): 'playoff' | 'playin' | 'none'} for every NBA team,
    derived from the current LeagueStandingsV3 PlayoffRank column.

    Seeding rules (per conference):
      rank 1–6  → direct playoff qualifier  ('playoff')
      rank 7–10 → play-in tournament        ('playin')
      rank 11+  → eliminated / not in       ('none')

    Results are cached for _PLAYOFF_CACHE_TTL seconds to avoid hammering the API
    on every page load.
    """
    global _playoff_cache
    now = time.time()

    if _playoff_cache and (now - _playoff_cache[0]) < _PLAYOFF_CACHE_TTL:
        return _playoff_cache[1]

    result: dict = {}
    try:
        from nba_api.stats.endpoints import leaguestandingsv3 as _ls
        standings = _ls.LeagueStandingsV3()
        time.sleep(0.6)
        df = standings.get_data_frames()[0]
        for _, row in df.iterrows():
            tid  = int(row["TeamID"])
            rank = int(row.get("PlayoffRank") or 0)
            if 1 <= rank <= 6:
                result[tid] = "playoff"
            elif rank <= 10:
                result[tid] = "playin"
            else:
                result[tid] = "none"
        logger.info("Playoff status refreshed: %d teams", len(result))
    except Exception as exc:
        logger.warning("get_playoff_status failed: %s", exc)

    if result:
        _playoff_cache = (now, result)
    return result


# ── Play-by-play helpers ───────────────────────────────────────────────────────

def _parse_clock(clock_str) -> float:
    """Parse 'PT07M30.00S' or '7:30' → seconds remaining (float)."""
    s = str(clock_str or "").strip()
    if not s:
        return 0.0
    try:
        s = s.replace("PT", "").replace("S", "").strip()
        if "M" in s:
            m, sec = s.split("M", 1)
            return float(m) * 60 + float(sec or 0)
        if ":" in s:
            m, sec = s.split(":", 1)
            return float(m) * 60 + float(sec or 0)
        return float(s)
    except (ValueError, AttributeError):
        return 0.0


def _secs_to_clock(secs: float) -> str:
    """Convert seconds remaining to 'M:SS' display string (internal use)."""
    secs = max(0.0, float(secs or 0))
    return f"{int(secs) // 60}:{int(secs) % 60:02d}"


def _secs_to_elapsed(remaining_secs: float, period_secs: float) -> str:
    """
    Convert seconds-remaining to elapsed time from period start.

    NBA clocks count DOWN: period starts at period_secs (e.g. 720 = 12:00)
    and ticks toward 0. This function converts to the intuitive "time into
    the period" (elapsed) format used in broadcasts.

    Examples (Q = 720 s):
      remaining=720 (period start) → elapsed=0   → "0:00"
      remaining=450 (7:30 left)    → elapsed=270  → "4:30"
      remaining=0   (period end)   → elapsed=720  → "12:00"
    """
    elapsed = max(0.0, period_secs - float(remaining_secs or 0))
    return f"{int(elapsed) // 60}:{int(elapsed) % 60:02d}"


def _duration_str(dur_secs: float) -> str:
    """Format a duration in seconds as 'M:SS' string."""
    dur_secs = max(0.0, float(dur_secs or 0))
    return f"{int(dur_secs) // 60}:{int(dur_secs) % 60:02d}"


def _fetch_pbp(game_id: str):
    """
    Fetch PlayByPlayV3 for a game, caching the result for _PBP_CACHE_TTL seconds.

    Returns (df, starts, player_names, player_teams):
      starts        — {player_id: start_position}  from player_boxscores
      player_names  — {player_id: player_name}     from PBP rows + boxscores
      player_teams  — {player_id: team_id}         from player_boxscores
    """
    now = time.time()
    cached = _pbp_cache.get(game_id)
    if cached and (now - cached[0]) < _PBP_CACHE_TTL:
        return cached[1], cached[2], cached[3], cached[4]

    pbp = playbyplayv3.PlayByPlayV3(game_id=game_id, timeout=30)
    time.sleep(0.6)
    df = pbp.get_data_frames()[0]

    conn = get_db()
    bs_rows = conn.execute(
        "SELECT player_id, start_position, team_id, player_name "
        "FROM player_boxscores WHERE game_id = ?",
        (game_id,),
    ).fetchall()
    conn.close()

    starts: dict       = {}
    player_teams: dict = {}
    player_names: dict = {}
    for r in bs_rows:
        pid = int(r["player_id"])
        starts[pid]       = r["start_position"] or ""
        player_teams[pid] = int(r["team_id"] or 0)
        if r["player_name"]:
            player_names[pid] = r["player_name"]

    # Supplement names from PBP (covers players missing from box score)
    if not df.empty:
        for _, row in df.iterrows():
            try:
                pid = int(row.get("personId") or 0)
            except (TypeError, ValueError):
                continue
            name = str(row.get("playerName") or "").strip()
            if pid and name and pid not in player_names:
                player_names[pid] = name

    _pbp_cache[game_id] = (now, df, starts, player_names, player_teams)
    return df, starts, player_names, player_teams


def _safe_pid(x) -> int:
    try:
        return int(x or 0)
    except (TypeError, ValueError):
        return 0


def _stint_stats(period_df, player_id: int,
                 enter_secs: float, exit_secs: float) -> dict:
    """Aggregate PBP stats for one player during the clock window [exit_secs, enter_secs]."""
    mask = (
        (period_df["personId"].map(_safe_pid) == player_id) &
        (period_df["_secs"] <= enter_secs) &
        (period_df["_secs"] >= exit_secs)
    )
    stats = {"pts": 0, "reb": 0, "fgm": 0, "fga": 0,
             "fg3m": 0, "fg3a": 0, "ftm": 0, "fta": 0, "turnovers": 0}
    for _, row in period_df[mask].iterrows():
        action = str(row.get("actionType") or "").strip()
        if action == "Made Shot":
            val = int(row.get("shotValue") or 2)
            stats["pts"] += val
            stats["fgm"] += 1
            stats["fga"] += 1
            if val == 3:
                stats["fg3m"] += 1
                stats["fg3a"] += 1
        elif action == "Missed Shot":
            stats["fga"] += 1
            if int(row.get("shotValue") or 0) == 3:
                stats["fg3a"] += 1
        elif action == "Free Throw":
            stats["fta"] += 1
            sh = str(row.get("scoreHome") or "").strip()
            if sh and sh.lstrip("-").isdigit():
                stats["pts"] += 1
                stats["ftm"] += 1
        elif action == "Rebound":
            stats["reb"] += 1
        elif action == "Turnover":
            stats["turnovers"] += 1
    return stats


def _make_stint(game_id, player_id, player_name, team_id, period,
                enter_secs, exit_secs, period_df, starts,
                is_active: bool = False) -> dict:
    """
    Build one stint dict.

    enter_clock / exit_clock are stored as elapsed time from period start
    (e.g. "4:30" = 4:30 into the period) so the UI can show "entered at 4:30,
    exited at 9:45, played 5:15".

    For live stints where the player is still on court, is_active=True and
    exit_clock is set to "Active".  Stats are computed up to the latest
    available PBP event in the period.
    """
    period_secs = 720.0 if period <= 4 else 300.0
    enter_elapsed = _secs_to_elapsed(enter_secs, period_secs)

    if is_active:
        # Use the most recent clock value we have as the current position
        live_exit_secs = float(period_df["_secs"].min()) if not period_df.empty else 0.0
        stats = _stint_stats(period_df, player_id, enter_secs, live_exit_secs)
        dur_secs   = max(0.0, enter_secs - live_exit_secs)
        exit_display = "Active"
        exit_secs_store = live_exit_secs
    else:
        stats = _stint_stats(period_df, player_id, enter_secs, exit_secs)
        dur_secs   = max(0.0, enter_secs - exit_secs)
        exit_display = _secs_to_elapsed(exit_secs, period_secs)
        exit_secs_store = exit_secs

    return {
        "game_id":        game_id,
        "player_id":      player_id,
        "player_name":    player_name,
        "team_id":        team_id,
        "period":         period,
        "enter_clock":    enter_elapsed,
        "exit_clock":     exit_display,
        "enter_secs":     enter_secs,
        "exit_secs":      exit_secs_store,
        "minutes_played": round(dur_secs / 60, 2),
        "duration_str":   _duration_str(dur_secs),
        "is_active":      is_active,
        "start_position": starts.get(player_id, ""),
        **stats,
    }


def _compute_stints(df, game_id: str, starts: dict, player_names: dict,
                    player_teams: dict | None = None,
                    is_live: bool = False) -> list:
    """
    Derive per-player stints from PlayByPlayV3 substitution events.

    ROOT CAUSE FIX: PlayByPlayV3 returns subType='' for ALL substitution rows.
    The in/out distinction is carried in the description field only:

        "SUB: <incoming_name> FOR <outgoing_name>"

    and personId = the OUTGOING player.  We parse the description to identify
    the incoming player's ID via a name→ID map built from all PBP rows.

    Clock convention: the PBP `clock` field is seconds REMAINING in the period
    (counts down).  We convert to elapsed-from-start for display.

    Algorithm per period:
      1. Build name→ID map for this period's players from all PBP rows.
      2. Pre-seed period-1 starters (from player_boxscores) so they are tracked
         even if they are subbed out before making any stat event.
      3. Process events (sorted DESC by remaining clock = period-start first):
         Substitution → personId is the outgoing player; parse description for
                        the incoming player's name, look up their ID.
                        Close outgoing, open incoming.
         Other events → player must be on court; open at period_secs if unseen.
      4. At period end: close all still-active players.
         For the current live period → mark them is_active=True.
    """
    import re as _re

    stints: list = []
    if df.empty:
        return stints

    max_period = int(df["period"].max())

    # ── Build name → player_id map from all PBP rows ──────────────────────────
    # PlayByPlayV3 rows contain playerName (short, e.g. "Trent Jr.") and personId.
    # We prefer the boxscore name from player_names but fall back to PBP names.
    name_to_id: dict = {}
    for p_id, p_name in player_names.items():
        if p_name:
            name_to_id[p_name] = p_id
    for _, row in df.iterrows():
        try:
            p_id = int(row.get("personId") or 0)
        except (TypeError, ValueError):
            continue
        p_name = str(row.get("playerName") or "").strip()
        if p_id and p_name and p_name not in name_to_id:
            name_to_id[p_name] = p_id

    for period in range(1, max_period + 1):
        period_df = df[df["period"] == period].copy()
        if period_df.empty:
            continue

        period_secs = 720.0 if period <= 4 else 300.0
        period_df["_secs"] = period_df["clock"].map(_parse_clock)

        # Sort: highest remaining time first (earliest in game).
        # For events with the same clock tick, sort by actionNumber ASC so
        # earlier actions (e.g. a foul that causes a sub) are processed before
        # the substitution event itself — preventing the sub from closing the
        # player's stint before their foul event re-seeds them as a ghost stint.
        if "actionNumber" in period_df.columns:
            sorted_df = period_df.sort_values(
                ["_secs", "actionNumber"], ascending=[False, True]
            )
        else:
            sorted_df = period_df.sort_values("_secs", ascending=False)

        # ── Per-period name/team helpers ──────────────────────────────────────
        # Build a per-period pid→(team_id, name) map so we don't lose data when
        # players have no events in a later period.
        pid_tid:   dict = dict(player_teams or {})
        pid_pname: dict = dict(player_names or {})
        for _, row in sorted_df.iterrows():
            try:
                p_id = int(row.get("personId") or 0)
                t_id = int(row.get("teamId")   or 0)
            except (TypeError, ValueError):
                continue
            if p_id and t_id and p_id not in pid_tid:
                pid_tid[p_id] = t_id
            nm = str(row.get("playerName") or "").strip()
            if p_id and nm and p_id not in pid_pname:
                pid_pname[p_id] = nm

        # ── Pre-seed period-1 starters ────────────────────────────────────────
        # active[player_id] = (enter_secs, team_id, player_name)
        # benched: players explicitly subbed OUT — don't re-seed from stat events
        active:  dict = {}
        benched: set  = set()
        if period == 1:
            for p_id, pos in starts.items():
                if not pos:          # bench — don't pre-seed
                    continue
                t_id = pid_tid.get(p_id, 0)
                if t_id:
                    active[p_id] = (period_secs, t_id, pid_pname.get(p_id, ""))

        # ── Main event loop ───────────────────────────────────────────────────
        for _, row in sorted_df.iterrows():
            try:
                pid = int(row.get("personId") or 0)
                tid = int(row.get("teamId")   or 0)
            except (TypeError, ValueError):
                continue
            if not pid or not tid:
                continue

            action = str(row.get("actionType") or "").strip()
            secs   = float(row["_secs"])
            pname  = pid_pname.get(pid) or str(row.get("playerName") or "").strip()

            if action == "Substitution":
                # ── personId = OUTGOING player ────────────────────────────────
                # description = "SUB: <incoming_name> FOR <outgoing_name>"
                out_pid   = pid
                out_tid   = tid
                out_pname = pname

                # Close outgoing player's stint
                if out_pid not in active:
                    # Starter or carry-over player with no prior stat events
                    active[out_pid] = (period_secs, out_tid, out_pname)
                enter_s, enter_t, enter_n = active.pop(out_pid)
                benched.add(out_pid)
                stints.append(_make_stint(
                    game_id, out_pid, enter_n or out_pname,
                    enter_t, period, enter_s, secs, period_df, starts,
                    is_active=False,
                ))

                # Open incoming player's stint
                desc = str(row.get("description") or "")
                m    = _re.match(r"SUB:\s+(.+?)\s+FOR\s+", desc, _re.IGNORECASE)
                if m:
                    in_name = m.group(1).strip()
                    in_pid  = name_to_id.get(in_name, 0)
                    if in_pid and in_pid != out_pid:
                        in_tid   = pid_tid.get(in_pid, out_tid)
                        in_pname = pid_pname.get(in_pid, in_name)
                        benched.discard(in_pid)
                        active[in_pid] = (secs, in_tid, in_pname)

            else:
                # Non-sub event — player is on court.
                # Only open a new stint if they haven't been explicitly benched
                # (benched players may have stat events at the same clock tick as
                # their substitution — e.g. a foul that triggers the sub).
                if pid not in active and pid not in benched:
                    active[pid] = (period_secs, tid, pname)

        # ── Close all still-active stints at period end ───────────────────────
        is_current = (period == max_period) and is_live
        for pid, (enter_s, tid, pname) in active.items():
            stints.append(_make_stint(
                game_id, pid, pname, tid, period,
                enter_s, 0.0, period_df, starts,
                is_active=is_current,
            ))

    return stints


# ── Rotation Analysis (leading / close / trailing) ─────────────────────────────

#: Within this many points at the start of a quarter = "close game" situation
CLOSE_MARGIN = 5


def get_rotation_stats(game_id: str) -> Optional[dict]:
    """
    Return starters-vs-bench stats split across three game situations per team:

    - **Leading**  : quarters entered with any positive margin
    - **Trailing** : quarters entered with any negative margin
    - **Close**    : quarters where |margin| ≤ CLOSE_MARGIN (5 pts) at tip-off,
                     including Q1 (always 0–0)

    The three categories can overlap: a team leading by 3 entering Q3 appears
    in both "leading" and "close".

    Returns None if the game is not in the DB.
    """
    conn = get_db()

    game = conn.execute(
        "SELECT * FROM games WHERE game_id = ?", (game_id,)
    ).fetchone()
    if not game:
        conn.close()
        return None
    game = dict(game)

    vis_tid  = game["visitor_team_id"]
    home_tid = game["home_team_id"]

    # ── Cumulative team scores per period ─────────────────────────────────────
    period_rows = conn.execute(
        "SELECT team_id, period, score FROM game_periods WHERE game_id = ? ORDER BY period",
        (game_id,),
    ).fetchall()

    cumul: dict[int, dict[int, int]] = {}
    for r in period_rows:
        tid, p, sc = r["team_id"], r["period"], r["score"] or 0
        if tid not in cumul:
            cumul[tid] = {}
        cumul[tid][p] = cumul[tid].get(p - 1, 0) + sc

    max_period = max((r["period"] for r in period_rows), default=0)

    period_count = conn.execute(
        "SELECT COUNT(*) FROM player_period_boxscores WHERE game_id = ?",
        (game_id,),
    ).fetchone()[0]
    has_data = period_count > 0

    # ── Period classification per team ────────────────────────────────────────
    def _classify(my_tid: int, opp_tid: int) -> dict:
        """Return {leading, trailing, close} period lists for this team."""
        my_c  = cumul.get(my_tid,  {})
        opp_c = cumul.get(opp_tid, {})
        leading = []
        trailing = []
        close = [1]  # Q1 always 0-0 → close

        for p in range(2, max_period + 2):
            prev   = p - 1
            margin = my_c.get(prev, 0) - opp_c.get(prev, 0)
            if margin > 0:
                leading.append(p)
            elif margin < 0:
                trailing.append(p)
            if abs(margin) <= CLOSE_MARGIN:
                close.append(p)

        # Live game: factor in current period
        if game.get("game_status_id") == 2:
            vis_pts  = game.get("visitor_pts")  or 0
            home_pts = game.get("home_pts")     or 0
            cur      = max_period + 1
            if my_tid == vis_tid:
                cur_margin = vis_pts - home_pts
            else:
                cur_margin = home_pts - vis_pts

            if cur_margin > 0 and cur not in leading:
                leading.append(cur)
            elif cur_margin < 0 and cur not in trailing:
                trailing.append(cur)
            if abs(cur_margin) <= CLOSE_MARGIN and cur not in close:
                close.append(cur)

        return {
            "leading":  sorted(set(leading)),
            "trailing": sorted(set(trailing)),
            "close":    sorted(set(close)),
        }

    def _period_label(p: int) -> str:
        return f"Q{p}" if p <= 4 else f"OT{p - 4}"

    def _build_situation(team_id: int, periods: list[int]) -> dict:
        """Aggregate player stats for the given period list, split starters/bench."""
        base: dict = {
            "periods": periods,
            "labels":  [_period_label(p) for p in periods],
            "starters":        None,
            "bench":           None,
            "starter_players": [],
            "bench_players":   [],
        }
        if not periods or not has_data:
            return base

        ph = ",".join("?" * len(periods))
        rows = conn.execute(
            f"""
            SELECT ppb.player_id, ppb.player_name,
                   ppb.pts, ppb.reb, ppb.fgm, ppb.fga,
                   ppb.fg3m, ppb.fg3a, ppb.ftm, ppb.fta, ppb.turnovers,
                   ppb.start_position
            FROM   player_period_boxscores ppb
            WHERE  ppb.game_id = ?
              AND  ppb.team_id = ?
              AND  ppb.period  IN ({ph})
            ORDER  BY ppb.player_id
            """,
            [game_id, team_id] + periods,
        ).fetchall()

        # Aggregate per player across all matching periods
        pa: dict[int, dict] = {}
        for r in rows:
            pid = r["player_id"]
            if pid not in pa:
                pa[pid] = {
                    "player_id":      pid,
                    "player_name":    r["player_name"],
                    "start_position": r["start_position"] or "",
                    "pts": 0, "reb": 0,
                    "fgm": 0, "fga": 0, "fg3m": 0, "fg3a": 0,
                    "ftm": 0, "fta": 0, "turnovers": 0,
                }
            a = pa[pid]
            for k in ("pts", "reb", "fgm", "fga", "fg3m", "fg3a",
                      "ftm", "fta", "turnovers"):
                a[k] += r[k] or 0

        # Attach full-game minutes from player_boxscores so the UI can display them
        if pa:
            pid_ph = ",".join("?" * len(pa))
            for mr in conn.execute(
                f"SELECT player_id, minutes FROM player_boxscores "
                f"WHERE game_id = ? AND player_id IN ({pid_ph})",
                [game_id] + list(pa.keys()),
            ).fetchall():
                pid = mr["player_id"]
                if pid in pa:
                    pa[pid]["minutes"] = mr["minutes"] or ""

        players  = list(pa.values())
        starters = [p for p in players if p["start_position"] not in ("", None)]
        bench    = [p for p in players if p["start_position"] in ("", None)]

        def _agg(group: list[dict]) -> Optional[dict]:
            if not group:
                return None
            fga  = sum(p["fga"]  for p in group)
            fg3a = sum(p["fg3a"] for p in group)
            fta  = sum(p["fta"]  for p in group)
            return {
                "count":   len(group),
                "pts":     sum(p["pts"]       for p in group),
                "reb":     sum(p["reb"]       for p in group),
                "to":      sum(p["turnovers"] for p in group),
                "fgm":     sum(p["fgm"]       for p in group),
                "fga":     fga,
                "fg_pct":  round(sum(p["fgm"]  for p in group) / fga  * 100, 1) if fga  else 0,
                "fg3m":    sum(p["fg3m"]       for p in group),
                "fg3a":    fg3a,
                "fg3_pct": round(sum(p["fg3m"] for p in group) / fg3a * 100, 1) if fg3a else 0,
                "ftm":     sum(p["ftm"]        for p in group),
                "fta":     fta,
                "ft_pct":  round(sum(p["ftm"]  for p in group) / fta  * 100, 1) if fta  else 0,
            }

        base["starters"]        = _agg(starters)
        base["bench"]           = _agg(bench)
        base["starter_players"] = sorted(starters, key=lambda p: p["pts"], reverse=True)
        base["bench_players"]   = sorted(bench,    key=lambda p: p["pts"], reverse=True)
        return base

    def _build_team(team_id: int, abbr: str, cls: dict) -> dict:
        return {
            "abbr":     abbr,
            "leading":  _build_situation(team_id, cls["leading"]),
            "trailing": _build_situation(team_id, cls["trailing"]),
            "close":    _build_situation(team_id, cls["close"]),
        }

    vis_cls  = _classify(vis_tid,  home_tid)
    home_cls = _classify(home_tid, vis_tid)

    vis_side  = _build_team(vis_tid,  game["visitor_abbr"], vis_cls)
    home_side = _build_team(home_tid, game["home_abbr"],    home_cls)

    conn.close()

    # ── Game duration ──────────────────────────────────────────────────────────
    def _period_min(p: int) -> int:
        return 12 if p <= 4 else 5

    game_minutes = sum(_period_min(p) for p in range(1, max_period + 1)) if max_period else 0
    ot_count     = max(0, max_period - 4) if max_period else 0
    if game_minutes == 0:
        duration_label = "—"
    elif ot_count == 0:
        duration_label = "48 min"
    elif ot_count == 1:
        duration_label = "53 min (OT)"
    else:
        duration_label = f"{game_minutes} min ({ot_count}×OT)"

    # ── Playoff / Play-In status per team ──────────────────────────────────────
    playoff_map = get_playoff_status()
    vis_side["playoff_status"]  = playoff_map.get(vis_tid,  "unknown")
    home_side["playoff_status"] = playoff_map.get(home_tid, "unknown")

    return {
        "visitor":        vis_side,
        "home":           home_side,
        "has_data":       has_data,
        "close_margin":   CLOSE_MARGIN,
        "game_minutes":   game_minutes,
        "duration_label": duration_label,
    }


# ── Rotation stints (exact substitution timing) ────────────────────────────────

def fetch_and_store_stints(game_id: str) -> int:
    """
    Parse PlayByPlayV3 substitution events to compute and store per-stint stats.
    Uses the shared _fetch_pbp cache, so if called right after fetch_and_store_period_stats
    no additional API request is made.

    Returns number of stints stored.
    """
    logger.info("  Computing rotation stints for game %s", game_id)
    try:
        df, starts, player_names, player_teams = _fetch_pbp(game_id)
    except Exception as exc:
        logger.error("  _fetch_pbp failed for stints game %s: %s", game_id, exc)
        return 0

    if df.empty:
        return 0

    # Determine if the game is currently live
    conn = get_db()
    game_row = conn.execute(
        "SELECT game_status_id FROM games WHERE game_id = ?", (game_id,)
    ).fetchone()
    is_live = bool(game_row and game_row["game_status_id"] == 2)
    conn.close()

    stints = _compute_stints(df, game_id, starts, player_names,
                             player_teams=player_teams, is_live=is_live)
    if not stints:
        logger.warning("  No stints computed for game %s", game_id)
        return 0

    conn = get_db()
    conn.execute("DELETE FROM player_stints WHERE game_id = ?", (game_id,))
    stored = 0
    for s in stints:
        try:
            conn.execute(
                """INSERT INTO player_stints
                   (game_id, player_id, player_name, team_id, period,
                    enter_clock, exit_clock, enter_secs, exit_secs, minutes_played,
                    duration_str, is_active,
                    pts, reb, fgm, fga, fg3m, fg3a, ftm, fta, turnovers,
                    start_position, updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,datetime('now'))""",
                (
                    s["game_id"], s["player_id"], s["player_name"],
                    s["team_id"], s["period"],
                    s["enter_clock"], s["exit_clock"],
                    s["enter_secs"],  s["exit_secs"],
                    s["minutes_played"],
                    s["duration_str"], int(s["is_active"]),
                    s["pts"], s["reb"],
                    s["fgm"], s["fga"], s["fg3m"], s["fg3a"],
                    s["ftm"], s["fta"], s["turnovers"],
                    s["start_position"],
                ),
            )
            stored += 1
        except Exception as exc:
            logger.warning("  Stint insert error game %s / pid %s: %s",
                           game_id, s.get("player_id"), exc)
    conn.commit()
    conn.close()
    logger.info("  Stints: %d rows stored for game %s", stored, game_id)
    return stored


def get_stints(game_id: str) -> Optional[dict]:
    """
    Return per-player rotation stints grouped by team.
    Returns None if the game is not in the DB.
    Returns {'has_data': False, ...} if no stints have been computed yet.
    Includes game_status_id and stints_updated_at for freshness display.
    """
    conn = get_db()
    game = conn.execute(
        "SELECT visitor_team_id, home_team_id, visitor_abbr, home_abbr, game_status_id "
        "FROM games WHERE game_id = ?",
        (game_id,),
    ).fetchone()
    if not game:
        conn.close()
        return None

    vis_tid        = game["visitor_team_id"]
    home_tid       = game["home_team_id"]
    game_status_id = game["game_status_id"]

    rows = conn.execute(
        """SELECT player_id, player_name, team_id, period,
                  enter_clock, exit_clock, enter_secs, exit_secs, minutes_played,
                  duration_str, is_active,
                  pts, reb, fgm, fga, fg3m, fg3a, ftm, fta, turnovers,
                  start_position, updated_at
           FROM   player_stints
           WHERE  game_id = ?
           ORDER  BY player_id, period, enter_secs DESC""",
        (game_id,),
    ).fetchall()
    conn.close()

    # Most-recent updated_at across all stints for this game
    stints_updated_at = None
    if rows:
        stints_updated_at = max(
            (r["updated_at"] for r in rows if r["updated_at"]), default=None
        )

    if not rows:
        return {
            "has_data":        False,
            "visitor":         None,
            "home":            None,
            "game_status_id":  game_status_id,
            "stints_updated_at": None,
        }

    def _period_label(p: int) -> str:
        return f"Q{p}" if p <= 4 else f"OT{p - 4}"

    def _build_team(team_id: int, abbr: str) -> dict:
        players_map: dict = {}
        for r in rows:
            if r["team_id"] != team_id:
                continue
            pid = r["player_id"]
            if pid not in players_map:
                players_map[pid] = {
                    "player_id":      pid,
                    "player_name":    r["player_name"] or "",
                    "is_starter":     bool(r["start_position"]),
                    "start_position": r["start_position"] or "",
                    "total_min":      0.0,
                    "stints":         [],
                }
            fga  = r["fga"]  or 0
            fg3a = r["fg3a"] or 0
            fta  = r["fta"]  or 0
            players_map[pid]["stints"].append({
                "period":        r["period"],
                "period_label":  _period_label(r["period"]),
                "enter_clock":   r["enter_clock"] or "—",
                "exit_clock":    r["exit_clock"]  or "—",
                "minutes_played": round(float(r["minutes_played"] or 0), 1),
                "duration_str":  r["duration_str"] or "—",
                "is_active":     bool(r["is_active"]),
                "pts":     r["pts"]       or 0,
                "reb":     r["reb"]       or 0,
                "fgm":     r["fgm"]       or 0,
                "fga":     fga,
                "fg_pct":  round((r["fgm"] or 0) / fga  * 100, 1) if fga  else 0,
                "fg3m":    r["fg3m"]      or 0,
                "fg3a":    fg3a,
                "fg3_pct": round((r["fg3m"] or 0) / fg3a * 100, 1) if fg3a else 0,
                "ftm":     r["ftm"]       or 0,
                "fta":     fta,
                "turnovers": r["turnovers"] or 0,
            })
            players_map[pid]["total_min"] += float(r["minutes_played"] or 0)

        for p in players_map.values():
            p["total_min"] = round(p["total_min"], 1)
            total_secs = p["total_min"] * 60
            p["total_min_str"] = f"{int(total_secs) // 60}:{int(total_secs) % 60:02d}"
            # Aggregate totals across all stints for this player
            tot_fga  = sum(s["fga"]  for s in p["stints"])
            tot_fg3a = sum(s["fg3a"] for s in p["stints"])
            tot_fta  = sum(s["fta"]  for s in p["stints"])
            p["totals"] = {
                "pts":      sum(s["pts"]       for s in p["stints"]),
                "reb":      sum(s["reb"]       for s in p["stints"]),
                "fgm":      sum(s["fgm"]       for s in p["stints"]),
                "fga":      tot_fga,
                "fg3m":     sum(s["fg3m"]      for s in p["stints"]),
                "fg3a":     tot_fg3a,
                "ftm":      sum(s["ftm"]       for s in p["stints"]),
                "fta":      tot_fta,
                "turnovers":sum(s["turnovers"] for s in p["stints"]),
            }

        starters = sorted(
            [p for p in players_map.values() if p["is_starter"]],
            key=lambda x: -x["total_min"],
        )
        bench = sorted(
            [p for p in players_map.values() if not p["is_starter"]],
            key=lambda x: -x["total_min"],
        )
        return {"abbr": abbr, "players": starters + bench}

    return {
        "has_data":          True,
        "visitor":           _build_team(vis_tid,  game["visitor_abbr"]),
        "home":              _build_team(home_tid, game["home_abbr"]),
        "game_status_id":    game_status_id,
        "stints_updated_at": stints_updated_at,
    }
