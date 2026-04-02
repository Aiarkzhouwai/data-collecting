"""
Fetch the NBA injury report from the ESPN public API and store it in the
injury_reports table.

Source: https://site.api.espn.com/apis/site/v2/sports/basketball/nba/injuries

ESPN groups injuries by team; each record includes player name, status, and
a human-readable comment. The feed is publicly accessible and does not require
authentication.

Notes:
  - ESPN status values differ from the official NBA report:
      Out, Day-To-Day, Questionable, Probable, Doubtful, Suspension, IR
  - game_date / game_time are stored as NULL — ESPN does not provide per-game
    injury scheduling like the NBA CDN JSON does.  The /injuries/tomorrow page
    matches players to tomorrow's games purely by team abbreviation.
  - Team abbreviations are resolved from nba_api's static team list using
    the full team name that ESPN provides.
"""

import requests
from collections import Counter
from datetime import date, datetime, timezone

from nba_api.stats.static import teams as nba_teams_static

from app.db import get_db
from app.logger import logger

ESPN_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/injuries"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
}

# Build a lookup from full team name → abbreviation, e.g. "Boston Celtics" → "BOS"
_TEAM_NAME_TO_ABBR: dict[str, str] = {
    t["full_name"]: t["abbreviation"]
    for t in nba_teams_static.get_teams()
}


# ── Main ingest function ──────────────────────────────────────────────────────

def fetch_and_store_injuries() -> int:
    """
    Download the latest ESPN NBA injury report and upsert into the DB.
    Returns the number of player records stored (0 on failure).
    """
    logger.info("Fetching injury report from ESPN")

    try:
        resp = requests.get(ESPN_URL, timeout=15, headers=_HEADERS)
        resp.raise_for_status()
        data = resp.json()
    except requests.HTTPError as exc:
        logger.error("ESPN returned HTTP %s: %s", exc.response.status_code, exc)
        return 0
    except Exception as exc:
        logger.error("Failed to fetch injury report: %s", exc, exc_info=True)
        return 0

    # ── Parse report metadata ─────────────────────────────────────────────────
    report_date = date.today().isoformat()
    # ESPN top-level timestamp, e.g. "2026-04-01T00:03:10Z"
    raw_ts = data.get("timestamp") or ""
    try:
        report_time = (
            datetime.strptime(raw_ts, "%Y-%m-%dT%H:%M:%SZ")
            .replace(tzinfo=timezone.utc)
            .strftime("%-I:%M %p UTC")
        )
    except ValueError:
        report_time = raw_ts

    team_groups = data.get("injuries") or []
    logger.info(
        "ESPN report as of %s — %d team group(s) received",
        raw_ts, len(team_groups),
    )

    # ── Flatten into individual injury records ────────────────────────────────
    flat: list[dict] = []
    for group in team_groups:
        team_name = group.get("displayName", "")
        team_abbr = _TEAM_NAME_TO_ABBR.get(team_name, "")
        if not team_abbr:
            logger.debug("Unknown team name from ESPN: %r", team_name)

        for inj in group.get("injuries") or []:
            athlete = inj.get("athlete") or {}
            flat.append({
                "team_name":      team_name,
                "team_abbr":      team_abbr,
                "player_first":   athlete.get("firstName", ""),
                "player_last":    athlete.get("lastName", ""),
                "player_status":  inj.get("status", ""),
                "player_comment": inj.get("shortComment") or inj.get("longComment") or "",
            })

    logger.info("%d player record(s) parsed", len(flat))
    if not flat:
        logger.warning("ESPN injury feed returned zero player entries — nothing stored")
        return 0

    # ── Store records ─────────────────────────────────────────────────────────
    conn = get_db()

    # Replace all rows for this report date
    deleted = conn.execute(
        "DELETE FROM injury_reports WHERE report_date = ?", (report_date,)
    ).rowcount
    if deleted:
        logger.info("Replaced %d stale record(s) for %s", deleted, report_date)

    stored = 0
    for item in flat:
        try:
            conn.execute(
                """
                INSERT INTO injury_reports
                    (report_date, report_time,
                     game_date,   game_time,   game_id,
                     team_id,     team_abbr,   team_city,  team_name,
                     player_first, player_last,
                     player_status, player_comment)
                VALUES (?, ?, NULL, NULL, NULL,
                        NULL, ?, NULL, ?,
                        ?, ?, ?, ?)
                """,
                (
                    report_date,  report_time,
                    item["team_abbr"],
                    item["team_name"],
                    item["player_first"],
                    item["player_last"],
                    item["player_status"],
                    item["player_comment"],
                ),
            )
            stored += 1
        except Exception as exc:
            logger.warning(
                "Could not store injury record (%s %s): %s",
                item.get("player_first"), item.get("player_last"), exc,
            )

    # Write report timestamp to meta
    conn.execute(
        "INSERT OR REPLACE INTO meta (key, value) VALUES ('injury_report_date', ?)",
        (report_date,),
    )
    conn.execute(
        "INSERT OR REPLACE INTO meta (key, value) VALUES ('injury_report_time', ?)",
        (report_time,),
    )

    conn.commit()
    conn.close()

    # ── Logging summary ───────────────────────────────────────────────────────
    teams_affected = len({i["team_abbr"] for i in flat if i["team_abbr"]})
    status_counts  = Counter(i["player_status"] for i in flat)

    logger.info("Stored %d record(s) across %d team(s)", stored, teams_affected)
    for status, n in status_counts.most_common():
        logger.info("  %-14s %d", status, n)

    return stored
