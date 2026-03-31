"""
Fetch the official NBA injury report from the NBA CDN JSON feed and store
it in the injury_reports table.

Source: https://cdn.nba.com/static/json/liveData/injuryreport/injuryreport.json

The feed is re-published several times a day.  Each call to
fetch_and_store_injuries() replaces all rows for the current report_date,
so running it repeatedly is safe and idempotent.

Key normalisation performed here:
  - Dates from the CDN arrive as "MM/DD/YYYY"; we store them as ISO "YYYY-MM-DD"
    so they join cleanly against the games table.
  - report_time and game_time are stored verbatim ("5:30 PM ET").
  - The report timestamp is also written to the meta table for easy template access.
"""

import requests
from collections import Counter
from datetime import date, datetime

from app.db import get_db
from app.logger import logger

CDN_URL = "https://cdn.nba.com/static/json/liveData/injuryreport/injuryreport.json"

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; NBA-Dashboard/1.0)",
    "Accept":     "application/json",
}


# ── Date normalisation ────────────────────────────────────────────────────────

def _to_iso(date_str: str | None) -> str | None:
    """
    Convert a date string to ISO format (YYYY-MM-DD).

    Handles:
      "MM/DD/YYYY"  →  "YYYY-MM-DD"   (CDN format)
      "YYYY-MM-DD"  →  unchanged       (already ISO)
    Returns None if the input is empty or unparseable.
    """
    if not date_str:
        return None
    s = date_str.strip()
    if len(s) == 10 and s[4] == "-":
        return s  # already ISO
    try:
        return datetime.strptime(s, "%m/%d/%Y").date().isoformat()
    except ValueError:
        logger.warning("Unrecognised date format in injury feed: %r", s)
        return s  # store as-is rather than silently dropping


# ── Main ingest function ──────────────────────────────────────────────────────

def fetch_and_store_injuries() -> int:
    """
    Download the latest NBA injury report and upsert into the DB.
    Returns the number of player records stored (0 on failure).
    """
    logger.info("Fetching injury report from NBA CDN")

    try:
        resp = requests.get(CDN_URL, timeout=15, headers=_HEADERS)
        resp.raise_for_status()
        data = resp.json()
    except requests.HTTPError as exc:
        logger.error("NBA CDN returned HTTP %s: %s", exc.response.status_code, exc)
        return 0
    except Exception as exc:
        logger.error("Failed to fetch injury report: %s", exc, exc_info=True)
        return 0

    # ── Parse report metadata ─────────────────────────────────────────────────
    report_date = _to_iso(data.get("injuryReportDate")) or date.today().isoformat()
    report_time = (data.get("injuryReportTime") or "").strip()
    injuries    = data.get("injuries") or []

    logger.info(
        "Report: %s %s — %d player entries received",
        report_date, report_time, len(injuries),
    )

    if not injuries:
        logger.warning("Injury feed returned zero entries — nothing stored")
        return 0

    # ── Store records ─────────────────────────────────────────────────────────
    conn = get_db()

    # Replace all rows for this report date so refreshing never duplicates
    deleted = conn.execute(
        "DELETE FROM injury_reports WHERE report_date = ?", (report_date,)
    ).rowcount
    if deleted:
        logger.info("Replaced %d stale record(s) for %s", deleted, report_date)

    stored = 0
    for item in injuries:
        game_date = _to_iso(item.get("gameDate"))
        game_time = (item.get("gameTime") or "").strip()

        try:
            conn.execute(
                """
                INSERT INTO injury_reports
                    (report_date, report_time,
                     game_date,   game_time,   game_id,
                     team_id,     team_abbr,   team_city,  team_name,
                     player_first, player_last,
                     player_status, player_comment)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    report_date,  report_time,
                    game_date,    game_time,    item.get("gameId"),
                    item.get("teamId"),
                    item.get("teamAbbreviation"),
                    item.get("teamCity"),
                    item.get("teamName"),
                    item.get("playerFirstName"),
                    item.get("playerLastName"),
                    item.get("playerStatus"),
                    item.get("playerComment"),
                ),
            )
            stored += 1
        except Exception as exc:
            logger.warning(
                "Could not store injury record (%s %s): %s",
                item.get("playerFirstName"), item.get("playerLastName"), exc,
            )

    # Write report timestamp to meta so templates can display it without a JOIN
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
    teams_affected = len({i.get("teamAbbreviation") for i in injuries})
    status_counts  = Counter(i.get("playerStatus") for i in injuries)

    logger.info(
        "Stored %d record(s) across %d team(s)", stored, teams_affected
    )
    for status in ("Out", "Doubtful", "Questionable", "Probable"):
        if status_counts[status]:
            logger.info("  %-14s %d", status, status_counts[status])

    return stored
