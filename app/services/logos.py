"""
app/services/logos.py

Download official NBA team logos from the NBA CDN and cache them locally
in app/static/logos/{ABBR}.svg.

Logo source:
  https://cdn.nba.com/logos/nba/{team_id}/global/L/logo.svg

Called automatically on startup (ensure_all_logos) — skips files that
already exist, so the cost after the first run is just a directory scan.

Manual one-shot download:
  python scripts/download_logos.py
"""

import os
import time

import requests
from nba_api.stats.static import teams as nba_teams_static

from app.logger import logger

# Absolute path to app/static/logos/
_LOGO_DIR = os.path.join(os.path.dirname(__file__), "..", "static", "logos")

_NBA_CDN = "https://cdn.nba.com/logos/nba/{team_id}/global/L/logo.svg"

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; NBA-Dashboard/1.0)",
    "Accept":     "image/svg+xml,image/*",
}


# ── Public helpers ────────────────────────────────────────────────────────────

def logo_url(abbr: str) -> str:
    """
    Return the static URL path for a team logo (used in <img src="...">.
    Example: logo_url("BOS") → "/static/logos/BOS.svg"
    """
    return f"/static/logos/{abbr}.svg"


def ensure_all_logos() -> None:
    """
    Download any missing team logos on startup.
    Already-present files are skipped — subsequent startups cost ~0 ms.
    Network errors are logged and skipped; startup always continues.
    """
    os.makedirs(_LOGO_DIR, exist_ok=True)
    existing = set(os.listdir(_LOGO_DIR))

    teams   = nba_teams_static.get_teams()
    missing = [t for t in teams if f"{t['abbreviation']}.svg" not in existing]

    if not missing:
        logger.info("Team logos: all %d present in static/logos/", len(teams))
        return

    logger.info("Team logos: downloading %d missing file(s)…", len(missing))
    ok = fail = 0

    for team in missing:
        success = _download_one(team["id"], team["abbreviation"])
        if success:
            ok += 1
        else:
            fail += 1
        time.sleep(0.15)   # be polite to the CDN

    logger.info(
        "Team logos: %d downloaded, %d failed%s",
        ok, fail,
        " (missing logos will be hidden on cards)" if fail else "",
    )


def download_all(force: bool = False) -> dict[str, bool]:
    """
    Download logos for all 30 teams.
    Pass force=True to re-download even if files exist.
    Returns {abbr: success}.
    """
    os.makedirs(_LOGO_DIR, exist_ok=True)
    existing = set(os.listdir(_LOGO_DIR))
    results  = {}

    for team in nba_teams_static.get_teams():
        abbr = team["abbreviation"]
        if not force and f"{abbr}.svg" in existing:
            results[abbr] = True
            continue
        results[abbr] = _download_one(team["id"], abbr)
        time.sleep(0.15)

    return results


# ── Internal ──────────────────────────────────────────────────────────────────

def _download_one(team_id: int, abbr: str) -> bool:
    url  = _NBA_CDN.format(team_id=team_id)
    path = os.path.join(_LOGO_DIR, f"{abbr}.svg")
    try:
        resp = requests.get(url, timeout=10, headers=_HEADERS)
        resp.raise_for_status()
        with open(path, "wb") as f:
            f.write(resp.content)
        logger.debug("  Logo saved: %s", abbr)
        return True
    except Exception as exc:
        logger.warning("  Logo failed (%s): %s", abbr, exc)
        return False
