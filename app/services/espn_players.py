"""
app/services/espn_players.py

Fetch NBA player profiles and season statistics from ESPN.

Strategy (three layers, each a fallback for the previous):
  1. ESPN Athlete JSON API  — bio, headshot, current-season averages
  2. ESPN Stats JSON API    — structured per-category season splits
  3. ESPN HTML stats page   — full season-by-season table (BeautifulSoup)

Player-ID mapping
-----------------
Our database stores nba_api player_ids.  ESPN uses its own athlete IDs.
We resolve the mapping once via ESPN's search API and cache the result
in process memory for the lifetime of the server.
"""

import json
import re
import time
from typing import Optional

import requests
from bs4 import BeautifulSoup

from app.logger import logger

# ── HTTP headers ──────────────────────────────────────────────────────────────

_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://www.espn.com/nba/",
}

_JSON_HEADERS = {
    "User-Agent": _BROWSER_HEADERS["User-Agent"],
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://www.espn.com",
    "Referer": "https://www.espn.com/nba/",
}

# ── In-memory caches ──────────────────────────────────────────────────────────

# nba_player_id (int) → (espn_athlete_id, search_content_dict)
_espn_id_cache: dict[int, tuple[int, dict]] = {}

# espn_athlete_id (int) → full profile dict
_profile_cache: dict[int, dict] = {}


# ── ESPN ID resolution ────────────────────────────────────────────────────────

def _extract_espn_id_from_content(content: dict) -> Optional[int]:
    """
    Extract the numeric ESPN athlete ID from a search result content object.

    ESPN search returns items with:
      uid  = "s:40~l:46~a:1966"        ← athlete ID is after "~a:"
      link.web = ".../id/1966/..."      ← fallback: parse from URL
    """
    # Method 1: uid field  e.g. "s:40~l:46~a:1966"
    uid = content.get("uid", "")
    m = re.search(r"~a:(\d+)", uid)
    if m:
        return int(m.group(1))

    # Method 2: web link  e.g. "https://www.espn.com/nba/player/_/id/1966/lebron-james"
    link = content.get("link", {}).get("web", "") or content.get("href", "")
    m = re.search(r"/id/(\d+)", link)
    if m:
        return int(m.group(1))

    # Method 3: top-level "id" field (older API shapes)
    raw_id = content.get("id", "")
    if str(raw_id).isdigit():
        return int(raw_id)

    return None


def _find_espn_id(player_name: str, nba_player_id: int) -> tuple[Optional[int], dict]:
    """
    Search ESPN for the player by name.
    Returns (espn_athlete_id, search_content_dict).
    espn_athlete_id is None on failure; search_content_dict may be empty.

    The ESPN search API (v2) returns:
      {
        "results": [
          { "type": "player", "contents": [ { "uid": "s:40~l:46~a:1966",
                                               "displayName": "LeBron James",
                                               "subtitle": "Los Angeles Lakers",
                                               "image": {"default": "...png"},
                                               "link": {"web": "...url"} } ] },
          ...
        ]
      }
    """
    if nba_player_id in _espn_id_cache:
        return _espn_id_cache[nba_player_id]

    logger.info("[ESPN] Searching for '%s' (NBA id=%d)", player_name, nba_player_id)

    try:
        resp = requests.get(
            "https://site.api.espn.com/apis/search/v2",
            params={"query": player_name, "limit": 10},
            headers=_JSON_HEADERS,
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()

        def _try_content(content: dict) -> Optional[int]:
            return _extract_espn_id_from_content(content)

        # Walk the "results" list and find the first "player" bucket
        for result_group in data.get("results", []):
            if result_group.get("type") not in ("player", "athlete"):
                continue
            for content in result_group.get("contents", []):
                espn_id = _try_content(content)
                if espn_id:
                    _espn_id_cache[nba_player_id] = (espn_id, content)
                    logger.info(
                        "[ESPN] Resolved '%s' → ESPN athlete ID %d", player_name, espn_id
                    )
                    return espn_id, content

        # Fallback: scan every content item regardless of result type
        for result_group in data.get("results", []):
            for content in result_group.get("contents", []):
                if content.get("type") in ("player", "athlete"):
                    espn_id = _try_content(content)
                    if espn_id:
                        _espn_id_cache[nba_player_id] = (espn_id, content)
                        logger.info(
                            "[ESPN] Resolved '%s' → ESPN athlete ID %d (fallback)",
                            player_name, espn_id,
                        )
                        return espn_id, content

    except requests.RequestException as exc:
        logger.warning("[ESPN] Search request failed for '%s': %s", player_name, exc)

    logger.warning("[ESPN] Could not resolve ESPN ID for '%s'", player_name)
    return None, {}


# ── ESPN JSON — athlete bio (scraped from player HTML page) ──────────────────

def _fetch_athlete_bio(espn_id: int, player_name: str, search_content: dict) -> dict:
    """
    Build a bio dict by combining:
      - Data from the ESPN search result (name, team, headshot URL)
      - Data scraped from the player HTML page header
    """
    # Headshot: ESPN CDN always serves player photos at this predictable URL
    headshot = (
        search_content.get("image", {}).get("default", "")
        or f"https://a.espncdn.com/i/headshots/nba/players/full/{espn_id}.png"
    )

    # Team from search subtitle e.g. "Los Angeles Lakers"
    team_name = search_content.get("subtitle", "—")

    bio: dict = {
        "full_name":  search_content.get("displayName", player_name),
        "headshot":   headshot,
        "team":       team_name,
        "team_abbr":  "—",
        "position":   "—",
        "jersey":     "—",
        "height":     "—",
        "weight":     "—",
        "age":        "—",
        "birthdate":  "—",
        "college":    "—",
        "experience": "—",
        "draft":      "—",
        "country":    "—",
    }

    # Supplement with HTML-scraped bio details
    try:
        url = f"https://www.espn.com/nba/player/_/id/{espn_id}"
        resp = requests.get(url, headers=_BROWSER_HEADERS, timeout=12)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # Jersey + position banner (e.g. "#23 • Small Forward")
        banner = soup.select_one(".PlayerHeader__Team_Number") or soup.select_one(".db")
        if banner:
            txt = banner.get_text(" ", strip=True)
            m = re.search(r"#(\d+)", txt)
            if m:
                bio["jersey"] = m.group(1)
            m = re.search(r"•\s*(.+)", txt)
            if m:
                bio["position"] = m.group(1).strip()

        # Bio stats list (height, weight, DOB, college, etc.)
        for item in soup.select(".PlayerHeader__Bio_List li, .PlayerHeader__Bio li, .bio-list li"):
            label_el = item.select_one(".ttu, .n8")
            value_el = item.select_one(".fw-medium, .n8 + span") or item
            label = label_el.get_text(strip=True).lower() if label_el else ""
            value = value_el.get_text(strip=True) if value_el else ""

            if "ht" in label or "height" in label:
                bio["height"] = value
            elif "wt" in label or "weight" in label:
                bio["weight"] = value
            elif "age" in label:
                bio["age"] = value
            elif "birth" in label:
                bio["birthdate"] = value
            elif "college" in label:
                bio["college"] = value
            elif "exp" in label:
                bio["experience"] = value
            elif "draft" in label:
                bio["draft"] = value
            elif "country" in label or "national" in label:
                bio["country"] = value

        # Abbreviation: try to find it in the team link or header
        abbr_el = soup.select_one(".PlayerHeader__Team_Abbrev")
        if abbr_el:
            bio["team_abbr"] = abbr_el.get_text(strip=True)

    except Exception as exc:
        logger.debug("[ESPN] Bio HTML scrape failed for ESPN ID %d: %s", espn_id, exc)

    return bio


# ── ESPN JSON — season statistics ─────────────────────────────────────────────

def _fetch_athlete_stats(espn_id: int) -> dict:
    """
    GET /apis/common/v3/sports/basketball/nba/athletes/{id}/stats
    Returns categories list: each has labels[], displayNames[], statistics[].
    The statistics list has one entry per season (most recent last).
    """
    url = (
        f"https://site.web.api.espn.com/apis/common/v3/sports/basketball/nba"
        f"/athletes/{espn_id}/stats"
    )
    resp = requests.get(url, headers=_JSON_HEADERS, timeout=10)
    resp.raise_for_status()
    return resp.json()


def _parse_stats_splits(data: dict) -> dict:
    """
    Extract current-season averages from the ESPN stats API response.

    Structure:
      data["categories"][n]
        .labels        = ["GP", "GS", "MIN", "FG", "FG%", "3PT", "3P%", ...]
        .displayNames  = ["Games Played", ...]
        .statistics[n]
          .season.displayName = "2024-25"
          .stats = ["79", "39.5", "7.9-18.9", "41.7", ...]

    We take the "averages" category and the most recent season entry.
    """
    result: dict[str, str] = {}

    for category in data.get("categories", []):
        # Only parse the averages category for the headline numbers
        if category.get("name") not in ("averages", "average", "season"):
            continue

        labels = category.get("labels", [])
        display_names = category.get("displayNames", [])
        stats_seasons = category.get("statistics", [])

        if not labels or not stats_seasons:
            continue

        # Most recent season is the last entry in the list
        latest = stats_seasons[-1]
        values = latest.get("stats", [])
        season_name = latest.get("season", {}).get("displayName", "")

        result["Season"] = season_name
        for label, display, value in zip(labels, display_names, values):
            if label and value not in (None, ""):
                result[label] = str(value)

        break  # only need the averages category

    return result


# ── ESPN HTML — BeautifulSoup stats page scraper ──────────────────────────────

# Ordered list of stat abbreviations we want to surface prominently
_KEY_STATS = ["GP", "GS", "MIN", "PTS", "REB", "AST", "STL", "BLK", "TO",
              "FG%", "3P%", "FT%"]

# Friendly display names for the key stats
_STAT_LABELS: dict[str, str] = {
    "GP":  "Games Played",
    "GS":  "Games Started",
    "MIN": "Minutes",
    "PTS": "Points",
    "REB": "Rebounds",
    "AST": "Assists",
    "STL": "Steals",
    "BLK": "Blocks",
    "TO":  "Turnovers",
    "FG%": "Field Goal %",
    "3P%": "3-Point %",
    "FT%": "Free Throw %",
}


def _scrape_player_page(espn_id: int) -> dict:
    """
    Scrape https://www.espn.com/nba/player/stats/_/id/{espn_id}
    using BeautifulSoup.

    Returns a dict with:
      "seasons"         – list of season-row dicts  (full career table)
      "current_season"  – dict for the most recent row  (headline numbers)
      "raw_headers"     – column headers from the stats table
    """
    url = f"https://www.espn.com/nba/player/stats/_/id/{espn_id}"
    logger.info("[ESPN] Scraping stats page: %s", url)

    resp = requests.get(url, headers=_BROWSER_HEADERS, timeout=15)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    seasons: list[dict] = []
    headers: list[str] = []

    # ── ESPN uses a split-table layout ─────────────────────────────────────
    # Left table (sticky):  season label + team
    # Right table (scroll): all stat columns
    # Both are siblings inside a .ResponsiveTable (or .Table__ScrollerWrapper)
    tables = soup.select("table.Table")

    if len(tables) >= 2:
        # Split-table layout (current ESPN design)
        left_table  = tables[0]
        right_table = tables[1]

        # Headers come from the right (scrollable) table
        headers = [th.get_text(strip=True) for th in right_table.select("thead th")]

        left_rows  = left_table.select("tbody tr")
        right_rows = right_table.select("tbody tr")

        for left_tr, right_tr in zip(left_rows, right_rows):
            left_cells  = [td.get_text(strip=True) for td in left_tr.select("td")]
            right_cells = [td.get_text(strip=True) for td in right_tr.select("td")]

            if not right_cells:
                continue

            row: dict[str, str] = {}
            if left_cells:
                row["Season"] = left_cells[0]
            if len(left_cells) > 1:
                row["Team"] = left_cells[1]

            for header, cell in zip(headers, right_cells):
                if header:
                    row[header] = cell

            seasons.append(row)

    elif len(tables) == 1:
        # Older single-table layout (fallback)
        table = tables[0]
        headers = [th.get_text(strip=True) for th in table.select("thead th")]
        for tr in table.select("tbody tr"):
            cells = [td.get_text(strip=True) for td in tr.select("td")]
            if len(cells) == len(headers):
                seasons.append(dict(zip(headers, cells)))

    else:
        # Tables may be inside divs with responsive wrappers
        logger.debug("[ESPN] No <table class='Table'> found for ESPN ID %d", espn_id)

    # Most recent season = last non-total row
    current: dict = {}
    for row in reversed(seasons):
        season_label = row.get("Season", "").lower()
        if season_label and season_label not in ("", "career", "total"):
            current = row
            break

    logger.info(
        "[ESPN] Scraped %d season rows for ESPN ID %d (current: %s)",
        len(seasons), espn_id, current.get("Season", "?"),
    )

    return {
        "seasons": seasons,
        "current_season": current,
        "raw_headers": headers,
    }


# ── Public API ────────────────────────────────────────────────────────────────

def get_player_profile_by_name(player_name: str) -> Optional[dict]:
    """
    Name-only profile lookup — no NBA player_id required.
    Used for injury-report pages where we only have a player's name.

    Uses a stable virtual NBA ID derived from the name hash so the
    in-memory caches behave identically to the regular code path.
    (Real NBA player IDs are < 5 million; virtual IDs start at 1 billion.)
    """
    virtual_id = abs(hash(player_name)) % (10 ** 8) + 10 ** 9
    return get_player_profile(virtual_id, player_name)


def get_player_profile(nba_player_id: int, player_name: str) -> Optional[dict]:
    """
    Build and return a complete player profile dict for the given player.

    Data sources (in order of reliability):
      1. ESPN Athlete JSON API  → bio + headshot + embedded quick-stats
      2. ESPN Stats JSON API    → structured season averages per category
      3. ESPN HTML page scrape  → full season-by-season career table

    Returns None only if the ESPN athlete ID cannot be resolved at all.
    Cache: full profiles are cached by ESPN ID for the process lifetime.
    """
    # ── 1. Resolve ESPN ID (also retrieves search content for quick bio seed) ──
    espn_id, search_content = _find_espn_id(player_name, nba_player_id)
    if espn_id is None:
        return None

    if espn_id in _profile_cache:
        logger.debug("[ESPN] Serving cached profile for ESPN ID %d", espn_id)
        return _profile_cache[espn_id]

    profile: dict = {
        "nba_player_id": nba_player_id,
        "espn_id": espn_id,
        "espn_url": f"https://www.espn.com/nba/player/_/id/{espn_id}",
        "player_name": player_name,
        "bio": {},
        "season_averages": {},   # from stats JSON
        "seasons": [],           # full career table from HTML scrape
        "current_season": {},    # most recent row from HTML scrape
        "raw_headers": [],
        "errors": [],
    }

    # ── 2. Bio: search result seed + HTML scrape for details ─────────────────
    try:
        profile["bio"] = _fetch_athlete_bio(espn_id, player_name, search_content)
        time.sleep(0.4)
        logger.info("[ESPN] Bio fetched for '%s'", player_name)
    except Exception as exc:
        msg = f"Bio fetch failed: {exc}"
        profile["errors"].append(msg)
        logger.warning("[ESPN] %s (ESPN ID %d)", msg, espn_id)

    # ── 3. Season averages from ESPN Stats API ────────────────────────────────
    try:
        stats_raw = _fetch_athlete_stats(espn_id)
        time.sleep(0.4)
        profile["season_averages"] = _parse_stats_splits(stats_raw)
        logger.info("[ESPN] Stats splits fetched for '%s'", player_name)

    except Exception as exc:
        msg = f"Stats API failed: {exc}"
        profile["errors"].append(msg)
        logger.warning("[ESPN] %s (ESPN ID %d)", msg, espn_id)

    # ── 4. Full career table from HTML scrape ─────────────────────────────────
    try:
        scraped = _scrape_player_page(espn_id)
        time.sleep(0.3)
        profile["seasons"]        = scraped["seasons"]
        profile["current_season"] = scraped["current_season"]
        profile["raw_headers"]    = scraped["raw_headers"]

    except Exception as exc:
        msg = f"HTML scrape failed: {exc}"
        profile["errors"].append(msg)
        logger.warning("[ESPN] %s (ESPN ID %d)", msg, espn_id)

    # ── Merge: build a unified averages dict ─────────────────────────────────
    # Priority (highest wins): season_averages (structured JSON) → current_season (HTML scrape)
    merged: dict[str, str] = {}
    for k, v in profile.get("current_season", {}).items():
        if k not in ("Season", "Team"):
            merged[k] = v
    merged.update(profile.get("season_averages", {}))
    profile["averages"] = merged

    # ── Build ordered key-stat list for the headline row ─────────────────────
    profile["key_stats"] = [
        {"abbr": abbr, "label": _STAT_LABELS.get(abbr, abbr), "value": merged.get(abbr, "—")}
        for abbr in _KEY_STATS
        if merged.get(abbr) not in (None, "", "—", "0")
    ]

    _profile_cache[espn_id] = profile
    return profile
