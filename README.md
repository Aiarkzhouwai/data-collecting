# NBA Dashboard

A minimal NBA data dashboard built with **FastAPI**, **Jinja2**, **SQLite**, and **nba_api**.

## Features

- Yesterday's final scores
- Tomorrow's scheduled games
- Official NBA injury report
- Automatic hourly data refresh via APScheduler
- Manual refresh endpoint at `/refresh`

## Project structure

```
.
├── app/
│   ├── main.py              # FastAPI app — routes + lifespan
│   ├── db.py                # SQLite connection + init_db()
│   ├── models.py            # CREATE TABLE SQL (schema)
│   ├── logger.py            # Logging: INFO → console, ERROR → logs/error.log
│   ├── services/
│   │   ├── games.py         # Fetch + store daily scoreboard (nba_api)
│   │   ├── boxscores.py     # Fetch + store post-game box scores (nba_api)
│   │   └── injuries.py      # Fetch + store injury report (NBA CDN JSON)
│   ├── tasks/
│   │   └── scheduler.py     # APScheduler background refresh (every hour)
│   ├── templates/
│   │   ├── base.html        # Shared layout
│   │   └── index.html       # Homepage
│   └── static/
│       └── style.css        # Dark NBA-themed stylesheet
├── scripts/
│   └── ingest_all.py        # Manual one-shot ingest (run before first use)
├── data/
│   └── nba.db               # SQLite database (created on first run)
├── logs/
│   └── error.log            # Error log (created on first run)
└── requirements.txt
```

## Setup

**1. Create and activate a virtual environment**

```bash
python -m venv .venv
source .venv/bin/activate      # Mac / Linux
.venv\Scripts\activate         # Windows
```

**2. Install dependencies**

```bash
pip install -r requirements.txt
```

## Seed the database

Run this once before starting the server to populate initial data:

```bash
python scripts/ingest_all.py
```

This fetches yesterday's scores, tomorrow's schedule, box scores for
completed games, and the current injury report.

## Start the server

```bash
uvicorn app.main:app --reload
```

Open **http://localhost:8000** in your browser.

> The scheduler runs automatically and refreshes data every hour.
> You can also trigger an immediate refresh at **http://localhost:8000/refresh**.

## Database schema

| Table               | Contents                                      |
|---------------------|-----------------------------------------------|
| `games`             | One row per game — scores, teams, status      |
| `team_boxscores`    | Team totals (pts/reb/ast/fg%) per game        |
| `player_boxscores`  | Individual player stats per game              |
| `injury_reports`    | NBA official injury report entries            |
| `meta`              | Key/value store (e.g. `last_updated`)         |

## Data sources

| Source | Used for |
|--------|----------|
| `nba_api` → `ScoreboardV2` | Daily game schedule + scores |
| `nba_api` → `BoxScoreTraditionalV2` | Post-game box scores |
| NBA CDN JSON (`cdn.nba.com`) | Official injury report |

## Adding a new data source

1. Create `app/services/your_source.py` with a `fetch_and_store_*()` function.
2. Call it from `app/tasks/scheduler.refresh_all()`.
3. Add any new tables to `app/models.py` — they are created automatically on startup.
