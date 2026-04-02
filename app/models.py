# SQL schema for all tables.
# init_db() in db.py runs this on startup.

CREATE_TABLES_SQL = """
-- One row per game per day
CREATE TABLE IF NOT EXISTS games (
    game_id         TEXT PRIMARY KEY,
    game_date       TEXT NOT NULL,       -- ISO date: 2024-01-05
    status_text     TEXT,                -- "Final", "Final/OT", "7:30 pm ET", etc.
    home_team_id    INTEGER,
    home_abbr       TEXT,
    home_name       TEXT,
    home_pts        INTEGER,             -- NULL for future games
    visitor_team_id INTEGER,
    visitor_abbr    TEXT,
    visitor_name    TEXT,
    visitor_pts     INTEGER,             -- NULL for future games
    updated_at      TEXT DEFAULT (datetime('now'))
);

-- One row per team per game (totals)
CREATE TABLE IF NOT EXISTS team_boxscores (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id     TEXT NOT NULL,
    team_id     INTEGER NOT NULL,
    team_abbr   TEXT,
    pts         INTEGER,
    reb         INTEGER,
    ast         INTEGER,
    stl         INTEGER,
    blk         INTEGER,
    fgm         INTEGER,
    fga         INTEGER,
    fg_pct      REAL,
    fg3m        INTEGER,
    fg3a        INTEGER,
    fg3_pct     REAL,
    ftm         INTEGER,
    fta         INTEGER,
    ft_pct      REAL,
    updated_at  TEXT DEFAULT (datetime('now')),
    UNIQUE(game_id, team_id)
);

-- One row per player per game
CREATE TABLE IF NOT EXISTS player_boxscores (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id         TEXT NOT NULL,
    team_id         INTEGER NOT NULL,
    player_id       INTEGER NOT NULL,
    player_name     TEXT,
    start_position  TEXT,               -- "G", "F", "C", or "" for bench
    minutes         TEXT,               -- "32:15"
    pts             INTEGER,
    reb             INTEGER,
    ast             INTEGER,
    stl             INTEGER,
    blk             INTEGER,
    turnovers       INTEGER,
    fgm             INTEGER,
    fga             INTEGER,
    fg_pct          REAL,
    fg3m            INTEGER,
    fg3a            INTEGER,
    fg3_pct         REAL,
    ftm             INTEGER,
    fta             INTEGER,
    ft_pct          REAL,
    plus_minus      INTEGER,
    updated_at      TEXT DEFAULT (datetime('now')),
    UNIQUE(game_id, player_id)
);

-- Official NBA injury report entries
CREATE TABLE IF NOT EXISTS injury_reports (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    report_date     TEXT NOT NULL,       -- ISO date the report was issued: 2024-01-05
    report_time     TEXT,                -- time the report was issued: "05:30 PM ET"
    game_date       TEXT,                -- ISO date of the affected game: 2024-01-06
    game_time       TEXT,                -- scheduled tip-off: "7:30 PM ET"
    game_id         TEXT,
    team_id         TEXT,
    team_abbr       TEXT,
    team_city       TEXT,
    team_name       TEXT,
    player_first    TEXT,
    player_last     TEXT,
    player_status   TEXT,               -- "Out", "Doubtful", "Questionable", "Probable"
    player_comment  TEXT,               -- e.g., "Right Ankle Sprain"
    created_at      TEXT DEFAULT (datetime('now'))
);

-- Quarter / OT period scores per team per game
CREATE TABLE IF NOT EXISTS game_periods (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id  TEXT    NOT NULL,
    team_id  INTEGER NOT NULL,
    period   INTEGER NOT NULL,   -- 1-4 regular, 5+ overtime
    score    INTEGER,
    UNIQUE(game_id, team_id, period)
);

-- Per-period (quarter/OT) player box scores — used for trailing-period analysis
CREATE TABLE IF NOT EXISTS player_period_boxscores (
    game_id         TEXT    NOT NULL,
    player_id       INTEGER NOT NULL,
    player_name     TEXT,
    team_id         INTEGER,
    period          INTEGER NOT NULL,   -- 1-4 regular, 5+ OT
    start_position  TEXT,               -- copied from full-game player_boxscores
    minutes         TEXT,
    pts             INTEGER,
    reb             INTEGER,
    ast             INTEGER,
    stl             INTEGER,
    blk             INTEGER,
    turnovers       INTEGER,
    fgm             INTEGER,
    fga             INTEGER,
    fg3m            INTEGER,
    fg3a            INTEGER,
    ftm             INTEGER,
    fta             INTEGER,
    plus_minus      INTEGER,
    updated_at      TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    PRIMARY KEY (game_id, player_id, period)
);

-- Per-player rotation stints derived from play-by-play substitution events
CREATE TABLE IF NOT EXISTS player_stints (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id        TEXT    NOT NULL,
    player_id      INTEGER NOT NULL,
    player_name    TEXT,
    team_id        INTEGER,
    period         INTEGER NOT NULL,
    enter_clock    TEXT,           -- elapsed from period start, e.g. "4:30"
    exit_clock     TEXT,           -- elapsed from period start, or "Active" for live
    enter_secs     REAL,           -- seconds remaining when entered
    exit_secs      REAL,           -- seconds remaining when exited
    minutes_played REAL,           -- stint duration in minutes
    duration_str   TEXT,           -- stint duration as "M:SS"
    is_active      INTEGER DEFAULT 0,  -- 1 = player currently on court (live game)
    pts            INTEGER DEFAULT 0,
    reb            INTEGER DEFAULT 0,
    fgm            INTEGER DEFAULT 0,
    fga            INTEGER DEFAULT 0,
    fg3m           INTEGER DEFAULT 0,
    fg3a           INTEGER DEFAULT 0,
    ftm            INTEGER DEFAULT 0,
    fta            INTEGER DEFAULT 0,
    turnovers      INTEGER DEFAULT 0,
    start_position TEXT,
    updated_at     TEXT    DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

CREATE INDEX IF NOT EXISTS idx_player_stints_game_player
    ON player_stints (game_id, player_id);

-- Key/value store for app metadata (e.g., last_updated timestamp)
CREATE TABLE IF NOT EXISTS meta (
    key     TEXT PRIMARY KEY,
    value   TEXT
);
"""
