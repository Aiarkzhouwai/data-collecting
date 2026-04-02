import os
import sqlite3

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(ROOT, "data", "nba.db")


def get_db() -> sqlite3.Connection:
    """Open and return a SQLite connection. Rows are accessible as dicts."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")  # allows concurrent reads during writes
    return conn


def init_db() -> None:
    """Create all tables if they don't already exist, then apply migrations."""
    from app.models import CREATE_TABLES_SQL
    conn = get_db()
    conn.executescript(CREATE_TABLES_SQL)
    _migrate(conn)
    conn.commit()
    conn.close()


def _migrate(conn: sqlite3.Connection) -> None:
    """
    Add columns introduced after the initial schema.
    SQLite has no IF NOT EXISTS for ALTER TABLE, so we swallow the
    'duplicate column' error — harmless on a fresh DB.
    """
    # Ensure game_periods table exists (added after initial schema)
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS game_periods (
                id       INTEGER PRIMARY KEY AUTOINCREMENT,
                game_id  TEXT    NOT NULL,
                team_id  INTEGER NOT NULL,
                period   INTEGER NOT NULL,
                score    INTEGER,
                UNIQUE(game_id, team_id, period)
            )
        """)
    except Exception:
        pass

    # Ensure player_period_boxscores table exists (added after initial schema)
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS player_period_boxscores (
                game_id         TEXT    NOT NULL,
                player_id       INTEGER NOT NULL,
                player_name     TEXT,
                team_id         INTEGER,
                period          INTEGER NOT NULL,
                start_position  TEXT,
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
            )
        """)
    except Exception:
        pass

    # Ensure player_stints table exists (added after initial schema)
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS player_stints (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                game_id        TEXT    NOT NULL,
                player_id      INTEGER NOT NULL,
                player_name    TEXT,
                team_id        INTEGER,
                period         INTEGER NOT NULL,
                enter_clock    TEXT,
                exit_clock     TEXT,
                enter_secs     REAL,
                exit_secs      REAL,
                minutes_played REAL,
                duration_str   TEXT,
                is_active      INTEGER DEFAULT 0,
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
                updated_at     TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_player_stints_game_player
            ON player_stints (game_id, player_id)
        """)
    except Exception:
        pass

    new_columns: dict[str, list[tuple[str, str]]] = {
        "games": [
            ("game_status_id", "INTEGER"),  # 1=pre-game, 2=live, 3=final
        ],
        "team_boxscores": [
            ("fgm", "INTEGER"), ("fga", "INTEGER"),
            ("fg3m", "INTEGER"), ("fg3a", "INTEGER"),
            ("ftm", "INTEGER"), ("fta", "INTEGER"),
        ],
        "player_boxscores": [
            ("fgm", "INTEGER"), ("fga", "INTEGER"),
            ("fg3m", "INTEGER"), ("fg3a", "INTEGER"),
            ("ftm", "INTEGER"), ("fta", "INTEGER"),
        ],
        "injury_reports": [
            ("report_time", "TEXT"),
            ("game_time",   "TEXT"),
        ],
        "player_stints": [
            ("duration_str", "TEXT"),
            ("is_active",    "INTEGER DEFAULT 0"),
        ],
    }
    for table, cols in new_columns.items():
        for col, typ in cols:
            try:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {typ}")
            except sqlite3.OperationalError:
                pass  # column already exists
