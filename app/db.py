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
    new_columns: dict[str, list[tuple[str, str]]] = {
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
    }
    for table, cols in new_columns.items():
        for col, typ in cols:
            try:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {typ}")
            except sqlite3.OperationalError:
                pass  # column already exists
