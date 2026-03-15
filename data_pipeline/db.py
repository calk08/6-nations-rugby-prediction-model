"""Database connection, table creation, and upsert helpers for SQLite."""

from __future__ import annotations

import json
import logging
import sqlite3
from collections.abc import Sequence
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Generator
from uuid import uuid4

from data_pipeline.config import COMPETITION_WEIGHTS, DB_PATH, TEAMS

logger = logging.getLogger(__name__)

# ── Schema DDL ────────────────────────────────────────────────────────────────
_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS teams (
    team_id TEXT PRIMARY KEY,
    name    TEXT NOT NULL,
    country TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS matches (
    match_id           TEXT PRIMARY KEY,
    date               TEXT NOT NULL,
    home_team          TEXT NOT NULL REFERENCES teams(team_id),
    away_team          TEXT NOT NULL REFERENCES teams(team_id),
    home_score         INTEGER,
    away_score         INTEGER,
    venue              TEXT,
    competition        TEXT,
    competition_weight REAL DEFAULT 1.0,
    recency_weight     REAL DEFAULT 1.0
);

CREATE TABLE IF NOT EXISTS player_stats (
    stat_id         TEXT PRIMARY KEY,
    match_id        TEXT NOT NULL REFERENCES matches(match_id),
    player_id       TEXT NOT NULL,
    player_name     TEXT,
    team_id         TEXT REFERENCES teams(team_id),
    position        TEXT,
    minutes_played  INTEGER DEFAULT 0,
    carries         INTEGER DEFAULT 0,
    metres_made     INTEGER DEFAULT 0,
    tackles_made    INTEGER DEFAULT 0,
    tackles_missed  INTEGER DEFAULT 0,
    lineouts_won    INTEGER DEFAULT 0,
    turnovers_won   INTEGER DEFAULT 0,
    handling_errors INTEGER DEFAULT 0,
    yellow_cards    INTEGER DEFAULT 0,
    red_cards       INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS h2h_records (
    pair_id      TEXT PRIMARY KEY,
    team_a       TEXT NOT NULL,
    team_b       TEXT NOT NULL,
    team_a_wins  INTEGER DEFAULT 0,
    team_b_wins  INTEGER DEFAULT 0,
    draws        INTEGER DEFAULT 0,
    last_updated TEXT
);

CREATE TABLE IF NOT EXISTS weather_forecasts (
    forecast_id TEXT PRIMARY KEY,
    match_id    TEXT NOT NULL REFERENCES matches(match_id),
    temperature REAL,
    precip_prob REAL,
    wind_speed  REAL,
    has_roof    INTEGER DEFAULT 0,
    fetched_at  TEXT
);

CREATE TABLE IF NOT EXISTS squad_availability (
    avail_id    TEXT PRIMARY KEY,
    match_id    TEXT NOT NULL REFERENCES matches(match_id),
    team_id     TEXT REFERENCES teams(team_id),
    player_id   TEXT,
    player_name TEXT,
    status      TEXT DEFAULT 'unconfirmed',
    confidence  REAL DEFAULT 0.0,
    fetched_at  TEXT
);

CREATE TABLE IF NOT EXISTS ingestion_log (
    run_id           TEXT PRIMARY KEY,
    run_at           TEXT NOT NULL,
    records_fetched  INTEGER DEFAULT 0,
    records_inserted INTEGER DEFAULT 0,
    records_skipped  INTEGER DEFAULT 0,
    errors           TEXT DEFAULT '[]'
);

CREATE TABLE IF NOT EXISTS rankings (
    ranking_id TEXT PRIMARY KEY,
    team_id    TEXT REFERENCES teams(team_id),
    position   INTEGER,
    points     REAL,
    fetched_at TEXT
);
"""


# ── Connection helpers ────────────────────────────────────────────────────────

@contextmanager
def get_connection(db_path: str | None = None) -> Generator[sqlite3.Connection, None, None]:
    """Yield an SQLite connection with WAL mode and foreign keys enabled.

    Args:
        db_path: Override path for the database file.  Defaults to ``config.DB_PATH``.

    Yields:
        An open ``sqlite3.Connection``.  The connection is committed on
        clean exit and rolled back on exception.
    """
    path = db_path or str(DB_PATH)
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db(db_path: str | None = None) -> None:
    """Create all tables if they do not exist and seed the teams table.

    Args:
        db_path: Override path for the database file.
    """
    with get_connection(db_path) as conn:
        conn.executescript(_SCHEMA_SQL)
        for team_id, info in TEAMS.items():
            conn.execute(
                "INSERT OR IGNORE INTO teams (team_id, name, country) VALUES (?, ?, ?)",
                (team_id, info["name"], info["country"]),
            )
    logger.info("Database initialised at %s", db_path or DB_PATH)


# ── Generic upsert ────────────────────────────────────────────────────────────

def upsert_row(conn: sqlite3.Connection, table: str, row: dict[str, Any]) -> bool:
    """Insert or replace a single row into *table*.

    Args:
        conn: An open SQLite connection.
        table: Target table name (must be a known table — validated internally).
        row: Column-name → value mapping.

    Returns:
        ``True`` if a new row was inserted, ``False`` if an existing row was
        replaced (i.e. duplicate detected).

    Raises:
        ValueError: If *table* is not in the known allow-list.
    """
    _ALLOWED_TABLES = {
        "matches", "player_stats", "h2h_records",
        "weather_forecasts", "squad_availability", "ingestion_log", "rankings",
    }
    if table not in _ALLOWED_TABLES:
        raise ValueError(f"Unknown table: {table!r}")

    cols = list(row.keys())
    placeholders = ", ".join("?" for _ in cols)
    col_names = ", ".join(cols)
    pk = cols[0]  # first column is always the PK by convention

    # Check for existing row to distinguish insert vs replace
    existing = conn.execute(
        f"SELECT 1 FROM {table} WHERE {pk} = ?", (row[pk],)  # noqa: S608
    ).fetchone()

    sql = f"INSERT OR REPLACE INTO {table} ({col_names}) VALUES ({placeholders})"
    conn.execute(sql, [row[c] for c in cols])

    return existing is None


def upsert_many(
    conn: sqlite3.Connection,
    table: str,
    rows: Sequence[dict[str, Any]],
) -> tuple[int, int]:
    """Upsert multiple rows and return (inserted, skipped) counts.

    Args:
        conn: An open SQLite connection.
        table: Target table name.
        rows: List of row dicts.

    Returns:
        Tuple of (newly inserted count, replaced/skipped count).
    """
    inserted = 0
    skipped = 0
    for row in rows:
        if upsert_row(conn, table, row):
            inserted += 1
        else:
            skipped += 1
    return inserted, skipped


# ── Ingestion log helper ─────────────────────────────────────────────────────

def log_run(
    conn: sqlite3.Connection,
    records_fetched: int,
    records_inserted: int,
    records_skipped: int,
    errors: list[str],
) -> str:
    """Write a row to the ``ingestion_log`` table.

    Args:
        conn: An open SQLite connection.
        records_fetched: Total records fetched from all sources.
        records_inserted: Total new records inserted.
        records_skipped: Total duplicate records skipped.
        errors: List of error message strings (empty list if clean run).

    Returns:
        The generated ``run_id``.
    """
    run_id = uuid4().hex[:12]
    upsert_row(conn, "ingestion_log", {
        "run_id": run_id,
        "run_at": datetime.now(timezone.utc).isoformat(),
        "records_fetched": records_fetched,
        "records_inserted": records_inserted,
        "records_skipped": records_skipped,
        "errors": json.dumps(errors),
    })
    return run_id
