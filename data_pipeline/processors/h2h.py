"""Recompute head-to-head records for all 15 Six Nations team pairs.

H2H records are derived from the **full accumulated history** in the
``matches`` table — not just recent results.  The 15 pairs are generated
from the 6 team_ids sorted alphabetically so that each pair has a canonical
``pair_id`` (e.g. ``"ENG_FRA"``).
"""

from __future__ import annotations

import itertools
import logging
import sqlite3
from datetime import datetime, timezone
from typing import TypedDict

from data_pipeline.config import TEAMS

logger = logging.getLogger(__name__)


class H2HRecord(TypedDict):
    pair_id: str
    team_a: str
    team_b: str
    team_a_wins: int
    team_b_wins: int
    draws: int
    last_updated: str


def _pair_id(t1: str, t2: str) -> tuple[str, str, str]:
    """Return (pair_id, team_a, team_b) with teams in alphabetical order."""
    a, b = sorted([t1, t2])
    return f"{a}_{b}", a, b


def recompute_h2h(conn: sqlite3.Connection) -> list[H2HRecord]:
    """Recompute all 15 H2H pair records from the matches table.

    Args:
        conn: An open SQLite connection.

    Returns:
        List of ``H2HRecord`` dicts that were upserted.
    """
    team_ids = sorted(TEAMS.keys())
    pairs = list(itertools.combinations(team_ids, 2))
    now = datetime.now(timezone.utc).isoformat()
    records: list[H2HRecord] = []

    for t1, t2 in pairs:
        pid, team_a, team_b = _pair_id(t1, t2)

        # Count wins for team_a
        a_wins_home = conn.execute(
            "SELECT COUNT(*) FROM matches WHERE home_team = ? AND away_team = ? AND home_score > away_score",
            (team_a, team_b),
        ).fetchone()[0]
        a_wins_away = conn.execute(
            "SELECT COUNT(*) FROM matches WHERE home_team = ? AND away_team = ? AND away_score > home_score",
            (team_b, team_a),
        ).fetchone()[0]

        # Count wins for team_b
        b_wins_home = conn.execute(
            "SELECT COUNT(*) FROM matches WHERE home_team = ? AND away_team = ? AND home_score > away_score",
            (team_b, team_a),
        ).fetchone()[0]
        b_wins_away = conn.execute(
            "SELECT COUNT(*) FROM matches WHERE home_team = ? AND away_team = ? AND away_score > home_score",
            (team_a, team_b),
        ).fetchone()[0]

        # Count draws (both directions)
        draws = conn.execute(
            """SELECT COUNT(*) FROM matches
               WHERE ((home_team = ? AND away_team = ?) OR (home_team = ? AND away_team = ?))
                 AND home_score = away_score
                 AND home_score IS NOT NULL""",
            (team_a, team_b, team_b, team_a),
        ).fetchone()[0]

        rec = H2HRecord(
            pair_id=pid,
            team_a=team_a,
            team_b=team_b,
            team_a_wins=a_wins_home + a_wins_away,
            team_b_wins=b_wins_home + b_wins_away,
            draws=draws,
            last_updated=now,
        )
        records.append(rec)

        conn.execute(
            """INSERT OR REPLACE INTO h2h_records
               (pair_id, team_a, team_b, team_a_wins, team_b_wins, draws, last_updated)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (pid, team_a, team_b, rec["team_a_wins"], rec["team_b_wins"], draws, now),
        )

    logger.info("Recomputed H2H records for %d pairs", len(records))
    return records
