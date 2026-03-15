"""Recompute exponential-decay recency weights for all matches.

On every pipeline run **every** historical match gets an updated weight:

    w = exp(-λ * weeks_since_match)

where λ = 0.035 (configurable in ``config.DECAY_LAMBDA``).

This must run over the *entire* matches table because the passage of time
shifts every weight, not just the newly-inserted rows.
"""

from __future__ import annotations

import logging
import math
import sqlite3
from datetime import datetime, timezone

from data_pipeline.config import DECAY_LAMBDA

logger = logging.getLogger(__name__)


def recompute_recency_weights(conn: sqlite3.Connection) -> int:
    """Update ``recency_weight`` for every row in the matches table.

    Args:
        conn: An open SQLite connection.

    Returns:
        The number of rows updated.
    """
    now = datetime.now(timezone.utc)
    cursor = conn.execute("SELECT match_id, date FROM matches")
    rows = cursor.fetchall()

    updated = 0
    for row in rows:
        match_id = row["match_id"] if isinstance(row, sqlite3.Row) else row[0]
        date_str = row["date"] if isinstance(row, sqlite3.Row) else row[1]

        try:
            match_dt = datetime.fromisoformat(date_str).replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            logger.warning("Invalid date for match %s: %r — skipping", match_id, date_str)
            continue

        delta = now - match_dt
        weeks_since = delta.total_seconds() / (7 * 24 * 3600)
        weight = math.exp(-DECAY_LAMBDA * weeks_since)

        conn.execute(
            "UPDATE matches SET recency_weight = ? WHERE match_id = ?",
            (round(weight, 6), match_id),
        )
        updated += 1

    logger.info("Updated recency weights for %d matches", updated)
    return updated
