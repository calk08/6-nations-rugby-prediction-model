"""Pipeline orchestrator — ties all fetchers and processors together.

Run directly with:

    python -m data_pipeline.pipeline

Each data source is fetched independently; if one fails the others still
proceed.  At the end of the run the ingestion log is written and a clean
summary is printed to stdout.
"""

from __future__ import annotations

import logging
import sys
import time
from logging.handlers import RotatingFileHandler
from typing import Any

from data_pipeline.config import LOG_BACKUP_COUNT, LOG_DIR, LOG_FILE, LOG_MAX_BYTES
from data_pipeline.db import get_connection, init_db, log_run, upsert_many

# ── Logging setup ─────────────────────────────────────────────────────────────

def _setup_logging() -> None:
    """Configure root logger with rotating file + console handlers."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    root = logging.getLogger()
    root.setLevel(logging.INFO)

    # Rotating file handler
    fh = RotatingFileHandler(
        str(LOG_FILE),
        maxBytes=LOG_MAX_BYTES,
        backupCount=LOG_BACKUP_COUNT,
        encoding="utf-8",
    )
    fh.setFormatter(logging.Formatter(
        "%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    ))
    root.addHandler(fh)

    # Console handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.WARNING)
    ch.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    root.addHandler(ch)


logger = logging.getLogger(__name__)


# ── Pipeline steps ────────────────────────────────────────────────────────────

def run_pipeline() -> None:
    """Execute the full ingestion pipeline.

    1. Initialise the database (create tables if needed, seed teams).
    2. Fetch match results.
    3. Fetch player stats for new matches.
    4. Fetch World Rugby rankings.
    5. Fetch weather forecasts for upcoming / recent matches.
    6. Fetch squad availability.
    7. Recompute recency weights for ALL matches.
    8. Recompute H2H records for all 15 pairs.
    9. Log the run and print a summary.

    Each fetcher step is wrapped in a try/except so a single failure
    does not crash the whole pipeline.
    """
    _setup_logging()
    start = time.monotonic()
    logger.info("=== Pipeline run started ===")

    init_db()

    total_fetched = 0
    total_inserted = 0
    total_skipped = 0
    errors: list[str] = []

    # ── 1. Match results ──────────────────────────────────────────────────
    match_records: list[dict[str, Any]] = []
    try:
        from data_pipeline.fetchers.matches import fetch_matches

        match_records = fetch_matches()  # type: ignore[assignment]
        total_fetched += len(match_records)

        with get_connection() as conn:
            ins, skp = upsert_many(conn, "matches", match_records)
            total_inserted += ins
            total_skipped += skp
        logger.info("Matches: %d inserted, %d skipped", ins, skp)
    except Exception as exc:
        msg = f"Match fetcher failed: {exc}"
        logger.error(msg)
        errors.append(msg)

    # ── 2. Player stats ───────────────────────────────────────────────────
    try:
        from data_pipeline.fetchers.player_stats import fetch_all_player_stats

        new_match_ids = [m["match_id"] for m in match_records]
        if new_match_ids:
            stats = fetch_all_player_stats(new_match_ids)
            total_fetched += len(stats)

            with get_connection() as conn:
                ins, skp = upsert_many(conn, "player_stats", stats)
                total_inserted += ins
                total_skipped += skp
            logger.info("Player stats: %d inserted, %d skipped", ins, skp)
        else:
            logger.info("No new matches — skipping player stats fetch")
    except Exception as exc:
        msg = f"Player stats fetcher failed: {exc}"
        logger.error(msg)
        errors.append(msg)

    # ── 3. World Rugby rankings ───────────────────────────────────────────
    try:
        from data_pipeline.fetchers.rankings import fetch_rankings

        rankings = fetch_rankings()
        total_fetched += len(rankings)

        with get_connection() as conn:
            ins, skp = upsert_many(conn, "rankings", rankings)
            total_inserted += ins
            total_skipped += skp
        logger.info("Rankings: %d inserted, %d skipped", ins, skp)
    except Exception as exc:
        msg = f"Rankings fetcher failed: {exc}"
        logger.error(msg)
        errors.append(msg)

    # ── 4. Weather forecasts ──────────────────────────────────────────────
    try:
        from data_pipeline.fetchers.weather import fetch_weather_batch

        # Fetch weather for upcoming matches (those without scores yet)
        # and recently completed matches
        with get_connection() as conn:
            upcoming = conn.execute(
                """SELECT match_id, venue, date FROM matches
                   WHERE home_score IS NULL
                   ORDER BY date ASC LIMIT 20"""
            ).fetchall()
            upcoming_dicts = [
                {"match_id": r["match_id"], "venue": r["venue"], "date": r["date"]}
                for r in upcoming
            ]

        if upcoming_dicts:
            weather_recs = fetch_weather_batch(upcoming_dicts)
            total_fetched += len(weather_recs)

            with get_connection() as conn:
                ins, skp = upsert_many(conn, "weather_forecasts", weather_recs)
                total_inserted += ins
                total_skipped += skp
            logger.info("Weather: %d inserted, %d skipped", ins, skp)
        else:
            # Also try getting weather for the most recent matches
            weather_from_new = fetch_weather_batch(match_records[:5])
            total_fetched += len(weather_from_new)

            with get_connection() as conn:
                ins, skp = upsert_many(conn, "weather_forecasts", weather_from_new)
                total_inserted += ins
                total_skipped += skp
            logger.info("Weather (recent): %d inserted, %d skipped", ins, skp)
    except Exception as exc:
        msg = f"Weather fetcher failed: {exc}"
        logger.error(msg)
        errors.append(msg)

    # ── 5. Squad availability ─────────────────────────────────────────────
    try:
        from data_pipeline.fetchers.squad import fetch_squad_batch

        with get_connection() as conn:
            upcoming = conn.execute(
                """SELECT match_id FROM matches
                   WHERE home_score IS NULL
                   ORDER BY date ASC LIMIT 10"""
            ).fetchall()
            upcoming_ids = [r["match_id"] for r in upcoming]

        if upcoming_ids:
            squad_recs = fetch_squad_batch(upcoming_ids)
            total_fetched += len(squad_recs)

            with get_connection() as conn:
                ins, skp = upsert_many(conn, "squad_availability", squad_recs)
                total_inserted += ins
                total_skipped += skp
            logger.info("Squad: %d inserted, %d skipped", ins, skp)
        else:
            logger.info("No upcoming matches — skipping squad availability")
    except Exception as exc:
        msg = f"Squad fetcher failed: {exc}"
        logger.error(msg)
        errors.append(msg)

    # ── 6. Recency weights (all matches) ──────────────────────────────────
    try:
        from data_pipeline.processors.recency import recompute_recency_weights

        with get_connection() as conn:
            updated = recompute_recency_weights(conn)
        logger.info("Recency weights updated for %d matches", updated)
    except Exception as exc:
        msg = f"Recency weight processor failed: {exc}"
        logger.error(msg)
        errors.append(msg)

    # ── 7. H2H records ────────────────────────────────────────────────────
    try:
        from data_pipeline.processors.h2h import recompute_h2h

        with get_connection() as conn:
            h2h_recs = recompute_h2h(conn)
        logger.info("H2H records recomputed for %d pairs", len(h2h_recs))
    except Exception as exc:
        msg = f"H2H processor failed: {exc}"
        logger.error(msg)
        errors.append(msg)

    # ── 8. Log the run ────────────────────────────────────────────────────
    with get_connection() as conn:
        run_id = log_run(conn, total_fetched, total_inserted, total_skipped, errors)

    elapsed = round(time.monotonic() - start, 2)

    # ── Summary ───────────────────────────────────────────────────────────
    summary = (
        f"\n{'=' * 50}\n"
        f"  Pipeline run complete  (run_id: {run_id})\n"
        f"  Records fetched:   {total_fetched}\n"
        f"  Records inserted:  {total_inserted}\n"
        f"  Records skipped:   {total_skipped}\n"
        f"  Errors:            {len(errors)}\n"
        f"  Time elapsed:      {elapsed}s\n"
        f"{'=' * 50}"
    )

    if errors:
        summary += "\n  Error details:\n"
        for e in errors:
            summary += f"    - {e}\n"

    print(summary)
    logger.info(summary)
    logger.info("=== Pipeline run finished ===")


# ── __main__ entry point ──────────────────────────────────────────────────────

if __name__ == "__main__":
    run_pipeline()
