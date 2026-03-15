"""Optional local scheduler using APScheduler.

Wraps the pipeline orchestrator so it can run on a recurring schedule
without relying on GitHub Actions or system cron.  Useful for local
development and testing.

Usage:

    python -m data_pipeline.scheduler          # starts the scheduler
    python -m data_pipeline.scheduler --once   # single immediate run
"""

from __future__ import annotations

import argparse
import logging
import sys

logger = logging.getLogger(__name__)


def _run_once() -> None:
    """Execute a single pipeline run."""
    from data_pipeline.pipeline import run_pipeline
    run_pipeline()


def main() -> None:
    """Parse CLI args and either run once or start a scheduled loop.

    Schedule: Saturday 23:00 UTC and Monday 06:00 UTC, matching the
    GitHub Actions cron configuration.
    """
    parser = argparse.ArgumentParser(description="Six Nations data pipeline scheduler")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run the pipeline once and exit (no scheduling)",
    )
    args = parser.parse_args()

    if args.once:
        _run_once()
        return

    try:
        from apscheduler.schedulers.blocking import BlockingScheduler
        from apscheduler.triggers.cron import CronTrigger
    except ImportError:
        print(
            "APScheduler is not installed.  Install it with:\n"
            "  pip install apscheduler\n"
            "Or use --once for a single run.",
            file=sys.stderr,
        )
        sys.exit(1)

    scheduler = BlockingScheduler()

    # Saturday 23:00 UTC — after most match days
    scheduler.add_job(
        _run_once,
        CronTrigger(day_of_week="sat", hour=23, minute=0, timezone="UTC"),
        id="saturday_run",
        name="Saturday post-match ingest",
    )

    # Monday 06:00 UTC — catch Sunday results + update weather
    scheduler.add_job(
        _run_once,
        CronTrigger(day_of_week="mon", hour=6, minute=0, timezone="UTC"),
        id="monday_run",
        name="Monday cleanup ingest",
    )

    logger.info("Scheduler started. Jobs: Saturday 23:00 UTC, Monday 06:00 UTC")
    print("Scheduler running — press Ctrl+C to stop.")

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped.")


if __name__ == "__main__":
    main()
