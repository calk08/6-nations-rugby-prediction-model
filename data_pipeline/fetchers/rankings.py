"""Fetch World Rugby international rankings.

Scrapes https://www.world.rugby/rankings/mru and parses the HTML rankings
table.  Only rows for the six participating nations are retained.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Any, TypedDict

import httpx
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_fixed

from data_pipeline.config import (
    HTTP_BACKOFF,
    HTTP_RETRIES,
    HTTP_TIMEOUT,
    USER_AGENT,
    WORLD_RUGBY_RANKINGS_URL,
)

logger = logging.getLogger(__name__)


class RankingRecord(TypedDict):
    ranking_id: str
    team_id: str
    position: int
    points: float
    fetched_at: str


# Map country names from World Rugby to our team_ids
_COUNTRY_TO_ID: dict[str, str] = {
    "england": "ENG",
    "france": "FRA",
    "ireland": "IRL",
    "scotland": "SCO",
    "wales": "WAL",
    "italy": "ITA",
}


@retry(stop=stop_after_attempt(HTTP_RETRIES), wait=wait_fixed(HTTP_BACKOFF), reraise=True)
def _get_page(url: str) -> str:
    """GET a page with retries.

    Args:
        url: Fully qualified URL.

    Returns:
        Response body as a string.

    Raises:
        httpx.HTTPStatusError: On 4xx/5xx after all retries.
    """
    with httpx.Client(timeout=HTTP_TIMEOUT, headers={"User-Agent": USER_AGENT}) as client:
        resp = client.get(url)
        resp.raise_for_status()
        return resp.text


def fetch_rankings() -> list[RankingRecord]:
    """Scrape the World Rugby men's rankings page.

    Parses the rankings table and returns rows for the six nations teams.
    Rankings are identified by a ``ranking_id`` combining the team_id and
    the current date so that historical snapshots are preserved.

    Returns:
        A list of ``RankingRecord`` dicts.  Empty list if the page cannot
        be fetched or parsed.

    Raises:
        No exceptions are raised — errors are logged and an empty list is
        returned so the pipeline can continue.
    """
    records: list[RankingRecord] = []
    fetched_at = datetime.now(timezone.utc).isoformat()
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    try:
        html = _get_page(WORLD_RUGBY_RANKINGS_URL)
    except Exception as exc:
        logger.error("Failed to fetch World Rugby rankings: %s", exc)
        return records

    soup = BeautifulSoup(html, "lxml")

    # Strategy 1: look for a <table> containing ranking rows
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        for row in rows:
            cells = row.find_all(["td", "th"])
            if len(cells) < 3:
                continue
            _try_parse_ranking_row(cells, records, today, fetched_at)

    # Strategy 2: look for structured div-based ranking entries
    if not records:
        for entry in soup.find_all("div", class_=re.compile(r"(ranking|team)", re.I)):
            text = entry.get_text(" ", strip=True).lower()
            for country, tid in _COUNTRY_TO_ID.items():
                if country in text:
                    # Extract position number and points
                    nums = re.findall(r"(\d+\.?\d*)", entry.get_text(" ", strip=True))
                    if len(nums) >= 2:
                        try:
                            pos = int(nums[0])
                            pts = float(nums[-1])
                            ranking_id = f"{tid}_{today}"
                            if not any(r["ranking_id"] == ranking_id for r in records):
                                records.append(RankingRecord(
                                    ranking_id=ranking_id,
                                    team_id=tid,
                                    position=pos,
                                    points=pts,
                                    fetched_at=fetched_at,
                                ))
                        except (ValueError, IndexError):
                            continue
                    break

    # Strategy 3: JSON-LD or script-embedded data
    if not records:
        for script in soup.find_all("script"):
            script_text = script.string or ""
            for country, tid in _COUNTRY_TO_ID.items():
                if country in script_text.lower():
                    # Try to find position/points patterns near the country name
                    pattern = re.compile(
                        rf"{country}.*?(\d+).*?(\d+\.\d+)",
                        re.I | re.DOTALL,
                    )
                    match = pattern.search(script_text)
                    if match:
                        ranking_id = f"{tid}_{today}"
                        if not any(r["ranking_id"] == ranking_id for r in records):
                            records.append(RankingRecord(
                                ranking_id=ranking_id,
                                team_id=tid,
                                position=int(match.group(1)),
                                points=float(match.group(2)),
                                fetched_at=fetched_at,
                            ))

    logger.info("Fetched %d ranking records", len(records))
    return records


def _try_parse_ranking_row(
    cells: list[Any],
    records: list[RankingRecord],
    today: str,
    fetched_at: str,
) -> None:
    """Attempt to parse a table row as a ranking entry."""
    try:
        row_text = " ".join(c.get_text(strip=True).lower() for c in cells)  # type: ignore[union-attr]
        for country, tid in _COUNTRY_TO_ID.items():
            if country in row_text:
                # First cell is typically position, last numeric cell is points
                nums: list[float] = []
                for c in cells:
                    txt = c.get_text(strip=True)  # type: ignore[union-attr]
                    try:
                        nums.append(float(txt))
                    except ValueError:
                        continue
                if len(nums) >= 2:
                    pos = int(nums[0])
                    pts = nums[-1]
                    ranking_id = f"{tid}_{today}"
                    if not any(r["ranking_id"] == ranking_id for r in records):
                        records.append(RankingRecord(
                            ranking_id=ranking_id,
                            team_id=tid,
                            position=pos,
                            points=pts,
                            fetched_at=fetched_at,
                        ))
                break
    except Exception as exc:
        logger.debug("Skipping unparseable ranking row: %s", exc)
