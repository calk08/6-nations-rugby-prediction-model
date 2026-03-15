"""Fetch Six Nations (and other international) match results.

Primary source: BBC Sport rugby scores & fixtures pages (web scraping).
Uses BeautifulSoup with lxml, respects a 2-second inter-request delay, and
sets a descriptive User-Agent header.

The fetcher infers the current Six Nations season from the current date and
also supports fetching Autumn Internationals / Summer Tours so that H2H and
form data stay current year-round.
"""

from __future__ import annotations

import logging
import re
import time
from datetime import datetime, timezone
from typing import TypedDict

import httpx
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_fixed

from data_pipeline.config import (
    BBC_RUGBY_RESULTS_URL,
    COMPETITION_WEIGHTS,
    HTTP_BACKOFF,
    HTTP_RETRIES,
    HTTP_TIMEOUT,
    SCRAPE_DELAY,
    TEAMS,
    USER_AGENT,
)

logger = logging.getLogger(__name__)

# ── Data transfer object ──────────────────────────────────────────────────────

class MatchRecord(TypedDict):
    match_id: str
    date: str
    home_team: str
    away_team: str
    home_score: int
    away_score: int
    venue: str
    competition: str
    competition_weight: float


# ── Team name → team_id mapping ──────────────────────────────────────────────

_NAME_TO_ID: dict[str, str] = {}
for _tid, _info in TEAMS.items():
    _NAME_TO_ID[_info["name"].lower()] = _tid
    _NAME_TO_ID[_info["country"].lower()] = _tid
# Common aliases
_NAME_TO_ID.update({
    "les bleus": "FRA",
    "azzurri": "ITA",
    "thistle": "SCO",
})

SIX_NATIONS_TEAM_IDS = set(TEAMS.keys())


def _resolve_team(name: str) -> str | None:
    """Resolve a free-text team name to a six-nations team_id, or None."""
    return _NAME_TO_ID.get(name.strip().lower())


def _infer_competition(date_str: str) -> str:
    """Guess competition from the match date."""
    try:
        dt = datetime.fromisoformat(date_str)
    except ValueError:
        return "Other"
    month = dt.month
    if 1 <= month <= 3:
        return "Six Nations"
    if 6 <= month <= 7:
        return "Summer Tour"
    if 10 <= month <= 12:
        return "Autumn Internationals"
    return "Other"


def _make_match_id(date_str: str, home: str, away: str) -> str:
    """Generate a deterministic match_id like '2025_ENG_FRA'."""
    try:
        year = datetime.fromisoformat(date_str).year
    except ValueError:
        year = datetime.now(timezone.utc).year
    return f"{year}_{home}_{away}"


# ── HTTP helper ───────────────────────────────────────────────────────────────

@retry(stop=stop_after_attempt(HTTP_RETRIES), wait=wait_fixed(HTTP_BACKOFF), reraise=True)
def _get_page(url: str) -> str:
    """GET a page with retries and return the response text.

    Args:
        url: Fully qualified URL.

    Returns:
        Response body as a string.

    Raises:
        httpx.HTTPStatusError: On 4xx/5xx after retries.
    """
    with httpx.Client(timeout=HTTP_TIMEOUT, headers={"User-Agent": USER_AGENT}) as client:
        resp = client.get(url)
        resp.raise_for_status()
        return resp.text


# ── Core fetcher ──────────────────────────────────────────────────────────────

def _current_six_nations_season() -> int:
    """Return the year of the current (or most recent) Six Nations season."""
    now = datetime.now(timezone.utc)
    # The tournament runs Jan–Mar; if we're past March the "current" season
    # is this year's completed tournament; before April it's still this year.
    return now.year


def fetch_matches(from_date: str | None = None) -> list[MatchRecord]:
    """Scrape BBC Sport for recent Six Nations and other international results.

    Parses the BBC rugby scores-fixtures page for completed matches involving
    any of the six teams.  Only matches between two Six Nations teams are
    returned so that the DB stays focused on relevant fixtures.

    Args:
        from_date: ISO-8601 date string.  If provided, only matches on or
            after this date are included.  Defaults to the start of the
            current Six Nations season (1 Jan of the inferred year).

    Returns:
        A list of ``MatchRecord`` dicts ready for DB upsert.

    Raises:
        httpx.HTTPStatusError: If the BBC page cannot be fetched after retries.
    """
    if from_date is None:
        season = _current_six_nations_season()
        from_date = f"{season}-01-01"

    from_dt = datetime.fromisoformat(from_date).replace(tzinfo=timezone.utc)
    records: list[MatchRecord] = []

    try:
        html = _get_page(BBC_RUGBY_RESULTS_URL)
    except Exception as exc:
        logger.error("Failed to fetch BBC results page: %s", exc)
        return records

    soup = BeautifulSoup(html, "lxml")

    # BBC structures results in date-grouped sections.
    # We look for match result elements containing team names & scores.
    # The exact markup evolves, so we try multiple selectors.

    # Strategy: find elements that look like completed fixtures
    for article in soup.select("article, [data-testid='match']"):
        _parse_bbc_match_element(article, records, from_dt)

    # Fallback: broader search if selectors didn't match
    if not records:
        for div in soup.find_all("div", class_=re.compile(r"(fixture|match|result)", re.I)):
            _parse_bbc_match_element(div, records, from_dt)

    # Additional date-specific pages for the current season
    season = _current_six_nations_season()
    for month in range(1, 4):  # Jan–Mar for Six Nations
        date_url = f"{BBC_RUGBY_RESULTS_URL}/{season}-{month:02d}"
        time.sleep(SCRAPE_DELAY)
        try:
            html = _get_page(date_url)
        except Exception:
            continue
        soup = BeautifulSoup(html, "lxml")
        for article in soup.select("article, [data-testid='match']"):
            _parse_bbc_match_element(article, records, from_dt)
        for div in soup.find_all("div", class_=re.compile(r"(fixture|match|result)", re.I)):
            _parse_bbc_match_element(div, records, from_dt)

    # Deduplicate by match_id
    seen: set[str] = set()
    unique: list[MatchRecord] = []
    for rec in records:
        if rec["match_id"] not in seen:
            seen.add(rec["match_id"])
            unique.append(rec)
    records = unique

    logger.info("Fetched %d match records from BBC Sport", len(records))
    return records


def _parse_bbc_match_element(
    element: BeautifulSoup,  # type: ignore[override]
    records: list[MatchRecord],
    from_dt: datetime,
) -> None:
    """Attempt to extract a match record from a BBC Sport HTML element.

    This is best-effort: if parsing fails the element is silently skipped.
    """
    try:
        text = element.get_text(" ", strip=True)

        # Try to find two Six Nations team names in the text
        found_teams: list[str] = []
        for name, tid in _NAME_TO_ID.items():
            if name in text.lower() and tid not in found_teams:
                found_teams.append(tid)
        if len(found_teams) < 2:
            return

        # Extract scores — look for patterns like "17 - 23" or "17-23"
        score_match = re.search(r"(\d{1,3})\s*[-–]\s*(\d{1,3})", text)
        if not score_match:
            return

        home_score = int(score_match.group(1))
        away_score = int(score_match.group(2))

        # Determine home/away by order of appearance in text
        home_team = found_teams[0]
        away_team = found_teams[1]

        # Try to extract date
        date_match = re.search(
            r"(\d{1,2})\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\w*\s+(\d{4})",
            text,
        )
        if date_match:
            date_str = datetime.strptime(
                f"{date_match.group(1)} {date_match.group(2)} {date_match.group(3)}",
                "%d %b %Y",
            ).strftime("%Y-%m-%d")
        else:
            date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        match_dt = datetime.fromisoformat(date_str).replace(tzinfo=timezone.utc)
        if match_dt < from_dt:
            return

        competition = _infer_competition(date_str)
        match_id = _make_match_id(date_str, home_team, away_team)

        records.append(MatchRecord(
            match_id=match_id,
            date=date_str,
            home_team=home_team,
            away_team=away_team,
            home_score=home_score,
            away_score=away_score,
            venue="",
            competition=competition,
            competition_weight=COMPETITION_WEIGHTS.get(competition, 1.0),
        ))
    except Exception as exc:
        logger.debug("Skipping unparseable element: %s", exc)
