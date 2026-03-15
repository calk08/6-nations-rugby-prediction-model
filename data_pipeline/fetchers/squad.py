"""Fetch squad availability / injury data (best-effort).

Scrapes BBC Sport rugby union news pages for injury updates, team
announcements, and squad news for the six nations teams.  All records
carry a ``confidence`` field (0.0–1.0) reflecting how reliable the source is.
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
    BBC_RUGBY_NEWS_URL,
    HTTP_BACKOFF,
    HTTP_RETRIES,
    HTTP_TIMEOUT,
    SCRAPE_DELAY,
    TEAMS,
    USER_AGENT,
)

logger = logging.getLogger(__name__)


class SquadRecord(TypedDict):
    avail_id: str
    match_id: str
    team_id: str
    player_id: str
    player_name: str
    status: str  # "available" | "injured" | "suspended" | "unconfirmed"
    confidence: float  # 0.0–1.0
    fetched_at: str


_INJURY_KEYWORDS = re.compile(
    r"\b(injur|ruled out|sidelined|hamstring|knee|concussion|shoulder|ankle|"
    r"head injury|broken|fracture|torn|strain|sprain|surgery|rehab)\b",
    re.I,
)
_SUSPENSION_KEYWORDS = re.compile(
    r"\b(suspend|ban|red card ban|citing|disciplinary)\b",
    re.I,
)
_AVAILABLE_KEYWORDS = re.compile(
    r"\b(return|fit|available|selected|named|starting|bench|replacement)\b",
    re.I,
)

_TEAM_NAMES_LOWER = {info["name"].lower(): tid for tid, info in TEAMS.items()}


def _slugify(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")


@retry(stop=stop_after_attempt(HTTP_RETRIES), wait=wait_fixed(HTTP_BACKOFF), reraise=True)
def _get_page(url: str) -> str:
    """GET a page with retries.

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


def _detect_status(text: str) -> tuple[str, float]:
    """Infer player status and confidence from surrounding news text.

    Args:
        text: Paragraph or sentence containing the player reference.

    Returns:
        Tuple of (status, confidence).
    """
    if _SUSPENSION_KEYWORDS.search(text):
        return "suspended", 0.7
    if _INJURY_KEYWORDS.search(text):
        return "injured", 0.6
    if _AVAILABLE_KEYWORDS.search(text):
        return "available", 0.65
    return "unconfirmed", 0.3


def _detect_team(text: str) -> str | None:
    """Try to identify which team a news snippet refers to."""
    text_lower = text.lower()
    for name, tid in _TEAM_NAMES_LOWER.items():
        if name in text_lower:
            return tid
    return None


def fetch_squad_availability(match_id: str, team_id: str | None = None) -> list[SquadRecord]:
    """Scrape BBC Sport for squad/injury news related to a match.

    This is flagged as **best-effort data**: the confidence field reflects
    how certain we are about each player's status.

    Args:
        match_id: Match identifier (used as a foreign key).
        team_id: If provided, filter news to this team only.

    Returns:
        A list of ``SquadRecord`` dicts.  May be empty if no relevant
        news is found.

    Raises:
        No exceptions propagate — errors are logged and an empty list
        is returned.
    """
    records: list[SquadRecord] = []
    fetched_at = datetime.now(timezone.utc).isoformat()

    try:
        html = _get_page(BBC_RUGBY_NEWS_URL)
    except Exception as exc:
        logger.error("Failed to fetch BBC rugby news: %s", exc)
        return records

    soup = BeautifulSoup(html, "lxml")

    # Collect article links that might contain team/injury news
    article_links: list[str] = []
    for a_tag in soup.find_all("a", href=True):
        href = str(a_tag["href"])
        link_text = a_tag.get_text(" ", strip=True).lower()
        if any(kw in link_text for kw in ("team", "squad", "injur", "line-up", "lineup", "select")):
            if href.startswith("/"):
                href = f"https://www.bbc.co.uk{href}"
            if href.startswith("https://"):
                article_links.append(href)

    # Limit to avoid excessive scraping
    article_links = list(dict.fromkeys(article_links))[:5]

    for url in article_links:
        time.sleep(SCRAPE_DELAY)
        try:
            page_html = _get_page(url)
        except Exception:
            continue

        page_soup = BeautifulSoup(page_html, "lxml")

        # Look for paragraphs with player names and status keywords
        for para in page_soup.find_all(["p", "li"]):
            text = para.get_text(" ", strip=True)
            if len(text) < 10:
                continue

            detected_team = _detect_team(text)
            if team_id and detected_team and detected_team != team_id:
                continue

            status, confidence = _detect_status(text)
            if status == "unconfirmed" and confidence < 0.4:
                continue

            # Try to extract player names — look for capitalised proper nouns
            # that aren't team/country names
            potential_names = re.findall(r"\b([A-Z][a-z]+ [A-Z][a-z]+(?:\s[A-Z][a-z]+)?)\b", text)
            for name in potential_names:
                if name.lower() in _TEAM_NAMES_LOWER:
                    continue
                if any(skip in name.lower() for skip in ("six nations", "world rugby", "rugby union")):
                    continue

                player_id = _slugify(name)
                avail_id = f"{match_id}_{detected_team or 'UNK'}_{player_id}"

                if not any(r["avail_id"] == avail_id for r in records):
                    records.append(SquadRecord(
                        avail_id=avail_id,
                        match_id=match_id,
                        team_id=detected_team or (team_id or ""),
                        player_id=player_id,
                        player_name=name,
                        status=status,
                        confidence=confidence,
                        fetched_at=fetched_at,
                    ))

    logger.info(
        "Fetched %d squad availability records for match %s",
        len(records),
        match_id,
    )
    return records


def fetch_squad_batch(match_ids: list[str]) -> list[SquadRecord]:
    """Fetch squad availability for multiple matches.

    Args:
        match_ids: List of match identifiers.

    Returns:
        Combined list of ``SquadRecord`` dicts.
    """
    all_records: list[SquadRecord] = []
    for mid in match_ids:
        recs = fetch_squad_availability(mid)
        all_records.extend(recs)
    return all_records
