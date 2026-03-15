"""Fetch per-player statistics for Six Nations matches.

Primary source: BBC Sport match detail pages (web scraping).
Falls back to parsing whatever tabular data is available on the match page.
"""

from __future__ import annotations

import logging
import re
import time
from typing import TypedDict

import httpx
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_fixed

from data_pipeline.config import (
    BBC_RUGBY_RESULTS_URL,
    HTTP_BACKOFF,
    HTTP_RETRIES,
    HTTP_TIMEOUT,
    SCRAPE_DELAY,
    USER_AGENT,
)

logger = logging.getLogger(__name__)


class PlayerStatRecord(TypedDict):
    stat_id: str
    match_id: str
    player_id: str
    player_name: str
    team_id: str
    position: str
    minutes_played: int
    carries: int
    metres_made: int
    tackles_made: int
    tackles_missed: int
    lineouts_won: int
    turnovers_won: int
    handling_errors: int
    yellow_cards: int
    red_cards: int


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


def _slugify(name: str) -> str:
    """Create a URL/ID-safe slug from a player name."""
    return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")


def _safe_int(value: str | None) -> int:
    """Parse a string to int, returning 0 on failure."""
    if not value:
        return 0
    try:
        return int(re.sub(r"[^\d]", "", value))
    except (ValueError, TypeError):
        return 0


def fetch_player_stats(match_id: str, match_url: str | None = None) -> list[PlayerStatRecord]:
    """Scrape player stats for a given match.

    Attempts to find and parse the BBC Sport match stats page.  The BBC
    typically publishes per-player stats tables for each match.

    Args:
        match_id: The match identifier (e.g. ``"2025_ENG_FRA"``).
        match_url: Direct URL to the match detail page.  If ``None``, a
            URL is constructed from the match_id.

    Returns:
        A list of ``PlayerStatRecord`` dicts ready for DB upsert.
        Returns an empty list if the page cannot be fetched or parsed.

    Raises:
        No exceptions are raised — errors are logged and an empty list
        is returned so the pipeline can continue.
    """
    records: list[PlayerStatRecord] = []

    # Build a search URL from the match_id components
    parts = match_id.split("_")
    if len(parts) < 3:
        logger.warning("Cannot parse match_id for player stats: %s", match_id)
        return records

    year, home, away = parts[0], parts[1], parts[2]

    # Try scraping a BBC match stats page
    if match_url is None:
        search_url = (
            f"https://www.bbc.co.uk/sport/rugby-union/"
            f"scores-fixtures/{year}"
        )
    else:
        search_url = match_url

    time.sleep(SCRAPE_DELAY)

    try:
        html = _get_page(search_url)
    except Exception as exc:
        logger.error("Failed to fetch player stats page for %s: %s", match_id, exc)
        return records

    soup = BeautifulSoup(html, "lxml")

    # Look for stats tables on the page
    tables = soup.find_all("table")
    for table in tables:
        _parse_stats_table(table, match_id, home, away, records)

    # Also check for structured data in div-based layouts
    for section in soup.find_all("div", class_=re.compile(r"(stats|lineup|team-list)", re.I)):
        _parse_stats_section(section, match_id, home, away, records)

    logger.info("Fetched %d player stat records for match %s", len(records), match_id)
    return records


def _parse_stats_table(
    table: BeautifulSoup,  # type: ignore[override]
    match_id: str,
    home: str,
    away: str,
    records: list[PlayerStatRecord],
) -> None:
    """Try to extract player stats from an HTML table element."""
    try:
        rows = table.find_all("tr")
        if len(rows) < 2:
            return

        # Try to determine header row
        headers_row = rows[0]
        headers = [th.get_text(strip=True).lower() for th in headers_row.find_all(["th", "td"])]

        if not any(kw in " ".join(headers) for kw in ("player", "name", "tackles", "carries")):
            return

        # Map header positions
        col_map: dict[str, int] = {}
        stat_keywords = {
            "player": "player_name",
            "name": "player_name",
            "pos": "position",
            "position": "position",
            "min": "minutes_played",
            "minutes": "minutes_played",
            "carries": "carries",
            "metres": "metres_made",
            "meters": "metres_made",
            "tackles made": "tackles_made",
            "tackles": "tackles_made",
            "missed": "tackles_missed",
            "lineout": "lineouts_won",
            "turnover": "turnovers_won",
            "handling": "handling_errors",
            "error": "handling_errors",
            "yellow": "yellow_cards",
            "red": "red_cards",
        }
        for i, h in enumerate(headers):
            for kw, field in stat_keywords.items():
                if kw in h and field not in col_map:
                    col_map[field] = i

        if "player_name" not in col_map:
            return

        current_team = home  # assume first table is home team

        for row in rows[1:]:
            cells = row.find_all(["td", "th"])
            if len(cells) <= col_map.get("player_name", 0):
                continue

            name_idx = col_map["player_name"]
            player_name = cells[name_idx].get_text(strip=True)
            if not player_name or len(player_name) < 2:
                continue

            player_id = _slugify(player_name)
            stat_id = f"{match_id}_{player_id}"

            rec = PlayerStatRecord(
                stat_id=stat_id,
                match_id=match_id,
                player_id=player_id,
                player_name=player_name,
                team_id=current_team,
                position=cells[col_map["position"]].get_text(strip=True) if "position" in col_map and len(cells) > col_map["position"] else "",
                minutes_played=_safe_int(cells[col_map["minutes_played"]].get_text(strip=True)) if "minutes_played" in col_map and len(cells) > col_map["minutes_played"] else 0,
                carries=_safe_int(cells[col_map["carries"]].get_text(strip=True)) if "carries" in col_map and len(cells) > col_map["carries"] else 0,
                metres_made=_safe_int(cells[col_map["metres_made"]].get_text(strip=True)) if "metres_made" in col_map and len(cells) > col_map["metres_made"] else 0,
                tackles_made=_safe_int(cells[col_map["tackles_made"]].get_text(strip=True)) if "tackles_made" in col_map and len(cells) > col_map["tackles_made"] else 0,
                tackles_missed=_safe_int(cells[col_map["tackles_missed"]].get_text(strip=True)) if "tackles_missed" in col_map and len(cells) > col_map["tackles_missed"] else 0,
                lineouts_won=_safe_int(cells[col_map["lineouts_won"]].get_text(strip=True)) if "lineouts_won" in col_map and len(cells) > col_map["lineouts_won"] else 0,
                turnovers_won=_safe_int(cells[col_map["turnovers_won"]].get_text(strip=True)) if "turnovers_won" in col_map and len(cells) > col_map["turnovers_won"] else 0,
                handling_errors=_safe_int(cells[col_map["handling_errors"]].get_text(strip=True)) if "handling_errors" in col_map and len(cells) > col_map["handling_errors"] else 0,
                yellow_cards=_safe_int(cells[col_map["yellow_cards"]].get_text(strip=True)) if "yellow_cards" in col_map and len(cells) > col_map["yellow_cards"] else 0,
                red_cards=_safe_int(cells[col_map["red_cards"]].get_text(strip=True)) if "red_cards" in col_map and len(cells) > col_map["red_cards"] else 0,
            )
            records.append(rec)
    except Exception as exc:
        logger.debug("Skipping unparseable stats table: %s", exc)


def _parse_stats_section(
    section: BeautifulSoup,  # type: ignore[override]
    match_id: str,
    home: str,
    away: str,
    records: list[PlayerStatRecord],
) -> None:
    """Try to extract player info from a div-based lineup/stats section."""
    try:
        # Look for player name elements within the section
        player_elements = section.find_all(
            ["span", "a", "li"],
            class_=re.compile(r"(player|name)", re.I),
        )
        team_id = home  # default to home

        # Check if section indicates which team
        section_text = section.get_text(" ", strip=True).lower()
        from data_pipeline.config import TEAMS
        for tid, info in TEAMS.items():
            if info["name"].lower() in section_text:
                team_id = tid
                break

        for elem in player_elements:
            player_name = elem.get_text(strip=True)
            if not player_name or len(player_name) < 2:
                continue
            player_id = _slugify(player_name)
            stat_id = f"{match_id}_{player_id}"

            # Minimal record — stats might not be available in this format
            if not any(r["stat_id"] == stat_id for r in records):
                records.append(PlayerStatRecord(
                    stat_id=stat_id,
                    match_id=match_id,
                    player_id=player_id,
                    player_name=player_name,
                    team_id=team_id,
                    position="",
                    minutes_played=0,
                    carries=0,
                    metres_made=0,
                    tackles_made=0,
                    tackles_missed=0,
                    lineouts_won=0,
                    turnovers_won=0,
                    handling_errors=0,
                    yellow_cards=0,
                    red_cards=0,
                ))
    except Exception as exc:
        logger.debug("Skipping unparseable stats section: %s", exc)


def fetch_all_player_stats(match_ids: list[str]) -> list[PlayerStatRecord]:
    """Convenience wrapper: fetch player stats for a batch of matches.

    Args:
        match_ids: List of match identifiers.

    Returns:
        Combined list of ``PlayerStatRecord`` dicts from all matches.
    """
    all_records: list[PlayerStatRecord] = []
    for mid in match_ids:
        stats = fetch_player_stats(mid)
        all_records.extend(stats)
        if match_ids.index(mid) < len(match_ids) - 1:
            time.sleep(SCRAPE_DELAY)
    return all_records
