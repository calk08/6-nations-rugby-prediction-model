"""Fetch weather forecasts for upcoming match venues via Open-Meteo.

Uses the free Open-Meteo forecast API (no API key required).  For each
upcoming fixture the forecast is extracted at the hour closest to kick-off
(UTC).  The ``has_roof`` flag is computed dynamically for Principality
Stadium: set to 1 when precipitation probability > 40 %, otherwise 0.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, TypedDict

import httpx
from tenacity import retry, stop_after_attempt, wait_fixed

from data_pipeline.config import (
    HTTP_BACKOFF,
    HTTP_RETRIES,
    HTTP_TIMEOUT,
    OPEN_METEO_FORECAST_URL,
    STADIUMS,
    TEAM_HOME_STADIUM,
    USER_AGENT,
)

logger = logging.getLogger(__name__)


class WeatherRecord(TypedDict):
    forecast_id: str
    match_id: str
    temperature: float
    precip_prob: float
    wind_speed: float
    has_roof: int
    fetched_at: str


@retry(stop=stop_after_attempt(HTTP_RETRIES), wait=wait_fixed(HTTP_BACKOFF), reraise=True)
def _fetch_forecast(lat: float, lon: float, date: str) -> dict[str, Any]:
    """Call the Open-Meteo forecast API for a single location and date.

    Args:
        lat: Latitude of the venue.
        lon: Longitude of the venue.
        date: ISO-8601 date string (``YYYY-MM-DD``).

    Returns:
        The parsed JSON response as a dict.

    Raises:
        httpx.HTTPStatusError: On API error after retries.
    """
    params: dict[str, str | float] = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m,precipitation_probability,windspeed_10m",
        "start_date": date,
        "end_date": date,
        "timezone": "UTC",
    }
    with httpx.Client(timeout=HTTP_TIMEOUT, headers={"User-Agent": USER_AGENT}) as client:
        resp = client.get(OPEN_METEO_FORECAST_URL, params=params)
        resp.raise_for_status()
        return resp.json()


def _extract_hourly(data: dict[str, Any], kickoff_hour: int) -> tuple[float, float, float]:
    """Pick the row from the hourly arrays closest to *kickoff_hour*.

    Args:
        data: Open-Meteo JSON response.
        kickoff_hour: Hour of kick-off in UTC (0–23).

    Returns:
        Tuple of (temperature_C, precipitation_probability_%, wind_speed_kmh).
    """
    hourly: dict[str, Any] = data.get("hourly", {})
    times: list[Any] = hourly.get("time", [])
    temps: list[Any] = hourly.get("temperature_2m", [])
    precips: list[Any] = hourly.get("precipitation_probability", [])
    winds: list[Any] = hourly.get("windspeed_10m", [])

    if not times:
        return 0.0, 0.0, 0.0

    # Find the index whose hour is closest to kickoff_hour
    best_idx = 0
    best_diff = 999
    for i, t in enumerate(times):
        try:
            hour = int(str(t).split("T")[1].split(":")[0])
        except (IndexError, ValueError):
            continue
        diff = abs(hour - kickoff_hour)
        if diff < best_diff:
            best_diff = diff
            best_idx = i

    temp: float = float(temps[best_idx]) if best_idx < len(temps) else 0.0
    precip: float = float(precips[best_idx]) if best_idx < len(precips) else 0.0
    wind: float = float(winds[best_idx]) if best_idx < len(winds) else 0.0
    return temp, precip, wind


def _resolve_has_roof(venue: str, precip_prob: float) -> int:
    """Determine ``has_roof`` for a venue.

    Principality Stadium has a retractable roof — closed (1) when
    precipitation probability > 40 %, open (0) otherwise.
    All other venues default to the value in ``config.STADIUMS``.

    Args:
        venue: Stadium name as it appears in ``config.STADIUMS``.
        precip_prob: Precipitation probability percentage from forecast.

    Returns:
        ``1`` if roof is closed / present, ``0`` otherwise.
    """
    if venue == "Principality Stadium":
        return 1 if precip_prob > 40 else 0
    stadium_info = STADIUMS.get(venue, {})
    return 1 if stadium_info.get("has_roof", False) else 0


def fetch_weather(
    match_id: str,
    venue: str,
    match_date: str,
    kickoff_hour: int = 15,
) -> WeatherRecord | None:
    """Fetch the weather forecast for a single match venue.

    Args:
        match_id: The match identifier.
        venue: Stadium name (must be a key in ``config.STADIUMS``).
        match_date: ISO-8601 date string for the match.
        kickoff_hour: Kick-off hour in UTC (default 15:00).

    Returns:
        A ``WeatherRecord`` dict, or ``None`` if the venue is unknown or
        the API call fails.

    Raises:
        No exceptions propagate — errors are logged and ``None`` is returned.
    """
    stadium = STADIUMS.get(venue)
    if stadium is None:
        # Try matching by home team
        parts = match_id.split("_")
        home_team = parts[1] if len(parts) >= 3 else None
        stadium_name = TEAM_HOME_STADIUM.get(home_team or "", "")
        stadium = STADIUMS.get(stadium_name)
        venue = stadium_name

    if stadium is None:
        logger.warning("Unknown venue %r for match %s — skipping weather", venue, match_id)
        return None

    lat = float(stadium["lat"])
    lon = float(stadium["lon"])

    try:
        data = _fetch_forecast(lat, lon, match_date)
    except Exception as exc:
        logger.error("Weather API failed for %s (%s): %s", match_id, venue, exc)
        return None

    temp, precip, wind = _extract_hourly(data, kickoff_hour)
    has_roof = _resolve_has_roof(venue, precip)

    return WeatherRecord(
        forecast_id=f"{match_id}_weather",
        match_id=match_id,
        temperature=round(temp, 1),
        precip_prob=round(precip, 1),
        wind_speed=round(wind, 1),
        has_roof=has_roof,
        fetched_at=datetime.now(timezone.utc).isoformat(),
    )


def fetch_weather_batch(
    matches: list[dict[str, str]],
    kickoff_hour: int = 15,
) -> list[WeatherRecord]:
    """Fetch weather for a batch of matches.

    Args:
        matches: List of dicts with at least ``match_id``, ``venue``, and
            ``date`` keys.
        kickoff_hour: Default kick-off hour if not specified per match.

    Returns:
        List of ``WeatherRecord`` dicts (skipping any that failed).
    """
    results: list[WeatherRecord] = []
    for m in matches:
        rec = fetch_weather(
            match_id=m["match_id"],
            venue=m.get("venue", ""),
            match_date=m["date"],
            kickoff_hour=kickoff_hour,
        )
        if rec is not None:
            results.append(rec)
    logger.info("Fetched %d weather forecasts", len(results))
    return results
