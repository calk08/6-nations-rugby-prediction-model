"""Configuration constants for the data pipeline."""

from pathlib import Path

# ── Paths ──────────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "rugby.db"
LOG_DIR = PROJECT_ROOT / "logs"
LOG_FILE = LOG_DIR / "ingestion.log"
LOG_MAX_BYTES = 5 * 1024 * 1024  # 5 MB
LOG_BACKUP_COUNT = 3

# ── Recency decay ─────────────────────────────────────────────────────────────
DECAY_LAMBDA = 0.035  # w = exp(-DECAY_LAMBDA * weeks_since_match)

# ── Competition weights (for form calculation downstream) ─────────────────────
# Six Nations matches are weighted 1.2× relative to other internationals.
# This is stored as a column on the matches table so the feature‑engineering
# layer can simply multiply: effective_weight = recency_weight * competition_weight
COMPETITION_WEIGHTS: dict[str, float] = {
    "Six Nations": 1.2,
    "Autumn Internationals": 1.0,
    "Summer Tour": 1.0,
    "Rugby World Cup": 1.0,
    "Other": 1.0,
}

# ── HTTP / scraping ───────────────────────────────────────────────────────────
HTTP_TIMEOUT = 10  # seconds
HTTP_RETRIES = 3
HTTP_BACKOFF = 2  # seconds between retries
SCRAPE_DELAY = 2  # seconds between page requests
USER_AGENT = (
    "SixNationsRugbyPredictionBot/1.0 "
    "(+https://github.com/calk08/6-nations-rugby-prediction-model; "
    "educational/non-commercial)"
)

# ── Six Nations teams ─────────────────────────────────────────────────────────
TEAMS: dict[str, dict[str, str]] = {
    "ENG": {"name": "England", "country": "England"},
    "FRA": {"name": "France", "country": "France"},
    "IRL": {"name": "Ireland", "country": "Ireland"},
    "SCO": {"name": "Scotland", "country": "Scotland"},
    "WAL": {"name": "Wales", "country": "Wales"},
    "ITA": {"name": "Italy", "country": "Italy"},
}

# ── Stadium coordinates & metadata ───────────────────────────────────────────
STADIUMS: dict[str, dict[str, float | str | bool]] = {
    "Twickenham": {
        "team": "ENG",
        "lat": 51.4558,
        "lon": -0.3415,
        "has_roof": False,
    },
    "Stade de France": {
        "team": "FRA",
        "lat": 48.9244,
        "lon": 2.3601,
        "has_roof": False,
    },
    "Aviva Stadium": {
        "team": "IRL",
        "lat": 53.3352,
        "lon": -6.2285,
        "has_roof": False,
    },
    "BT Murrayfield": {
        "team": "SCO",
        "lat": 55.9465,
        "lon": -3.2410,
        "has_roof": False,  # partial roof – conservative default
    },
    "Principality Stadium": {
        "team": "WAL",
        "lat": 51.4782,
        "lon": -3.1826,
        "has_roof": True,  # retractable – resolved dynamically in weather fetcher
    },
    "Stadio Olimpico": {
        "team": "ITA",
        "lat": 41.9335,
        "lon": 12.4548,
        "has_roof": False,
    },
}

# Reverse lookup: team_id → home stadium name
TEAM_HOME_STADIUM: dict[str, str] = {
    v["team"]: k for k, v in STADIUMS.items()  # type: ignore[misc]
}

# ── URLs ──────────────────────────────────────────────────────────────────────
BBC_RUGBY_RESULTS_URL = "https://www.bbc.co.uk/sport/rugby-union/scores-fixtures"
WORLD_RUGBY_RANKINGS_URL = "https://www.world.rugby/rankings/mru"
OPEN_METEO_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
BBC_RUGBY_NEWS_URL = "https://www.bbc.co.uk/sport/rugby-union"
