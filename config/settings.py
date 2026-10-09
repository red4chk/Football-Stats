"""
Application configuration loaded from environment variables.

All values are read once at import time. Override them in a local
``.env`` file (see ``.env.example`` for the template).
"""
import os
from dotenv import load_dotenv

load_dotenv(override=True)

# ---------------------------------------------------------------------------
# API-Football / RapidAPI credentials
# ---------------------------------------------------------------------------
RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY", "")
# api-sports.io accepts the same key via a different header name — keep an alias.
API_SPORTS_KEY = RAPIDAPI_KEY
RAPIDAPI_HOST = "api-football-v1.p.rapidapi.com"
API_BASE_URL = "https://v3.football.api-sports.io"

# ---------------------------------------------------------------------------
# MongoDB
# ---------------------------------------------------------------------------
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://admin:password@localhost:27017/")
MONGODB_DB = os.getenv("MONGODB_DB", "football_stats")

# ---------------------------------------------------------------------------
# Default league / season scope for fetchers and assets
# ---------------------------------------------------------------------------
# 39 = English Premier League, 140 = La Liga, 135 = Serie A, 78 = Bundesliga,
# 61 = Ligue 1, 2 = UEFA Champions League
DEFAULT_LEAGUE_ID = int(os.getenv("DEFAULT_LEAGUE_ID", "39"))
DEFAULT_SEASON = int(os.getenv("DEFAULT_SEASON", "2024"))

# ---------------------------------------------------------------------------
# HTTP client behaviour
# ---------------------------------------------------------------------------
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
RETRY_BACKOFF = int(os.getenv("RETRY_BACKOFF", "2"))
# Seconds to sleep between paginated API pages to stay under the free-tier
# rate limit. Paid-tier users can lower this via an env override.
PAGINATION_SLEEP = int(os.getenv("PAGINATION_SLEEP", "7"))
