"""
Match fixture fetcher.

Pulls every fixture for a (league, season) — optionally bounded by a date
range — and stores them in the ``matches`` collection.  Each fixture is
keyed by ``fixture.id`` so repeated pulls are idempotent upserts.

Also exposes :func:`fetch_recent_matches`, a convenience for catch-up syncs
that only pulls the last ``N`` days of fixtures.
"""
import logging
from datetime import datetime, timedelta

from pymongo import UpdateOne

from config.settings import DEFAULT_LEAGUE_ID, DEFAULT_SEASON
from db.collections import get_collection
from ingestion.base import paginate

logger = logging.getLogger(__name__)


def fetch_matches(
    league_id: int = DEFAULT_LEAGUE_ID,
    season: int = DEFAULT_SEASON,
    from_date: str | None = None,
    to_date: str | None = None,
) -> int:
    """Fetch match fixtures and upsert them into MongoDB.

    Args:
        league_id: API-Football league id (e.g. 39 for the Premier League).
        season: Season year (e.g. 2024).
        from_date: Optional ISO date ``YYYY-MM-DD`` lower bound.
        to_date: Optional ISO date ``YYYY-MM-DD`` upper bound.

    Returns:
        Number of fixtures upserted or modified.
    """
    params: dict = {"league": league_id, "season": season}
    if from_date:
        params["from"] = from_date
    if to_date:
        params["to"] = to_date

    fixtures = paginate("fixtures", params)
    if not fixtures:
        logger.info("No fixtures returned for league=%s season=%s", league_id, season)
        return 0

    collection = get_collection("matches")
    ops = [
        UpdateOne(
            {"fixture.id": f["fixture"]["id"]},
            {"$set": f},
            upsert=True,
        )
        for f in fixtures
    ]
    result = collection.bulk_write(ops, ordered=False)
    count = result.upserted_count + result.modified_count
    logger.info(
        "Matches upserted=%d modified=%d for league=%s season=%s",
        result.upserted_count,
        result.modified_count,
        league_id,
        season,
    )
    return count


def fetch_recent_matches(
    days: int = 7,
    league_id: int = DEFAULT_LEAGUE_ID,
    season: int = DEFAULT_SEASON,
) -> int:
    """Fetch fixtures from the last ``days`` days (inclusive of today)."""
    today = datetime.utcnow()
    from_date = (today - timedelta(days=days)).strftime("%Y-%m-%d")
    to_date = today.strftime("%Y-%m-%d")
    return fetch_matches(league_id, season, from_date, to_date)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    fetch_matches()
