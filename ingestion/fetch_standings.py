"""
League standings fetcher.

Pulls the full standings table for a given (league, season) from API-Football
and stores each league entry (one per league/season — the API returns one
``standings`` array for the whole league) into the ``standings`` collection.
"""
import logging

from pymongo import UpdateOne

from config.settings import DEFAULT_LEAGUE_ID, DEFAULT_SEASON
from db.collections import get_collection
from ingestion.base import api_get

logger = logging.getLogger(__name__)


def fetch_standings(
    league_id: int = DEFAULT_LEAGUE_ID,
    season: int = DEFAULT_SEASON,
) -> int:
    """Fetch league standings and upsert them into MongoDB.

    Returns the number of records upserted or modified.
    """
    data = api_get("standings", {"league": league_id, "season": season})
    leagues = data.get("response", [])
    if not leagues:
        logger.info("No standings returned for league=%s season=%s", league_id, season)
        return 0

    collection = get_collection("standings")
    ops = [
        UpdateOne(
            {"league.id": league_id, "league.season": season},
            {"$set": entry},
            upsert=True,
        )
        for entry in leagues
    ]
    result = collection.bulk_write(ops, ordered=False)
    count = result.upserted_count + result.modified_count
    logger.info(
        "Standings upserted=%d modified=%d for league=%s season=%s",
        result.upserted_count,
        result.modified_count,
        league_id,
        season,
    )
    return count


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    fetch_standings()
