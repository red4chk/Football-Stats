"""
Top assists fetcher.

Pulls the top-assists leaderboard for a (league, season) from API-Football
and stores each entry in the ``top_assists`` collection, keyed by
``(player.id, season, league_id)``.
"""
import logging

from pymongo import UpdateOne

from config.settings import DEFAULT_LEAGUE_ID, DEFAULT_SEASON
from db.collections import get_collection
from ingestion.base import api_get

logger = logging.getLogger(__name__)


def fetch_top_assists(
    league_id: int = DEFAULT_LEAGUE_ID,
    season: int = DEFAULT_SEASON,
) -> int:
    """Fetch top assists and upsert them into MongoDB.

    Returns the number of records upserted or modified.
    """
    data = api_get("players/topassists", {"league": league_id, "season": season})
    assists = data.get("response", [])
    if not assists:
        logger.info("No top assists returned for league=%s season=%s", league_id, season)
        return 0

    collection = get_collection("top_assists")
    ops = [
        UpdateOne(
            {"player.id": a["player"]["id"], "season": season, "league_id": league_id},
            {"$set": {**a, "season": season, "league_id": league_id}},
            upsert=True,
        )
        for a in assists
    ]
    result = collection.bulk_write(ops, ordered=False)
    count = result.upserted_count + result.modified_count
    logger.info(
        "Top assists upserted=%d modified=%d for league=%s season=%s",
        result.upserted_count,
        result.modified_count,
        league_id,
        season,
    )
    return count


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    fetch_top_assists()
