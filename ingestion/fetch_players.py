"""
Player roster fetcher.

Pulls every player registered for a (league, season) from API-Football and
stores them in the ``players`` collection.  Each document is keyed by
``(player.id, statistics.0.league.season)`` so re-runs are idempotent.

The /players endpoint is heavily paginated (a full EPL season returns 30+
pages).  ``MAX_PLAYER_PAGES`` caps the pull to stay under the free-tier
rate budget; override via the ``max_pages`` argument or env var.
"""
import logging
import os

from pymongo import UpdateOne

from config.settings import DEFAULT_LEAGUE_ID, DEFAULT_SEASON
from db.collections import get_collection
from ingestion.base import paginate

logger = logging.getLogger(__name__)

MAX_PLAYER_PAGES = int(os.getenv("MAX_PLAYER_PAGES", "0")) or None


def fetch_players(
    league_id: int = DEFAULT_LEAGUE_ID,
    season: int = DEFAULT_SEASON,
    max_pages: int | None = MAX_PLAYER_PAGES,
) -> int:
    """Fetch player roster and upsert into MongoDB.

    Args:
        league_id: API-Football league id.
        season: Season year.
        max_pages: Optional hard cap on paginated pages (None = all pages).

    Returns:
        Number of player documents upserted or modified.
    """
    players = paginate("players", {"league": league_id, "season": season}, max_pages)
    if not players:
        logger.info("No players returned for league=%s season=%s", league_id, season)
        return 0

    collection = get_collection("players")
    ops = []
    for p in players:
        season_val = (
            p.get("statistics", [{}])[0].get("league", {}).get("season", season)
        )
        ops.append(
            UpdateOne(
                {"player.id": p["player"]["id"], "statistics.0.league.season": season_val},
                {"$set": p},
                upsert=True,
            )
        )

    result = collection.bulk_write(ops, ordered=False)
    count = result.upserted_count + result.modified_count
    logger.info(
        "Players upserted=%d modified=%d for league=%s season=%s",
        result.upserted_count,
        result.modified_count,
        league_id,
        season,
    )
    return count


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    fetch_players()
