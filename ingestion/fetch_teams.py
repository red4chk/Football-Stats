"""
Team info and team-statistics fetcher.

* :func:`fetch_teams` — the list of teams participating in a (league, season).
  Denormalizes ``league_id`` and ``season`` onto each document so teams can
  be filtered per season without re-joining on the standings endpoint.
* :func:`fetch_team_statistics` — per-team deep stats (home/away splits,
  biggest wins, clean sheets, etc.) for a specific team.
"""
import logging

from pymongo import UpdateOne

from config.settings import DEFAULT_LEAGUE_ID, DEFAULT_SEASON
from db.collections import get_collection
from ingestion.base import api_get

logger = logging.getLogger(__name__)


def fetch_teams(
    league_id: int = DEFAULT_LEAGUE_ID,
    season: int = DEFAULT_SEASON,
) -> int:
    """Fetch the team list for a league/season and upsert into MongoDB.

    Returns the number of records upserted or modified.
    """
    data = api_get("teams", {"league": league_id, "season": season})
    teams = data.get("response", [])
    if not teams:
        logger.info("No teams returned for league=%s season=%s", league_id, season)
        return 0

    collection = get_collection("teams")
    ops = [
        UpdateOne(
            {"team.id": t["team"]["id"]},
            {"$set": {**t, "league_id": league_id, "season": season}},
            upsert=True,
        )
        for t in teams
    ]
    result = collection.bulk_write(ops, ordered=False)
    count = result.upserted_count + result.modified_count
    logger.info(
        "Teams upserted=%d modified=%d for league=%s season=%s",
        result.upserted_count,
        result.modified_count,
        league_id,
        season,
    )
    return count


def fetch_team_statistics(
    team_id: int,
    league_id: int = DEFAULT_LEAGUE_ID,
    season: int = DEFAULT_SEASON,
) -> dict:
    """Fetch detailed statistics for a single team and store them.

    Returns the raw stats dict returned by the API (empty dict if none).
    """
    data = api_get(
        "teams/statistics",
        {"team": team_id, "league": league_id, "season": season},
    )
    stats = data.get("response", {})
    if stats:
        collection = get_collection("team_statistics")
        collection.update_one(
            {
                "team.id": team_id,
                "league.id": league_id,
                "league.season": season,
            },
            {"$set": stats},
            upsert=True,
        )
        logger.info("Team statistics saved for team=%s", team_id)
    return stats


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    fetch_teams()
