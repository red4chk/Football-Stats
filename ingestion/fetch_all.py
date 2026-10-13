"""
Full ingestion orchestrator.

Runs every raw-data fetcher in dependency order so a single command populates
all raw collections:

    standings -> teams -> team_statistics -> matches -> players
    -> top_scorers -> top_assists

``teams`` is fetched before ``team_statistics`` (which iterates the stored
team list) and before ``players`` (which only needs league/season but is
logically grouped after the team list is available).
"""
import logging

from config.settings import DEFAULT_LEAGUE_ID, DEFAULT_SEASON
from db.collections import get_collection
from ingestion.fetch_standings import fetch_standings
from ingestion.fetch_teams import fetch_teams, fetch_team_statistics
from ingestion.fetch_matches import fetch_matches
from ingestion.fetch_players import fetch_players
from ingestion.fetch_top_scorers import fetch_top_scorers
from ingestion.fetch_top_assists import fetch_top_assists

logger = logging.getLogger(__name__)


def fetch_all(
    league_id: int = DEFAULT_LEAGUE_ID,
    season: int = DEFAULT_SEASON,
) -> dict[str, int]:
    """Run every fetcher for a league/season and return per-step counts.

    Returns a dict mapping fetcher name -> records upserted/modified.
    """
    counts: dict[str, int] = {}

    logger.info("=== Starting full ingestion for league=%s season=%s ===", league_id, season)

    counts["standings"] = fetch_standings(league_id, season)
    counts["teams"] = fetch_teams(league_id, season)

    # Per-team statistics depend on the teams we just fetched.
    team_ids = [
        t["team"]["id"]
        for t in get_collection("teams").find(
            {"league_id": league_id, "season": season}, {"team.id": 1}
        )
    ]
    stats_count = 0
    for tid in team_ids:
        fetch_team_statistics(tid, league_id, season)
        stats_count += 1
    counts["team_statistics"] = stats_count

    counts["matches"] = fetch_matches(league_id, season)
    counts["players"] = fetch_players(league_id, season)
    counts["top_scorers"] = fetch_top_scorers(league_id, season)
    counts["top_assists"] = fetch_top_assists(league_id, season)

    logger.info("=== Ingestion complete: %s ===", counts)
    return counts


if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    parser = argparse.ArgumentParser(description="Run full ingestion for a league/season.")
    parser.add_argument("--league", type=int, default=DEFAULT_LEAGUE_ID, help="League id")
    parser.add_argument("--season", type=int, default=DEFAULT_SEASON, help="Season year")
    args = parser.parse_args()

    fetch_all(args.league, args.season)
