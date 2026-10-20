"""
Full processing orchestrator.

Runs every transformation stage in dependency order so a single command
rebuilds all ``processed_*`` collections from the raw data:

    processed_matches -> processed_teams -> processed_form
    -> processed_players -> processed_league_summary
    -> processed_top_scorers -> processed_top_assists

Match processing runs first because team stats and form tables are derived
from the flattened frame; the league summary is computed once team stats and
the raw leaderboards are in place.
"""
import logging

from config.settings import DEFAULT_LEAGUE_ID, DEFAULT_SEASON
from processing.clean_matches import (
    compute_form_table,
    load_matches_df,
    save_processed_form,
    save_processed_matches,
)
from processing.clean_players import (
    compute_player_rankings,
    load_players_df,
    save_processed_players,
)
from processing.stats import (
    compute_league_summary,
    compute_team_stats,
    compute_top_assists,
    compute_top_scorers,
    save_league_summary,
    save_processed_teams,
    save_top_assists,
    save_top_scorers,
)

logger = logging.getLogger(__name__)


def process_all(
    league_id: int = DEFAULT_LEAGUE_ID,
    season: int = DEFAULT_SEASON,
) -> dict[str, int]:
    """Run every processing stage for a league/season.

    Returns a dict mapping the target collection name -> records written.
    """
    counts: dict[str, int] = {}

    logger.info("=== Starting processing for league=%s season=%s ===", league_id, season)

    matches_df = load_matches_df(league_id, season)
    counts["processed_matches"] = save_processed_matches(matches_df)

    counts["processed_teams"] = save_processed_teams(compute_team_stats(matches_df))
    counts["processed_form"] = save_processed_form(compute_form_table(matches_df))

    players_df = load_players_df(league_id, season)
    counts["processed_players"] = save_processed_players(
        compute_player_rankings(players_df)
    )

    counts["processed_league_summary"] = save_league_summary(
        compute_league_summary(league_id, season)
    )
    counts["processed_top_scorers"] = save_top_scorers(
        compute_top_scorers(league_id, season)
    )
    counts["processed_top_assists"] = save_top_assists(
        compute_top_assists(league_id, season)
    )

    logger.info("=== Processing complete: %s ===", counts)
    return counts


if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    parser = argparse.ArgumentParser(description="Run full processing for a league/season.")
    parser.add_argument("--league", type=int, default=DEFAULT_LEAGUE_ID, help="League id")
    parser.add_argument("--season", type=int, default=DEFAULT_SEASON, help="Season year")
    args = parser.parse_args()

    process_all(args.league, args.season)
