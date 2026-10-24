"""
Dagster software-defined assets for the football statistics platform.

The assets mirror the two stages of the pipeline and encode their ordering
through explicit dependencies, so Dagster can materialize the graph in the
correct sequence:

* **ingestion** — pull raw payloads from API-Football into MongoDB
  (``standings``, ``teams``, ``matches``, ``players``, ``top_stats``).
* **processing** — transform the raw collections into the query-ready
  ``processed_*`` collections that Metabase reads.

Every asset returns an :class:`~dagster.Output` whose metadata carries the
number of records written, so row counts are visible in the Dagster UI as soon
as a run finishes.
"""
import logging

from dagster import AssetExecutionContext, Config, MetadataValue, Output, asset

from config.settings import DEFAULT_LEAGUE_ID, DEFAULT_SEASON
from ingestion.fetch_matches import fetch_matches
from ingestion.fetch_players import fetch_players
from ingestion.fetch_standings import fetch_standings
from ingestion.fetch_teams import fetch_teams
from ingestion.fetch_top_assists import fetch_top_assists
from ingestion.fetch_top_scorers import fetch_top_scorers
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


class LeagueSeasonConfig(Config):
    """Per-run scope for every asset.

    Both fields default to the configured league/season, so a run can be
    launched with no config at all or overridden from the Dagster launchpad.
    """

    league_id: int = DEFAULT_LEAGUE_ID
    season: int = DEFAULT_SEASON


def _records_output(
    context: AssetExecutionContext, name: str, count: int
) -> Output[int]:
    """Wrap a record count in an ``Output`` carrying integer metadata."""
    logger.info("%s: %d records written", name, count)
    context.log.info("%s: %d records written", name, count)
    return Output(int(count), metadata={"records": MetadataValue.int(int(count))})


# ---------------------------------------------------------------------------
# Ingestion assets
# ---------------------------------------------------------------------------
@asset(group_name="ingestion")
def standings(
    context: AssetExecutionContext, config: LeagueSeasonConfig
) -> Output[int]:
    """League standings table for the configured league/season."""
    count = fetch_standings(config.league_id, config.season)
    return _records_output(context, "standings", count)


@asset(group_name="ingestion")
def teams(context: AssetExecutionContext, config: LeagueSeasonConfig) -> Output[int]:
    """List of teams participating in the configured league/season."""
    count = fetch_teams(config.league_id, config.season)
    return _records_output(context, "teams", count)


@asset(group_name="ingestion")
def matches(context: AssetExecutionContext, config: LeagueSeasonConfig) -> Output[int]:
    """Every fixture for the configured league/season."""
    count = fetch_matches(config.league_id, config.season)
    return _records_output(context, "matches", count)


@asset(group_name="ingestion")
def players(context: AssetExecutionContext, config: LeagueSeasonConfig) -> Output[int]:
    """Full player roster with per-league statistics for the season."""
    count = fetch_players(config.league_id, config.season)
    return _records_output(context, "players", count)


@asset(group_name="ingestion", deps=["players"])
def top_stats(context: AssetExecutionContext, config: LeagueSeasonConfig) -> Output[int]:
    """Top-scorer and top-assist leaderboards.

    Grouped after ``players`` because both leaderboards are player-centric and
    are pulled from the same endpoint family.
    """
    scorers = fetch_top_scorers(config.league_id, config.season)
    assists = fetch_top_assists(config.league_id, config.season)
    logger.info("top_stats: %d scorers + %d assisters", scorers, assists)
    context.log.info("top_stats: %d scorers + %d assisters", scorers, assists)
    return Output(
        scorers + assists,
        metadata={
            "records": MetadataValue.int(scorers + assists),
            "top_scorers": MetadataValue.int(scorers),
            "top_assists": MetadataValue.int(assists),
        },
    )


# ---------------------------------------------------------------------------
# Processing assets
# ---------------------------------------------------------------------------
@asset(group_name="processing", deps=["matches"])
def processed_matches(
    context: AssetExecutionContext, config: LeagueSeasonConfig
) -> Output[int]:
    """Flattened fixture rows keyed by ``fixture_id``."""
    matches_df = load_matches_df(config.league_id, config.season)
    count = save_processed_matches(matches_df)
    return _records_output(context, "processed_matches", count)


@asset(group_name="processing", deps=["players"])
def processed_players(
    context: AssetExecutionContext, config: LeagueSeasonConfig
) -> Output[int]:
    """Per-90 ranked player rows keyed by ``(player_id, season)``."""
    players_df = load_players_df(config.league_id, config.season)
    count = save_processed_players(compute_player_rankings(players_df))
    return _records_output(context, "processed_players", count)


@asset(group_name="processing", deps=["processed_matches"])
def processed_teams(
    context: AssetExecutionContext, config: LeagueSeasonConfig
) -> Output[int]:
    """Aggregated per-team season table (points, goal difference, win rate)."""
    matches_df = load_matches_df(config.league_id, config.season)
    count = save_processed_teams(compute_team_stats(matches_df))
    return _records_output(context, "processed_teams", count)


@asset(group_name="processing", deps=["processed_matches"])
def processed_form(
    context: AssetExecutionContext, config: LeagueSeasonConfig
) -> Output[int]:
    """Recent-form table (last 5 matches) per team."""
    matches_df = load_matches_df(config.league_id, config.season)
    count = save_processed_form(compute_form_table(matches_df))
    return _records_output(context, "processed_form", count)


@asset(group_name="processing", deps=["processed_players"])
def processed_top_scorers(
    context: AssetExecutionContext, config: LeagueSeasonConfig
) -> Output[int]:
    """Ranked top-scorer leaderboard with per-90 metrics."""
    count = save_top_scorers(compute_top_scorers(config.league_id, config.season))
    return _records_output(context, "processed_top_scorers", count)


@asset(group_name="processing", deps=["top_stats"])
def processed_top_assists(
    context: AssetExecutionContext, config: LeagueSeasonConfig
) -> Output[int]:
    """Ranked top-assist leaderboard with per-90 metrics."""
    count = save_top_assists(compute_top_assists(config.league_id, config.season))
    return _records_output(context, "processed_top_assists", count)


@asset(group_name="processing", deps=["processed_matches", "processed_players"])
def league_summary(
    context: AssetExecutionContext, config: LeagueSeasonConfig
) -> Output[int]:
    """Single season-summary document for the league.

    Depends on both ``processed_matches`` (season totals) and
    ``processed_players`` (leader references) being materialized first.
    """
    count = save_league_summary(
        compute_league_summary(config.league_id, config.season)
    )
    return _records_output(context, "league_summary", count)


INGESTION_ASSETS = [standings, teams, matches, players, top_stats]

PROCESSING_ASSETS = [
    processed_matches,
    processed_players,
    processed_teams,
    processed_form,
    processed_top_scorers,
    processed_top_assists,
    league_summary,
]

ALL_ASSETS = INGESTION_ASSETS + PROCESSING_ASSETS
