"""
MongoDB collection registry and index bootstrap.

All indexes used by the ingestion + processing pipelines are declared in
:func:`setup_indexes`.  Running it at the start of a full data pull (or of a
Dagster job) guarantees that the query patterns used by downstream code are
backed by unique or secondary indexes.
"""
import logging

from pymongo import ASCENDING, DESCENDING
from pymongo.collection import Collection

from db.connection import get_db

logger = logging.getLogger(__name__)

# Canonical list of raw-ingestion collection names.  Kept in one place so the
# reset utility and index setup stay in sync.
RAW_COLLECTIONS = [
    "matches",
    "players",
    "teams",
    "standings",
    "top_scorers",
    "top_assists",
    "team_statistics",
]

# Canonical list of processed/derived collection names.
PROCESSED_COLLECTIONS = [
    "processed_matches",
    "processed_players",
    "processed_teams",
    "processed_form",
    "processed_top_scorers",
    "processed_top_assists",
    "processed_league_summary",
]


def get_collection(name: str) -> Collection:
    """Return a collection handle from the configured database."""
    return get_db()[name]


def setup_indexes() -> None:
    """Create (idempotently) every index required by the application.

    Unique compound indexes enforce "upsert by key" semantics for fetchers;
    secondary indexes cover the most common filter/sort patterns used by the
    processing layer and by Metabase queries.
    """
    db = get_db()

    # -- matches ----------------------------------------------------------
    db["matches"].create_index([("fixture.id", ASCENDING)], unique=True)
    db["matches"].create_index([("league.id", ASCENDING)])
    db["matches"].create_index([("fixture.date", DESCENDING)])
    db["matches"].create_index([("teams.home.id", ASCENDING)])
    db["matches"].create_index([("teams.away.id", ASCENDING)])

    # -- players ----------------------------------------------------------
    db["players"].create_index(
        [("player.id", ASCENDING), ("statistics.0.league.season", ASCENDING)],
        unique=True,
    )
    db["players"].create_index([("statistics.0.league.id", ASCENDING)])
    db["players"].create_index([("statistics.0.team.id", ASCENDING)])

    # -- teams ------------------------------------------------------------
    db["teams"].create_index([("team.id", ASCENDING)], unique=True)
    db["teams"].create_index([("league_id", ASCENDING)])
    db["teams"].create_index([("season", ASCENDING)])

    # -- standings --------------------------------------------------------
    db["standings"].create_index(
        [("league.id", ASCENDING), ("league.season", ASCENDING)],
        unique=True,
    )

    # -- top_scorers / top_assists ---------------------------------------
    for name in ("top_scorers", "top_assists"):
        db[name].create_index(
            [("player.id", ASCENDING), ("season", ASCENDING), ("league_id", ASCENDING)],
            unique=True,
        )

    # -- team_statistics --------------------------------------------------
    db["team_statistics"].create_index(
        [("team.id", ASCENDING), ("league.id", ASCENDING), ("league.season", ASCENDING)],
        unique=True,
    )

    # -- processed_matches ------------------------------------------------
    db["processed_matches"].create_index([("fixture_id", ASCENDING)], unique=True)
    db["processed_matches"].create_index([("league_id", ASCENDING)])
    db["processed_matches"].create_index([("season", ASCENDING)])
    db["processed_matches"].create_index([("date", DESCENDING)])

    # -- processed_players ------------------------------------------------
    db["processed_players"].create_index(
        [("player_id", ASCENDING), ("season", ASCENDING)],
        unique=True,
    )
    db["processed_players"].create_index([("team_id", ASCENDING)])
    db["processed_players"].create_index([("league_id", ASCENDING)])

    # -- processed_teams --------------------------------------------------
    db["processed_teams"].create_index(
        [("team_id", ASCENDING), ("season", ASCENDING)],
        unique=True,
    )
    db["processed_teams"].create_index([("league_id", ASCENDING)])
    db["processed_teams"].create_index([("points", DESCENDING)])

    # -- processed_form ---------------------------------------------------
    db["processed_form"].create_index(
        [("team_id", ASCENDING), ("season", ASCENDING)],
        unique=True,
    )

    # -- processed_top_scorers / processed_top_assists --------------------
    for name in ("processed_top_scorers", "processed_top_assists"):
        db[name].create_index(
            [("player_id", ASCENDING), ("season", ASCENDING), ("league_id", ASCENDING)],
            unique=True,
        )
        db[name].create_index([("rank", ASCENDING)])

    # -- processed_league_summary ----------------------------------------
    db["processed_league_summary"].create_index(
        [("league_id", ASCENDING), ("season", ASCENDING)],
        unique=True,
    )

    logger.info("All MongoDB indexes created")
