"""
Player normalization and per-90 ranking.

The raw ``players`` collection stores the complete API-Football payload,
where each document carries a ``statistics`` array (one entry per
league/team the player featured for).  This module explodes that array into
one flat row per player/team/season tuple, derives per-90 efficiency metrics,
and persists the ranked result to ``processed_players``.
"""
import logging

import numpy as np
import pandas as pd

from config.settings import DEFAULT_LEAGUE_ID, DEFAULT_SEASON
from db.collections import bulk_upsert, get_collection

logger = logging.getLogger(__name__)

# Counting statistics that should never be null (missing => 0).
_COUNT_COLUMNS = [
    "appearances",
    "lineups",
    "minutes",
    "goals",
    "assists",
    "shots_total",
    "shots_on",
    "passes_total",
    "passes_key",
    "tackles_total",
    "duels_won",
    "dribbles_success",
    "cards_yellow",
    "cards_red",
    "penalties_scored",
    "penalties_missed",
]


def _flatten_statistic(player: dict, statistic: dict) -> dict:
    """Flatten one ``player`` + one ``statistics[]`` entry into a single row."""
    league = statistic.get("league", {}) or {}
    team = statistic.get("team", {}) or {}
    games = statistic.get("games", {}) or {}
    goals = statistic.get("goals", {}) or {}
    shots = statistic.get("shots", {}) or {}
    passes = statistic.get("passes", {}) or {}
    tackles = statistic.get("tackles", {}) or {}
    duels = statistic.get("duels", {}) or {}
    dribbles = statistic.get("dribbles", {}) or {}
    cards = statistic.get("cards", {}) or {}
    penalty = statistic.get("penalty", {}) or {}

    return {
        "player_id": player.get("id"),
        "player_name": player.get("name"),
        "nationality": player.get("nationality"),
        "team_id": team.get("id"),
        "team_name": team.get("name"),
        "league_id": league.get("id"),
        "league_name": league.get("name"),
        "season": league.get("season"),
        "position": games.get("position"),
        "rating": games.get("rating"),
        "appearances": games.get("appearences"),
        "lineups": games.get("lineups"),
        "minutes": games.get("minutes"),
        "goals": goals.get("total"),
        "assists": goals.get("assists"),
        "shots_total": shots.get("total"),
        "shots_on": shots.get("on"),
        "passes_total": passes.get("total"),
        "passes_key": passes.get("key"),
        "passes_accuracy": passes.get("accuracy"),
        "tackles_total": tackles.get("total"),
        "duels_won": duels.get("won"),
        "dribbles_success": dribbles.get("success"),
        "cards_yellow": cards.get("yellow"),
        "cards_red": cards.get("red"),
        "penalties_scored": penalty.get("scored"),
        "penalties_missed": penalty.get("missed"),
    }


def load_players_df(
    league_id: int = DEFAULT_LEAGUE_ID,
    season: int = DEFAULT_SEASON,
) -> pd.DataFrame:
    """Load raw players and return one flat row per player/team/season tuple.

    Rows lacking a ``player_id`` or ``season`` are dropped, and counting
    statistics are coerced to ``int`` with missing values becoming ``0`` so
    downstream arithmetic never trips on nulls.
    """
    cursor = get_collection("players").find(
        {"statistics.0.league.id": league_id, "statistics.0.league.season": season}
    )

    rows = []
    for doc in cursor:
        player = doc.get("player", {}) or {}
        for statistic in doc.get("statistics", []) or []:
            rows.append(_flatten_statistic(player, statistic))

    if not rows:
        logger.info("No raw players for league=%s season=%s", league_id, season)
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df = df.dropna(subset=["player_id", "season"]).reset_index(drop=True)

    for column in _COUNT_COLUMNS:
        df[column] = (
            pd.to_numeric(df[column], errors="coerce").fillna(0).astype("int64")
        )
    df["rating"] = pd.to_numeric(df["rating"], errors="coerce")
    df["passes_accuracy"] = (
        pd.to_numeric(df["passes_accuracy"], errors="coerce").fillna(0).astype("int64")
    )

    logger.info(
        "Loaded %d player rows for league=%s season=%s", len(df), league_id, season
    )
    return df


def compute_player_rankings(
    df: pd.DataFrame,
    min_appearances: int = 1,
) -> pd.DataFrame:
    """Enrich a players frame with per-90 efficiency metrics and rank it.

    Adds ``goals_per_90``, ``assists_per_90``, ``goal_contributions``, and
    ``shot_accuracy``; drops players below ``min_appearances``; and sorts by
    goal contributions (goals + assists) descending.

    Division-by-zero on ``minutes``/``shots_total`` is guarded so fringe
    players with no playing time yield ``0.0`` instead of ``inf``/``nan``.
    """
    if df.empty:
        return df.copy()

    ranked = df[df["appearances"] >= min_appearances].copy()

    minutes = ranked["minutes"].replace(0, np.nan)
    ranked["goals_per_90"] = (ranked["goals"] * 90 / minutes).round(2).fillna(0.0)
    ranked["assists_per_90"] = (ranked["assists"] * 90 / minutes).round(2).fillna(0.0)
    ranked["goal_contributions"] = ranked["goals"] + ranked["assists"]

    shots_total = ranked["shots_total"].replace(0, np.nan)
    ranked["shot_accuracy"] = (ranked["shots_on"] / shots_total).round(3).fillna(0.0)

    ranked = ranked.sort_values(
        ["goal_contributions", "goals", "assists"], ascending=False
    ).reset_index(drop=True)

    logger.info(
        "Ranked %d players (min_appearances=%d)", len(ranked), min_appearances
    )
    return ranked


def save_processed_players(df: pd.DataFrame) -> int:
    """Upsert a ranked players frame into ``processed_players``.

    Keyed by ``(player_id, season)``.  Returns the number of documents
    upserted or modified.
    """
    if df.empty:
        logger.info("No processed players to save")
        return 0
    return bulk_upsert("processed_players", df.to_dict("records"), ["player_id", "season"])


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    players = load_players_df()
    print(f"Flattened player rows: {len(players)}")
    if not players.empty:
        rankings = compute_player_rankings(players)
        print(rankings.head(10).to_string())
