"""
Aggregated statistics: team tables, xG proxy, leaderboards, league summary.

Everything in this module is derived from already-ingested data: the
flattened matches frame produced by :mod:`processing.clean_matches`, the raw
leaderboards, and the raw player statistics.  Outputs are written to the
``processed_teams``, ``processed_top_scorers``, ``processed_top_assists``, and
``processed_league_summary`` collections.
"""
import logging

import numpy as np
import pandas as pd
from pymongo import DESCENDING

from config.settings import DEFAULT_LEAGUE_ID, DEFAULT_SEASON
from db.collections import bulk_upsert, get_collection

logger = logging.getLogger(__name__)

_RESULT_POINTS = {"W": 3, "D": 1, "L": 0}

# League-average conversion weights for the free-tier xG proxy.
_SHOT_WEIGHT = 0.10
_SHOT_ON_TARGET_WEIGHT = 0.20
_GOAL_WEIGHT = 0.15


def estimate_xg(shots_total, shots_on_target, goals) -> float:
    """Return a simplified proxy xG estimate for a player or team.

    API-Football's free tier does not expose true expected-goals data, so this
    approximates it from shot volume: every shot carries a league-average
    conversion weight, every shot on target a premium, and actual goals a small
    contribution so the metric stays grounded when shot counts are missing.
    When no shot data is available at all the observed goals are returned as-is.

    This is an approximation, not a model output — treat it as directional.
    """
    shots_total = shots_total or 0
    shots_on_target = shots_on_target or 0
    goals = goals or 0

    if shots_total == 0 and shots_on_target == 0:
        return float(goals)

    estimate = (
        _SHOT_WEIGHT * shots_total
        + _SHOT_ON_TARGET_WEIGHT * shots_on_target
        + _GOAL_WEIGHT * goals
    )
    return round(estimate, 2)


def compute_team_stats(matches_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate a flattened matches frame into a per-team season table.

    Only finished matches (both goal counts present) are counted.  Each match
    is exploded into a home and an away row so a single club accumulates one
    entry per fixture, then grouped into
    played/wins/draws/losses/goals for/against/goal difference/points/win rate.
    """
    columns = [
        "team_id",
        "team_name",
        "league_id",
        "season",
        "played",
        "wins",
        "draws",
        "losses",
        "goals_for",
        "goals_against",
        "goal_diff",
        "points",
        "win_rate",
    ]
    if matches_df.empty:
        return pd.DataFrame(columns=columns)

    played = matches_df.dropna(subset=["home_goals", "away_goals"])

    home = pd.DataFrame(
        {
            "team_id": played["home_team_id"],
            "team_name": played["home_team_name"],
            "league_id": played["league_id"],
            "season": played["season"],
            "goals_for": played["home_goals"],
            "goals_against": played["away_goals"],
        }
    )
    away = pd.DataFrame(
        {
            "team_id": played["away_team_id"],
            "team_name": played["away_team_name"],
            "league_id": played["league_id"],
            "season": played["season"],
            "goals_for": played["away_goals"],
            "goals_against": played["home_goals"],
        }
    )
    long = pd.concat([home, away], ignore_index=True)

    long["result"] = np.where(
        long["goals_for"] > long["goals_against"],
        "W",
        np.where(long["goals_for"] < long["goals_against"], "L", "D"),
    )
    long["points"] = long["result"].map(_RESULT_POINTS)

    grouped = (
        long.groupby(["team_id", "team_name", "league_id", "season"], dropna=False)
        .agg(
            played=("result", "size"),
            wins=("result", lambda s: int((s == "W").sum())),
            draws=("result", lambda s: int((s == "D").sum())),
            losses=("result", lambda s: int((s == "L").sum())),
            goals_for=("goals_for", "sum"),
            goals_against=("goals_against", "sum"),
            points=("points", "sum"),
        )
        .reset_index()
    )

    grouped["goal_diff"] = grouped["goals_for"] - grouped["goals_against"]
    grouped["win_rate"] = (grouped["wins"] / grouped["played"]).round(3)

    logger.info("Computed team stats for %d teams", len(grouped))
    return grouped.sort_values(
        ["points", "goal_diff"], ascending=False
    ).reset_index(drop=True)[columns]


def _load_leaderboard(collection_name: str, league_id: int, season: int) -> pd.DataFrame:
    """Flatten a raw leaderboard collection into a compact DataFrame."""
    rows = []
    for doc in get_collection(collection_name).find(
        {"league_id": league_id, "season": season}
    ):
        player = doc.get("player", {}) or {}
        statistic = (doc.get("statistics") or [{}])[0] or {}
        team = statistic.get("team", {}) or {}
        games = statistic.get("games", {}) or {}
        goals = statistic.get("goals", {}) or {}
        rows.append(
            {
                "player_id": player.get("id"),
                "player_name": player.get("name"),
                "team_id": team.get("id"),
                "team_name": team.get("name"),
                "league_id": doc.get("league_id", league_id),
                "season": doc.get("season", season),
                "appearances": games.get("appearences"),
                "minutes": games.get("minutes"),
                "goals": goals.get("total"),
                "assists": goals.get("assists"),
            }
        )

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    for column in ("appearances", "minutes", "goals", "assists"):
        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0).astype("int64")
    return df


def _enrich_leaderboard(df: pd.DataFrame, metric: str, limit: int) -> pd.DataFrame:
    """Add per-90 metrics, rank by ``metric``, and cap to ``limit`` rows."""
    if df.empty:
        return df.copy()

    minutes = df["minutes"].replace(0, np.nan)
    df["goals_per_90"] = (df["goals"] * 90 / minutes).round(2).fillna(0.0)
    df["assists_per_90"] = (df["assists"] * 90 / minutes).round(2).fillna(0.0)
    df["goal_contributions"] = df["goals"] + df["assists"]

    df = (
        df.sort_values([metric, "goal_contributions"], ascending=False)
        .head(limit)
        .reset_index(drop=True)
    )
    df["rank"] = df.index + 1
    return df


def compute_top_scorers(
    league_id: int = DEFAULT_LEAGUE_ID,
    season: int = DEFAULT_SEASON,
    limit: int = 20,
) -> pd.DataFrame:
    """Return the top-``limit`` scorers enriched with per-90 metrics and a rank."""
    df = _load_leaderboard("top_scorers", league_id, season)
    return _enrich_leaderboard(df, "goals", limit)


def compute_top_assists(
    league_id: int = DEFAULT_LEAGUE_ID,
    season: int = DEFAULT_SEASON,
    limit: int = 20,
) -> pd.DataFrame:
    """Return the top-``limit`` assisters enriched with per-90 metrics and a rank."""
    df = _load_leaderboard("top_assists", league_id, season)
    return _enrich_leaderboard(df, "assists", limit)


def _best_team(league_id: int, season: int) -> tuple:
    """Return ``(team_name, points, goal_diff)`` for the best processed team."""
    doc = get_collection("processed_teams").find_one(
        {"league_id": league_id, "season": season},
        sort=[("points", DESCENDING), ("goal_diff", DESCENDING)],
    )
    if not doc:
        return None, None, None
    return doc.get("team_name"), doc.get("points"), doc.get("goal_diff")


def _leaderboard_leader(collection_name: str, metric_path: str, league_id: int, season: int) -> tuple:
    """Return ``(player_name, metric_value)`` for the top entry of a raw board."""
    doc = get_collection(collection_name).find_one(
        {"league_id": league_id, "season": season},
        sort=[(metric_path, DESCENDING)],
    )
    if not doc:
        return None, None
    statistic = (doc.get("statistics") or [{}])[0] or {}
    goals = statistic.get("goals", {}) or {}
    return doc.get("player", {}).get("name"), goals.get(metric_path.rsplit(".", 1)[-1])


def compute_league_summary(
    league_id: int = DEFAULT_LEAGUE_ID,
    season: int = DEFAULT_SEASON,
) -> dict:
    """Build a single season-summary document for a league.

    Totals are computed with a MongoDB aggregation over ``processed_matches``;
    the top scorer, top assister, and best team are looked up from the raw
    leaderboards and the processed team table.
    """
    pipeline = [
        {"$match": {"league_id": league_id, "season": season}},
        {
            "$group": {
                "_id": None,
                "matches_played": {"$sum": 1},
                "total_home_goals": {"$sum": "$home_goals"},
                "total_away_goals": {"$sum": "$away_goals"},
            }
        },
    ]
    aggregated = list(get_collection("processed_matches").aggregate(pipeline))
    totals = aggregated[0] if aggregated else {}

    matches_played = totals.get("matches_played", 0) or 0
    total_home_goals = totals.get("total_home_goals", 0) or 0
    total_away_goals = totals.get("total_away_goals", 0) or 0
    total_goals = total_home_goals + total_away_goals

    top_scorer, top_scorer_goals = _leaderboard_leader(
        "top_scorers", "statistics.0.goals.total", league_id, season
    )
    top_assister, top_assister_assists = _leaderboard_leader(
        "top_assists", "statistics.0.goals.assists", league_id, season
    )
    best_team, best_team_points, best_team_goal_diff = _best_team(league_id, season)

    summary = {
        "league_id": league_id,
        "season": season,
        "matches_played": matches_played,
        "total_goals": total_goals,
        "total_home_goals": total_home_goals,
        "total_away_goals": total_away_goals,
        "goals_per_match": round(total_goals / matches_played, 2) if matches_played else 0.0,
        "top_scorer": top_scorer,
        "top_scorer_goals": top_scorer_goals,
        "top_assister": top_assister,
        "top_assister_assists": top_assister_assists,
        "best_team": best_team,
        "best_team_points": best_team_points,
        "best_team_goal_diff": best_team_goal_diff,
    }
    logger.info("Computed league summary for league=%s season=%s", league_id, season)
    return summary


def save_processed_teams(df: pd.DataFrame) -> int:
    """Upsert a team-stats frame into ``processed_teams`` by ``(team_id, season)``."""
    if df.empty:
        logger.info("No processed teams to save")
        return 0
    return bulk_upsert("processed_teams", df.to_dict("records"), ["team_id", "season"])


def save_top_scorers(df: pd.DataFrame) -> int:
    """Upsert a scorer leaderboard into ``processed_top_scorers``."""
    if df.empty:
        logger.info("No top scorers to save")
        return 0
    return bulk_upsert(
        "processed_top_scorers", df.to_dict("records"), ["player_id", "season", "league_id"]
    )


def save_top_assists(df: pd.DataFrame) -> int:
    """Upsert an assist leaderboard into ``processed_top_assists``."""
    if df.empty:
        logger.info("No top assists to save")
        return 0
    return bulk_upsert(
        "processed_top_assists", df.to_dict("records"), ["player_id", "season", "league_id"]
    )


def save_league_summary(summary: dict) -> int:
    """Upsert a league-summary document into ``processed_league_summary``."""
    if not summary:
        logger.info("No league summary to save")
        return 0
    return bulk_upsert("processed_league_summary", [summary], ["league_id", "season"])


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    from processing.clean_matches import load_matches_df

    matches = load_matches_df()
    print(compute_team_stats(matches).head(20).to_string())
