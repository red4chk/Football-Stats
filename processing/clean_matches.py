"""
Match fixture normalization, recent-form tables, and head-to-head lookup.

The raw ``matches`` collection stores the deeply nested API-Football fixture
payload.  This module flattens it into a rectangular, query-friendly frame
(one row per fixture) which is then persisted to ``processed_matches`` and
reused to derive team form tables and head-to-head histories.
"""
import logging

import pandas as pd

from config.settings import DEFAULT_LEAGUE_ID, DEFAULT_SEASON
from db.collections import bulk_upsert, get_collection

logger = logging.getLogger(__name__)

# Order matters: it defines the column layout of the flattened frame.
MATCH_COLUMNS = [
    "fixture_id",
    "date",
    "status",
    "venue",
    "league_id",
    "league_name",
    "season",
    "round",
    "home_team_id",
    "home_team_name",
    "away_team_id",
    "away_team_name",
    "home_goals",
    "away_goals",
    "home_winner",
    "away_winner",
    "halftime_home",
    "halftime_away",
]

# Result → points mapping used when aggregating form.
_RESULT_POINTS = {"W": 3, "D": 1, "L": 0}


def _flatten_fixture(doc: dict) -> dict:
    """Convert one nested raw fixture document into a flat row dict."""
    fixture = doc.get("fixture", {}) or {}
    league = doc.get("league", {}) or {}
    teams = doc.get("teams", {}) or {}
    home = teams.get("home", {}) or {}
    away = teams.get("away", {}) or {}
    goals = doc.get("goals", {}) or {}
    score = doc.get("score", {}) or {}
    halftime = score.get("halftime", {}) or {}
    venue = fixture.get("venue", {}) or {}
    status = fixture.get("status", {}) or {}

    return {
        "fixture_id": fixture.get("id"),
        "date": fixture.get("date"),
        "status": status.get("short"),
        "venue": venue.get("name"),
        "league_id": league.get("id"),
        "league_name": league.get("name"),
        "season": league.get("season"),
        "round": league.get("round"),
        "home_team_id": home.get("id"),
        "home_team_name": home.get("name"),
        "away_team_id": away.get("id"),
        "away_team_name": away.get("name"),
        "home_goals": goals.get("home"),
        "away_goals": goals.get("away"),
        "home_winner": home.get("winner"),
        "away_winner": away.get("winner"),
        "halftime_home": halftime.get("home"),
        "halftime_away": halftime.get("away"),
    }


def load_matches_df(
    league_id: int = DEFAULT_LEAGUE_ID,
    season: int = DEFAULT_SEASON,
) -> pd.DataFrame:
    """Load raw fixtures and return a flattened DataFrame (one row per match).

    Datetimes are coerced to timezone-aware UTC ``Timestamp`` objects and
    numeric columns are kept nullable (``Int64``) so unplayed fixtures — whose
    goals are ``null`` in the API — do not corrupt aggregates.
    """
    cursor = get_collection("matches").find(
        {"league.id": league_id, "league.season": season}
    )
    rows = [_flatten_fixture(doc) for doc in cursor]

    if not rows:
        logger.info("No raw matches for league=%s season=%s", league_id, season)
        return pd.DataFrame(columns=MATCH_COLUMNS)

    df = pd.DataFrame(rows, columns=MATCH_COLUMNS)
    df["date"] = pd.to_datetime(df["date"], utc=True, errors="coerce")

    int_cols = [
        "fixture_id",
        "league_id",
        "season",
        "home_team_id",
        "away_team_id",
        "home_goals",
        "away_goals",
        "halftime_home",
        "halftime_away",
    ]
    for col in int_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    logger.info(
        "Loaded %d raw matches for league=%s season=%s", len(df), league_id, season
    )
    return df.sort_values("date").reset_index(drop=True)


def _explode_team_matches(df: pd.DataFrame) -> pd.DataFrame:
    """Explode each match into two team-centric rows (home and away)."""
    played = df.dropna(subset=["home_goals", "away_goals"]).copy()

    home = pd.DataFrame(
        {
            "fixture_id": played["fixture_id"],
            "date": played["date"],
            "league_id": played["league_id"],
            "season": played["season"],
            "team_id": played["home_team_id"],
            "team_name": played["home_team_name"],
            "opponent_id": played["away_team_id"],
            "goals_for": played["home_goals"],
            "goals_against": played["away_goals"],
        }
    )
    away = pd.DataFrame(
        {
            "fixture_id": played["fixture_id"],
            "date": played["date"],
            "league_id": played["league_id"],
            "season": played["season"],
            "team_id": played["away_team_id"],
            "team_name": played["away_team_name"],
            "opponent_id": played["home_team_id"],
            "goals_for": played["away_goals"],
            "goals_against": played["home_goals"],
        }
    )

    exploded = pd.concat([home, away], ignore_index=True)
    exploded["goal_diff"] = exploded["goals_for"] - exploded["goals_against"]
    exploded["result"] = exploded.apply(
        lambda r: "W"
        if r["goals_for"] > r["goals_against"]
        else ("L" if r["goals_for"] < r["goals_against"] else "D"),
        axis=1,
    )
    exploded["points"] = exploded["result"].map(_RESULT_POINTS).astype("Int64")
    return exploded.sort_values(["team_id", "date"]).reset_index(drop=True)


def compute_form_table(df: pd.DataFrame, last_n: int = 5) -> pd.DataFrame:
    """Aggregate each team's last ``last_n`` matches into a form summary.

    Args:
        df: Flattened matches frame from :func:`load_matches_df`.
        last_n: Number of most recent matches per team to consider.

    Returns:
        One row per ``(team_id, league_id, season)`` with played/wins/draws/
        losses/goals for/against/goal difference/points and a chronological
        form string (e.g. ``"WWDLW"``).
    """
    if df.empty:
        return pd.DataFrame(
            columns=[
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
                "form",
            ]
        )

    exploded = _explode_team_matches(df)

    # Keep only the last N matches per team; within a team the frame is already
    # sorted chronologically so ``tail`` yields the most recent fixtures.
    recent = (
        exploded.groupby("team_id", group_keys=False)
        .apply(lambda g: g.tail(last_n))
        .reset_index(drop=True)
    )

    records = []
    keys = ["team_id", "team_name", "league_id", "season"]
    for key, group in recent.groupby(keys, dropna=False):
        form = "".join(group["result"].tolist())
        records.append(
            {
                "team_id": key[0],
                "team_name": key[1],
                "league_id": key[2],
                "season": key[3],
                "played": len(group),
                "wins": int((group["result"] == "W").sum()),
                "draws": int((group["result"] == "D").sum()),
                "losses": int((group["result"] == "L").sum()),
                "goals_for": int(group["goals_for"].sum()),
                "goals_against": int(group["goals_against"].sum()),
                "goal_diff": int(group["goal_diff"].sum()),
                "points": int(group["points"].sum()),
                "form": form,
            }
        )

    form_df = pd.DataFrame(records)
    return form_df.sort_values(
        ["points", "goal_diff"], ascending=False
    ).reset_index(drop=True)


def head_to_head(df: pd.DataFrame, team_a_id: int, team_b_id: int) -> pd.DataFrame:
    """Return the historical matches played between two clubs, oldest first."""
    if df.empty:
        return df.copy()

    mask = (
        ((df["home_team_id"] == team_a_id) & (df["away_team_id"] == team_b_id))
        | ((df["home_team_id"] == team_b_id) & (df["away_team_id"] == team_a_id))
    )
    return df.loc[mask].sort_values("date").reset_index(drop=True)


def save_processed_matches(df: pd.DataFrame) -> int:
    """Upsert the flattened frame into ``processed_matches`` by ``fixture_id``.

    Returns the number of documents upserted or modified.
    """
    if df.empty:
        logger.info("No processed matches to save")
        return 0
    return bulk_upsert("processed_matches", df.to_dict("records"), ["fixture_id"])


def save_processed_form(df: pd.DataFrame) -> int:
    """Upsert a form table into ``processed_form`` keyed by ``(team_id, season)``.

    Returns the number of documents upserted or modified.
    """
    if df.empty:
        logger.info("No form rows to save")
        return 0
    return bulk_upsert("processed_form", df.to_dict("records"), ["team_id", "season"])


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    matches = load_matches_df()
    print(f"Flattened matches: {len(matches)}")
    if not matches.empty:
        print(matches.head().to_string())
        print("\nForm table (last 5):")
        print(compute_form_table(matches).head(10).to_string())
