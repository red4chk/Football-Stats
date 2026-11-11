"""
Unit tests for the processing layer.

Synthetic in-memory DataFrames are used throughout, so the tests exercise the
pandas logic end-to-end without touching MongoDB: 12 match rows across four
clubs and a small player frame with known per-90 target values.
"""
import pandas as pd
import pytest

from processing.clean_matches import (
    MATCH_COLUMNS,
    compute_form_table,
    head_to_head,
)
from processing.clean_players import compute_player_rankings
from processing.stats import compute_team_stats, estimate_xg

# (fixture_id, date, home_id, home_name, away_id, away_name, home_goals, away_goals)
# Clubs 10/20 and 30/40 play mirrored six-match series, giving two independent
# groups whose form strings can be asserted exactly.
_RAW_MATCHES = [
    (1, "2024-08-01", 10, "Alpha", 20, "Beta", 2, 1),
    (2, "2024-08-08", 20, "Beta", 10, "Alpha", 1, 1),
    (3, "2024-08-15", 10, "Alpha", 20, "Beta", 0, 1),
    (4, "2024-08-22", 20, "Beta", 10, "Alpha", 0, 2),
    (5, "2024-08-29", 10, "Alpha", 20, "Beta", 3, 0),
    (6, "2024-09-05", 20, "Beta", 10, "Alpha", 2, 2),
    (7, "2024-08-02", 30, "Gamma", 40, "Delta", 2, 1),
    (8, "2024-08-09", 40, "Delta", 30, "Gamma", 1, 1),
    (9, "2024-08-16", 30, "Gamma", 40, "Delta", 0, 1),
    (10, "2024-08-23", 40, "Delta", 30, "Gamma", 0, 2),
    (11, "2024-08-30", 30, "Gamma", 40, "Delta", 3, 0),
    (12, "2024-09-06", 40, "Delta", 30, "Gamma", 2, 2),
]

_INT_COLUMNS = [
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


def _matches_df() -> pd.DataFrame:
    """Build a flattened matches frame with the same dtypes as ``load_matches_df``."""
    rows = []
    for fixture_id, date, home_id, home_name, away_id, away_name, hg, ag in _RAW_MATCHES:
        rows.append(
            {
                "fixture_id": fixture_id,
                "date": pd.Timestamp(date, tz="UTC"),
                "status": "FT",
                "venue": "Ground",
                "league_id": 39,
                "league_name": "Premier League",
                "season": 2024,
                "round": "Regular Season - 1",
                "home_team_id": home_id,
                "home_team_name": home_name,
                "away_team_id": away_id,
                "away_team_name": away_name,
                "home_goals": hg,
                "away_goals": ag,
                "home_winner": None,
                "away_winner": None,
                "halftime_home": 0,
                "halftime_away": 0,
            }
        )
    df = pd.DataFrame(rows, columns=MATCH_COLUMNS)
    for column in _INT_COLUMNS:
        df[column] = df[column].astype("Int64")
    return df.sort_values("date").reset_index(drop=True)


def _players_df() -> pd.DataFrame:
    """Four synthetic players with hand-computed per-90 expectations."""
    rows = [
        {
            "player_id": 1,
            "player_name": "Striker",
            "nationality": "FR",
            "team_id": 10,
            "team_name": "Alpha",
            "league_id": 39,
            "league_name": "Premier League",
            "season": 2024,
            "position": "F",
            "rating": 7.8,
            "appearances": 10,
            "lineups": 10,
            "minutes": 900,
            "goals": 9,
            "assists": 2,
            "shots_total": 40,
            "shots_on": 20,
            "passes_total": 300,
            "passes_key": 30,
            "passes_accuracy": 82,
            "tackles_total": 5,
            "duels_won": 40,
            "dribbles_success": 20,
            "cards_yellow": 1,
            "cards_red": 0,
            "penalties_scored": 1,
            "penalties_missed": 0,
        },
        {
            "player_id": 2,
            "player_name": "Playmaker",
            "nationality": "ES",
            "team_id": 20,
            "team_name": "Beta",
            "league_id": 39,
            "league_name": "Premier League",
            "season": 2024,
            "position": "M",
            "rating": 7.4,
            "appearances": 20,
            "lineups": 20,
            "minutes": 1800,
            "goals": 4,
            "assists": 8,
            "shots_total": 30,
            "shots_on": 9,
            "passes_total": 900,
            "passes_key": 60,
            "passes_accuracy": 88,
            "tackles_total": 30,
            "duels_won": 80,
            "dribbles_success": 35,
            "cards_yellow": 2,
            "cards_red": 0,
            "penalties_scored": 0,
            "penalties_missed": 1,
        },
        {
            # Zero minutes — per-90 metrics must fall back to 0.0, never inf/nan.
            "player_id": 3,
            "player_name": "Substitute",
            "nationality": "DE",
            "team_id": 30,
            "team_name": "Gamma",
            "league_id": 39,
            "league_name": "Premier League",
            "season": 2024,
            "position": "F",
            "rating": 6.1,
            "appearances": 3,
            "lineups": 0,
            "minutes": 0,
            "goals": 0,
            "assists": 1,
            "shots_total": 0,
            "shots_on": 0,
            "passes_total": 12,
            "passes_key": 1,
            "passes_accuracy": 70,
            "tackles_total": 0,
            "duels_won": 2,
            "dribbles_success": 0,
            "cards_yellow": 0,
            "cards_red": 0,
            "penalties_scored": 0,
            "penalties_missed": 0,
        },
        {
            # Below the appearance threshold — must be filtered out.
            "player_id": 4,
            "player_name": "Unused",
            "nationality": "IT",
            "team_id": 40,
            "team_name": "Delta",
            "league_id": 39,
            "league_name": "Premier League",
            "season": 2024,
            "position": "D",
            "rating": None,
            "appearances": 0,
            "lineups": 0,
            "minutes": 0,
            "goals": 0,
            "assists": 0,
            "shots_total": 0,
            "shots_on": 0,
            "passes_total": 0,
            "passes_key": 0,
            "passes_accuracy": 0,
            "tackles_total": 0,
            "duels_won": 0,
            "dribbles_success": 0,
            "cards_yellow": 0,
            "cards_red": 0,
            "penalties_scored": 0,
            "penalties_missed": 0,
        },
    ]
    df = pd.DataFrame(rows)
    for column in [
        "appearances",
        "lineups",
        "minutes",
        "goals",
        "assists",
        "shots_total",
        "shots_on",
        "passes_total",
        "passes_key",
        "passes_accuracy",
        "tackles_total",
        "duels_won",
        "dribbles_success",
        "cards_yellow",
        "cards_red",
        "penalties_scored",
        "penalties_missed",
    ]:
        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0).astype("int64")
    df["rating"] = pd.to_numeric(df["rating"], errors="coerce")
    return df


# -- form table -------------------------------------------------------------
def test_compute_form_table_form_strings_are_chronological():
    form = compute_form_table(_matches_df(), last_n=5).set_index("team_id")

    # Alpha: W D L W W D -> last five D L W W D
    assert form.loc[10, "form"] == "DLWWD"
    # Beta: L D W L L D -> last five D W L L D
    assert form.loc[20, "form"] == "DWLLD"
    # Gamma mirrors Alpha.
    assert form.loc[30, "form"] == "DLWWD"
    assert form.loc[40, "form"] == "DWLLD"


def test_compute_form_table_aggregates_points_and_goal_difference():
    form = compute_form_table(_matches_df(), last_n=5).set_index("team_id")

    alpha = form.loc[10]
    assert alpha["played"] == 5
    assert (alpha["wins"], alpha["draws"], alpha["losses"]) == (2, 2, 1)
    assert alpha["points"] == 8
    assert alpha["goals_for"] == 8
    assert alpha["goals_against"] == 4
    assert alpha["goal_diff"] == 4

    beta = form.loc[20]
    assert beta["points"] == 5
    assert beta["goals_for"] == 4
    assert beta["goals_against"] == 8
    assert beta["goal_diff"] == -4


def test_compute_form_table_respects_last_n():
    form = compute_form_table(_matches_df(), last_n=3).set_index("team_id")

    # Alpha's last three of W D L W W D: W W D
    assert form.loc[10, "form"] == "WWD"
    assert form.loc[10, "played"] == 3
    assert form.loc[10, "points"] == 7


def test_compute_form_table_ignores_unplayed_fixtures():
    df = _matches_df()
    extra = df.iloc[[0]].copy()
    extra["fixture_id"] = 99
    extra["home_goals"] = pd.NA
    extra["away_goals"] = pd.NA
    extra["date"] = pd.Timestamp("2024-09-20", tz="UTC")

    form = compute_form_table(pd.concat([df, extra], ignore_index=True), last_n=5)
    form = form.set_index("team_id")

    assert form.loc[10, "played"] == 5
    assert form.loc[10, "form"] == "DLWWD"


def test_compute_form_table_returns_empty_frame_for_empty_input():
    result = compute_form_table(pd.DataFrame(columns=MATCH_COLUMNS))

    assert result.empty
    assert "form" in result.columns


def test_compute_form_table_is_sorted_by_points():
    form = compute_form_table(_matches_df(), last_n=5)

    points = form["points"].tolist()
    assert points == sorted(points, reverse=True)
    assert form.iloc[0]["team_id"] == 10


# -- team stats -------------------------------------------------------------
def test_compute_team_stats_sums_points_over_all_matches():
    stats = compute_team_stats(_matches_df()).set_index("team_id")

    alpha = stats.loc[10]
    assert alpha["played"] == 6
    assert (alpha["wins"], alpha["draws"], alpha["losses"]) == (3, 2, 1)
    assert alpha["points"] == 11
    assert alpha["goals_for"] == 10
    assert alpha["goals_against"] == 5
    assert alpha["goal_diff"] == 5
    assert alpha["win_rate"] == 0.5

    beta = stats.loc[20]
    assert beta["points"] == 5
    assert beta["goals_for"] == 5
    assert beta["goal_diff"] == -5


def test_compute_team_stats_returns_expected_columns_and_order():
    stats = compute_team_stats(_matches_df())

    assert list(stats.columns) == [
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
    assert stats["points"].tolist() == sorted(stats["points"].tolist(), reverse=True)


def test_compute_team_stats_conserves_totals():
    df = _matches_df()
    stats = compute_team_stats(df)

    # Every match contributes exactly two team appearances and one win/loss pair.
    assert stats["played"].sum() == 2 * len(df)
    assert stats["wins"].sum() == stats["losses"].sum()


def test_compute_team_stats_returns_empty_frame_for_empty_input():
    result = compute_team_stats(pd.DataFrame(columns=MATCH_COLUMNS))

    assert result.empty
    assert "points" in result.columns


# -- head to head -----------------------------------------------------------
def test_head_to_head_returns_only_matches_between_the_two_clubs():
    df = _matches_df()
    h2h = head_to_head(df, 10, 20)

    assert len(h2h) == 6
    assert set(h2h["home_team_id"]).union(h2h["away_team_id"]) == {10, 20}
    assert h2h["date"].is_monotonic_increasing


def test_head_to_head_is_symmetric():
    df = _matches_df()

    assert len(head_to_head(df, 10, 20)) == len(head_to_head(df, 20, 10))


def test_head_to_head_returns_empty_when_clubs_never_met():
    assert head_to_head(_matches_df(), 10, 30).empty


# -- player rankings --------------------------------------------------------
def test_compute_player_rankings_per_90_metrics():
    rankings = compute_player_rankings(_players_df()).set_index("player_id")

    striker = rankings.loc[1]
    assert striker["goals_per_90"] == pytest.approx(0.9)
    assert striker["assists_per_90"] == pytest.approx(0.2)
    assert striker["goal_contributions"] == 11
    assert striker["shot_accuracy"] == pytest.approx(0.5)

    playmaker = rankings.loc[2]
    assert playmaker["goals_per_90"] == pytest.approx(0.2)
    assert playmaker["assists_per_90"] == pytest.approx(0.4)
    assert playmaker["goal_contributions"] == 12


def test_compute_player_rankings_has_no_null_metrics():
    rankings = compute_player_rankings(_players_df())

    for column in ["goals_per_90", "assists_per_90", "goal_contributions", "shot_accuracy"]:
        assert rankings[column].notna().all(), column


def test_compute_player_rankings_zero_minutes_yields_zero_not_inf():
    rankings = compute_player_rankings(_players_df()).set_index("player_id")

    substitute = rankings.loc[3]
    assert substitute["goals_per_90"] == 0.0
    assert substitute["assists_per_90"] == 0.0
    assert substitute["shot_accuracy"] == 0.0


def test_compute_player_rankings_drops_players_below_min_appearances():
    rankings = compute_player_rankings(_players_df(), min_appearances=5)

    assert 4 not in rankings["player_id"].tolist()
    assert 3 not in rankings["player_id"].tolist()
    assert set(rankings["player_id"]) == {1, 2}


def test_compute_player_rankings_sorts_by_goal_contributions():
    rankings = compute_player_rankings(_players_df())

    assert rankings["player_id"].tolist() == [2, 1, 3]
    contributions = rankings["goal_contributions"].tolist()
    assert contributions == sorted(contributions, reverse=True)


def test_compute_player_rankings_returns_empty_frame_for_empty_input():
    assert compute_player_rankings(pd.DataFrame()).empty


# -- xG proxy ---------------------------------------------------------------
def test_estimate_xg_falls_back_to_goals_without_shot_data():
    assert estimate_xg(0, 0, 2) == 2.0
    assert estimate_xg(None, None, 3) == 3.0


def test_estimate_xg_combines_shot_volume_and_goals():
    # 0.10 * 10 shots + 0.20 * 4 on target + 0.15 * 2 goals
    assert estimate_xg(10, 4, 2) == pytest.approx(2.1)


def test_estimate_xg_is_monotonic_in_shot_volume():
    assert estimate_xg(20, 5, 1) > estimate_xg(10, 5, 1)
