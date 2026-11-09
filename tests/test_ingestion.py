"""
Unit tests for the ingestion fetchers.

``api_get`` / ``paginate`` are mocked to return canned API payloads and the
MongoDB collection handle is mocked so the fetchers' ``bulk_write`` calls can
be inspected without reaching the network or a database.
"""
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from ingestion.fetch_matches import fetch_matches, fetch_recent_matches
from ingestion.fetch_players import fetch_players
from ingestion.fetch_standings import fetch_standings
from ingestion.fetch_teams import fetch_teams
from ingestion.fetch_top_assists import fetch_top_assists
from ingestion.fetch_top_scorers import fetch_top_scorers


def _collection(upserted: int = 0, modified: int = 0) -> MagicMock:
    """A mocked collection whose ``bulk_write`` reports the given counts."""
    collection = MagicMock()
    collection.bulk_write.return_value = SimpleNamespace(
        upserted_count=upserted, modified_count=modified
    )
    return collection


def _ops(collection: MagicMock) -> list:
    """Return the list of write operations passed to ``bulk_write``."""
    return collection.bulk_write.call_args.args[0]


def _filters(collection: MagicMock) -> list[dict]:
    """Return the filter document of each operation (pymongo stores it as ``_filter``)."""
    return [op._filter for op in _ops(collection)]


def _fixture(fixture_id: int) -> dict:
    return {
        "fixture": {
            "id": fixture_id,
            "date": "2024-08-16T19:00:00+00:00",
            "status": {"short": "FT"},
            "venue": {"name": "Stadium"},
        },
        "league": {"id": 39, "name": "Premier League", "season": 2024, "round": "R1"},
        "teams": {"home": {"id": 10}, "away": {"id": 20}},
        "goals": {"home": 2, "away": 1},
    }


def _player(player_id: int, season: int = 2024) -> dict:
    return {
        "player": {"id": player_id, "name": f"Player {player_id}"},
        "statistics": [{"league": {"id": 39, "season": season}, "team": {"id": 10}}],
    }


# -- standings --------------------------------------------------------------
def test_fetch_standings_upserts_one_op_per_league_entry():
    payload = {"response": [{"league": {"id": 39, "season": 2024}}]}
    collection = _collection(upserted=1)

    with patch("ingestion.fetch_standings.api_get", return_value=payload), patch(
        "ingestion.fetch_standings.get_collection", return_value=collection
    ):
        count = fetch_standings(39, 2024)

    assert count == 1
    assert len(_ops(collection)) == 1
    assert _filters(collection) == [{"league.id": 39, "league.season": 2024}]


def test_fetch_standings_returns_zero_on_empty_response():
    with patch("ingestion.fetch_standings.api_get", return_value={"response": []}), patch(
        "ingestion.fetch_standings.get_collection"
    ) as get_collection:
        count = fetch_standings(39, 2024)

    assert count == 0
    get_collection.assert_not_called()


# -- matches ----------------------------------------------------------------
def test_fetch_matches_keys_each_op_by_fixture_id():
    fixtures = [_fixture(1), _fixture(2), _fixture(3)]
    collection = _collection(upserted=3)

    with patch("ingestion.fetch_matches.paginate", return_value=fixtures), patch(
        "ingestion.fetch_matches.get_collection", return_value=collection
    ):
        count = fetch_matches(39, 2024)

    assert count == 3
    assert len(_ops(collection)) == 3
    assert _filters(collection) == [
        {"fixture.id": 1},
        {"fixture.id": 2},
        {"fixture.id": 3},
    ]
    assert all(op._upsert for op in _ops(collection))


def test_fetch_matches_passes_league_season_and_date_bounds():
    collection = _collection()

    with patch("ingestion.fetch_matches.paginate", return_value=[]) as paginate, patch(
        "ingestion.fetch_matches.get_collection", return_value=collection
    ):
        fetch_matches(140, 2023, from_date="2023-08-01", to_date="2023-08-31")

    paginate.assert_called_once_with(
        "fixtures",
        {"league": 140, "season": 2023, "from": "2023-08-01", "to": "2023-08-31"},
    )


def test_fetch_matches_returns_zero_and_skips_write_when_empty():
    with patch("ingestion.fetch_matches.paginate", return_value=[]), patch(
        "ingestion.fetch_matches.get_collection"
    ) as get_collection:
        count = fetch_matches(39, 2024)

    assert count == 0
    get_collection.assert_not_called()


def test_fetch_recent_matches_builds_a_date_range():
    collection = _collection()

    with patch("ingestion.fetch_matches.paginate", return_value=[]) as paginate, patch(
        "ingestion.fetch_matches.get_collection", return_value=collection
    ):
        fetch_recent_matches(days=7, league_id=39, season=2024)

    params = paginate.call_args.args[1]
    assert params["league"] == 39
    assert "from" in params and "to" in params


# -- players ----------------------------------------------------------------
def test_fetch_players_keys_by_player_and_season():
    players = [_player(1), _player(2)]
    collection = _collection(upserted=2)

    with patch("ingestion.fetch_players.paginate", return_value=players), patch(
        "ingestion.fetch_players.get_collection", return_value=collection
    ):
        count = fetch_players(39, 2024)

    assert count == 2
    assert _filters(collection) == [
        {"player.id": 1, "statistics.0.league.season": 2024},
        {"player.id": 2, "statistics.0.league.season": 2024},
    ]


def test_fetch_players_forwards_max_pages_cap():
    collection = _collection()

    with patch("ingestion.fetch_players.paginate", return_value=[]) as paginate, patch(
        "ingestion.fetch_players.get_collection", return_value=collection
    ):
        fetch_players(39, 2024, max_pages=2)

    assert paginate.call_args.args[2] == 2


def test_fetch_players_returns_zero_when_no_players():
    with patch("ingestion.fetch_players.paginate", return_value=[]), patch(
        "ingestion.fetch_players.get_collection"
    ) as get_collection:
        count = fetch_players(39, 2024)

    assert count == 0
    get_collection.assert_not_called()


# -- teams ------------------------------------------------------------------
def test_fetch_teams_denormalizes_league_and_season():
    payload = {"response": [{"team": {"id": 10, "name": "Arsenal"}}]}
    collection = _collection(upserted=1)

    with patch("ingestion.fetch_teams.api_get", return_value=payload), patch(
        "ingestion.fetch_teams.get_collection", return_value=collection
    ):
        count = fetch_teams(140, 2023)

    assert count == 1
    assert _filters(collection) == [{"team.id": 10}]
    document = _ops(collection)[0]._doc["$set"]
    assert document["league_id"] == 140
    assert document["season"] == 2023


# -- leaderboards -----------------------------------------------------------
@pytest.mark.parametrize(
    "module_path, func, endpoint",
    [
        ("ingestion.fetch_top_scorers", fetch_top_scorers, "players/topscorers"),
        ("ingestion.fetch_top_assists", fetch_top_assists, "players/topassists"),
    ],
)
def test_leaderboard_fetchers_key_by_player_season_and_league(module_path, func, endpoint):
    payload = {"response": [{"player": {"id": 7, "name": "Striker"}}]}
    collection = _collection(upserted=1)

    with patch(f"{module_path}.api_get", return_value=payload) as api_get, patch(
        f"{module_path}.get_collection", return_value=collection
    ):
        count = func(39, 2024)

    assert count == 1
    api_get.assert_called_once_with(endpoint, {"league": 39, "season": 2024})
    assert _filters(collection) == [{"player.id": 7, "season": 2024, "league_id": 39}]
    document = _ops(collection)[0]._doc["$set"]
    assert document["season"] == 2024
    assert document["league_id"] == 39
