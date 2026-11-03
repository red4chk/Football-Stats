"""
Unit tests for the MongoDB layer.

``MongoClient`` is mocked with :mod:`unittest.mock` so no server is required;
the tests cover the client singleton, connection health checking, teardown,
and the set of indexes declared by :func:`setup_indexes`.
"""
from unittest.mock import MagicMock, patch

import pytest
from pymongo.errors import ConnectionFailure

import db.connection as connection
from db.collections import (
    PROCESSED_COLLECTIONS,
    RAW_COLLECTIONS,
    get_collection,
    setup_indexes,
)


@pytest.fixture(autouse=True)
def _reset_singleton():
    """Ensure the cached client never leaks between tests."""
    connection._client = None
    yield
    connection._client = None


def _fake_db():
    """Return a MagicMock database that yields a stable mock per collection name."""
    collections: dict[str, MagicMock] = {}

    def _getitem(name):
        return collections.setdefault(name, MagicMock())

    db = MagicMock()
    db.__getitem__.side_effect = _getitem
    return db, collections


def _unique_specs(collection: MagicMock) -> list[list[tuple[str, int]]]:
    """Extract the key specs of every ``create_index(unique=True)`` call."""
    return [
        call.args[0]
        for call in collection.create_index.call_args_list
        if call.kwargs.get("unique") is True
    ]


def test_get_client_returns_singleton():
    with patch.object(connection, "MongoClient") as mock_client:
        first = connection.get_client()
        second = connection.get_client()

    assert first is second
    mock_client.assert_called_once()


def test_get_client_pings_server_on_creation():
    with patch.object(connection, "MongoClient") as mock_client:
        connection.get_client()

    mock_client.return_value.admin.command.assert_called_once_with("ping")


def test_get_client_raises_on_connection_failure():
    with patch.object(connection, "MongoClient") as mock_client:
        mock_client.return_value.admin.command.side_effect = ConnectionFailure("boom")
        with pytest.raises(ConnectionFailure):
            connection.get_client()


def test_close_client_closes_and_resets():
    with patch.object(connection, "MongoClient") as mock_client:
        client = connection.get_client()
        connection.close_client()

    client.close.assert_called_once()
    assert connection._client is None


def test_close_client_is_safe_without_connection():
    connection.close_client()  # must not raise
    assert connection._client is None


def test_get_collection_returns_named_collection():
    db, _ = _fake_db()
    with patch("db.collections.get_db", return_value=db):
        collection = get_collection("matches")

    assert collection is db["matches"]


def test_setup_indexes_declares_unique_index_for_every_collection():
    db, collections = _fake_db()
    with patch("db.collections.get_db", return_value=db):
        setup_indexes()

    with_unique = {
        name for name, mock in collections.items() if _unique_specs(mock)
    }
    assert with_unique == set(RAW_COLLECTIONS) | set(PROCESSED_COLLECTIONS)


def test_setup_indexes_unique_key_specs_match_data_model():
    db, collections = _fake_db()
    with patch("db.collections.get_db", return_value=db):
        setup_indexes()

    expected = {
        "matches": [("fixture.id", 1)],
        "players": [("player.id", 1), ("statistics.0.league.season", 1)],
        "teams": [("team.id", 1)],
        "standings": [("league.id", 1), ("league.season", 1)],
        "top_scorers": [("player.id", 1), ("season", 1), ("league_id", 1)],
        "top_assists": [("player.id", 1), ("season", 1), ("league_id", 1)],
        "team_statistics": [
            ("team.id", 1),
            ("league.id", 1),
            ("league.season", 1),
        ],
        "processed_matches": [("fixture_id", 1)],
        "processed_players": [("player_id", 1), ("season", 1)],
        "processed_teams": [("team_id", 1), ("season", 1)],
        "processed_form": [("team_id", 1), ("season", 1)],
        "processed_top_scorers": [("player_id", 1), ("season", 1), ("league_id", 1)],
        "processed_top_assists": [("player_id", 1), ("season", 1), ("league_id", 1)],
        "processed_league_summary": [("league_id", 1), ("season", 1)],
    }

    for name, spec in expected.items():
        assert spec in _unique_specs(collections[name]), f"{name} unique index"


def test_setup_indexes_creates_secondary_indexes_for_matches():
    db, collections = _fake_db()
    with patch("db.collections.get_db", return_value=db):
        setup_indexes()

    # 1 unique (fixture.id) + 4 secondary (league.id, date, home/away team).
    assert collections["matches"].create_index.call_count == 5


def test_setup_indexes_is_idempotent_safe_to_call_twice():
    db, collections = _fake_db()
    with patch("db.collections.get_db", return_value=db):
        setup_indexes()
        setup_indexes()

    # Calling twice simply re-issues the same idempotent create_index calls.
    assert collections["processed_matches"].create_index.call_count == 8
