"""
Unit tests for :mod:`config.settings`.

Every setting is read from the environment at import time, so these tests
reload the module with ``dotenv.load_dotenv`` stubbed out and a controlled
environment, asserting both the fallback defaults and the override behaviour.
Stubbing ``load_dotenv`` keeps the tests independent of any local ``.env``.
"""
import importlib

import pytest


def _reload_settings(monkeypatch, **env):
    """Reload ``config.settings`` with the given env vars and no ``.env`` load.

    Pass ``None`` for a variable to ensure it is absent from the environment.
    """
    import dotenv

    monkeypatch.setattr(dotenv, "load_dotenv", lambda *args, **kwargs: None)
    for key, value in env.items():
        if value is None:
            monkeypatch.delenv(key, raising=False)
        else:
            monkeypatch.setenv(key, value)

    import config.settings as settings

    return importlib.reload(settings)


@pytest.fixture(scope="module", autouse=True)
def _restore_settings():
    """Reload settings from the real environment once this module is done."""
    yield
    import config.settings as settings

    importlib.reload(settings)


def test_default_league_and_season(monkeypatch):
    settings = _reload_settings(monkeypatch, DEFAULT_LEAGUE_ID=None, DEFAULT_SEASON=None)

    assert settings.DEFAULT_LEAGUE_ID == 39
    assert settings.DEFAULT_SEASON == 2024


def test_league_and_season_env_override(monkeypatch):
    settings = _reload_settings(monkeypatch, DEFAULT_LEAGUE_ID="140", DEFAULT_SEASON="2023")

    assert settings.DEFAULT_LEAGUE_ID == 140
    assert settings.DEFAULT_SEASON == 2023
    assert isinstance(settings.DEFAULT_LEAGUE_ID, int)


def test_default_retry_and_timeout_values(monkeypatch):
    settings = _reload_settings(
        monkeypatch,
        REQUEST_TIMEOUT=None,
        MAX_RETRIES=None,
        RETRY_BACKOFF=None,
        PAGINATION_SLEEP=None,
    )

    assert settings.REQUEST_TIMEOUT == 30
    assert settings.MAX_RETRIES == 3
    assert settings.RETRY_BACKOFF == 2
    assert settings.PAGINATION_SLEEP == 7


def test_retry_backoff_env_override(monkeypatch):
    settings = _reload_settings(
        monkeypatch,
        MAX_RETRIES="5",
        RETRY_BACKOFF="4",
        REQUEST_TIMEOUT="10",
        PAGINATION_SLEEP="1",
    )

    assert settings.MAX_RETRIES == 5
    assert settings.RETRY_BACKOFF == 4
    assert settings.REQUEST_TIMEOUT == 10
    assert settings.PAGINATION_SLEEP == 1


def test_mongodb_defaults(monkeypatch):
    settings = _reload_settings(monkeypatch, MONGODB_URI=None, MONGODB_DB=None)

    assert settings.MONGODB_URI == "mongodb://admin:password@localhost:27017/"
    assert settings.MONGODB_DB == "football_stats"


def test_mongodb_env_override(monkeypatch):
    settings = _reload_settings(
        monkeypatch,
        MONGODB_URI="mongodb://user:pw@mongo:27017/",
        MONGODB_DB="custom_db",
    )

    assert settings.MONGODB_URI == "mongodb://user:pw@mongo:27017/"
    assert settings.MONGODB_DB == "custom_db"


def test_api_sports_key_aliases_rapidapi_key(monkeypatch):
    settings = _reload_settings(monkeypatch, RAPIDAPI_KEY="secret-key")

    assert settings.RAPIDAPI_KEY == "secret-key"
    assert settings.API_SPORTS_KEY == settings.RAPIDAPI_KEY


def test_api_base_url_is_constant(monkeypatch):
    settings = _reload_settings(monkeypatch)

    assert settings.API_BASE_URL == "https://v3.football.api-sports.io"
    assert settings.RAPIDAPI_HOST == "api-football-v1.p.rapidapi.com"
