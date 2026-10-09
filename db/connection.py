"""
MongoDB client singleton and database access helpers.

The module caches a single ``MongoClient`` instance for the lifetime of the
process. Import :func:`get_db` anywhere in the project to obtain a handle on
the ``football_stats`` database (or whatever ``MONGODB_DB`` was set to).
"""
import logging

from pymongo import MongoClient
from pymongo.database import Database
from pymongo.errors import ConnectionFailure

from config.settings import MONGODB_URI, MONGODB_DB

logger = logging.getLogger(__name__)

_client: MongoClient | None = None


def get_client() -> MongoClient:
    """Return the shared ``MongoClient`` instance, creating it on first call.

    Raises :class:`pymongo.errors.ConnectionFailure` if the server cannot be
    reached within the selection timeout.
    """
    global _client
    if _client is None:
        _client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=10_000)
        try:
            _client.admin.command("ping")
        except ConnectionFailure as exc:
            logger.error("MongoDB connection failed: %s", exc)
            raise
        logger.info("Connected to MongoDB at %s", MONGODB_URI)
    return _client


def get_db() -> Database:
    """Return a handle on the configured application database."""
    return get_client()[MONGODB_DB]


def close_client() -> None:
    """Close the cached ``MongoClient`` (if any) and release its sockets.

    Safe to call multiple times; a subsequent :func:`get_client` call will
    open a fresh connection.
    """
    global _client
    if _client is not None:
        _client.close()
        _client = None
        logger.info("MongoDB connection closed")
