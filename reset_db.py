"""
Clean the football_stats database by dropping collections.

Usage::

    python reset_db.py              # drop ALL collections (raw + processed)
    python reset_db.py --raw        # drop only raw ingestion collections
    python reset_db.py --processed  # drop only processed/derived collections

The script intentionally does not create or require a running Dagster
instance — it only needs a reachable MongoDB server matching ``MONGODB_URI``.
"""
import argparse
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from db.collections import PROCESSED_COLLECTIONS, RAW_COLLECTIONS
from db.connection import get_db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def drop_collections(names: list[str]) -> None:
    """Drop each named collection from the DB if it exists."""
    db = get_db()
    existing = set(db.list_collection_names())
    for name in names:
        if name in existing:
            db[name].drop()
            logger.info("Dropped collection: %s", name)
        else:
            logger.info("Skipped (not found): %s", name)


def main() -> None:
    parser = argparse.ArgumentParser(description="Clean the football_stats database")
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--raw",
        action="store_true",
        help="Drop only raw ingestion collections",
    )
    group.add_argument(
        "--processed",
        action="store_true",
        help="Drop only processed collections",
    )
    args = parser.parse_args()

    if args.raw:
        logger.info("=== Dropping RAW collections ===")
        drop_collections(RAW_COLLECTIONS)
    elif args.processed:
        logger.info("=== Dropping PROCESSED collections ===")
        drop_collections(PROCESSED_COLLECTIONS)
    else:
        logger.info("=== Dropping ALL collections ===")
        drop_collections(RAW_COLLECTIONS + PROCESSED_COLLECTIONS)

    logger.info("Done.")


if __name__ == "__main__":
    main()
