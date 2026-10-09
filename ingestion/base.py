"""
Shared HTTP client for API-Football (api-sports.io / RapidAPI).

Provides:

* :func:`build_session` — a ``requests.Session`` pre-wired with a retry
  strategy that is safe to call concurrently.
* :func:`api_get` — a single-page GET with rate-limit awareness.  Handles
  the API's ``errors.rateLimit`` response by sleeping 65s and retrying once,
  and also pre-emptively sleeps when the
  ``x-ratelimit-requests-remaining`` header is below 3.
* :func:`paginate` — loops over a paged endpoint until all pages are fetched,
  with a configurable hard upper bound via ``max_pages``.  Sleeps
  ``PAGINATION_SLEEP`` seconds between page requests to respect the free
  tier's per-minute request quota.
"""
import logging
import time

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config.settings import (
    API_BASE_URL,
    MAX_RETRIES,
    PAGINATION_SLEEP,
    RAPIDAPI_KEY,
    REQUEST_TIMEOUT,
    RETRY_BACKOFF,
)

logger = logging.getLogger(__name__)


def build_session() -> requests.Session:
    """Return a ``requests.Session`` with retry + connection pooling configured."""
    session = requests.Session()
    retry = Retry(
        total=MAX_RETRIES,
        backoff_factor=RETRY_BACKOFF,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    return session


def api_get(endpoint: str, params: dict) -> dict:
    """Perform a single GET request against ``API_BASE_URL/<endpoint>``.

    Raises :class:`RuntimeError` if ``RAPIDAPI_KEY`` is empty,
    :class:`HTTPError` on non-recoverable HTTP status codes, or
    :class:`ValueError` if the API returns non-empty ``errors`` after any
    automatic rate-limit recovery.
    """
    if not RAPIDAPI_KEY:
        raise RuntimeError(
            "RAPIDAPI_KEY is not set. Add it to your .env file or environment."
        )

    url = f"{API_BASE_URL}/{endpoint}"
    headers = {"x-apisports-key": RAPIDAPI_KEY}

    session = build_session()
    logger.info("GET %s params=%s", url, params)

    response = session.get(url, headers=headers, params=params, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()

    data = response.json()
    errors = data.get("errors", {})
    if errors:
        rate_err = errors.get("rateLimit", "")
        if rate_err:
            logger.warning("Rate limit hit — waiting 65 seconds: %s", rate_err)
            time.sleep(65)
            response = session.get(
                url, headers=headers, params=params, timeout=REQUEST_TIMEOUT
            )
            response.raise_for_status()
            data = response.json()
            errors = data.get("errors", {})
        if errors:
            raise ValueError(f"API returned errors: {errors}")

    remaining = response.headers.get("x-ratelimit-requests-remaining")
    if remaining is not None and int(remaining) < 3:
        logger.warning(
            "Rate limit nearly exhausted (%s remaining) — waiting 65s",
            remaining,
        )
        time.sleep(65)

    return data


def paginate(
    endpoint: str,
    params: dict,
    max_pages: int | None = None,
) -> list[dict]:
    """Fetch all pages of a paginated endpoint and concatenate responses.

    Pages are requested with ``?page=1``, ``?page=2``… as long as the API
    reports more pages via ``paging.total``.  Pass ``max_pages`` to cap the
    total (useful for the API-Football free plan whose /players endpoint
    would otherwise pull 30+ pages for a full league squad).
    """
    results: list[dict] = []
    page = 1
    while True:
        page_params = {**params}
        if page > 1:
            page_params["page"] = page
        data = api_get(endpoint, page_params)
        response_data = data.get("response", [])
        results.extend(response_data)

        paging = data.get("paging", {})
        total_pages = paging.get("total", 1)
        if max_pages is not None:
            total_pages = min(total_pages, max_pages)
        logger.info(
            "Fetched page %d / %d from %s (%d records)",
            page,
            total_pages,
            endpoint,
            len(response_data),
        )

        if page >= total_pages:
            break
        page += 1
        time.sleep(PAGINATION_SLEEP)

    return results
