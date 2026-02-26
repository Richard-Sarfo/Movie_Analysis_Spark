"""Data extraction module for TMDB movie data.

This module handles fetching movie data from The Movie Database (TMDB) API.
It provides robust HTTP session management with automatic retries,
configurable timeouts, and comprehensive error handling.

The base API URL and default timeout values are configurable via environment
variables or function parameters, making the module suitable for both
development and production environments.

Environment Variables:
    TMDB_BASE_URL: Override the default TMDB API base URL.
                   Default: 'https://api.themoviedb.org/3'
    TMDB_CONNECT_TIMEOUT: HTTP connection timeout in seconds. Default: 5
    TMDB_READ_TIMEOUT: HTTP read timeout in seconds. Default: 10

Functions:
    movie: Fetch detailed movie data (including credits) by movie ID.
    fetch_movies: Fetch data for multiple movie IDs with summary reporting.

Typical usage:
    >>> from Functions.data_extraction import movie, fetch_movies
    >>> data = movie(movie_id=299534, key='your_api_key')
    >>> results = fetch_movies(movie_ids=[299534, 19995], key='your_api_key')
"""

import json
import logging
import os
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configure logger for data extraction module
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Create console handler with formatting
if not logger.handlers:
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    # Create file handler for persistent logs
    file_handler = logging.FileHandler('data_extraction.log')
    file_handler.setLevel(logging.DEBUG)

    # Create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    # Add handlers to logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

# ---------------------------------------------------------------------------
# Configurable constants – override via environment variables as needed
# ---------------------------------------------------------------------------
TMDB_BASE_URL = os.getenv("TMDB_BASE_URL", "https://api.themoviedb.org/3")
DEFAULT_CONNECT_TIMEOUT = int(os.getenv("TMDB_CONNECT_TIMEOUT", "5"))
DEFAULT_READ_TIMEOUT = int(os.getenv("TMDB_READ_TIMEOUT", "10"))


def movie(movie_id, key, base_url=None, timeout=None):
    """Fetch detailed movie data from the TMDB API.

    Retrieves movie metadata along with cast and crew credits for the
    given movie ID. Includes automatic retry logic for transient errors
    and configurable timeouts.

    Args:
        movie_id (int): The TMDB movie ID to fetch.
        key (str): A valid TMDB API key.
        base_url (str, optional): Override the default TMDB API base URL.
            Defaults to the TMDB_BASE_URL environment variable or
            'https://api.themoviedb.org/3'.
        timeout (tuple[int, int], optional): A (connect, read) timeout
            tuple in seconds. Defaults to (TMDB_CONNECT_TIMEOUT,
            TMDB_READ_TIMEOUT) from environment variables or (5, 10).

    Returns:
        dict or None: The parsed JSON response as a dictionary on
        success, or None if the request failed.

    Raises:
        ValueError: If movie_id is not a positive integer or key is
            empty/None.

    Example:
        >>> data = movie(299534, 'your_api_key')
        >>> print(data['title'])
        'Avengers: Endgame'
    """
    # ---- Input validation ----
    if not isinstance(movie_id, int) or movie_id <= 0:
        raise ValueError(
            f"movie_id must be a positive integer, got {movie_id!r}"
        )
    if not key or not isinstance(key, str):
        raise ValueError("A valid TMDB API key (non-empty string) is required.")

    # ---- Build request URL ----
    api_base = base_url or TMDB_BASE_URL
    url = f"{api_base}/movie/{movie_id}?append_to_response=credits&api_key={key}"

    logger.debug("Fetching movie data for ID: %s from %s", movie_id, api_base)

    # ---- Configure retry strategy ----
    retry_strategy = Retry(
        total=3,                                       # Total number of retries
        backoff_factor=1,                              # Wait 1s, 2s, 4s between retries
        status_forcelist=[429, 500, 502, 503, 504],    # Retry on these status codes
        allowed_methods=["GET"]                        # Only retry GET requests
    )

    # ---- Create session with retry adapter ----
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    connect_timeout, read_timeout = timeout or (DEFAULT_CONNECT_TIMEOUT, DEFAULT_READ_TIMEOUT)

    try:
        response = session.get(url, timeout=(connect_timeout, read_timeout))
        response.raise_for_status()
        logger.info("Successfully fetched movie ID: %s", movie_id)
        return response.json()

    except requests.exceptions.HTTPError as http_err:
        if response.status_code == 404:
            logger.warning("Movie ID %s not found (404). Skipping.", movie_id)
        elif response.status_code == 401:
            logger.error("Invalid API key (401). Check your KEY.")
        else:
            logger.error("HTTP error occurred for Movie ID %s: %s", movie_id, http_err)
        return None

    except requests.exceptions.Timeout:
        logger.error("Request timed out for Movie ID %s", movie_id)
        return None

    except requests.exceptions.RequestException as err:
        logger.error("Request error for Movie ID %s: %s", movie_id, err)
        return None

    finally:
        session.close()
        logger.debug("Session closed for movie ID: %s", movie_id)


def fetch_movies(movie_ids, key, base_url=None, timeout=None):
    """Fetch data for a list of movie IDs with summary logging.

    Iterates over the provided movie IDs, fetches each one using the
    :func:`movie` function, and returns a list of successfully retrieved
    records. Logs a summary at the end showing success/failure counts.

    Args:
        movie_ids (list[int]): A list of TMDB movie IDs to fetch.
        key (str): A valid TMDB API key.
        base_url (str, optional): Override the default TMDB API base URL.
        timeout (tuple[int, int], optional): A (connect, read) timeout tuple.

    Returns:
        list[dict]: A list of successfully fetched movie data dicts.
            Failed fetches are excluded (not None entries).

    Raises:
        ValueError: If movie_ids is empty or not a list.

    Example:
        >>> results = fetch_movies([299534, 19995], 'your_api_key')
        >>> print(len(results))
        2
    """
    if not movie_ids or not isinstance(movie_ids, list):
        raise ValueError("movie_ids must be a non-empty list of integers.")

    results = []
    failed_ids = []

    for mid in movie_ids:
        data = movie(mid, key, base_url=base_url, timeout=timeout)
        if data is not None:
            results.append(data)
        else:
            failed_ids.append(mid)

    logger.info(
        "Fetch complete: %d/%d succeeded, %d failed. Failed IDs: %s",
        len(results), len(movie_ids), len(failed_ids), failed_ids or "none"
    )
    return results