import json
import logging
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

def movie(movie_id, key):
    url = f"https://api.themoviedb.org/3/movie/{movie_id}?append_to_response=credits&api_key={key}"
    
    logger.debug(f"Fetching movie data for ID: {movie_id}")
    
    # Configure retry strategy
    retry_strategy = Retry(
        total=3,  # Total number of retries
        backoff_factor=1,  # Wait 1s, 2s, 4s between retries
        status_forcelist=[429, 500, 502, 503, 504],  # Retry on these status codes
        allowed_methods=["GET"]  # Only retry GET requests
    )
    
    # Create session with retry adapter
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    
    try:
        # Connect timeout: 5s, Read timeout: 10s
        response = session.get(url, timeout=(5, 10))
        response.raise_for_status()
        logger.info(f"Successfully fetched movie ID: {movie_id}")
        return response.json()
        
    except requests.exceptions.HTTPError as http_err:
        if response.status_code == 404:
            logger.warning(f"Movie ID {movie_id} not found (404). Skipping.")
        elif response.status_code == 401:
            logger.error("Invalid API key (401). Check your KEY.")
        else:
            logger.error(f"HTTP error occurred for Movie ID {movie_id}: {http_err}")
        return None
        
    except requests.exceptions.Timeout:
        logger.error(f"Request timed out for Movie ID {movie_id}")
        return None
        
    except requests.exceptions.RequestException as err:
        logger.error(f"Request error for Movie ID {movie_id}: {err}")
        return None
    
    finally:
        session.close()
        logger.debug(f"Session closed for movie ID: {movie_id}")