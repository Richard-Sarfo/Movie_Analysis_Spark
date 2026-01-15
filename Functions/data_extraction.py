import json
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def movie(movie_id, key):
    url = f"https://api.themoviedb.org/3/movie/{movie_id}?append_to_response=credits&api_key={key}"
    
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
        return response.json()
        
    except requests.exceptions.HTTPError as http_err:
        if response.status_code == 404:
            print(f"Movie ID {movie_id} not found (404). Skipping.")
        elif response.status_code == 401:
            print("Invalid API key (401). Check your KEY.")
        else:
            print(f"HTTP error occurred for Movie ID {movie_id}: {http_err}")
        return None
        
    except requests.exceptions.Timeout:
        print(f"Request timed out for Movie ID {movie_id}")
        return None
        
    except requests.exceptions.RequestException as err:
        print(f"Request error for Movie ID {movie_id}: {err}")
        return None
    
    finally:
        session.close()