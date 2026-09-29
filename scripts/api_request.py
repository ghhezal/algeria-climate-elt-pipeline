import requests
import time

def fetch_data(url, max_retries=3, backoff_factor=2):
    """
    Fetch data from Open-Meteo REST API with timeout and retry backoff.
    """
    print(f"Fetching data from URL: {url}")
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            print("API request successful.")
            return response.json()
        except requests.RequestException as e:
            print(f"Attempt {attempt}/{max_retries} failed for {url}: {e}")
            if attempt == max_retries:
                raise
            sleep_time = backoff_factor ** attempt
            print(f"Waiting {sleep_time} seconds before retrying...")
            time.sleep(sleep_time)
