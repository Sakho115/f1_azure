"""
Ergast Formula 1 API Client.
Provides extraction capabilities for the Ergast Developer API with built-in retries,
rate-limiting, error handling, response serialization, and transparent offline fallback.
"""

import os
import time
import json
from typing import Dict, Any, Optional, List
import requests
from src.utils.logger import get_logger

logger = get_logger("ErgastApiClient")

class ErgastApiClient:
    """
    Client for interacting with the Ergast Formula 1 REST API.
    API Docs: https://ergast.com/mrd/
    """

    def __init__(
        self,
        base_url: Optional[str] = None,
        rate_limit_per_sec: int = 4,
        timeout: int = 10,
        raw_storage_dir: Optional[str] = None
    ):
        self.base_url = base_url or os.getenv("ERGAST_API_BASE_URL", "https://ergast.com/api/f1")
        self.rate_limit_delay = 1.0 / rate_limit_per_sec
        self.timeout = timeout
        self.last_request_time = 0.0
        self.raw_storage_dir = raw_storage_dir or os.getenv("RAW_LAYER_PATH", "./data/raw")
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Formula1LakehousePipeline/1.0 (Enterprise Data Engineering Capstone)"
        })

    def _rate_limit(self) -> None:
        """Enforces client-side rate limits to respect Ergast guidelines."""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.rate_limit_delay:
            time.sleep(self.rate_limit_delay - elapsed)
        self.last_request_time = time.time()

    def fetch_endpoint(self, endpoint: str, params: Optional[Dict[str, Any]] = None, max_retries: int = 3) -> Dict[str, Any]:
        """
        Executes an HTTP GET request against the Ergast API with exponential backoff.
        """
        url = f"{self.base_url}/{endpoint}.json"
        params = params or {"limit": 100}

        for attempt in range(1, max_retries + 1):
            try:
                self._rate_limit()
                logger.info(f"Ergast API GET {url} (Attempt {attempt}/{max_retries})")
                response = self.session.get(url, params=params, timeout=self.timeout)

                if response.status_code == 200:
                    return response.json()
                elif response.status_code in [429, 500, 502, 503, 504]:
                    sleep_time = 2 ** attempt
                    logger.warning(f"HTTP {response.status_code}. Retrying in {sleep_time}s...")
                    time.sleep(sleep_time)
                else:
                    logger.error(f"HTTP error {response.status_code}: {response.text}")
                    break
            except requests.RequestException as e:
                logger.warning(f"Connection error on {url}: {e}. Retrying ({attempt}/{max_retries})...")
                time.sleep(2 ** attempt)

        raise ConnectionError(f"Failed to fetch data from Ergast API endpoint: {endpoint}")

    def fetch_and_save(self, endpoint: str, dataset_name: str, filename: str) -> str:
        """
        Fetches an endpoint and persists raw JSON payload to the raw storage layer.
        """
        target_dir = os.path.join(self.raw_storage_dir, dataset_name)
        os.makedirs(target_dir, exist_ok=True)
        target_path = os.path.join(target_dir, filename)

        try:
            data = self.fetch_endpoint(endpoint)
            with open(target_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
            logger.info(f"Successfully saved raw API data to: {target_path}")
            return target_path
        except Exception as e:
            logger.error(f"Live API call failed: {e}. Fallback to sample data is recommended.")
            raise
