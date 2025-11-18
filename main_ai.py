import os
import sys
import time
import logging
from typing import Dict, List, Optional

import requests
from requests.exceptions import RequestException


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

# Interval between checks (default: 10 minutes)
CHECK_INTERVAL_SECONDS = int(os.environ.get("CHECK_INTERVAL_SECONDS", "600"))
# Time after which a stalled download is considered dead (default: 1 hour)
STALL_TIMEOUT_SECONDS = int(os.environ.get("STALL_TIMEOUT_SECONDS", "3600"))

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    format="%(asctime)s [%(levelname)s]: %(message)s",
    level=logging.INFO,
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Time helpers
# ---------------------------------------------------------------------------

# Use monotonic time to avoid issues if system clock changes
_START_TIME = time.monotonic()


def elapsed_seconds() -> int:
    """Return the number of seconds elapsed since the script started."""
    return int(time.monotonic() - _START_TIME)


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------


class ApiResponse:
    """Simple wrapper to hold HTTP status and parsed JSON data."""

    def __init__(self, data=None, status: Optional[int] = 200):
        self.status = status
        self.data = data

    def ok(self) -> bool:
        """Return True if status is in the 2xx range."""
        return isinstance(self.status, int) and 200 <= self.status <= 299


def http_get(url: str, api_key: str, params: Optional[dict] = None) -> ApiResponse:
    """Perform a GET request with Radarr/Sonarr API key."""
    try:
        headers = {"X-Api-Key": api_key}
        response = requests.get(url, params=params, headers=headers, timeout=30)
        response.raise_for_status()
        return ApiResponse(status=response.status_code, data=response.json())
    except (RequestException, ValueError) as e:
        logger.error("GET %s failed: %s", url, e)
        return ApiResponse(status=None, data=None)


def http_delete(url: str, api_key: str, params: Optional[dict] = None) -> ApiResponse:
    """Perform a DELETE request with Radarr/Sonarr API key."""
    try:
        headers = {"X-Api-Key": api_key}
        response = requests.delete(url, params=params, headers=headers, timeout=30)
        response.raise_for_status()
        # Some APIs do not return JSON for DELETE; handle that gracefully
        try:
            data = response.json()
        except ValueError:
            data = None
        return ApiResponse(status=response.status_code, data=data)
    except (RequestException, ValueError) as e:
        logger.error("DELETE %s failed: %s", url, e)
        return ApiResponse(status=None, data=None)


# ---------------------------------------------------------------------------
# Queue helpers
# ---------------------------------------------------------------------------


def is_stalled(record: dict) -> bool:
    """
    Return True if the record represents a stalled download.

    The heuristic here is:
      - status == "warning"
      - errorMessage contains the word "stalled"
    """
    status = str(record.get("status", "")).lower()
    error_message = str(record.get("errorMessage", "") or "").lower()
    return status == "warning" and "stalled" in error_message


def fetch_queue_page(api_url: str, api_key: str, page: int, page_size: int) -> ApiResponse:
    """Fetch one page of the download queue."""
    queue_url = f"{api_url}/queue"
    params = {"page": str(page), "pageSize": str(page_size)}
    return http_get(queue_url, api_key, params)


def count_records(api_url: str, api_key: str) -> Optional[int]:
    """
    Return the total number of queue records.

    Uses pageSize=1 just to read the 'totalRecords' field.
    Returns None in case of error.
    """
    queue = fetch_queue_page(api_url, api_key, page=1, page_size=1)
    if not queue.ok():
        return None
    if not isinstance(queue.data, dict):
        return 0
    return int(queue.data.get("totalRecords", 0))


def fetch_stalled_ids(api_url: str, api_key: str, total_records: int) -> Optional[List[int]]:
    """
    Return the list of queue IDs currently stalled.

    It uses a single page with pageSize = total_records, like your original script.
    Returns:
        - list of IDs
        - [] if no stalled items
        - None on error (e.g., network issue)
    """
    if total_records == 0:
        return []

    queue = fetch_queue_page(api_url, api_key, page=1, page_size=total_records)
    if not queue.ok():
        logger.error("Failed to fetch queue from %s", api_url)
        return None

    data = queue.data
    if not isinstance(data, dict) or "records" not in data:
        logger.info("Queue from %s is empty or malformed", api_url)
        return []

    stalled_ids: List[int] = []
    for item in data["records"]:
        try:
            if is_stalled(item):
                stalled_ids.append(item["id"])
        except Exception as e:
            # A single malformed record should not break the entire loop
            logger.warning("Error while checking record %s: %s", item, e)

    return stalled_ids


# ---------------------------------------------------------------------------
# Core logic: tracking stalled downloads and cleaning them up
# ---------------------------------------------------------------------------


def process_instance(
    name: str,
    api_url: str,
    api_key: str,
    stalled_since: Dict[int, int],
) -> None:
    """
    Process one instance (Radarr or Sonarr).

    Args:
        name:       Human-readable name for logging (e.g., "radarr").
        api_url:    Base API URL (e.g., http://host:7878/api/v3).
        api_key:    API key for this instance.
        stalled_since:
                    Dictionary mapping queue_id -> first time (elapsed seconds)
                    when we detected the item as stalled.
    """
    total = count_records(api_url, api_key)
    if total is None:
        logger.error("[%s] Failed to get queue size, skipping this cycle", name)
        return

    if total == 0:
        # If queue is empty, clear any cached stalled items
        if stalled_since:
            logger.info("[%s] Queue empty, clearing %d cached stalled items", name, len(stalled_since))
        stalled_since.clear()
        return

    logger.info("[%s] Queue has %d items", name, total)

    stalled_ids = fetch_stalled_ids(api_url, api_key, total)
    if stalled_ids is None:
        # On temporary error, keep the cache so we don't lose timers
        return

    now = elapsed_seconds()
    new_cache: Dict[int, int] = {}

    # Update or create timers for currently stalled IDs
    for download_id in stalled_ids:
        first_seen = stalled_since.get(download_id, now)
        if download_id not in stalled_since:
            logger.info("[%s] Marking %s as stalled (timer starts at %ss)", name, download_id, now)
        else:
            logger.debug(
                "[%s] %s still stalled: elapsed=%ss / timeout=%ss",
                name,
                download_id,
                now - first_seen,
                STALL_TIMEOUT_SECONDS,
            )
        new_cache[download_id] = first_seen

    # Remove and blocklist items that exceeded the timeout
    for download_id, first_seen in list(new_cache.items()):
        stalled_for = now - first_seen
        if stalled_for >= STALL_TIMEOUT_SECONDS:
            logger.warning(
                "[%s] Download %s stalled for %ss (>= %ss), removing and blacklisting",
                name,
                download_id,
                stalled_for,
                STALL_TIMEOUT_SECONDS,
            )
            res = http_delete(
                f"{api_url}/queue/{download_id}",
                api_key,
                {"removeFromClient": "true", "blocklist": "true"},
            )
            if res.ok():
                logger.info("[%s] Successfully removed %s from queue and blocklisted it", name, download_id)
                new_cache.pop(download_id, None)
            else:
                # Keep it in cache so we can retry on the next cycle
                logger.error("[%s] Failed to remove %s from queue (will retry next cycle)", name, download_id)

    # Replace the old cache with the updated one
    stalled_since.clear()
    stalled_since.update(new_cache)


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------


def main() -> None:
    radarr_url = os.environ.get("RADARR_URL") or ""
    radarr_key = os.environ.get("RADARR_API_KEY") or ""

    sonarr_url = os.environ.get("SONARR_URL") or ""
    sonarr_key = os.environ.get("SONARR_API_KEY") or ""

    # If API key is missing, disable that instance
    if not radarr_key:
        radarr_url = ""
    if not sonarr_key:
        sonarr_url = ""

    if not radarr_url and not sonarr_url:
        logger.error("No RADARR_URL/SONARR_URL provided (or missing API keys). Exiting.")
        sys.exit(1)

    logger.info(
        "Starting stalled-download cleaner | Radarr: %s | Sonarr: %s | check every %ss | timeout=%ss",
        radarr_url or "disabled",
        sonarr_url or "disabled",
        CHECK_INTERVAL_SECONDS,
        STALL_TIMEOUT_SECONDS,
    )

    # Per-instance caches for stalled timers
    radarr_stalled: Dict[int, int] = {}
    sonarr_stalled: Dict[int, int] = {}

    while True:
        try:
            if radarr_url:
                process_instance("radarr", f"{radarr_url}/api/v3", radarr_key, radarr_stalled)
            if sonarr_url:
                process_instance("sonarr", f"{sonarr_url}/api/v3", sonarr_key, sonarr_stalled)
        except Exception as e:
            # Any unexpected error in a single cycle should not kill the process
            logger.exception("Unexpected error in main loop: %s", e)

        time.sleep(CHECK_INTERVAL_SECONDS)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Exiting")
        try:
            sys.exit(130)
        except SystemExit:
            os._exit(130)
