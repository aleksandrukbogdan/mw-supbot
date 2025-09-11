import json
import os
from logger import logger

log = logger.get_logger(__name__)

DASHBOARD_MESSAGES_FILE = "dashboard_messages.json"

def save_dashboard_messages(messages: list[dict]):
    """
    Saves a list of dashboard message objects to the JSON file.
    Each object should be a dictionary, e.g., {'id': 123, 'timestamp': '...'}
    """
    try:
        with open(DASHBOARD_MESSAGES_FILE, "w") as f:
            json.dump(messages, f, indent=4)
        log.info(f"Successfully saved {len(messages)} dashboard message(s) to {DASHBOARD_MESSAGES_FILE}")
    except Exception as e:
        log.error(f"Failed to save dashboard messages to {DASHBOARD_MESSAGES_FILE}: {e}", exc_info=True)

def load_dashboard_messages() -> list[dict]:
    """
    Loads the list of dashboard message objects from the JSON file.
    Returns an empty list if the file doesn't exist or is empty.
    """
    if not os.path.exists(DASHBOARD_MESSAGES_FILE):
        log.warning(f"{DASHBOARD_MESSAGES_FILE} not found. Returning empty list.")
        return []

    try:
        with open(DASHBOARD_MESSAGES_FILE, "r") as f:
            content = f.read()
            if not content:
                log.warning(f"{DASHBOARD_MESSAGES_FILE} is empty. Returning empty list.")
                return []
            messages = json.loads(content)
            log.info(f"Successfully loaded {len(messages)} dashboard message(s) from {DASHBOARD_MESSAGES_FILE}")
            return messages
    except json.JSONDecodeError:
        log.error(f"Could not decode JSON from {DASHBOARD_MESSAGES_FILE}. The file might be corrupted. Returning empty list.")
        return []
    except Exception as e:
        log.error(f"Failed to load dashboard messages from {DASHBOARD_MESSAGES_FILE}: {e}", exc_info=True)
        return []
