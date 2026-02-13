"""Mock business logic shared across demo scripts.

The function below is resolved at runtime via ``import_string``, so its
fully-qualified path must be passed as ``func_path`` when scheduling tasks.
"""

import logging
import random
import time

from examples.config import TASK_SLEEP_MAX, TASK_SLEEP_MIN

logger = logging.getLogger("examples.tasks")

FUNC_PATH = "examples.tasks.mock_api_call"
FAILING_FUNC_PATH = "examples.tasks.mock_api_call_failing"


def mock_api_call(user_id: int, priority: int = 100) -> None:
    """Simulate a slow outbound API call."""
    delay = random.uniform(TASK_SLEEP_MIN, TASK_SLEEP_MAX)
    logger.debug("user_id=%d prio=%d starting (%.3fs delay)", user_id, priority, delay)
    time.sleep(delay)
    logger.debug("user_id=%d prio=%d done", user_id, priority)


def mock_api_call_failing(user_id: int, priority: int = 100) -> None:
    """Simulate an API call that fails after a short delay."""
    delay = random.uniform(TASK_SLEEP_MIN, TASK_SLEEP_MAX)
    logger.debug("user_id=%d prio=%d starting (will fail after %.3fs)", user_id, priority, delay)
    time.sleep(delay)
    logger.error("user_id=%d prio=%d raising RuntimeError", user_id, priority)
    raise RuntimeError(f"Simulated API failure for user_id={user_id}")
