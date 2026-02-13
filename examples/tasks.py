"""Mock business logic shared across the demonstration scripts.

The functions defined herein are resolved at runtime via ``import_string``;
as such, the fully-qualified path of each function must be provided as the
``func_path`` argument when scheduling tasks.
"""

import logging
import random
import time

from examples.config import TASK_SLEEP_MAX, TASK_SLEEP_MIN

logger = logging.getLogger("examples.tasks")

FUNC_PATH = "examples.tasks.mock_api_call"
FAILING_FUNC_PATH = "examples.tasks.mock_api_call_failing"


def mock_api_call(user_id: int, priority: int = 100) -> None:
    """Simulate a slow outbound API call with a randomised latency delay."""
    delay = random.uniform(TASK_SLEEP_MIN, TASK_SLEEP_MAX)
    logger.debug("user_id=%d prio=%d starting (%.3fs delay)", user_id, priority, delay)
    time.sleep(delay)
    logger.debug("user_id=%d prio=%d done", user_id, priority)


def mock_api_call_failing(user_id: int, priority: int = 100) -> None:
    """Simulate an API call that raises an exception after a short delay."""
    delay = random.uniform(TASK_SLEEP_MIN, TASK_SLEEP_MAX)
    logger.debug("user_id=%d prio=%d starting (will fail after %.3fs)", user_id, priority, delay)
    time.sleep(delay)
    logger.error("user_id=%d prio=%d raising RuntimeError", user_id, priority)
    raise RuntimeError(f"Simulated API failure for user_id={user_id}")
