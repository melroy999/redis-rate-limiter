"""Mock business logic shared across demo scripts.

The function below is resolved at runtime via ``import_string``, so its
fully-qualified path must be passed as ``func_path`` when scheduling tasks.
"""

import random
import time

FUNC_PATH = "examples.tasks.mock_api_call"


def mock_api_call(user_id: int) -> None:
    """Simulate a slow outbound API call."""
    time.sleep(random.uniform(0.1, 0.3))
