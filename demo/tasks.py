"""Mock task function for the K8s/Docker Compose demonstration.

The function path ``demo.tasks.mock_work`` is passed to
``limiter.schedule_task()`` and imported at runtime by the rate limiter's
``import_string`` utility.
"""

import time

from prometheus_client import Counter

from demo.config import LIMITER_ID, TASK_DURATION

FUNC_PATH = "demo.tasks.mock_work"

_tasks_executed = Counter(
    "redis_rate_limiter_demo_tasks_executed_total",
    "Total tasks executed by this worker pod.",
    ["limiter_id"],
)


def mock_work(_rate_limit_task_id: str = "", **kwargs) -> None:
    """Simulate a short unit of work (e.g., an external API call)."""
    _tasks_executed.labels(limiter_id=LIMITER_ID).inc()
    time.sleep(TASK_DURATION)
