import logging
from typing import Any, Optional

from huey.api import Huey, TaskWrapper

from redis_rate_limiter.core import import_string, rate_limited

logger = logging.getLogger(__name__)

generic_rate_limited_worker: Optional[TaskWrapper] = None


def _get_limiter(limiter_id: str) -> Any:
    from redis_rate_limiter.backends.huey.limiter import HueyRateLimiter

    return HueyRateLimiter.get(limiter_id)


def register_worker(huey_instance: Huey) -> None:
    """Register the generic worker as a Huey task bound to the given instance."""
    global generic_rate_limited_worker

    # Skip if already registered with this Huey instance.
    if generic_rate_limited_worker is not None:
        if getattr(generic_rate_limited_worker, "huey", None) is huey_instance:
            return

    # Clear a stale registration before re-registering.
    task_name = "redis_rate_limiter.backends.huey.tasks.worker._worker"
    if task_name in huey_instance._registry._registry:
        del huey_instance._registry._registry[task_name]

    @huey_instance.task()
    @rate_limited(get_limiter=_get_limiter)
    def _worker(limiter_id: str, func_path: str, payload: dict) -> Any:
        """Execute a function identified by its fully qualified import path.

        Args:
            limiter_id: The identifier of the rate limiter instance that scheduled this
                task. Must remain in the signature; the ``@rate_limited`` decorator
                extracts it from kwargs to resolve the limiter instance.
            func_path: The dot-separated import path of the function to be executed.
            payload: A dictionary of keyword arguments to be passed to the target function.

        Returns:
            The return value produced by the target function.
        """
        logger.debug(
            "[HueyGenericWorker] Executing task: limiter=%s, func_path=%s.",
            limiter_id,
            func_path,
        )

        target_func = import_string(func_path)
        return target_func(**payload)

    generic_rate_limited_worker = _worker
