import logging
from typing import Any

from celery import shared_task

from celery_rate_limiter.backends.celery.limiter import CeleryRateLimiter
from celery_rate_limiter.core import import_string, rate_limited

logger = logging.getLogger(__name__)


@shared_task(name="celery_rate_limiter.generic_worker")
@rate_limited(get_limiter=CeleryRateLimiter.get)
def generic_rate_limited_worker(limiter_id: str, func_path: str, payload: dict) -> Any:
    """Execute a function identified by its fully qualified import path.

    Args:
        limiter_id: The identifier of the rate limiter instance that scheduled this task.
        func_path: The dot-separated import path of the function to be executed.
        payload: A dictionary of keyword arguments to be passed to the target function.

    Returns:
        The return value produced by the target function.
    """
    # The limiter_id parameter must be present in the signature for routing purposes.
    logger.debug(
        "Generic worker executing task: limiter=%s, func_path=%s.",
        limiter_id,
        func_path,
    )

    # Resolve and invoke the target function.
    target_func = import_string(func_path)
    return target_func(**payload)
