import logging
from typing import Any

from celery import shared_task

from celery_rate_limiter.core.decorators import rate_limited
from celery_rate_limiter.core.importing import import_string

logger = logging.getLogger(__name__)


@shared_task(name="celery_rate_limiter.generic_worker")
@rate_limited()
def generic_rate_limited_worker(limiter_id: str, func_path: str, payload: dict) -> Any:
    """Execute a function by its import path.

    Args:
        limiter_id: The id of the rate limiter instance.
        func_path: The dot-separated path to the function to execute.
        payload: The arguments to pass to the function.

    Returns:
        The result of the function execution.
    """
    # limiter_id needs to be included.
    _ = limiter_id
    logger.debug(
        "Generic worker executing task: limiter_id=%s, func_path=%s.",
        limiter_id,
        func_path,
    )

    # Execute the function.
    target_func = import_string(func_path)
    return target_func(**payload)
