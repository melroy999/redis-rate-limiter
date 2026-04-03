import logging
from typing import Any

from redis_rate_limiter.backends.rq.limiter import RQRateLimiter
from redis_rate_limiter.core import import_string, rate_limited

logger = logging.getLogger(__name__)


@rate_limited(get_limiter=RQRateLimiter.get)
def generic_rate_limited_worker(limiter_id: str, func_path: str, payload: dict) -> Any:
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
        "[RQGenericWorker] Executing task: limiter=%s, func_path=%s.",
        limiter_id,
        func_path,
    )

    target_func = import_string(func_path)
    return target_func(**payload)
