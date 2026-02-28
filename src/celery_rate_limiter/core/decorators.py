import functools
import logging
from typing import Any, Callable, Optional, TypeVar, cast

from celery_rate_limiter.core.limiters import AbstractDistributedRateLimiter

# Generic type variable used to preserve the decorated callable's signature.
T = TypeVar("T", bound=Callable[..., Any])
logger = logging.getLogger(__name__)


def _get_default_limiter(limiter_id: str) -> AbstractDistributedRateLimiter:
    """Resolve the default rate limiter implementation.

    The ThreadPool backend is used by default such that this module remains
    functional without a Celery installation. A custom ``get_limiter`` callable
    may be passed to ``@rate_limited()`` to specify an alternative backend.
    """
    # A lazy import is performed here to avoid circular import dependencies.
    from celery_rate_limiter.backends.threading import ThreadPoolRateLimiter

    return ThreadPoolRateLimiter.get(limiter_id)


def rate_limited(
    limiter_id: Optional[str] = None,
    *,
    get_limiter: Optional[Callable[[str], AbstractDistributedRateLimiter]] = None,
) -> Callable[[T], T]:
    """Decorator that applies rate limiter lifecycle handling around a task.

    The decorated function is executed within the context of a rate limiter's
    task lifecycle manager, ensuring that acquisition and release semantics
    are observed.

    Args:
        limiter_id: The identifier of the rate limiter instance to be used.
        get_limiter: An optional resolver callable used to retrieve limiter
            instances by their identifier. If not provided, the default
            ThreadPool-backed resolution is used.
    """

    def decorator(func: T) -> T:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            # Resolve the limiter identifier.
            _limiter_id = limiter_id or kwargs.get("limiter_id")
            if _limiter_id is None:
                raise ValueError(
                    "Missing limiter id. Pass limiter_id to @rate_limited() "
                    "or provide limiter_id in function kwargs."
                )

            # Retrieve the limiter instance and the associated task identifier.
            limiter_getter = get_limiter or _get_default_limiter
            limiter = limiter_getter(_limiter_id)
            task_id = kwargs.pop("_rate_limit_task_id")
            logger.debug(
                "Rate-limited decorator entered: limiter=%s, task_id=%s, func=%s.",
                _limiter_id,
                task_id,
                func.__qualname__,
            )

            # Execute the task within the rate limiter's lifecycle context manager.
            with limiter.task_lifecycle(task_id):
                result = func(*args, **kwargs)

            logger.debug(
                "Task execution completed under rate-limited lifecycle: limiter=%s, task_id=%s, func=%s.",
                _limiter_id,
                task_id,
                func.__qualname__,
            )
            return result

        # noinspection PyUnnecessaryCast
        # This cast is, in fact, necessary to satisfy mypy type validation.
        # fmt: off
        return cast(  # pragma: no mutate
            T, wrapper
        )
        # fmt: on

    return decorator
