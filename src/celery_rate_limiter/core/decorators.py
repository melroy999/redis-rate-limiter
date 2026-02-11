import functools
import logging
from typing import Any, Callable, Optional, TypeVar, cast

from celery_rate_limiter.core.limiters import AbstractDistributedRateLimiter

# Use generic types.
T = TypeVar("T", bound=Callable[..., Any])
logger = logging.getLogger(__name__)


def _get_default_limiter(limiter_id: str) -> AbstractDistributedRateLimiter:
    """Resolve the default limiter implementation lazily.

    Celery-specific imports stay local so this module remains importable when
    Celery is not installed and an alternative limiter resolver is supplied.
    """
    from celery_rate_limiter.backends.celery.limiter import CeleryRateLimiter

    return cast(AbstractDistributedRateLimiter, CeleryRateLimiter.get(limiter_id))


def rate_limited(
    limiter_id: Optional[str] = None,
    *,
    get_limiter: Optional[Callable[[str], AbstractDistributedRateLimiter]] = None,
) -> Callable[[T], T]:
    """Decorator to apply rate limiter lifecycle handling around a task.

    Args:
        limiter_id: The id of the rate limiter instance to use.
        get_limiter: Optional resolver used to fetch limiter instances by id.
            Defaults to lazy Celery-backed resolution for backward compatibility.
    """
    def decorator(func: T) -> T:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            # Resolve the limiter id.
            l_id = limiter_id or kwargs.get("limiter_id")
            if l_id is None:
                raise ValueError(
                    "Missing limiter id. Pass limiter_id to @rate_limited() "
                    "or provide limiter_id in function kwargs."
                )

            # Fetch the limiter and the task id.
            limiter_getter = get_limiter or _get_default_limiter
            limiter = limiter_getter(l_id)
            task_id = kwargs.pop("_rate_limit_task_id")
            logger.debug(
                "Rate-limited decorator entered: limiter_id=%s, task_id=%s, func=%s.",
                l_id,
                task_id,
                func.__qualname__,
            )

            # Wrap task execution in the lifecycle manager.
            with limiter.task_lifecycle(task_id):
                result = func(*args, **kwargs)

            logger.debug(
                "Task execution completed under rate-limited lifecycle: limiter_id=%s, task_id=%s, func=%s.",
                l_id,
                task_id,
                func.__qualname__,
            )
            return result

        # noinspection PyUnnecessaryCast
        # This is, in fact, necessary to pass mypy validation.
        return cast(T, wrapper)

    return decorator
