import functools
from typing import Any, Callable, Optional, TypeVar, cast

from config import factory

# Use generic types.
T = TypeVar("T", bound=Callable[..., Any])


def rate_limited(limiter_id: Optional[str] = None) -> Callable[[T], T]:
    """
    Decorator to apply rate limiting lifecycle to a standard Celery task.
    :param limiter_id: The id of the rate limiter instance to use.
    """

    def decorator(func: T) -> T:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            # Resolve the limiter id.
            l_id = limiter_id or kwargs.get("limiter_id")

            # Fetch the limiter and the task id.
            limiter = factory.registry.get(l_id)
            task_id = kwargs.pop("_rate_limit_task_id")

            # Wrap task execution in the lifecycle manager.
            with limiter.task_lifecycle(task_id):
                return func(*args, **kwargs)

        # noinspection PyUnnecessaryCast
        # This is, in fact, necessary to pass mypy validation.
        return cast(T, wrapper)

    return decorator
