import functools
from config import factory


def rate_limited(limiter_id: str = None):
    """
    Decorator to apply rate limiting lifecycle to a standard Celery task.
    :param limiter_id: The id of the rate limiter instance to use.
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Resolve the limiter id.
            l_id = limiter_id or kwargs.get('limiter_id')

            # Fetch the limiter and the task id.
            limiter = factory.registry.get(l_id)
            task_id = kwargs.pop('_rate_limit_task_id')

            # Wrap task execution in the lifecycle manager.
            with limiter.task_lifecycle(task_id):
                return func(*args, **kwargs)

        return wrapper

    return decorator
