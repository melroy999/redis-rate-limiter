from celery import shared_task

from config import factory


@shared_task(name="celery_rate_limiter.attempt_consume")
def attempt_consume(limiter_id: str) -> None:
    """Attempt to consume a task from the task queue.

    Args:
        limiter_id: The id of the rate limiter instance to use.
    """
    limiter = factory.registry.get(limiter_id)
    limiter.drain()
