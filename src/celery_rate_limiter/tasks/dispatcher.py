from celery import shared_task

from config import factory, celery_app


@shared_task(name="celery_rate_limiter.attempt_consume")
def attempt_consume(limiter_id: str):
    """
    Attempt to consume a task from the task queue.
    :param limiter_id: The id of the rate limiter instance to use.
    """
    limiter = factory.registry.get(limiter_id)
    limiter.drain()
