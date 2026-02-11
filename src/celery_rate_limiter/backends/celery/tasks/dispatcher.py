import logging

from celery import shared_task

from celery_rate_limiter.backends.celery import CeleryRateLimiter

logger = logging.getLogger(__name__)


@shared_task(name="celery_rate_limiter.attempt_consume")
def attempt_consume(limiter_id: str) -> None:
    """Attempt to consume a task from the task queue.

    Args:
        limiter_id: The id of the rate limiter instance to use.
    """
    logger.debug("Attempt-consume task triggered: limiter_id=%s.", limiter_id)
    limiter = CeleryRateLimiter.get(limiter_id)
    limiter.drain()
