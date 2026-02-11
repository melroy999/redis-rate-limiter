from celery_rate_limiter.backends.celery.tasks.dispatcher import attempt_consume
from celery_rate_limiter.backends.celery.tasks.worker import generic_rate_limited_worker

__all__ = ["attempt_consume", "generic_rate_limited_worker"]
