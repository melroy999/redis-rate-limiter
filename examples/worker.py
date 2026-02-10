# Load shared infrastructure and configure the limiter for this process.
from config import celery_app, configure_limiter

from celery_rate_limiter.limiters import CeleryRateLimiter

configure_limiter()

# Register the tasks.
celery_app.conf.imports = [
    "celery_rate_limiter.tasks.dispatcher",
    "celery_rate_limiter.tasks.worker",
]

# Self-initialize: create the limiter (idempotent via override=True).
#    This eliminates the need for a separate "Initialize Environment" step.
limiter = CeleryRateLimiter.create(
    limiter_id="test_api", limit=5, window=1, max_concurrency=100, override=True
)
limiter.drain()
