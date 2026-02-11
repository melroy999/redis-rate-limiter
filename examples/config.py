import redis
from celery import Celery

from celery_rate_limiter import CeleryRateLimiter

# Create the redis and celery instances here so everyone can use the same configuration.
redis_client = redis.Redis(host="localhost", port=6379, decode_responses=True)
celery_app = Celery(
    "rate_limiter_demo", broker="redis://localhost:6379/0", worker_prefetch_multiplier=1
)

_is_configured = False


def configure_limiter() -> None:
    """Configure CeleryRateLimiter once for example scripts."""
    global _is_configured
    if _is_configured:
        return

    CeleryRateLimiter.configure(redis_client, celery_app=celery_app)
    _is_configured = True
