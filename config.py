import redis
from celery import Celery

from src.rate_limiter.celery import CeleryRateLimiter
from src.rate_limiter.registry import register_limiter

# Create the celery instance here so everyone can use it
app = Celery('rate_limiter_demo', broker='redis://localhost:6379/0')


def init_infrastructure():
    """Initializes Redis and all rate limiter instances."""
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)

    # Define your limiters here
    test_limiter = CeleryRateLimiter(
        redis_client=r,
        base_key="test_api",
        limit=50,
        window=10,
        max_concurrency=10
    )

    # Add them to the registry
    register_limiter(test_limiter)

    return r, test_limiter
