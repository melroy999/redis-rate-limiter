import redis
from celery import Celery

from src import CeleryRateLimiterFactory

# Create the redis and celery instances here so everyone can use the same configuration.
redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)
celery_app = Celery('rate_limiter_demo', broker='redis://localhost:6379/0')

# The rate limiter factory.
factory = CeleryRateLimiterFactory(redis_client, celery_app)

# Create a test limiter.
test_limiter = factory.create_limiter(
    limiter_id="test_api",
    limit=50,
    window=10,
    max_concurrency=10
)
