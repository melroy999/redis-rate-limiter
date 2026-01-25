from typing import override

import redis
from celery import Celery

from src import CeleryRateLimiterFactory

# Create the redis and celery instances here so everyone can use the same configuration.
redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)
celery_app = Celery('rate_limiter_demo', broker='redis://localhost:6379/0', worker_prefetch_multiplier=1)

# The rate limiter factory.
factory = CeleryRateLimiterFactory(redis_client, celery_app)
