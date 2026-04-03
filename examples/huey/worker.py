"""Huey consumer entry point for the rate limiter demo.

This module defines the ``huey_instance`` that the Huey consumer CLI
imports. It configures the ``HueyRateLimiter`` in the consumer process,
ensuring that the ``@rate_limited`` decorator can resolve limiter
instances via ``HueyRateLimiter.get()``.

Usage:
    python -m huey.bin.huey_consumer examples.huey.worker.huey_instance
"""

import redis

from examples.config import LIMIT, MAX_CONCURRENCY, REDIS_HOST, REDIS_PORT, WINDOW
from huey import RedisHuey
from redis_rate_limiter import HueyRateLimiter

LIMITER_ID = "huey_demo"

# Module-level Huey instance (imported by the Huey consumer CLI).
huey_instance = RedisHuey(
    "rate_limited",
    host=REDIS_HOST,
    port=int(REDIS_PORT),
)

# Rate limiter state connection (decode_responses=True for the library).
limiter_conn = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

# Configure the rate limiter so that the @rate_limited decorator
# can resolve instances via HueyRateLimiter.get().
# This also calls register_worker() to bind the generic worker
# task to this Huey instance.
HueyRateLimiter.configure(limiter_conn, huey=huey_instance)
HueyRateLimiter.create(
    limiter_id=LIMITER_ID,
    limit=LIMIT,
    window=WINDOW,
    max_concurrency=MAX_CONCURRENCY,
    override=True,
)
