"""Dramatiq worker entry point for the rate limiter demo.

This module is imported by the ``dramatiq`` CLI when starting worker
processes. It configures the ``DramatiqRateLimiter`` in the worker
process before the worker loop starts, ensuring that the ``@rate_limited``
decorator can resolve limiter instances via ``DramatiqRateLimiter.get()``.

Usage:
    python -m dramatiq examples.dramatiq.worker
"""

import redis

import dramatiq
from dramatiq.brokers.redis import RedisBroker
from examples.config import LIMIT, MAX_CONCURRENCY, REDIS_HOST, REDIS_PORT, WINDOW
from redis_rate_limiter import DramatiqRateLimiter

LIMITER_ID = "dramatiq_demo"

# Set up the Dramatiq broker (must happen before actor registration).
broker = RedisBroker(host=REDIS_HOST, port=int(REDIS_PORT))
dramatiq.set_broker(broker)

# Rate limiter state connection (decode_responses=True for the library).
limiter_conn = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

# Configure the rate limiter so that the @rate_limited decorator
# can resolve instances via DramatiqRateLimiter.get().
DramatiqRateLimiter.configure(limiter_conn, broker=broker)
DramatiqRateLimiter.create(
    limiter_id=LIMITER_ID,
    limit=LIMIT,
    window=WINDOW,
    max_concurrency=MAX_CONCURRENCY,
    override=True,
)

# Import the generic worker to register it as a Dramatiq actor.
import redis_rate_limiter.backends.dramatiq.tasks.worker  # noqa: E402, F401
