"""RQ worker entry point for the rate limiter demo.

This module is spawned as a subprocess by ``demo.py``. It configures the
``RQRateLimiter`` in the worker process before starting the RQ worker
loop, ensuring that the ``@rate_limited`` decorator can resolve limiter
instances via ``RQRateLimiter.get()``.

Usage:
    python -m examples.rq.worker
"""

import redis

from examples.config import LIMIT, MAX_CONCURRENCY, REDIS_HOST, REDIS_PORT, WINDOW
from redis_rate_limiter import RQRateLimiter
from rq import Queue, SimpleWorker

LIMITER_ID = "rq_demo"
QUEUE_NAME = "rate_limited"


def main() -> None:
    # Rate limiter state connection (decode_responses=True for the library).
    limiter_conn = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    # RQ connection (decode_responses=False, the default, for binary job data).
    rq_conn = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
    queue = Queue(QUEUE_NAME, connection=rq_conn)

    # Configure the rate limiter so that the @rate_limited decorator
    # can resolve instances via RQRateLimiter.get().
    RQRateLimiter.configure(limiter_conn, queue=queue)
    RQRateLimiter.create(
        limiter_id=LIMITER_ID,
        limit=LIMIT,
        window=WINDOW,
        max_concurrency=MAX_CONCURRENCY,
        override=True,
    )

    # SimpleWorker processes jobs in the main process (no fork), which
    # avoids Redis connection sharing issues across forked children.
    worker = SimpleWorker([queue], connection=rq_conn)
    worker.work(burst=False)


if __name__ == "__main__":
    main()
