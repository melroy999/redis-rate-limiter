"""Self-contained ThreadPoolRateLimiter demo.

Everything runs in a single Python process: task scheduling, execution
via a thread pool, and live dashboard monitoring.

Usage (Docker Redis on 6380):
    REDIS_HOST=localhost REDIS_PORT=6380 poetry run python -m examples.threadpool.demo

Usage (local Redis on 6379):
    poetry run python -m examples.threadpool.demo
"""

import os
from concurrent.futures import ThreadPoolExecutor

from celery_rate_limiter import ThreadPoolRateLimiter
from examples.runner import (
    LIMIT,
    MAX_CONCURRENCY,
    WINDOW,
    connect_redis,
    flush_stale_keys,
    run_demo,
    setup_logging,
)

# ---------------------------------------------------------------------------
# Threadpool Configuration
# ---------------------------------------------------------------------------


LIMITER_ID = "threadpool_demo"
MAX_WORKERS = 4  # thread pool size


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    setup_logging(os.path.dirname(__file__))

    redis_client = connect_redis()
    flush_stale_keys(redis_client, LIMITER_ID)

    executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
    ThreadPoolRateLimiter.configure(redis_client, executor=executor)
    limiter = ThreadPoolRateLimiter.create(
        limiter_id=LIMITER_ID,
        limit=LIMIT,
        window=WINDOW,
        max_concurrency=MAX_CONCURRENCY,
        override=True,
    )

    def cleanup():
        limiter.shutdown()
        executor.shutdown(wait=True)

    run_demo(limiter=limiter, limiter_id=LIMITER_ID, cleanup=cleanup)


if __name__ == "__main__":
    main()
