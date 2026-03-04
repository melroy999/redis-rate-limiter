"""Self-contained ThreadPoolRateLimiter demonstration.

All operations are performed within a single Python process: task scheduling,
execution via a thread pool, and live dashboard monitoring.

Usage (Docker Redis on 6380):
    REDIS_HOST=localhost REDIS_PORT=6380 poetry run python -m examples.threadpool.demo

Usage (local Redis on 6379):
    poetry run python -m examples.threadpool.demo
"""

import os
from concurrent.futures import ThreadPoolExecutor

from examples.config import LIMIT, MAX_CONCURRENCY, THREADPOOL_MAX_WORKERS, WINDOW
from examples.runner import (
    connect_redis,
    flush_stale_keys,
    run_demo,
    setup_logging,
)
from redis_rate_limiter import ThreadPoolRateLimiter

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

LIMITER_ID = "threadpool_demo"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    setup_logging(os.path.dirname(__file__))

    redis_client = connect_redis()
    flush_stale_keys(redis_client, LIMITER_ID)

    executor = ThreadPoolExecutor(max_workers=THREADPOOL_MAX_WORKERS)
    ThreadPoolRateLimiter.configure(redis_client, executor=executor)

    scheduler = ThreadPoolRateLimiter.create(
        limiter_id=LIMITER_ID,
        limit=LIMIT,
        window=WINDOW,
        max_concurrency=MAX_CONCURRENCY,
        drain_enabled=False,
        override=True,
    )

    def create_consumer():
        return ThreadPoolRateLimiter.create(
            limiter_id=LIMITER_ID,
            limit=LIMIT,
            window=WINDOW,
            max_concurrency=MAX_CONCURRENCY,
            override=True,
            persist=False,
        )

    def cleanup():
        scheduler.shutdown()
        executor.shutdown(wait=True)

    run_demo(
        scheduler=scheduler,
        create_consumer=create_consumer,
        limiter_id=LIMITER_ID,
        cleanup=cleanup,
    )


if __name__ == "__main__":
    main()
