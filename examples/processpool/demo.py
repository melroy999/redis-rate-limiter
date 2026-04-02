"""Self-contained ProcessPoolRateLimiter demonstration.

All operations are performed within a single Python process: task scheduling,
execution via a process pool, and live dashboard monitoring.

Usage (Docker Redis on 6380):
    REDIS_HOST=localhost REDIS_PORT=6380 poetry run python -m examples.processpool.demo

Usage (local Redis on 6379):
    poetry run python -m examples.processpool.demo
"""

import os
from concurrent.futures import ProcessPoolExecutor

from examples.config import LIMIT, MAX_CONCURRENCY, WINDOW, WORKER_COUNT
from examples.runner import (
    connect_redis,
    flush_stale_keys,
    run_demo,
    setup_logging,
)
from redis_rate_limiter import ProcessPoolRateLimiter

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

LIMITER_ID = "processpool_demo"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    setup_logging(os.path.dirname(__file__))

    redis_client = connect_redis()
    flush_stale_keys(redis_client, LIMITER_ID)

    executor = ProcessPoolExecutor(max_workers=WORKER_COUNT)
    ProcessPoolRateLimiter.configure(redis_client, executor=executor)

    scheduler = ProcessPoolRateLimiter.create(
        limiter_id=LIMITER_ID,
        limit=LIMIT,
        window=WINDOW,
        max_concurrency=MAX_CONCURRENCY,
        drain_enabled=False,
        override=True,
    )

    def create_consumer():
        return ProcessPoolRateLimiter.create(
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
