"""Self-contained HueyRateLimiter demonstration.

This script spawns Huey consumer subprocess(es), schedules rate-limited
tasks, and monitors progress via the shared dashboard. Like the Celery,
RQ and Dramatiq demonstrations, Huey requires separate consumer processes;
hence, this script manages the full consumer lifecycle automatically.

Usage (Docker Redis on 6380):
    REDIS_HOST=localhost REDIS_PORT=6380 poetry run python -m examples.huey.demo

Usage (local Redis on 6379):
    poetry run python -m examples.huey.demo
"""

import logging
import os
import subprocess
import sys
import time

from examples.config import (
    LIMIT,
    MAX_CONCURRENCY,
    REDIS_HOST,
    REDIS_PORT,
    WINDOW,
    WORKER_COUNT,
)
from examples.runner import (
    connect_redis,
    flush_stale_keys,
    run_demo,
    setup_logging,
)
from huey import RedisHuey
from redis_rate_limiter import HueyRateLimiter

logger = logging.getLogger("examples.huey.demo")


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

LIMITER_ID = "huey_demo"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    setup_logging(os.path.dirname(__file__))

    redis_client = connect_redis()
    flush_stale_keys(redis_client, LIMITER_ID)

    huey_instance = RedisHuey(
        "rate_limited",
        host=REDIS_HOST,
        port=int(REDIS_PORT),
    )

    HueyRateLimiter.configure(redis_client, huey=huey_instance)

    scheduler = HueyRateLimiter.create(
        limiter_id=LIMITER_ID,
        limit=LIMIT,
        window=WINDOW,
        max_concurrency=MAX_CONCURRENCY,
        drain_enabled=False,
        override=True,
    )

    # Start Huey consumer subprocesses.
    # Each consumer runs the huey_consumer CLI pointing at the Huey
    # instance defined in examples.huey.worker.
    logger.info("Starting %d Huey consumer subprocess(es)...", WORKER_COUNT)
    worker_procs = []
    for _ in range(WORKER_COUNT):
        proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "huey.bin.huey_consumer",
                "examples.huey.worker.huey_instance",
                "--workers",
                "1",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        worker_procs.append(proc)
    time.sleep(2)  # Allow the consumers sufficient time to connect.
    logger.info("Consumers ready.")

    def create_consumer():
        return HueyRateLimiter.create(
            limiter_id=LIMITER_ID,
            limit=LIMIT,
            window=WINDOW,
            max_concurrency=MAX_CONCURRENCY,
            override=True,
            persist=False,
        )

    def cleanup():
        scheduler.shutdown()
        logger.info("Shutting down consumers...")
        for proc in worker_procs:
            proc.terminate()
        for proc in worker_procs:
            proc.wait(timeout=5)

    run_demo(
        scheduler=scheduler,
        create_consumer=create_consumer,
        limiter_id=LIMITER_ID,
        cleanup=cleanup,
    )


if __name__ == "__main__":
    main()
