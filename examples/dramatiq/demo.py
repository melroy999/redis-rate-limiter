"""Self-contained DramatiqRateLimiter demonstration.

This script spawns Dramatiq worker subprocess(es), schedules rate-limited
tasks, and monitors progress via the shared dashboard. Like the Celery
and RQ demonstrations, Dramatiq requires separate worker processes; hence,
this script manages the full worker lifecycle automatically.

Usage (Docker Redis on 6380):
    REDIS_HOST=localhost REDIS_PORT=6380 poetry run python -m examples.dramatiq.demo

Usage (local Redis on 6379):
    poetry run python -m examples.dramatiq.demo
"""

import logging
import os
import subprocess
import sys
import time

import dramatiq
from dramatiq.brokers.redis import RedisBroker
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
from redis_rate_limiter import DramatiqRateLimiter

logger = logging.getLogger("examples.dramatiq.demo")


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

LIMITER_ID = "dramatiq_demo"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    setup_logging(os.path.dirname(__file__))

    redis_client = connect_redis()
    flush_stale_keys(redis_client, LIMITER_ID)

    broker = RedisBroker(host=REDIS_HOST, port=int(REDIS_PORT))
    dramatiq.set_broker(broker)

    DramatiqRateLimiter.configure(redis_client, broker=broker)

    scheduler = DramatiqRateLimiter.create(
        limiter_id=LIMITER_ID,
        limit=LIMIT,
        window=WINDOW,
        max_concurrency=MAX_CONCURRENCY,
        drain_enabled=False,
        override=True,
    )

    # Start a single Dramatiq CLI invocation with multiple worker processes.
    # Each process configures the rate limiter independently because
    # examples.dramatiq.worker runs module-level setup on import.
    logger.info("Starting Dramatiq with %d worker process(es)...", WORKER_COUNT)
    worker_proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "dramatiq",
            "examples.dramatiq.worker",
            "--processes",
            str(WORKER_COUNT),
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    time.sleep(2)  # Allow the workers sufficient time to connect.
    logger.info("Workers ready.")

    def create_consumer():
        return DramatiqRateLimiter.create(
            limiter_id=LIMITER_ID,
            limit=LIMIT,
            window=WINDOW,
            max_concurrency=MAX_CONCURRENCY,
            override=True,
            persist=False,
        )

    def cleanup():
        scheduler.shutdown()
        logger.info("Shutting down workers...")
        worker_proc.terminate()
        worker_proc.wait(timeout=5)

    run_demo(
        scheduler=scheduler,
        create_consumer=create_consumer,
        limiter_id=LIMITER_ID,
        cleanup=cleanup,
    )


if __name__ == "__main__":
    main()
