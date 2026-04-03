"""Self-contained RQRateLimiter demonstration.

This script spawns RQ worker subprocess(es), schedules rate-limited tasks,
and monitors progress via the shared dashboard. Like the Celery
demonstration, RQ requires separate worker processes; hence, this script
manages the full worker lifecycle automatically.

Usage (Docker Redis on 6380):
    REDIS_HOST=localhost REDIS_PORT=6380 poetry run python -m examples.rq.demo

Usage (local Redis on 6379):
    poetry run python -m examples.rq.demo
"""

import logging
import os
import subprocess
import sys
import time

import redis

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
from redis_rate_limiter import RQRateLimiter
from rq import Queue

logger = logging.getLogger("examples.rq.demo")


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

LIMITER_ID = "rq_demo"
QUEUE_NAME = "rate_limited"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    setup_logging(os.path.dirname(__file__))

    redis_client = connect_redis()
    flush_stale_keys(redis_client, LIMITER_ID)

    # RQ requires a connection without decode_responses for binary job data.
    rq_conn = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
    rq_queue = Queue(QUEUE_NAME, connection=rq_conn)

    RQRateLimiter.configure(redis_client, queue=rq_queue)

    scheduler = RQRateLimiter.create(
        limiter_id=LIMITER_ID,
        limit=LIMIT,
        window=WINDOW,
        max_concurrency=MAX_CONCURRENCY,
        drain_enabled=False,
        override=True,
    )

    # Start RQ worker subprocesses.
    # Each worker runs examples.rq.worker, which configures the rate limiter
    # in its own process before starting the RQ SimpleWorker loop.
    logger.info("Starting %d RQ worker subprocess(es)...", WORKER_COUNT)
    worker_procs = []
    for _ in range(WORKER_COUNT):
        proc = subprocess.Popen(
            [sys.executable, "-m", "examples.rq.worker"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        worker_procs.append(proc)
    time.sleep(2)  # Allow the workers sufficient time to connect.
    logger.info("Workers ready.")

    def create_consumer():
        return RQRateLimiter.create(
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
