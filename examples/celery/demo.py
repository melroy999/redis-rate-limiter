"""Self-contained CeleryRateLimiter demo.

Spawns a Celery worker subprocess, schedules rate-limited tasks, and
monitors progress via the shared dashboard.  Unlike the threadpool demo,
Celery requires a separate worker process--this script manages its full
lifecycle automatically.

Usage (Docker Redis on 6380):
    REDIS_HOST=localhost REDIS_PORT=6380 poetry run python -m examples.celery.demo

Usage (local Redis on 6379):
    poetry run python -m examples.celery.demo
"""

import logging
import os
import subprocess
import sys
import time

import redis
from celery import Celery
from celery.signals import worker_init

from celery_rate_limiter import CeleryRateLimiter
from examples.config import (
    CELERY_WORKER_CONCURRENCY,
    CELERY_WORKER_PREFETCH_MULTIPLIER,
    LIMIT,
    MAX_CONCURRENCY,
    REDIS_HOST,
    REDIS_PORT,
    WINDOW,
)
from examples.runner import (
    connect_redis,
    flush_stale_keys,
    run_demo,
    setup_logging,
)

logger = logging.getLogger("examples.celery.demo")

LIMITER_ID = "celery_demo"
REDIS_URL = f"redis://{REDIS_HOST}:{REDIS_PORT}/0"

# ---------------------------------------------------------------------------
# Celery app (imported by the worker subprocess via -A flag)
# ---------------------------------------------------------------------------

celery_app = Celery("celery_demo", broker=REDIS_URL)
celery_app.conf.worker_prefetch_multiplier = CELERY_WORKER_PREFETCH_MULTIPLIER
celery_app.conf.imports = [
    "celery_rate_limiter.backends.celery.tasks.worker",
]


# ---------------------------------------------------------------------------
# Worker process initialization
# ---------------------------------------------------------------------------


@worker_init.connect
def _init_worker(**_kwargs):
    """Configure the rate limiter inside the worker subprocess.

    The ``@rate_limited`` decorator resolves the limiter via ``.get()``, so
    both ``configure()`` and ``create()`` must run in every process.
    """
    client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    CeleryRateLimiter.configure(client, celery_app=celery_app)
    CeleryRateLimiter.create(
        limiter_id=LIMITER_ID,
        limit=LIMIT,
        window=WINDOW,
        max_concurrency=MAX_CONCURRENCY,
        override=True,
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    setup_logging(os.path.dirname(__file__))

    redis_client = connect_redis()
    flush_stale_keys(redis_client, LIMITER_ID)

    CeleryRateLimiter.configure(redis_client, celery_app=celery_app)
    limiter = CeleryRateLimiter.create(
        limiter_id=LIMITER_ID,
        limit=LIMIT,
        window=WINDOW,
        max_concurrency=MAX_CONCURRENCY,
        override=True,
    )

    # Start an embedded Celery worker as a subprocess.
    logger.info("Starting Celery worker subprocess...")
    worker_proc = subprocess.Popen(
        [
            sys.executable, "-m", "celery",
            "-A", "examples.celery.demo:celery_app",
            "worker",
            f"--concurrency={CELERY_WORKER_CONCURRENCY}",
            "--loglevel=warning",
            "--without-heartbeat",
            "--without-mingle",
            "--without-gossip",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    time.sleep(3)  # Give the worker time to connect to the broker.
    logger.info("Worker ready.")

    def cleanup():
        logger.info("Shutting down worker...")
        worker_proc.terminate()
        worker_proc.wait(timeout=5)
        limiter.shutdown()

    run_demo(limiter=limiter, limiter_id=LIMITER_ID, cleanup=cleanup)


if __name__ == "__main__":
    main()
