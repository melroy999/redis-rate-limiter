"""Shared demo runner for rate limiter examples.

Provides the common demo sequence (dedup, burst, live monitoring) so each
backend demo only needs to handle its own setup and teardown.
"""

import logging
import os
import time
from typing import Callable

import redis

from examples.dashboard import Dashboard
from examples.tasks import FUNC_PATH

# ---------------------------------------------------------------------------
# Shared configuration
# ---------------------------------------------------------------------------

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

LIMIT = 10  # tokens per window
WINDOW = 5.0  # seconds
MAX_CONCURRENCY = 3  # concurrent tasks executing at once


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------


def connect_redis() -> redis.Redis:
    """Connect to Redis and fail fast if unreachable."""
    client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    client.ping()
    return client


def setup_logging(log_dir: str) -> None:
    """Write debug-level rate limiter logs to ``demo_debug.log``."""
    log_file = os.path.join(log_dir, "demo_debug.log")
    handler = logging.FileHandler(log_file, mode="w")
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(
        logging.Formatter(
            "%(asctime)s.%(msecs)03d %(levelname)s %(name)s %(message)s",
            datefmt="%H:%M:%S",
        )
    )
    logging.getLogger("celery_rate_limiter").addHandler(handler)
    logging.getLogger("celery_rate_limiter").setLevel(logging.DEBUG)


def flush_stale_keys(redis_client: redis.Redis, limiter_id: str) -> None:
    """Remove keys from any previous demo run.

    Without this, leftover window keys inflate the per-window display.
    """
    for key in redis_client.scan_iter(f"{limiter_id}:*"):
        redis_client.delete(key)


# ---------------------------------------------------------------------------
# Demo sequence
# ---------------------------------------------------------------------------


def run_demo(
    *,
    limiter,
    limiter_id: str,
    cleanup: Callable[[], None],
) -> None:
    """Run the standard demo sequence: dedup, burst, monitor, cleanup.

    Args:
        limiter: A configured rate limiter instance.
        limiter_id: Display identifier for the dashboard header.
        cleanup: Called after monitoring finishes (e.g. shutdown workers).
    """
    print(f"Limiter created: limit={LIMIT}/{WINDOW}s, concurrency={MAX_CONCURRENCY}")

    # --- Deduplication demo ---------------------------------------------------
    print("\n--- Deduplication demo ---")
    print("Scheduling the same task 10 times...")
    accepted = sum(
        limiter.schedule_task(FUNC_PATH, {"user_id": 1})[0] for _ in range(10)
    )
    print(f"  Accepted: {accepted}/10 (duplicates rejected)\n")

    # --- Burst demo -----------------------------------------------------------
    print("--- Burst demo ---")
    print("Scheduling 100 unique tasks...")
    for i in range(2, 102):
        limiter.schedule_task(FUNC_PATH, {"user_id": i})
    print("  All 100 queued.\n")

    # The drain loop starts automatically when the first task is scheduled
    # (via trigger_consume -> DrainLoop.wake).
    print("Drain loop started automatically via task scheduling.\n")
    time.sleep(0.5)  # Brief pause so the setup output is readable.

    # --- Live monitoring ------------------------------------------------------
    dashboard = Dashboard(limiter_id=limiter_id, limit=LIMIT)

    # Don't check the exit condition until the drain loop has had time to start
    # consuming.  Without this grace period the monitor may see a momentary
    # buffer=0 / concurrency=0 snapshot before the first drain cycle fires.
    grace_period = 2.0
    start = time.time()

    try:
        while True:
            status = limiter.get_status()
            dashboard.render(status)

            elapsed = time.time() - start
            if elapsed > grace_period:
                if (
                    status["buffer"]["count"] == 0
                    and status["concurrency"]["current"] == 0
                ):
                    print("All tasks completed!")
                    break

            time.sleep(0.05)
    except KeyboardInterrupt:
        print("\nInterrupted.")

    # --- Cleanup --------------------------------------------------------------
    cleanup()
    elapsed = time.time() - start
    print(f"\nDone. Total elapsed: {elapsed:.1f}s")
