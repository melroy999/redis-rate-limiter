"""Shared demo runner for rate limiter examples.

Provides the common demo sequence (dedup, burst, live monitoring) so each
backend demo only needs to handle its own setup and teardown.
"""

import logging
import os
import random
import time
from typing import Callable

import redis

from examples.config import (
    BURST_COUNT,
    DEDUP_COUNT,
    ERROR_COUNT,
    LIMIT,
    MAX_CONCURRENCY,
    PRIORITY_SEED,
    REDIS_HOST,
    REDIS_PORT,
    WINDOW,
)
from examples.dashboard import Dashboard
from examples.tasks import FAILING_FUNC_PATH, FUNC_PATH

logger = logging.getLogger("examples.runner")


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------


def connect_redis() -> redis.Redis:
    """Connect to Redis and fail fast if unreachable."""
    client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    client.ping()
    return client


def setup_logging(log_dir: str) -> None:
    """Set up two log files: one for the demo, one for the rate limiter."""
    fmt = logging.Formatter(
        "%(asctime)s.%(msecs)03d %(levelname)s %(name)s %(message)s",
        datefmt="%H:%M:%S",
    )

    # Demo log: captures output from all examples.* loggers.
    demo_handler = logging.FileHandler(
        os.path.join(log_dir, "demo.log"), mode="w",
    )
    demo_handler.setLevel(logging.DEBUG)
    demo_handler.setFormatter(fmt)
    logging.getLogger("examples").addHandler(demo_handler)
    logging.getLogger("examples").setLevel(logging.DEBUG)

    # Rate-limiter internals log: captures the library's debug output.
    limiter_handler = logging.FileHandler(
        os.path.join(log_dir, "limiter_debug.log"), mode="w",
    )
    limiter_handler.setLevel(logging.DEBUG)
    limiter_handler.setFormatter(fmt)
    logging.getLogger("celery_rate_limiter").addHandler(limiter_handler)
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
    rng = random.Random(PRIORITY_SEED)

    logger.info(
        "Limiter created: limit=%d/%.1fs, concurrency=%d",
        LIMIT, WINDOW, MAX_CONCURRENCY,
    )

    # --- Deduplication demo ---------------------------------------------------
    logger.info("--- Deduplication demo ---")
    logger.info("Scheduling the same task %d times...", DEDUP_COUNT)
    accepted = sum(
        limiter.schedule_task(FUNC_PATH, {"user_id": 1})[0]
        for _ in range(DEDUP_COUNT)
    )
    logger.info("Accepted: %d/%d (duplicates rejected)", accepted, DEDUP_COUNT)

    # --- Build task list with random priorities -------------------------------
    tasks: list[tuple[str, dict, int]] = []

    # Normal burst tasks.
    burst_start = DEDUP_COUNT + 1
    for i in range(burst_start, burst_start + BURST_COUNT):
        priority = rng.randint(1, 1000)
        tasks.append((FUNC_PATH, {"user_id": i, "priority": priority}, priority))

    # Failing tasks (interleaved via priority).
    if ERROR_COUNT > 0:
        error_start = burst_start + BURST_COUNT
        for i in range(error_start, error_start + ERROR_COUNT):
            priority = rng.randint(1, 1000)
            tasks.append(
                (FAILING_FUNC_PATH, {"user_id": i, "priority": priority}, priority)
            )

    # Shuffle so the scheduling order itself is also mixed.
    rng.shuffle(tasks)

    # --- Schedule all tasks ---------------------------------------------------
    total = BURST_COUNT + ERROR_COUNT
    logger.info(
        "Scheduling %d tasks (seed=%d): %d normal + %d failing, interleaved by priority",
        total, PRIORITY_SEED, BURST_COUNT, ERROR_COUNT,
    )
    for func_path, payload, priority in tasks:
        limiter.schedule_task(func_path, payload, priority=priority)
    logger.info("All %d tasks queued", total)

    # The drain loop starts automatically when the first task is scheduled
    # (via trigger_consume -> DrainLoop.wake).
    logger.info("Drain loop started automatically via task scheduling")
    time.sleep(0.5)  # Brief pause so the first drain cycle can fire.

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
                    logger.info("All tasks completed!")
                    break

            time.sleep(0.05)
    except KeyboardInterrupt:
        logger.info("Interrupted")

    # --- Cleanup --------------------------------------------------------------
    cleanup()
    elapsed = time.time() - start
    logger.info("Done. Total elapsed: %.1fs", elapsed)
