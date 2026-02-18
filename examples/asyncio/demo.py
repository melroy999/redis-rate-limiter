"""Self-contained AsyncIOTaskLimiter demonstration.

All operations are performed within a single Python process and a single event
loop: task scheduling, execution via ``asyncio.create_task``, and live dashboard
monitoring.

Usage (Docker Redis on 6380):
    REDIS_HOST=localhost REDIS_PORT=6380 poetry run python -m examples.asyncio.demo

Usage (local Redis on 6379):
    poetry run python -m examples.asyncio.demo
"""

import asyncio
import logging
import os
import random
import time

import redis.asyncio

from celery_rate_limiter import AsyncIOTaskLimiter
from examples.config import (
    ASYNCIO_MAX_TASKS,
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
from examples.tasks import ASYNC_FAILING_FUNC_PATH, ASYNC_FUNC_PATH

logger = logging.getLogger("examples.asyncio_demo")

LIMITER_ID = "asyncio_demo"


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------


def setup_logging() -> None:
    """Initialise log files for the demonstration output and limiter internals."""
    fmt = logging.Formatter(
        "%(asctime)s.%(msecs)03d %(levelname)s %(name)s %(message)s",
        datefmt="%H:%M:%S",
    )

    log_dir = os.path.dirname(__file__)

    demo_handler = logging.FileHandler(
        os.path.join(log_dir, "demo.log"),
        mode="w",
    )
    demo_handler.setLevel(logging.DEBUG)
    demo_handler.setFormatter(fmt)
    logging.getLogger("examples").addHandler(demo_handler)
    logging.getLogger("examples").setLevel(logging.DEBUG)

    limiter_handler = logging.FileHandler(
        os.path.join(log_dir, "limiter_debug.log"),
        mode="w",
    )
    limiter_handler.setLevel(logging.DEBUG)
    limiter_handler.setFormatter(fmt)
    logging.getLogger("celery_rate_limiter").addHandler(limiter_handler)
    logging.getLogger("celery_rate_limiter").setLevel(logging.DEBUG)


async def connect_redis() -> redis.asyncio.Redis:
    """Establish an async connection to Redis."""
    client = redis.asyncio.Redis(
        host=REDIS_HOST, port=REDIS_PORT, decode_responses=True
    )
    await client.ping()
    return client


async def flush_stale_keys(redis_client: redis.asyncio.Redis, limiter_id: str) -> None:
    """Remove Redis keys remaining from any previous demonstration run."""
    async for key in redis_client.scan_iter(f"{limiter_id}:*"):
        await redis_client.delete(key)


# ---------------------------------------------------------------------------
# Demo sequence
# ---------------------------------------------------------------------------


async def run_async_demo(limiter: AsyncIOTaskLimiter) -> None:
    """Execute the standard demonstration sequence using async operations."""
    rng = random.Random(PRIORITY_SEED)

    logger.info(
        "Limiter created: limit=%d/%.1fs, concurrency=%d, max_tasks=%d",
        LIMIT,
        WINDOW,
        MAX_CONCURRENCY,
        ASYNCIO_MAX_TASKS,
    )

    # --- Deduplication demonstration ------------------------------------------
    logger.info("--- Deduplication demo ---")
    logger.info("Scheduling the same task %d times...", DEDUP_COUNT)
    accepted = 0
    for _ in range(DEDUP_COUNT):
        scheduled, _ = await limiter.schedule_task(ASYNC_FUNC_PATH, {"user_id": 1})
        if scheduled:
            accepted += 1
    logger.info("Accepted: %d/%d (duplicates rejected)", accepted, DEDUP_COUNT)

    # --- Build the task list with random priorities --------------------------
    tasks: list[tuple[str, dict, int]] = []

    burst_start = DEDUP_COUNT + 1
    for i in range(burst_start, burst_start + BURST_COUNT):
        priority = rng.randint(1, 1000)
        tasks.append((ASYNC_FUNC_PATH, {"user_id": i, "priority": priority}, priority))

    if ERROR_COUNT > 0:
        error_start = burst_start + BURST_COUNT
        for i in range(error_start, error_start + ERROR_COUNT):
            priority = rng.randint(1, 1000)
            tasks.append(
                (
                    ASYNC_FAILING_FUNC_PATH,
                    {"user_id": i, "priority": priority},
                    priority,
                )
            )

    rng.shuffle(tasks)

    # --- Schedule all tasks ---------------------------------------------------
    total = BURST_COUNT + ERROR_COUNT
    logger.info(
        "Scheduling %d tasks (seed=%d): %d normal + %d failing, interleaved by priority",
        total,
        PRIORITY_SEED,
        BURST_COUNT,
        ERROR_COUNT,
    )
    for func_path, payload, priority in tasks:
        await limiter.schedule_task(func_path, payload, priority=priority)
    logger.info("All %d tasks queued", total)

    await asyncio.sleep(0.5)

    # --- Live monitoring ------------------------------------------------------
    dashboard = Dashboard(limiter_id=LIMITER_ID, limit=LIMIT)
    grace_period = 2.0
    start = time.time()

    try:
        while True:
            status = await limiter.get_status()
            dashboard.render(status)

            elapsed = time.time() - start
            if elapsed > grace_period:
                if (
                    status["buffer"]["count"] == 0
                    and status["concurrency"]["current"] == 0
                ):
                    logger.info("All tasks completed!")
                    break

            await asyncio.sleep(0.05)
    except KeyboardInterrupt:
        logger.info("Interrupted")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


async def main() -> None:
    setup_logging()

    redis_client = await connect_redis()
    await flush_stale_keys(redis_client, LIMITER_ID)

    AsyncIOTaskLimiter.configure(redis_client, max_tasks=ASYNCIO_MAX_TASKS)
    limiter = await AsyncIOTaskLimiter.create(
        limiter_id=LIMITER_ID,
        limit=LIMIT,
        window=WINDOW,
        max_concurrency=MAX_CONCURRENCY,
        override=True,
    )

    try:
        await run_async_demo(limiter)
    finally:
        await limiter.shutdown()
        await redis_client.aclose()

    logger.info("Done.")


if __name__ == "__main__":
    asyncio.run(main())
