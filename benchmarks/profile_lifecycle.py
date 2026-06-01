"""Profile task_lifecycle.__enter__ and __exit__ to find Python overhead.

Runs a tight loop of consume + lifecycle and prints the cProfile output
sorted by cumulative time. Helps identify which calls dominate the
Python overhead per consumed task observed in the benchmarks.

Both the sync (``AbstractDistributedRateLimiter``) and async
(``AbstractAsyncDistributedRateLimiter``) paths are profiled so the
asyncio overhead can be compared directly.

Usage:
    poetry run python -m benchmarks.profile_lifecycle
"""

import asyncio
import cProfile
import os
import pstats
from uuid import uuid4

import redis
import redis.asyncio as aioredis

from benchmarks.helpers import bulk_fill_buffer
from redis_rate_limiter import AbstractDistributedRateLimiter
from redis_rate_limiter.core.async_limiters import AbstractAsyncDistributedRateLimiter

ROUNDS = 5000


class _BenchmarkLimiter(AbstractDistributedRateLimiter):
    def _dispatch_task(self, func_path, payload, task_id):
        pass

    def _schedule_drain(self, delay=0.0):
        pass


class _AsyncBenchmarkLimiter(AbstractAsyncDistributedRateLimiter):
    async def _dispatch_task(self, func_path, payload, task_id):
        pass

    def _schedule_drain(self, delay=0.0):
        pass


def _print_profile(profiler, label):
    stats = pstats.Stats(profiler).strip_dirs()
    print(
        f"\n=== [{label}] Top 30 by cumulative time ({ROUNDS} consume+lifecycle calls) ==="
    )
    stats.sort_stats("cumulative").print_stats(30)
    print(
        f"\n=== [{label}] Top 30 by total (self) time ({ROUNDS} consume+lifecycle calls) ==="
    )
    stats.sort_stats("tottime").print_stats(30)


def _profile_sync(host, port):
    client = redis.Redis(host=host, port=port, decode_responses=True)
    client.flushdb()

    limiter = _BenchmarkLimiter(
        redis_client=client,
        limiter_id=f"profile_sync_{uuid4().hex[:12]}",
        limit=10_000_000,
        window=60,
        max_concurrency=10_000_000,
        max_age=3600,
        lease_duration=30,
    )

    bulk_fill_buffer(limiter, ROUNDS + 100)

    for _ in range(10):
        result = limiter.consume()
        if result["success"]:
            with limiter.task_lifecycle(result["task"]["id"]):
                pass

    profiler = cProfile.Profile()
    profiler.enable()
    for _ in range(ROUNDS):
        result = limiter.consume()
        if result["success"]:
            with limiter.task_lifecycle(result["task"]["id"]):
                pass
    profiler.disable()

    _print_profile(profiler, "sync")
    limiter.shutdown()
    client.close()


def _profile_async(host, port):
    loop = asyncio.new_event_loop()
    sync_client = redis.Redis(host=host, port=port, decode_responses=True)
    async_client = aioredis.Redis(host=host, port=port, decode_responses=True)
    sync_client.flushdb()

    limiter = _AsyncBenchmarkLimiter(
        redis_client=async_client,
        limiter_id=f"profile_async_{uuid4().hex[:12]}",
        limit=10_000_000,
        window=60,
        max_concurrency=10_000_000,
        max_age=3600,
        lease_duration=30,
        drain_enabled=False,
    )
    loop.run_until_complete(limiter.start())

    bulk_fill_buffer(limiter, ROUNDS + 100, redis_client=sync_client)

    async def _consume_lifecycle():
        result = await limiter.consume()
        if result["success"]:
            async with limiter.task_lifecycle(result["task"]["id"]):
                pass

    for _ in range(10):
        loop.run_until_complete(_consume_lifecycle())

    profiler = cProfile.Profile()
    profiler.enable()
    for _ in range(ROUNDS):
        loop.run_until_complete(_consume_lifecycle())
    profiler.disable()

    _print_profile(profiler, "async")
    loop.run_until_complete(limiter.shutdown())
    loop.run_until_complete(async_client.aclose())
    sync_client.close()
    loop.close()


def main():
    host = os.getenv("REDIS_HOST", "localhost")
    port = int(os.getenv("REDIS_PORT", "6380"))

    _profile_sync(host, port)
    _profile_async(host, port)


if __name__ == "__main__":
    main()
