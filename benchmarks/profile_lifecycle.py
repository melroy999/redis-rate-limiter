"""Profile task_lifecycle.__enter__ and __exit__ to find Python overhead.

Runs a tight loop of consume + lifecycle and prints the cProfile output
sorted by cumulative time. Helps identify which calls dominate the
~150us of Python overhead per consumed task observed in the benchmarks.

Usage:
    poetry run python -m benchmarks.profile_lifecycle
"""

import cProfile
import os
import pstats
from uuid import uuid4

import redis

from benchmarks.helpers import bulk_fill_buffer
from redis_rate_limiter import AbstractDistributedRateLimiter

ROUNDS = 5000


class _BenchmarkLimiter(AbstractDistributedRateLimiter):
    def _dispatch_task(self, func_path, payload, task_id):
        pass

    def _schedule_drain(self, delay=0.0):
        pass


def main() -> None:
    host = os.getenv("REDIS_HOST", "localhost")
    port = int(os.getenv("REDIS_PORT", "6380"))
    client = redis.Redis(host=host, port=port, decode_responses=True)
    client.flushdb()

    limiter = _BenchmarkLimiter(
        redis_client=client,
        limiter_id=f"profile_{uuid4().hex[:12]}",
        limit=10_000_000,
        window=60,
        max_concurrency=10_000_000,
        max_age=3600,
        lease_duration=30,
    )

    bulk_fill_buffer(limiter, ROUNDS + 100)

    # Warm up so script load and one-off init does not pollute the profile.
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

    stats = pstats.Stats(profiler).strip_dirs()
    print(f"\n=== Top 30 by cumulative time ({ROUNDS} consume+lifecycle calls) ===")
    stats.sort_stats("cumulative").print_stats(30)
    print(f"\n=== Top 30 by total (self) time ({ROUNDS} consume+lifecycle calls) ===")
    stats.sort_stats("tottime").print_stats(30)

    limiter.shutdown()
    client.close()


if __name__ == "__main__":
    main()
