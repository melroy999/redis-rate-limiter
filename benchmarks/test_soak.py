"""Sustained soak test for detecting resource leaks and gradual degradation.

Runs a rate limiter under continuous load for a configurable duration
(default 60 seconds) with a background feeder maintaining buffer churn
(ZADD+ZREM) and a monitoring thread sampling process, limiter, pool,
and Redis metrics every second.

Two variants cover both deployment shapes:

- **sync** (``ThreadPoolRateLimiter``): thread-based dispatch via a
  ``ThreadPoolExecutor``. Known to suffer GIL contention at high
  drainer counts; included to detect GIL-specific degradation.
- **async** (``AsyncIOTaskLimiter``): coroutine-based dispatch. No GIL
  contention; gives a clean baseline for comparison.

Post-test analysis fits linear regressions to each time series and
flags statistically significant trends that exceed safety thresholds.
"""

import asyncio
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import redis
import redis.asyncio as aioredis

from benchmarks.conftest import REDIS_HOST, REDIS_PORT
from benchmarks.helpers import GCTracker, bulk_fill_buffer
from benchmarks.soak_analyzer import SoakAnalyzer
from benchmarks.soak_collector import SoakCollector, _DispatchCounter
from redis_rate_limiter import ThreadPoolRateLimiter
from redis_rate_limiter.backends.asyncio.limiter import AsyncIOTaskLimiter

SOAK_DURATION = float(os.getenv("SOAK_DURATION", "60"))
WARMUP_SECONDS = 5.0
BUFFER_WATERMARK = 500
FEEDER_INTERVAL = 0.2
FEEDER_BATCH = 200

FUNC_PATH = "benchmarks.test_soak._noop"
ASYNC_FUNC_PATH = "benchmarks.test_soak._async_noop"

LIMITER_CONFIG = {
    "limit": 500,
    "window": 1.0,
    "max_concurrency": 10_000_000,
    "max_age": 3600,
    "lease_duration": 2,
    "jitter_enabled": False,
}


def _noop(**kwargs):
    pass


async def _async_noop(**kwargs):
    pass


def _feeder_loop(
    feeder_redis,
    limiter,
    stop_event,
    func_path,
):
    seq = 0
    while not stop_event.wait(FEEDER_INTERVAL):
        current = feeder_redis.zcard(limiter.buffer_key)
        if current < BUFFER_WATERMARK:
            deficit = BUFFER_WATERMARK - current + FEEDER_BATCH
            bulk_fill_buffer(
                limiter,
                deficit,
                start_id=seq,
                func_path=func_path,
                redis_client=feeder_redis,
            )
            seq += deficit


async def _async_feeder_loop(
    feeder_redis,
    limiter,
):
    seq = 0
    while True:
        await asyncio.sleep(FEEDER_INTERVAL)
        current = feeder_redis.zcard(limiter.buffer_key)
        if current < BUFFER_WATERMARK:
            deficit = BUFFER_WATERMARK - current + FEEDER_BATCH
            bulk_fill_buffer(
                limiter,
                deficit,
                start_id=seq,
                func_path=ASYNC_FUNC_PATH,
                redis_client=feeder_redis,
            )
            seq += deficit


def test_soak_sync(
    redis_client,
    request,
):
    """Run sustained sync load and assert no resource leaks or degradation."""
    # Arrange
    redis_client.flushdb()
    limiter_id = f"soak_sync_{uuid4().hex[:12]}"

    executor = ThreadPoolExecutor(max_workers=8)
    barrier = threading.Barrier(8)
    futures = [executor.submit(barrier.wait) for _ in range(8)]
    for f in futures:
        f.result(timeout=5.0)

    counter = _DispatchCounter()
    limiter = ThreadPoolRateLimiter(
        redis_client=redis_client,
        executor=executor,
        _sentinel=ThreadPoolRateLimiter._SENTINEL,
        limiter_id=limiter_id,
        metrics_callback=counter,
        **LIMITER_CONFIG,
    )

    feeder_redis = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    monitor_redis = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    bulk_fill_buffer(
        limiter, BUFFER_WATERMARK, func_path=FUNC_PATH, redis_client=feeder_redis
    )

    collector = SoakCollector(
        limiter=limiter,
        monitor_redis=monitor_redis,
        dispatch_counter=counter,
        interval=0.25,
    )

    feeder_stop = threading.Event()
    feeder_thread = threading.Thread(
        target=_feeder_loop,
        args=(feeder_redis, limiter, feeder_stop, FUNC_PATH),
        daemon=True,
    )

    # Act
    feeder_thread.start()
    limiter.trigger_consume()
    time.sleep(WARMUP_SECONDS)

    start_mono = time.monotonic()
    with GCTracker(start_mono) as gc_tracker:
        collector.start()
        time.sleep(SOAK_DURATION)

    feeder_stop.set()
    feeder_thread.join(timeout=5.0)
    snapshots = collector.stop()
    gc_result = gc_tracker.stats()

    limiter.shutdown()
    executor.shutdown(wait=True)
    feeder_redis.close()
    monitor_redis.close()

    # Assert
    analyzer = SoakAnalyzer(snapshots)
    results = analyzer.analyze()

    request.node.user_properties.append(
        ("soak_result", {**analyzer.summary_dict(), "variant": "sync", **gc_result})
    )
    request.node.user_properties.append(
        ("gc_summary_result", {"test": "soak", "scenario": "sync", **gc_result})
    )

    failures = [r for r in results if not r.passed]
    assert not failures, f"{len(failures)} soak metric(s) failed (sync):\n" + "\n".join(
        f"  {r.metric}: {r.failure_reason}" for r in failures
    )


async def test_soak_async(
    redis_client,
    request,
):
    """Run sustained async load and assert no resource leaks or degradation."""
    # Arrange
    redis_client.flushdb()
    limiter_id = f"soak_async_{uuid4().hex[:12]}"

    counter = _DispatchCounter()
    async_client = aioredis.Redis(
        host=REDIS_HOST, port=REDIS_PORT, decode_responses=True
    )
    limiter = AsyncIOTaskLimiter(
        redis_client=async_client,
        max_tasks=100,
        _sentinel=AsyncIOTaskLimiter._SENTINEL,
        limiter_id=limiter_id,
        metrics_callback=counter,
        **LIMITER_CONFIG,
    )
    await limiter.start()

    feeder_redis = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    monitor_redis = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    bulk_fill_buffer(
        limiter,
        BUFFER_WATERMARK,
        func_path=ASYNC_FUNC_PATH,
        redis_client=feeder_redis,
    )

    collector = SoakCollector(
        limiter=limiter,
        monitor_redis=monitor_redis,
        dispatch_counter=counter,
        interval=0.25,
    )

    feeder_task = asyncio.create_task(_async_feeder_loop(feeder_redis, limiter))

    # Act
    await limiter.trigger_consume()
    await asyncio.sleep(WARMUP_SECONDS)

    start_mono = time.monotonic()
    with GCTracker(start_mono) as gc_tracker:
        collector.start()
        await asyncio.sleep(SOAK_DURATION)

    feeder_task.cancel()
    try:
        await feeder_task
    except asyncio.CancelledError:
        pass
    snapshots = collector.stop()
    gc_result = gc_tracker.stats()

    await limiter.shutdown()
    await async_client.aclose()
    feeder_redis.close()
    monitor_redis.close()

    # Assert
    analyzer = SoakAnalyzer(snapshots)
    results = analyzer.analyze()

    request.node.user_properties.append(
        ("soak_result", {**analyzer.summary_dict(), "variant": "async", **gc_result})
    )
    request.node.user_properties.append(
        ("gc_summary_result", {"test": "soak", "scenario": "async", **gc_result})
    )

    failures = [r for r in results if not r.passed]
    assert not failures, (
        f"{len(failures)} soak metric(s) failed (async):\n"
        + "\n".join(f"  {r.metric}: {r.failure_reason}" for r in failures)
    )
