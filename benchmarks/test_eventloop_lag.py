"""Event-loop scheduling lag benchmark.

Measures how much the asyncio event loop falls behind under varying
limiter workloads. A background sampler coroutine calls
``asyncio.sleep(0.01)`` in a loop and records the overshoot (actual
elapsed minus requested sleep). Higher overshoot means the event loop
is starved by limiter drain cycles, Redis I/O, or task dispatch.

Five scenarios form a progression from zero to maximum pressure:

- **idle**: no drainers, pure event-loop baseline (noise floor).
- **light**: 1 drainer.
- **moderate**: 4 drainers.
- **heavy**: 8 drainers.
- **saturated**: 8 drainers with non-zero task duration and a low
  max_tasks cap, maximizing event-loop occupancy.

All non-idle scenarios share the same rate limit parameters (500/s,
1s window) so the only variables are drainer count and saturation.

Output is latency distributions (median/p95/p99) and a time series of
lag samples, recorded for the HTML report. No pass/fail thresholds are
asserted.
"""

import asyncio
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import numpy as np
import pytest
import redis.asyncio as aioredis

from benchmarks.conftest import REDIS_HOST, REDIS_PORT
from benchmarks.helpers import GCTracker, bulk_fill_buffer
from benchmarks.test_contention import _make_async_limiter, _make_limiter

LAG_SAMPLE_INTERVAL = 0.01
DURATION = 10.0

SCENARIOS = {
    "idle": {
        "num_drainers": 0,
        "task_duration": 0.0,
        "max_tasks": 100,
    },
    "light": {
        "num_drainers": 1,
        "task_duration": 0.0,
        "max_tasks": 100,
    },
    "moderate": {
        "num_drainers": 4,
        "task_duration": 0.0,
        "max_tasks": 100,
    },
    "heavy": {
        "num_drainers": 8,
        "task_duration": 0.0,
        "max_tasks": 100,
    },
    "saturated": {
        "num_drainers": 8,
        "task_duration": 0.1,
        "max_tasks": 10,
    },
}

LIMIT = 500
WINDOW = 1.0
MAX_CONCURRENCY = 10_000_000
FUNC_PATH = "benchmarks.test_eventloop_lag._async_noop"


async def _async_noop(**kwargs):
    sleep_for = kwargs.get("sleep", 0.0)
    if sleep_for:
        await asyncio.sleep(sleep_for)


async def _lag_sampler(duration, interval, start_mono):
    """Sample event-loop scheduling lag for ``duration`` seconds."""
    samples = []
    raw_lags = []
    deadline = start_mono + duration
    while time.monotonic() < deadline:
        t0 = time.monotonic()
        await asyncio.sleep(interval)
        t1 = time.monotonic()
        lag = (t1 - t0) - interval
        samples.append((round(t1 - start_mono, 3), round(lag * 1000, 3)))
        raw_lags.append(lag)
    return {"samples": samples, "raw_lags": raw_lags}


def _lag_stats(raw_lags):
    if not raw_lags:
        return {
            "median_ms": 0.0,
            "mean_ms": 0.0,
            "p95_ms": 0.0,
            "p99_ms": 0.0,
            "max_ms": 0.0,
            "sample_count": 0,
        }
    arr = np.array(raw_lags) * 1000
    return {
        "median_ms": float(np.median(arr)),
        "mean_ms": float(np.mean(arr)),
        "p95_ms": float(np.percentile(arr, 95)),
        "p99_ms": float(np.percentile(arr, 99)),
        "max_ms": float(np.max(arr)),
        "sample_count": len(arr),
    }


async def _run_eventloop_lag_trial(redis_client, scenario):
    """Set up the limiter workload and run the lag sampler alongside it."""
    cfg = SCENARIOS[scenario]
    num_drainers = cfg["num_drainers"]
    max_tasks = cfg["max_tasks"]
    task_duration = cfg["task_duration"]
    task_payload = {"sleep": task_duration} if task_duration else {}

    async_clients = []
    limiters = []
    counters = []

    if num_drainers > 0:
        redis_client.flushdb()
        limiter_id = f"elag_{scenario}_{uuid4().hex[:8]}"

        prefill = max(1500, int(LIMIT / WINDOW * DURATION * 1.5))
        seeder_executor = ThreadPoolExecutor(max_workers=2)
        seeder, _ = _make_limiter(
            redis_client, limiter_id, seeder_executor, LIMIT, WINDOW, MAX_CONCURRENCY
        )
        try:
            bulk_fill_buffer(seeder, prefill, func_path=FUNC_PATH, payload=task_payload)
        finally:
            seeder.shutdown()
            seeder_executor.shutdown(wait=False)

        for _ in range(num_drainers):
            client = aioredis.Redis(
                host=REDIS_HOST, port=REDIS_PORT, decode_responses=True
            )
            async_clients.append(client)
            limiter, counter = await _make_async_limiter(
                client, limiter_id, max_tasks, LIMIT, WINDOW, MAX_CONCURRENCY
            )
            limiters.append(limiter)
            counters.append(counter)

    try:
        for limiter in limiters:
            await limiter.trigger_consume()

        start_mono = time.monotonic()
        with GCTracker(start_mono) as gc_tracker:
            sampler_task = asyncio.create_task(
                _lag_sampler(DURATION, LAG_SAMPLE_INTERVAL, start_mono)
            )
            await asyncio.sleep(DURATION)
            lag_result = await sampler_task
        gc_result = gc_tracker.stats()
    finally:
        for limiter in limiters:
            await limiter.shutdown()
        for client in async_clients:
            await client.aclose()

    per_drainer = [c.count for c in counters]
    total_dispatches = sum(per_drainer)
    return lag_result, total_dispatches, per_drainer, gc_result


@pytest.mark.parametrize("scenario", list(SCENARIOS.keys()))
async def test_eventloop_lag(redis_client, request, scenario):
    """Measure event-loop scheduling lag under varying async limiter workloads."""
    # Arrange
    label = f"eventloop_lag/{scenario}"
    sys.stderr.write(f"\n[{label}] starting\n")
    sys.stderr.flush()

    # Act
    (
        lag_result,
        total_dispatches,
        per_drainer,
        gc_result,
    ) = await _run_eventloop_lag_trial(redis_client, scenario)

    # Assert
    stats = _lag_stats(lag_result["raw_lags"])
    assert stats["sample_count"] >= 10, "too few lag samples collected"

    cfg = SCENARIOS[scenario]
    request.node.user_properties.append(
        (
            "eventloop_lag_result",
            {
                "scenario": scenario,
                "num_drainers": cfg["num_drainers"],
                "duration": DURATION,
                "total_dispatches": total_dispatches,
                "per_drainer": per_drainer,
                "time_series": lag_result["samples"],
                **stats,
                **gc_result,
            },
        )
    )
    request.node.user_properties.append(
        (
            "gc_summary_result",
            {
                "test": "eventloop_lag",
                "scenario": scenario,
                **gc_result,
            },
        )
    )
