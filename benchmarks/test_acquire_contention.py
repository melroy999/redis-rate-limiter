"""Latency scaling under contention benchmarks.

Measures how per-call latency scales with the number of concurrent
callers. Three tests cover distinct code paths:

- **acquire_lua**: N threads calling acquire.lua (ASGI admission path)
  concurrently. Isolates Redis EVALSHA queuing overhead.
- **limiter_acquire**: N caller threads competing for limiter.acquire()
  on a single limiter under rate-limited and concurrency-capped
  scenarios. Measures the full cycle: schedule marker, drain loop wake,
  consume.lua admission, BLPOP signal, and release.
- **release_lua**: N threads calling release.lua concurrently from
  pre-seeded concurrency slots.

Output is latency distributions (median/p95/p99) and throughput at each
N, recorded for cross-run comparison in the HTML report. No pass/fail
latency thresholds are asserted.
"""

import threading
import time
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import numpy as np
import pytest
import redis

from benchmarks.conftest import REDIS_HOST, REDIS_PORT
from redis_rate_limiter import ThreadPoolRateLimiter
from redis_rate_limiter.core.base import AcquireTimeout

DURATION = 5.0
CALLER_COUNTS = [1, 2, 4, 8]

SCENARIOS = {
    "uncapped": {
        "limit": 10_000,
        "window": 1.0,
        "max_concurrency": 10_000_000,
        "hold_time": 0.0,
        "caller_counts": CALLER_COUNTS,
    },
    "concurrency_capped": {
        "limit": 10_000_000,
        "window": 1.0,
        "max_concurrency": 3,
        "hold_time": 0.02,
        "caller_counts": [4, 8, 12, 16],
    },
}


def _latency_stats(latencies):
    if not latencies:
        return {"median_us": 0.0, "mean_us": 0.0, "p95_us": 0.0, "p99_us": 0.0}
    arr = np.array(latencies) * 1e6
    return {
        "median_us": float(np.median(arr)),
        "mean_us": float(np.mean(arr)),
        "p95_us": float(np.percentile(arr, 95)),
        "p99_us": float(np.percentile(arr, 99)),
    }


# ---------------------------------------------------------------------------
# Test 1: acquire.lua latency scaling
# ---------------------------------------------------------------------------


def _acquire_lua_worker(
    host,
    port,
    key,
    barrier,
    duration,
    results,
    index,
):
    client = redis.Redis(host=host, port=port, decode_responses=True)
    try:
        sha_cache = {}

        def _eval_acquire():
            script_name = "acquire.lua"
            if script_name not in sha_cache:
                from redis_rate_limiter.core.scripts import load_lua_script

                script = load_lua_script(script_name)
                sha_cache[script_name] = client.script_load(script)
            return client.evalsha(sha_cache[script_name], 1, key, 60, 1_000_000)

        _eval_acquire()
        barrier.wait()
        deadline = time.monotonic() + duration
        latencies = []
        admitted = 0
        while time.monotonic() < deadline:
            t0 = time.monotonic()
            result = _eval_acquire()
            latencies.append(time.monotonic() - t0)
            if result[0] == 1:
                admitted += 1

        results[index] = (len(latencies), admitted, latencies)
    finally:
        client.close()


@pytest.mark.parametrize("num_callers", CALLER_COUNTS)
def test_acquire_lua_latency_scaling(
    redis_client,
    request,
    num_callers,
):
    """Measure acquire.lua per-call latency under N concurrent callers."""
    # Arrange
    redis_client.flushdb()
    key = f"bench_acq_lua_{uuid4().hex[:12]}"
    barrier = threading.Barrier(num_callers)
    results = [None] * num_callers

    # Act
    threads = [
        threading.Thread(
            target=_acquire_lua_worker,
            args=(REDIS_HOST, REDIS_PORT, key, barrier, DURATION, results, i),
        )
        for i in range(num_callers)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=DURATION + 10)

    # Assert
    all_latencies = []
    total_calls = 0
    total_admitted = 0
    per_caller_calls = []
    for entry in results:
        if entry is None:
            continue
        count, admitted, latencies = entry
        total_calls += count
        total_admitted += admitted
        per_caller_calls.append(count)
        all_latencies.extend(latencies)

    assert total_calls >= 10, "too few calls completed"

    pcts = _latency_stats(all_latencies)
    throughput = total_calls / DURATION

    request.node.user_properties.append(
        (
            "acquire_contention_result",
            {
                "test": "acquire_lua",
                "num_callers": num_callers,
                "total_calls": total_calls,
                "total_admitted": total_admitted,
                "throughput": round(throughput, 1),
                "per_caller": per_caller_calls,
                **pcts,
            },
        )
    )


# ---------------------------------------------------------------------------
# Test 2: Buffered acquire() cycle latency scaling
# ---------------------------------------------------------------------------


def _acquire_slot_worker(
    limiter,
    hold_time,
    barrier,
    duration,
    results,
    index,
):
    latencies = []
    count = 0
    barrier.wait()
    deadline = time.monotonic() + duration
    while time.monotonic() < deadline:
        t0 = time.monotonic()
        try:
            with limiter.acquire(timeout=10.0):
                latencies.append(time.monotonic() - t0)
                count += 1
                if hold_time > 0:
                    time.sleep(hold_time)
        except AcquireTimeout:
            break
    results[index] = (count, latencies)


_SLOT_PARAMS = list(
    (s, n) for s, cfg in SCENARIOS.items() for n in cfg["caller_counts"]
)


@pytest.mark.parametrize(("scenario", "num_callers"), _SLOT_PARAMS)
def test_acquire_slot_latency_scaling(
    redis_client,
    request,
    scenario,
    num_callers,
):
    """Measure full acquire() cycle latency under N competing callers."""
    # Arrange
    redis_client.flushdb()
    cfg = SCENARIOS[scenario]
    limiter_id = f"bench_acq_slot_{uuid4().hex[:12]}"

    executor = ThreadPoolExecutor(max_workers=8)
    warmup_barrier = threading.Barrier(8)
    futures = [executor.submit(warmup_barrier.wait) for _ in range(8)]
    for f in futures:
        f.result(timeout=5.0)

    limiter = ThreadPoolRateLimiter(
        redis_client=redis_client,
        executor=executor,
        _sentinel=ThreadPoolRateLimiter._SENTINEL,
        limiter_id=limiter_id,
        limit=cfg["limit"],
        window=cfg["window"],
        max_concurrency=cfg["max_concurrency"],
        max_age=3600,
        lease_duration=30,
        jitter_enabled=False,
    )

    barrier = threading.Barrier(num_callers)
    results = [None] * num_callers

    # Act
    threads = [
        threading.Thread(
            target=_acquire_slot_worker,
            args=(limiter, cfg["hold_time"], barrier, DURATION, results, i),
        )
        for i in range(num_callers)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=DURATION + 30)

    limiter.shutdown()
    executor.shutdown(wait=True)

    # Assert
    all_latencies = []
    total = 0
    per_caller = []
    for entry in results:
        if entry is None:
            continue
        count, latencies = entry
        total += count
        per_caller.append(count)
        all_latencies.extend(latencies)

    assert total >= 10, "too few acquisitions completed"

    pcts = _latency_stats(all_latencies)
    throughput = total / DURATION

    request.node.user_properties.append(
        (
            "acquire_contention_result",
            {
                "test": "limiter_acquire",
                "scenario": scenario,
                "num_callers": num_callers,
                "total": total,
                "throughput": round(throughput, 1),
                "per_caller": per_caller,
                **pcts,
            },
        )
    )


# ---------------------------------------------------------------------------
# Test 3: release.lua latency scaling
# ---------------------------------------------------------------------------


def _release_worker(
    host,
    port,
    concurrency_key,
    drain_channel,
    task_ids,
    barrier,
    results,
    index,
):
    client = redis.Redis(host=host, port=port, decode_responses=True)
    try:
        sha_cache = {}

        def _eval_release(task_id):
            script_name = "release.lua"
            if script_name not in sha_cache:
                from redis_rate_limiter.core.scripts import load_lua_script

                script = load_lua_script(script_name)
                sha_cache[script_name] = client.script_load(script)
            return client.evalsha(
                sha_cache[script_name],
                3,
                concurrency_key,
                "",
                drain_channel,
                task_id,
                f"worker_{index}",
            )

        _eval_release("__warmup__")
        barrier.wait()
        latencies = []
        for task_id in task_ids:
            t0 = time.monotonic()
            _eval_release(task_id)
            latencies.append(time.monotonic() - t0)

        results[index] = latencies
    finally:
        client.close()


RELEASE_ROUNDS = 2000


@pytest.mark.parametrize("num_callers", CALLER_COUNTS)
def test_release_latency_scaling(
    redis_client,
    request,
    num_callers,
):
    """Measure release.lua per-call latency under N concurrent releases."""
    # Arrange
    redis_client.flushdb()
    limiter_id = f"bench_rel_{uuid4().hex[:12]}"
    concurrency_key = f"{limiter_id}:concurrency"
    drain_channel = f"{limiter_id}:drain_signal"

    task_ids_per_worker = [
        [f"t_{i}_{r}" for r in range(RELEASE_ROUNDS)] for i in range(num_callers)
    ]

    for worker_ids in task_ids_per_worker:
        mapping = {tid: 9999999999 for tid in worker_ids}
        redis_client.zadd(concurrency_key, mapping)

    barrier = threading.Barrier(num_callers)
    results = [None] * num_callers

    # Act
    threads = [
        threading.Thread(
            target=_release_worker,
            args=(
                REDIS_HOST,
                REDIS_PORT,
                concurrency_key,
                drain_channel,
                task_ids_per_worker[i],
                barrier,
                results,
                i,
            ),
        )
        for i in range(num_callers)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)

    # Assert
    all_latencies = []
    for latencies in results:
        if latencies is None:
            continue
        all_latencies.extend(latencies)

    assert len(all_latencies) >= 10, "too few releases completed"

    pcts = _latency_stats(all_latencies)

    request.node.user_properties.append(
        (
            "acquire_contention_result",
            {
                "test": "release_lua",
                "num_callers": num_callers,
                "total_calls": len(all_latencies),
                **pcts,
            },
        )
    )
