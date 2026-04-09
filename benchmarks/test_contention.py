"""Contention benchmark for the distributed rate limiter dispatch loop.

The existing single-call latency benchmarks (``test_hot_path.py``,
``test_lua_scripts.py``) measure the rate limiter from inside a single
process with no competing drainers, so the contention-aware fairness
mechanism in the dispatch lock (per-worker cooldown, backup-drain
re-arm) is never on the critical path. This file fills that gap by
exercising the same ``ThreadPoolRateLimiter`` the demo uses.

Four scenarios isolate four code paths in the dispatch loop:

- **burst**: large window (10s) with a 2s run. The fleet spends the
  entire run inside one rate-limit window; every drain cycle returns
  success until the full window of tokens is exhausted. Exercises the
  happy path and confirms fairness. Expected total: ``LIMIT`` (1000)
  regardless of N.
- **steady**: small window (1s) with a 10s run that crosses ten window
  boundaries. After the opening burst the fleet spends the run in the
  rate-limited retry branch. Expected aggregate rate: ``LIMIT/WINDOW``
  (500/s) regardless of N.
- **saturated**: small executor (10 slots) plus non-zero task duration
  (0.1s), with ``max_concurrency`` left effectively unbounded so the
  executor saturates first and the local-capacity branch
  (``_has_local_capacity()`` False, re-arm at ``_token_interval``)
  becomes load-bearing. Expected per-drainer rate:
  ``executor_workers / task_duration`` (100/s).
- **concurrency_capped**: executor (12 slots) strictly larger than
  ``max_concurrency`` (10) with the same task duration, so the
  limiter's concurrency cap binds first and the concurrency-limit
  branch is exercised (which does *not* re-arm via ``_token_interval``;
  it relies on ``task_lifecycle`` completion to trigger the next drain).
  Expected per-drainer rate: ``max_concurrency / task_duration``
  (100/s) - identical to ``saturated`` by construction so any
  divergence isolates the bug to one branch.

Three variants cover three deployment shapes:

- **threads**: N threads inside one process driving
  ``ThreadPoolRateLimiter``, sharing one Redis client and one per-drainer
  ``ThreadPoolExecutor``. Per-thread limiter instances bypass the
  class-level instance cache by direct construction with ``_SENTINEL`` so
  each drainer gets its own ``_worker_id``. Known to suffer GIL +
  shared-connection-pool serialisation at high N; kept as a diagnostic
  variant only and not subject to assertions.
- **processes**: N OS processes spawned via ``spawn``, each driving its
  own ``ThreadPoolRateLimiter`` against an independent Redis connection.
  Mirrors the demo deployment shape and exercises the cross-process
  drain-signal Pub/Sub path. This is the canonical regression-detection
  variant.
- **coroutines**: N ``asyncio.Task`` drainers driving
  ``AsyncIOTaskLimiter`` inside one event loop. Each drainer has its own
  ``redis.asyncio.Redis`` client. No threads, no GIL contention; gives a
  clean second data point on the same scenario knobs.

Each test pre-fills the buffer past what the rate limiter can admit in
the run window, then lets the real ``DrainLoop`` of every limiter run
autonomously for the scenario's duration. Successful dispatches are
counted via a ``metrics_callback`` that increments a per-instance
counter on consume-success events. Results are recorded via
``record_property`` and rendered as a summary table by the hook in
``conftest.py``. The processes and coroutines variants additionally
assert that aggregate throughput stays inside per-scenario tolerance
bands derived from the verified theoretical ceilings, so that any
future regression in the dispatch path surfaces as a test failure.
"""

import asyncio
import multiprocessing as mp
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from uuid import uuid4

import pytest
import redis
import redis.asyncio as aioredis

from benchmarks.conftest import REDIS_HOST, REDIS_PORT
from benchmarks.helpers import bulk_fill_buffer
from redis_rate_limiter import ThreadPoolRateLimiter
from redis_rate_limiter.backends.asyncio.limiter import AsyncIOTaskLimiter

DRAINER_COUNTS = [1, 2, 4, 8]
FUNC_PATH = "benchmarks.test_contention._noop"
ASYNC_FUNC_PATH = "benchmarks.test_contention._async_noop"

SCENARIOS: dict[str, dict[str, Any]] = {
    "burst": {
        "limit": 1000,
        "window": 10.0,
        "duration": 2.0,
        "max_concurrency": 10_000_000,
        "task_duration": 0.0,
        "executor_workers": 8,
    },
    "steady": {
        "limit": 500,
        "window": 1.0,
        "duration": 10.0,
        "max_concurrency": 10_000_000,
        "task_duration": 0.0,
        "executor_workers": 8,
    },
    "saturated": {
        "limit": 1000,
        "window": 1.0,
        "duration": 10.0,
        "max_concurrency": 10_000_000,
        "task_duration": 0.1,
        "executor_workers": 10,
    },
    "concurrency_capped": {
        "limit": 1000,
        "window": 1.0,
        "duration": 10.0,
        "max_concurrency": 10,
        "task_duration": 0.1,
        "executor_workers": 12,
    },
}


def _noop(**kwargs: Any) -> None:
    """Trivial dispatch target; optionally sleeps to hold a concurrency slot."""
    sleep_for = kwargs.get("sleep", 0.0)
    if sleep_for:
        time.sleep(sleep_for)


async def _async_noop(**kwargs: Any) -> None:
    """Async dispatch target; optionally awaits to hold a concurrency slot."""
    sleep_for = kwargs.get("sleep", 0.0)
    if sleep_for:
        await asyncio.sleep(sleep_for)


class _DispatchCounter:
    """metrics_callback that counts consume-success events on one limiter instance."""

    def __init__(self) -> None:
        self.count = 0

    def __call__(self, event: str, data: dict) -> None:
        if event == "consume" and data.get("success"):
            self.count += 1


def _make_limiter(
    redis_client: redis.Redis,
    limiter_id: str,
    executor: ThreadPoolExecutor,
    limit: int,
    window: float,
    max_concurrency: int,
) -> tuple[ThreadPoolRateLimiter, _DispatchCounter]:
    counter = _DispatchCounter()
    limiter = ThreadPoolRateLimiter(
        redis_client=redis_client,
        executor=executor,
        _sentinel=ThreadPoolRateLimiter._SENTINEL,
        limiter_id=limiter_id,
        limit=limit,
        window=window,
        max_concurrency=max_concurrency,
        max_age=3600,
        lease_duration=30,
        jitter_enabled=False,
        metrics_callback=counter,
    )
    return limiter, counter


async def _make_async_limiter(
    async_client: aioredis.Redis,
    limiter_id: str,
    max_tasks: int,
    limit: int,
    window: float,
    max_concurrency: int,
) -> tuple[AsyncIOTaskLimiter, _DispatchCounter]:
    counter = _DispatchCounter()
    limiter = AsyncIOTaskLimiter(
        redis_client=async_client,
        max_tasks=max_tasks,
        _sentinel=AsyncIOTaskLimiter._SENTINEL,
        limiter_id=limiter_id,
        limit=limit,
        window=window,
        max_concurrency=max_concurrency,
        max_age=3600,
        lease_duration=30,
        jitter_enabled=False,
        metrics_callback=counter,
    )
    await limiter.start()
    return limiter, counter


def _expected_total(scenario: str, num_drainers: int) -> int:
    """Theoretical dispatch ceiling for a given scenario and fleet size."""
    cfg = SCENARIOS[scenario]
    duration = cfg["duration"]
    if scenario == "burst":
        return cfg["limit"]
    if scenario == "steady":
        return int(cfg["limit"] / cfg["window"] * duration)
    if scenario == "saturated":
        per_drainer = int(cfg["executor_workers"] / cfg["task_duration"] * duration)
        return per_drainer * num_drainers
    if scenario == "concurrency_capped":
        return int(cfg["max_concurrency"] / cfg["task_duration"] * duration)
    raise ValueError(f"unknown scenario: {scenario}")


# burst windows are wall-clock aligned, so a RUN-second run can straddle a
# boundary and admit up to (1 + RUN/WINDOW) × LIMIT. With RUN=2s, WINDOW=10s
# the worst case is 1.20 × LIMIT; 1.25 leaves headroom for jitter.
_TOLERANCES: dict[str, tuple[float, float]] = {
    "burst": (0.90, 1.25),
    "steady": (0.90, 1.15),
    "saturated": (0.90, 1.10),
    "concurrency_capped": (0.90, 1.20),
}


def _required_prefill(scenario: str) -> int:
    """Buffer size that comfortably exceeds any allowed dispatch total."""
    max_n = max(DRAINER_COUNTS)
    upper = _expected_total(scenario, max_n) * _TOLERANCES[scenario][1]
    return max(1500, int(upper * 1.5))


def _assert_throughput(
    variant: str, scenario: str, num_drainers: int, total: int
) -> None:
    if variant == "threads":
        return
    expected = _expected_total(scenario, num_drainers)
    lo_pct, hi_pct = _TOLERANCES[scenario]
    lo, hi = expected * lo_pct, expected * hi_pct
    assert lo <= total <= hi, (
        f"contention {variant}/{scenario} N={num_drainers}: total={total} "
        f"outside [{lo:.0f}, {hi:.0f}] (expected ~{expected})"
    )


def _record(
    request: pytest.FixtureRequest,
    variant: str,
    scenario: str,
    num_drainers: int,
    total: int,
    duration: float,
    per_drainer: list[int],
) -> None:
    request.node.user_properties.append(
        (
            "contention_result",
            {
                "variant": variant,
                "scenario": scenario,
                "num_drainers": num_drainers,
                "total": total,
                "duration": duration,
                "per_drainer": per_drainer,
            },
        )
    )


# ---------------------------------------------------------------------------
# Threaded variant
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("scenario", list(SCENARIOS.keys()))
@pytest.mark.parametrize("num_drainers", DRAINER_COUNTS)
def test_contention_threads(
    redis_client: redis.Redis,
    request: pytest.FixtureRequest,
    scenario: str,
    num_drainers: int,
) -> None:
    """Spawn N threaded drainers competing for one limiter; record throughput."""
    cfg = SCENARIOS[scenario]
    limit = cfg["limit"]
    window = cfg["window"]
    duration = cfg["duration"]
    max_concurrency = cfg["max_concurrency"]
    task_duration = cfg["task_duration"]
    executor_workers = cfg["executor_workers"]
    prefill = _required_prefill(scenario)
    task_payload = {"sleep": task_duration} if task_duration else {}

    redis_client.flushdb()
    limiter_id = f"contention_thr_{scenario}_{uuid4().hex[:8]}"

    # Each drainer gets its own executor so the _has_local_capacity() check
    # reflects per-drainer saturation, exactly as in the demo's per-pod pools.
    seeder_executor = ThreadPoolExecutor(max_workers=2)
    seeder, _ = _make_limiter(
        redis_client, limiter_id, seeder_executor, limit, window, max_concurrency
    )
    try:
        bulk_fill_buffer(seeder, prefill, func_path=FUNC_PATH, payload=task_payload)
    finally:
        seeder.shutdown()
        seeder_executor.shutdown(wait=False)

    barrier = threading.Barrier(num_drainers)
    counters: list[_DispatchCounter | None] = [None] * num_drainers
    drainer_executors: list[ThreadPoolExecutor] = []

    def _drain(index: int) -> None:
        drainer_executor = ThreadPoolExecutor(max_workers=executor_workers)
        drainer_executors.append(drainer_executor)
        limiter, counter = _make_limiter(
            redis_client,
            limiter_id,
            drainer_executor,
            limit,
            window,
            max_concurrency,
        )
        counters[index] = counter
        try:
            barrier.wait()
            deadline = time.monotonic() + duration
            limiter.trigger_consume()
            while time.monotonic() < deadline:
                time.sleep(0.05)
        finally:
            limiter.shutdown()

    threads = [
        threading.Thread(target=_drain, args=(i,)) for i in range(num_drainers)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    for ex in drainer_executors:
        ex.shutdown(wait=False)

    results = [c.count if c is not None else 0 for c in counters]
    _record(request, "threads", scenario, num_drainers, sum(results), duration, results)
    _assert_throughput("threads", scenario, num_drainers, sum(results))


# ---------------------------------------------------------------------------
# Multiprocess variant
# ---------------------------------------------------------------------------


def _process_drainer(
    redis_host: str,
    redis_port: int,
    limiter_id: str,
    limit: int,
    window: float,
    max_concurrency: int,
    executor_workers: int,
    duration: float,
    queue: "mp.Queue[tuple[int, int]]",
    index: int,
    start_at: float,
) -> None:
    client = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
    executor = ThreadPoolExecutor(max_workers=executor_workers)
    limiter, counter = _make_limiter(
        client, limiter_id, executor, limit, window, max_concurrency
    )
    try:
        delay = start_at - time.time()
        if delay > 0:
            time.sleep(delay)
        deadline = time.monotonic() + duration
        limiter.trigger_consume()
        while time.monotonic() < deadline:
            time.sleep(0.05)
        queue.put((index, counter.count))
    finally:
        limiter.shutdown()
        executor.shutdown(wait=False)
        client.close()


@pytest.mark.parametrize("scenario", list(SCENARIOS.keys()))
@pytest.mark.parametrize("num_drainers", DRAINER_COUNTS)
def test_contention_processes(
    redis_client: redis.Redis,
    request: pytest.FixtureRequest,
    scenario: str,
    num_drainers: int,
) -> None:
    """Spawn N OS-process drainers competing for one limiter; record throughput."""
    cfg = SCENARIOS[scenario]
    limit = cfg["limit"]
    window = cfg["window"]
    duration = cfg["duration"]
    max_concurrency = cfg["max_concurrency"]
    task_duration = cfg["task_duration"]
    executor_workers = cfg["executor_workers"]
    prefill = _required_prefill(scenario)
    task_payload = {"sleep": task_duration} if task_duration else {}

    redis_client.flushdb()
    limiter_id = f"contention_proc_{scenario}_{uuid4().hex[:8]}"

    seeder_executor = ThreadPoolExecutor(max_workers=2)
    seeder, _ = _make_limiter(
        redis_client, limiter_id, seeder_executor, limit, window, max_concurrency
    )
    try:
        bulk_fill_buffer(seeder, prefill, func_path=FUNC_PATH, payload=task_payload)
    finally:
        seeder.shutdown()
        seeder_executor.shutdown(wait=False)

    ctx = mp.get_context("spawn")
    queue: "mp.Queue[tuple[int, int]]" = ctx.Queue()

    # grace period for spawn + import + connect.
    start_at = time.time() + 1.5
    procs = [
        ctx.Process(
            target=_process_drainer,
            args=(
                REDIS_HOST,
                REDIS_PORT,
                limiter_id,
                limit,
                window,
                max_concurrency,
                executor_workers,
                duration,
                queue,
                i,
                start_at,
            ),
        )
        for i in range(num_drainers)
    ]
    for p in procs:
        p.start()
    for p in procs:
        p.join()

    results: list[int] = [0] * num_drainers
    while not queue.empty():
        idx, count = queue.get_nowait()
        results[idx] = count

    _record(request, "processes", scenario, num_drainers, sum(results), duration, results)
    _assert_throughput("processes", scenario, num_drainers, sum(results))


# ---------------------------------------------------------------------------
# AsyncIO coroutines variant
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("scenario", list(SCENARIOS.keys()))
@pytest.mark.parametrize("num_drainers", DRAINER_COUNTS)
async def test_contention_coroutines(
    redis_client: redis.Redis,
    request: pytest.FixtureRequest,
    scenario: str,
    num_drainers: int,
) -> None:
    """Spawn N coroutine drainers (single event loop) competing for one limiter."""
    cfg = SCENARIOS[scenario]
    limit = cfg["limit"]
    window = cfg["window"]
    duration = cfg["duration"]
    max_concurrency = cfg["max_concurrency"]
    task_duration = cfg["task_duration"]
    executor_workers = cfg["executor_workers"]
    prefill = _required_prefill(scenario)
    task_payload = {"sleep": task_duration} if task_duration else {}

    redis_client.flushdb()
    limiter_id = f"contention_coro_{scenario}_{uuid4().hex[:8]}"

    # Prefill via the existing sync helper, but using the async _noop func_path
    # so the dispatch path resolves a coroutine.
    seeder_executor = ThreadPoolExecutor(max_workers=2)
    seeder, _ = _make_limiter(
        redis_client, limiter_id, seeder_executor, limit, window, max_concurrency
    )
    try:
        bulk_fill_buffer(seeder, prefill, func_path=ASYNC_FUNC_PATH, payload=task_payload)
    finally:
        seeder.shutdown()
        seeder_executor.shutdown(wait=False)

    async_clients: list[aioredis.Redis] = []
    limiters: list[AsyncIOTaskLimiter] = []
    counters: list[_DispatchCounter] = []
    for _ in range(num_drainers):
        client = aioredis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        async_clients.append(client)
        limiter, counter = await _make_async_limiter(
            client, limiter_id, executor_workers, limit, window, max_concurrency,
        )
        limiters.append(limiter)
        counters.append(counter)

    try:
        for limiter in limiters:
            await limiter.trigger_consume()
        await asyncio.sleep(duration)
    finally:
        for limiter in limiters:
            await limiter.shutdown()
        for client in async_clients:
            await client.aclose()

    results = [c.count for c in counters]
    _record(request, "coroutines", scenario, num_drainers, sum(results), duration, results)
    _assert_throughput("coroutines", scenario, num_drainers, sum(results))
