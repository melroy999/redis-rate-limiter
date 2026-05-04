"""Benchmark suite configuration and fixtures.

Provides Redis connection fixtures, limiter factories, and a custom
terminal summary that reports p95/p99 latency percentiles for each
benchmark. Percentiles are also embedded into the JSON output via the
``pytest_benchmark_update_json`` hook.
"""

import asyncio
import os
import time as _time
from typing import Any, Callable, Optional
from uuid import uuid4

import numpy as np
import pytest
import redis
import redis.asyncio as aioredis
from pytest_benchmark.stats import Stats

from redis_rate_limiter import AbstractDistributedRateLimiter
from redis_rate_limiter.core.async_limiters import AbstractAsyncDistributedRateLimiter

# ---------------------------------------------------------------------------
# Redis configuration
# ---------------------------------------------------------------------------

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6380"))


# ---------------------------------------------------------------------------
# Percentile helpers
# ---------------------------------------------------------------------------


def _percentiles_for(stats) -> dict[str, float]:
    """Return p95, p99, and p99.9 for a benchmark's stats object."""
    data = np.array(stats.data)
    return {
        "p95": float(np.percentile(data, 95)),
        "p99": float(np.percentile(data, 99)),
        "p99.9": float(np.percentile(data, 99.9)),
    }


# ---------------------------------------------------------------------------
# pytest-benchmark hooks: embed percentiles in JSON output
# ---------------------------------------------------------------------------


def pytest_benchmark_update_json(config, benchmarks, output_json):
    """Attach p95, p99, and p99.9 to each benchmark entry in the JSON output."""
    for bench in output_json.get("benchmarks", []):
        name = bench.get("name", "")
        matching = [b for b in benchmarks if b.name == name]
        if matching:
            pcts = _percentiles_for(matching[0].stats)
            bench.setdefault("stats", {}).update(pcts)


# ---------------------------------------------------------------------------
# pytest hook: print percentile summary table
# ---------------------------------------------------------------------------


def pytest_terminal_summary(terminalreporter, exitstatus, config):
    """Print percentile, cost-decomposition, and contention summary tables."""
    _print_contention_results(terminalreporter)

    session = getattr(config, "_benchmarksession", None)
    if session is None:
        return

    benchmarks = session.benchmarks
    if not benchmarks:
        return

    _print_percentiles(terminalreporter, benchmarks)
    _print_instrumented_decomposition(terminalreporter, benchmarks)


def _print_contention_results(terminalreporter):
    """Print a summary of contention benchmark results collected via record_property."""
    results = []
    for report in terminalreporter.stats.get("passed", []):
        for key, value in getattr(report, "user_properties", []):
            if key == "contention_result":
                results.append(value)

    if not results:
        return

    terminalreporter.section("contention benchmark")
    header = (
        f"{'Variant':<10} {'Scenario':<10} {'N':>4} "
        f"{'Total':>10} {'Rate/s':>10} {'Per-drainer'}"
    )
    terminalreporter.line(header)
    terminalreporter.line("-" * len(header))

    def _sort_key(r):
        return (r["variant"], r["scenario"], r["num_drainers"])

    for r in sorted(results, key=_sort_key):
        rate = r["total"] / r["duration"] if r["duration"] > 0 else 0.0
        shares = ", ".join(str(c) for c in r["per_drainer"])
        terminalreporter.line(
            f"{r['variant']:<10} {r['scenario']:<10} {r['num_drainers']:>4} "
            f"{r['total']:>10} {rate:>10.1f} [{shares}]"
        )


def _print_instrumented_decomposition(terminalreporter, benchmarks):
    """Print per-iteration cost decomposition from instrumented limiter tests."""
    results = []
    for report in terminalreporter.stats.get("passed", []):
        for key, value in getattr(report, "user_properties", []):
            if key == "decomposition_result":
                results.append(value)

    if not results:
        return

    lua_vm_by_name: dict[str, float] = {}
    for bench in benchmarks:
        if bench.group == "lua-vm":
            lua_vm_by_name[bench.name] = bench.stats.median

    terminalreporter.section("instrumented cost decomposition (per-iteration median)")
    header = f"{'Variant':<8} {'Name':<30} {'Lua VM':>12} {'RTT':>12} {'Py':>12} {'Total':>12}"
    terminalreporter.line(header)
    terminalreporter.line("-" * len(header))

    for r in sorted(results, key=lambda x: (x["variant"], x["test_name"])):
        lua = lua_vm_by_name.get(r["test_name"], 0.0) * 1e6
        rtt = r["rtt_median"] * 1e6
        py = r["py_median"] * 1e6
        total = r["e2e_median"] * 1e6
        lua_str = f"{lua:>10.2f}us" if lua > 0 else f"{'—':>12}"
        terminalreporter.line(
            f"{r['variant']:<8} {r['test_name']:<30} "
            f"{lua_str} {rtt:>10.2f}us {py:>10.2f}us {total:>10.2f}us"
        )


def _print_percentiles(terminalreporter, benchmarks):
    """Print a per-benchmark p95/p99/p99.9 table."""
    terminalreporter.section("benchmark percentiles")
    header = f"{'Name':<60} {'p95':>12} {'p99':>12} {'p99.9':>12}"
    terminalreporter.line(header)
    terminalreporter.line("-" * len(header))

    for bench in benchmarks:
        pcts = _percentiles_for(bench.stats)
        p95_us = pcts["p95"] * 1e6
        p99_us = pcts["p99"] * 1e6
        p999_us = pcts["p99.9"] * 1e6
        terminalreporter.line(
            f"{bench.name:<60} {p95_us:>10.2f}us {p99_us:>10.2f}us {p999_us:>10.2f}us"
        )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def redis_client():
    """Provide a session-scoped Redis client for benchmarks."""
    client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    client.ping()
    yield client
    client.close()


@pytest.fixture
def limiter_id() -> str:
    """Provide a unique limiter identifier for each benchmark."""
    return f"bench_{uuid4().hex[:12]}"


class _BenchmarkLimiter(AbstractDistributedRateLimiter):
    """Minimal concrete limiter for benchmarking the core hot path.

    Dispatch and drain hooks are no-ops so that measurements isolate
    the schedule/consume overhead from any backend-specific work.
    """

    def _dispatch_task(self, func_path: str, payload: dict, task_id: str) -> None:
        pass

    def _schedule_drain(self, delay: float = 0.0) -> None:
        pass


class _InstrumentedLimiter(AbstractDistributedRateLimiter):
    """Limiter that times every Redis call for per-iteration cost decomposition."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._call_timings: list[float] = []

    def _dispatch_task(self, func_path: str, payload: dict, task_id: str) -> None:
        pass

    def _schedule_drain(self, delay: float = 0.0) -> None:
        pass

    def _eval_script(self, script_name: str, num_keys: int, *args: Any) -> Any:
        t0 = _time.perf_counter()
        result = super()._eval_script(script_name, num_keys, *args)
        self._call_timings.append(_time.perf_counter() - t0)
        return result

    def _publish_drain_signal(self) -> None:
        t0 = _time.perf_counter()
        super()._publish_drain_signal()
        self._call_timings.append(_time.perf_counter() - t0)

    def schedule_task(self, func_path, payload, priority=100, max_age=None):
        # Wrap the SET NX call inside schedule_task by timing the whole
        # method and letting _eval_script and _publish_drain_signal record
        # their own sub-timings. The SET NX timing is captured by
        # overriding the redis.set call path.
        original_set = self.redis.set

        def _timed_set(*a: Any, **kw: Any) -> Any:
            t0 = _time.perf_counter()
            result = original_set(*a, **kw)
            self._call_timings.append(_time.perf_counter() - t0)
            return result

        self.redis.set = _timed_set  # type: ignore[assignment]
        try:
            return super().schedule_task(func_path, payload, priority, max_age)
        finally:
            self.redis.set = original_set  # type: ignore[assignment]

    def pop_timings(self) -> list[float]:
        timings = self._call_timings
        self._call_timings = []
        return timings


class _AsyncInstrumentedLimiter(AbstractAsyncDistributedRateLimiter):
    """Async limiter that times every Redis call for per-iteration cost decomposition."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._call_timings: list[float] = []

    async def _dispatch_task(self, func_path: str, payload: dict, task_id: str) -> None:
        pass

    def _schedule_drain(self, delay: float = 0.0) -> None:
        pass

    async def _eval_script(self, script_name: str, num_keys: int, *args: Any) -> Any:
        t0 = _time.perf_counter()
        result = await super()._eval_script(script_name, num_keys, *args)
        self._call_timings.append(_time.perf_counter() - t0)
        return result

    async def _publish_drain_signal(self) -> None:
        t0 = _time.perf_counter()
        await super()._publish_drain_signal()
        self._call_timings.append(_time.perf_counter() - t0)

    async def schedule_task(self, func_path, payload, priority=100, max_age=None):
        original_set = self.redis.set

        async def _timed_set(*a: Any, **kw: Any) -> Any:
            t0 = _time.perf_counter()
            result = await original_set(*a, **kw)
            self._call_timings.append(_time.perf_counter() - t0)
            return result

        self.redis.set = _timed_set  # type: ignore[assignment]
        try:
            return await super().schedule_task(func_path, payload, priority, max_age)
        finally:
            self.redis.set = original_set  # type: ignore[assignment]

    def pop_timings(self) -> list[float]:
        timings = self._call_timings
        self._call_timings = []
        return timings


@pytest.fixture
def instrumented_limiter(redis_client, limiter_id):
    """Instrumented sync limiter that records per-Redis-call timings."""
    redis_client.flushdb()
    instance = _InstrumentedLimiter(
        redis_client=redis_client,
        limiter_id=limiter_id,
        limit=10_000_000,
        window=60,
        max_concurrency=10_000_000,
        max_age=3600,
        lease_duration=30,
    )
    yield instance
    instance.shutdown()


@pytest.fixture
def async_instrumented_limiter(redis_client, limiter_id):
    """Instrumented async limiter that records per-Redis-call timings."""
    redis_client.flushdb()
    loop = asyncio.new_event_loop()
    async_redis = aioredis.Redis(
        host=REDIS_HOST, port=REDIS_PORT, decode_responses=True
    )
    instance = _AsyncInstrumentedLimiter(
        redis_client=async_redis,
        limiter_id=limiter_id,
        limit=10_000_000,
        window=60,
        max_concurrency=10_000_000,
        max_age=3600,
        lease_duration=30,
        drain_enabled=False,
    )
    loop.run_until_complete(instance.start())
    try:
        yield loop, instance
    finally:
        loop.run_until_complete(instance.shutdown())
        loop.run_until_complete(async_redis.aclose())
        loop.close()


@pytest.fixture
def limiter(redis_client, limiter_id):
    """Provide a lightweight limiter instance for benchmarking.

    Uses a large window and a very high limit and concurrency cap so
    that neither rate limiting nor concurrency limiting can throttle
    benchmark throughput, even across hundreds of thousands of rounds.
    """
    # Flush to counter bias caused by leftover keys from prior tests.
    redis_client.flushdb()
    instance = _BenchmarkLimiter(
        redis_client=redis_client,
        limiter_id=limiter_id,
        limit=10_000_000,
        window=60,
        max_concurrency=10_000_000,
        max_age=3600,
        lease_duration=30,
    )
    yield instance
    instance.shutdown()


class _AsyncBenchmarkLimiter(AbstractAsyncDistributedRateLimiter):
    """Async mirror of ``_BenchmarkLimiter`` for the async hot-path tests."""

    async def _dispatch_task(self, func_path: str, payload: dict, task_id: str) -> None:
        pass

    def _schedule_drain(self, delay: float = 0.0) -> None:
        pass


@pytest.fixture
def async_limiter(redis_client, limiter_id):
    """Provide an async limiter plus the event loop driving it.

    Returns ``(loop, limiter)``. Tests pass ``loop.run_until_complete(coro)``
    inside the benchmark callable so the standard ``benchmark(...)`` /
    ``pedantic`` mechanics work unchanged on async code.
    """
    redis_client.flushdb()
    loop = asyncio.new_event_loop()
    async_redis = aioredis.Redis(
        host=REDIS_HOST, port=REDIS_PORT, decode_responses=True
    )
    instance = _AsyncBenchmarkLimiter(
        redis_client=async_redis,
        limiter_id=limiter_id,
        limit=10_000_000,
        window=60,
        max_concurrency=10_000_000,
        max_age=3600,
        lease_duration=30,
        drain_enabled=False,
    )
    loop.run_until_complete(instance.start())
    try:
        yield loop, instance
    finally:
        loop.run_until_complete(instance.shutdown())
        loop.run_until_complete(async_redis.aclose())
        loop.close()


# ---------------------------------------------------------------------------
# SLOWLOG-based benchmarking
# ---------------------------------------------------------------------------

# Number of SLOWLOG entries Redis will retain. Must exceed the number of
# script executions per test, otherwise the oldest entries are dropped.
SLOWLOG_MAX_LEN = 20000


@pytest.fixture
def slowlog_enabled(redis_client):
    """Capture every Redis command in SLOWLOG; restore prior settings after."""
    original_threshold = redis_client.config_get("slowlog-log-slower-than")[
        "slowlog-log-slower-than"
    ]
    original_max_len = redis_client.config_get("slowlog-max-len")["slowlog-max-len"]

    redis_client.config_set("slowlog-log-slower-than", 0)
    redis_client.config_set("slowlog-max-len", SLOWLOG_MAX_LEN)
    redis_client.slowlog_reset()

    yield

    redis_client.config_set("slowlog-log-slower-than", original_threshold)
    redis_client.config_set("slowlog-max-len", original_max_len)
    redis_client.slowlog_reset()


@pytest.fixture
def lua_benchmark(benchmark, redis_client, slowlog_enabled):
    """Measure Lua script execution time via Redis SLOWLOG.

    Substitutes SLOWLOG-reported EVALSHA durations into the standard
    benchmark fixture's stats so that the percentile summary and JSON
    output reflect server-side script time, excluding network RTT.

    If ``setup`` is provided it is called once before each measured
    invocation. Setup may itself execute Lua scripts; pass ``sha_filter``
    with the SHA of the script under test to ignore unrelated EVALSHA
    entries (e.g., schedule.lua executed by setup to refill a buffer).
    """

    def _run(
        callable_to_run: Callable[[], None],
        rounds: int = 2000,
        setup: Optional[Callable[[], None]] = None,
        sha_filter: Optional[str] = None,
    ) -> None:
        # Warm the script cache so the first call does not include a
        # SCRIPT LOAD round trip in the measured set.
        callable_to_run()
        redis_client.slowlog_reset()

        def _drive() -> None:
            for _ in range(rounds):
                if setup is not None:
                    setup()
                callable_to_run()

        benchmark.pedantic(_drive, rounds=1, iterations=1)

        entries = redis_client.slowlog_get(SLOWLOG_MAX_LEN)
        if sha_filter is not None:
            sha_bytes = sha_filter.encode()
            evalsha_durations_us = [
                entry["duration"]
                for entry in entries
                if entry["command"].startswith(b"EVALSHA")
                and sha_bytes in entry["command"]
            ]
        else:
            evalsha_durations_us = [
                entry["duration"]
                for entry in entries
                if entry["command"].startswith(b"EVALSHA")
            ]

        if not evalsha_durations_us:
            raise RuntimeError("no EVALSHA entries captured in SLOWLOG")

        # Replace the inner Stats with one populated from SLOWLOG durations
        # (microseconds), converted to seconds to match pytest-benchmark.
        new_stats = Stats()
        for duration_us in evalsha_durations_us:
            new_stats.update(duration_us / 1e6)
        benchmark.stats.stats = new_stats

    return _run


@pytest.fixture
def wall_benchmark(benchmark):
    """Measure wall-clock time of a callable (RTT + Lua VM execution).

    Mirrors the lua_benchmark interface so that the same test body can
    be used to compare wall-clock and SLOWLOG measurements. Uses
    ``pedantic`` with one iteration per round so that each measurement
    captures a single script invocation. The optional ``setup`` callback
    is invoked between rounds and is excluded from the measurement.
    """

    def _run(
        callable_to_run: Callable[[], None],
        rounds: int = 2000,
        setup: Optional[Callable[[], None]] = None,
    ) -> None:
        # Warm the script cache so the first call does not include a
        # SCRIPT LOAD round trip in the measured set.
        callable_to_run()
        benchmark.pedantic(callable_to_run, setup=setup, rounds=rounds, iterations=1)

    return _run
