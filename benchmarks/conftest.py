"""Benchmark suite configuration and fixtures.

Provides Redis connection fixtures, limiter factories, and a custom
terminal summary that reports p95/p99 latency percentiles for each
benchmark. Percentiles are also embedded into the JSON output via the
``pytest_benchmark_update_json`` hook.
"""

import os
from typing import Callable, Optional
from uuid import uuid4

import numpy as np
import pytest
import redis
from pytest_benchmark.stats import Stats

from redis_rate_limiter import AbstractDistributedRateLimiter


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
    """Print percentile and cost-decomposition summary tables."""
    session = getattr(config, "_benchmarksession", None)
    if session is None:
        return

    benchmarks = session.benchmarks
    if not benchmarks:
        return

    _print_percentiles(terminalreporter, benchmarks)
    _print_cost_decomposition(terminalreporter, benchmarks)


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


# Number of Redis round trips an end-to-end test performs. The cost
# decomposition multiplies the per-call RTT by this count so the
# Python overhead column reflects only Python work, not unaccounted
# RTTs from secondary Redis calls. Tests not listed default to 1.
#
# consume + lifecycle now does 2 calls: consume.lua + release.lua
# (release.lua atomically performs ZREM + DEL + PUBLISH on the drain
# signal, so the publish no longer adds a separate round trip).
# schedule_task does 3 calls: SET NX inflight + schedule.lua +
# drain-signal PUBLISH.
REDIS_CALLS_BY_TEST = {
    "test_consume_empty_buffer": 1,
    "test_consume_with_tasks": 2,
    "test_consume_vs_buffer_depth": 2,  # all parametrizations
    "test_schedule_task": 3,
    "test_schedule_task_with_priority": 3,  # all parametrizations
    "test_schedule_and_consume": 5,  # schedule_task (3) + consume + release (2)
}


def _redis_calls_for(test_name: str) -> int:
    """Return the expected Redis call count for the given test."""
    base = test_name.split("[", 1)[0]
    return REDIS_CALLS_BY_TEST.get(base, 1)


def _print_cost_decomposition(terminalreporter, benchmarks):
    """Print a per-test breakdown of Lua VM, RTT, and Python overhead.

    For every test name that has a measurement in all three groups
    (lua-vm, evalsha-wallclock, end-to-end) the median latency is split
    into:

        Lua VM    = primary script's lua-vm median (single script)
        RTT (Nx)  = N * (evalsha-wallclock - lua-vm), where N is the
                    number of Redis calls the end-to-end test makes
        Py        = end-to-end - Lua VM - RTT
                    (residual; includes any secondary Lua scripts'
                     execution time, which is small compared to RTT)
        Total     = end-to-end median
    """
    by_name: dict[str, dict[str, float]] = {}
    for bench in benchmarks:
        by_name.setdefault(bench.name, {})[bench.group] = bench.stats.median

    decomposable = [
        (name, groups)
        for name, groups in by_name.items()
        if {"lua-vm", "evalsha-wallclock", "end-to-end"} <= groups.keys()
    ]
    if not decomposable:
        return

    terminalreporter.section("benchmark cost decomposition (median)")
    header = (
        f"{'Name':<40} {'Lua VM':>12} {'RTT (Nx)':>16} "
        f"{'Py':>12} {'Total':>12}"
    )
    terminalreporter.line(header)
    terminalreporter.line("-" * len(header))

    for name, groups in sorted(decomposable):
        lua = groups["lua-vm"] * 1e6
        wall = groups["evalsha-wallclock"] * 1e6
        e2e = groups["end-to-end"] * 1e6
        per_call_rtt = wall - lua
        n_calls = _redis_calls_for(name)
        rtt = per_call_rtt * n_calls
        py = e2e - lua - rtt
        rtt_label = f"{rtt:>10.2f}us ({n_calls}x)"
        terminalreporter.line(
            f"{name:<40} {lua:>10.2f}us {rtt_label:>16} "
            f"{py:>10.2f}us {e2e:>10.2f}us"
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
        benchmark.pedantic(
            callable_to_run, setup=setup, rounds=rounds, iterations=1
        )

    return _run
