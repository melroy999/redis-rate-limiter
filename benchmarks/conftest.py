"""Benchmark suite configuration and fixtures.

Provides Redis connection fixtures, limiter factories, and a custom
terminal summary that reports p95/p99 latency percentiles for each
benchmark. Percentiles are also embedded into the JSON output via the
``pytest_benchmark_update_json`` hook.
"""

import os
from uuid import uuid4

import numpy as np
import pytest
import redis

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
    """Return p95 and p99 for a benchmark's stats object."""
    data = np.array(stats.data)
    return {
        "p95": float(np.percentile(data, 95)),
        "p99": float(np.percentile(data, 99)),
    }


# ---------------------------------------------------------------------------
# pytest-benchmark hooks: embed percentiles in JSON output
# ---------------------------------------------------------------------------


def pytest_benchmark_update_json(config, benchmarks, output_json):
    """Attach p95 and p99 to each benchmark entry in the JSON output."""
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
    """Print a percentile summary table after the benchmark results."""
    session = getattr(config, "_benchmarksession", None)
    if session is None:
        return

    benchmarks = session.benchmarks
    if not benchmarks:
        return

    terminalreporter.section("benchmark percentiles")
    header = f"{'Name':<60} {'p95':>12} {'p99':>12}"
    terminalreporter.line(header)
    terminalreporter.line("-" * len(header))

    for bench in benchmarks:
        pcts = _percentiles_for(bench.stats)
        p95_us = pcts["p95"] * 1e6
        p99_us = pcts["p99"] * 1e6
        terminalreporter.line(
            f"{bench.name:<60} {p95_us:>10.2f}us {p99_us:>10.2f}us"
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

    Uses a large window (60s) and high limit (10000) so that rate
    limiting does not interfere with throughput measurements.
    """
    instance = _BenchmarkLimiter(
        redis_client=redis_client,
        limiter_id=limiter_id,
        limit=10000,
        window=60,
        max_concurrency=10000,
        max_age=3600,
        lease_duration=30,
    )
    yield instance
    instance.shutdown()
