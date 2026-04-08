"""Benchmarks for the Lua scripts, measured two ways for direct comparison.

Each script is measured under two groups so that the difference between
them isolates the network round-trip cost:

- ``lua-vm``: server-side execution time read from Redis SLOWLOG. This
  reflects only the time spent inside Redis executing the Lua script.
- ``evalsha-wallclock``: Python wall-clock time of the same EVALSHA call.
  This reflects RTT + Lua VM execution.

Subtracting one from the other gives the network round-trip cost in
isolation. Comparing both against ``test_hot_path.py`` (which goes
through the full ``limiter.consume()`` / ``limiter.schedule_task()``
methods) further isolates the Python-side limiter overhead.
"""

import itertools
import json

import pytest

from benchmarks.helpers import bulk_fill_buffer


# ---------------------------------------------------------------------------
# Script invocation helpers
# ---------------------------------------------------------------------------


def call_consume_lua(limiter):
    """Invoke consume.lua via _eval_script with no Python-side bookkeeping."""
    return limiter._eval_script(
        "consume.lua",
        4,
        limiter.id,
        limiter.buffer_key,
        limiter.concurrency_key,
        limiter.dlq_key,
        limiter.window,
        limiter.limit,
        limiter.max_concurrency,
        limiter.max_age,
        limiter.lease_duration,
    )


def call_schedule_lua(limiter, task_json: str):
    """Invoke schedule.lua via _eval_script with no Python-side bookkeeping."""
    return limiter._eval_script(
        "schedule.lua",
        1,
        limiter.buffer_key,
        task_json,
        100,
        "",
    )


def call_acquire_lua(limiter, key: str):
    """Invoke acquire.lua via _eval_script with no Python-side bookkeeping."""
    return limiter._eval_script("acquire.lua", 1, key, 60, 1000000)


# ---------------------------------------------------------------------------
# Shared test bodies
# ---------------------------------------------------------------------------

ROUNDS = 2000


def _run_consume_empty(bench_fn, limiter):
    bench_fn(lambda: call_consume_lua(limiter), rounds=ROUNDS)


def _run_consume_with_tasks(bench_fn, limiter):
    # Pre-populate with one extra to cover the warm-up call. The benchmark
    # call drains exactly ROUNDS tasks; the +1 covers the warm-up.
    bulk_fill_buffer(limiter, ROUNDS + 1)
    bench_fn(lambda: call_consume_lua(limiter), rounds=ROUNDS)


def _run_schedule(bench_fn, limiter):
    counter = itertools.count()

    def _call():
        i = next(counter)
        call_schedule_lua(limiter, json.dumps({"id": f"t{i}", "seq": i}))

    bench_fn(_call, rounds=ROUNDS)


def _run_acquire(bench_fn, limiter):
    key = f"{limiter.id}:acquire"
    bench_fn(lambda: call_acquire_lua(limiter, key), rounds=ROUNDS)


BUFFER_DEPTHS = [10, 100, 1000, 10000, 100000, 1000000]
DEPTH_ROUNDS = 1000


def _run_consume_at_depth(bench_fn, limiter, buffer_depth: int, **kwargs):
    """Measure consume.lua against a buffer held at constant ``buffer_depth``.

    Pre-fills the buffer once via a single bulk ZADD, then uses a setup
    callback to add one task back between every measured call so the
    buffer never deviates from ``buffer_depth``.
    """
    bulk_fill_buffer(limiter, buffer_depth)

    refill_counter = itertools.count(start=buffer_depth)

    def _refill_one():
        i = next(refill_counter)
        bulk_fill_buffer(limiter, 1, start_id=i)

    bench_fn(
        lambda: call_consume_lua(limiter),
        rounds=DEPTH_ROUNDS,
        setup=_refill_one,
        **kwargs,
    )


# ---------------------------------------------------------------------------
# lua-vm group: SLOWLOG measurements
# ---------------------------------------------------------------------------


@pytest.mark.benchmark(group="lua-vm")
class TestLuaVM:
    """Server-side Lua execution time, read from Redis SLOWLOG."""

    def test_consume_empty_buffer(self, lua_benchmark, limiter):
        _run_consume_empty(lua_benchmark, limiter)

    def test_consume_with_tasks(self, lua_benchmark, limiter):
        _run_consume_with_tasks(lua_benchmark, limiter)

    def test_schedule_task(self, lua_benchmark, limiter):
        _run_schedule(lua_benchmark, limiter)

    def test_acquire(self, lua_benchmark, limiter):
        _run_acquire(lua_benchmark, limiter)

    @pytest.mark.parametrize("buffer_depth", BUFFER_DEPTHS)
    def test_consume_vs_buffer_depth(self, lua_benchmark, limiter, buffer_depth):
        # Filter SLOWLOG to consume.lua's SHA so the ZADD/EVALSHAs from
        # the bulk fill setup do not contaminate the measurement set.
        limiter._register_script("consume.lua")
        consume_sha = limiter._script_shas["consume.lua"]
        _run_consume_at_depth(
            lua_benchmark, limiter, buffer_depth, sha_filter=consume_sha
        )


# ---------------------------------------------------------------------------
# evalsha-wallclock group: Python wall-clock of the same EVALSHA call
# ---------------------------------------------------------------------------


@pytest.mark.benchmark(group="evalsha-wallclock")
class TestEvalshaWallClock:
    """Python wall-clock time of the same EVALSHA invocations as TestLuaVM."""

    def test_consume_empty_buffer(self, wall_benchmark, limiter):
        _run_consume_empty(wall_benchmark, limiter)

    def test_consume_with_tasks(self, wall_benchmark, limiter):
        _run_consume_with_tasks(wall_benchmark, limiter)

    def test_schedule_task(self, wall_benchmark, limiter):
        _run_schedule(wall_benchmark, limiter)

    def test_acquire(self, wall_benchmark, limiter):
        _run_acquire(wall_benchmark, limiter)

    @pytest.mark.parametrize("buffer_depth", BUFFER_DEPTHS)
    def test_consume_vs_buffer_depth(self, wall_benchmark, limiter, buffer_depth):
        _run_consume_at_depth(wall_benchmark, limiter, buffer_depth)
