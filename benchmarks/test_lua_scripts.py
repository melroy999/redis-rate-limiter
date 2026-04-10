"""Benchmarks for the Lua scripts, measured three ways for direct comparison.

Each script is measured under three groups so that the difference between
them isolates the network round-trip cost and the asyncio client overhead:

- ``lua-vm``: server-side execution time read from Redis SLOWLOG. This
  reflects only the time spent inside Redis executing the Lua script.
- ``evalsha-wallclock``: Python wall-clock time of the same EVALSHA call
  through the sync ``redis.Redis`` client. This reflects RTT + Lua VM.
- ``async-evalsha-wallclock``: Python wall-clock time of the same EVALSHA
  call through the async ``redis.asyncio.Redis`` client, driven by
  ``loop.run_until_complete``. Comparing this to ``evalsha-wallclock``
  isolates the asyncio + async-client overhead per round trip.

Subtracting ``lua-vm`` from either wallclock group gives the RTT cost
(sync or async respectively). Comparing both against ``test_hot_path.py``
(which goes through the full ``limiter.consume()`` / ``schedule_task()``
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


async def async_call_consume_lua(limiter):
    return await limiter._eval_script(
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


async def async_call_schedule_lua(limiter, task_json: str):
    return await limiter._eval_script(
        "schedule.lua",
        1,
        limiter.buffer_key,
        task_json,
        100,
        "",
    )


async def async_call_acquire_lua(limiter, key: str):
    return await limiter._eval_script("acquire.lua", 1, key, 60, 1000000)


def call_release_lua(limiter, task_id: str):
    return limiter._eval_script(
        "release.lua",
        3,
        limiter.concurrency_key,
        "",
        limiter._drain_signal_channel,
        task_id,
        limiter._worker_id,
    )


def call_set_nx(limiter, key: str):
    limiter.redis.set(key, "1", nx=True, ex=3600)


def call_publish(limiter):
    limiter.redis.publish(limiter._drain_signal_channel, limiter._worker_id)


async def async_call_release_lua(limiter, task_id: str):
    return await limiter._eval_script(
        "release.lua",
        3,
        limiter.concurrency_key,
        "",
        limiter._drain_signal_channel,
        task_id,
        limiter._worker_id,
    )


async def async_call_set_nx(limiter, key: str):
    await limiter.redis.set(key, "1", nx=True, ex=3600)


async def async_call_publish(limiter):
    await limiter.redis.publish(limiter._drain_signal_channel, limiter._worker_id)


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


def _run_async_consume_empty(bench_fn, loop, limiter):
    bench_fn(lambda: loop.run_until_complete(async_call_consume_lua(limiter)), rounds=ROUNDS)


def _run_async_consume_with_tasks(bench_fn, loop, limiter, redis_client):
    bulk_fill_buffer(limiter, ROUNDS + 1, redis_client=redis_client)
    bench_fn(lambda: loop.run_until_complete(async_call_consume_lua(limiter)), rounds=ROUNDS)


def _run_async_schedule(bench_fn, loop, limiter):
    counter = itertools.count()

    def _call():
        i = next(counter)
        loop.run_until_complete(
            async_call_schedule_lua(limiter, json.dumps({"id": f"t{i}", "seq": i}))
        )

    bench_fn(_call, rounds=ROUNDS)


def _run_async_acquire(bench_fn, loop, limiter):
    key = f"{limiter.id}:acquire"
    bench_fn(lambda: loop.run_until_complete(async_call_acquire_lua(limiter, key)), rounds=ROUNDS)


def _run_release(bench_fn, limiter, **kwargs):
    counter = itertools.count()

    def _setup():
        i = next(counter)
        limiter.redis.zadd(limiter.concurrency_key, {f"t{i}": 9999999999})

    # Pre-seed so the warm-up call has a slot to release.
    limiter.redis.zadd(limiter.concurrency_key, {"t_warmup": 9999999999})
    release_counter = itertools.count()

    def _call():
        j = next(release_counter)
        call_release_lua(limiter, f"t{j}" if j > 0 else "t_warmup")

    bench_fn(_call, rounds=ROUNDS, setup=_setup, **kwargs)


def _run_set_nx(bench_fn, limiter):
    counter = itertools.count()

    def _call():
        i = next(counter)
        call_set_nx(limiter, f"{limiter.id}:bench_nx:{i}")

    bench_fn(_call, rounds=ROUNDS)


def _run_publish(bench_fn, limiter):
    bench_fn(lambda: call_publish(limiter), rounds=ROUNDS)


def _run_async_release(bench_fn, loop, limiter, redis_client):
    counter = itertools.count()

    def _call():
        i = next(counter)
        redis_client.zadd(limiter.concurrency_key, {f"t{i}": 9999999999})
        loop.run_until_complete(async_call_release_lua(limiter, f"t{i}"))

    bench_fn(_call, rounds=ROUNDS)


def _run_async_set_nx(bench_fn, loop, limiter):
    counter = itertools.count()

    def _call():
        i = next(counter)
        loop.run_until_complete(async_call_set_nx(limiter, f"{limiter.id}:bench_nx:{i}"))

    bench_fn(_call, rounds=ROUNDS)


def _run_async_publish(bench_fn, loop, limiter):
    bench_fn(lambda: loop.run_until_complete(async_call_publish(limiter)), rounds=ROUNDS)


def _run_async_consume_at_depth(bench_fn, loop, limiter, buffer_depth: int, redis_client):
    bulk_fill_buffer(limiter, buffer_depth, redis_client=redis_client)

    refill_counter = itertools.count(start=buffer_depth)

    def _refill_one():
        i = next(refill_counter)
        bulk_fill_buffer(limiter, 1, start_id=i, redis_client=redis_client)

    bench_fn(
        lambda: loop.run_until_complete(async_call_consume_lua(limiter)),
        rounds=DEPTH_ROUNDS,
        setup=_refill_one,
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

    def test_release(self, lua_benchmark, limiter):
        limiter._register_script("release.lua")
        release_sha = limiter._script_shas["release.lua"]
        _run_release(lua_benchmark, limiter, sha_filter=release_sha)

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

    def test_release(self, wall_benchmark, limiter):
        _run_release(wall_benchmark, limiter)

    def test_set_nx(self, wall_benchmark, limiter):
        _run_set_nx(wall_benchmark, limiter)

    def test_publish(self, wall_benchmark, limiter):
        _run_publish(wall_benchmark, limiter)

    @pytest.mark.parametrize("buffer_depth", BUFFER_DEPTHS)
    def test_consume_vs_buffer_depth(self, wall_benchmark, limiter, buffer_depth):
        _run_consume_at_depth(wall_benchmark, limiter, buffer_depth)


# ---------------------------------------------------------------------------
# async-evalsha-wallclock group: async client wall-clock of the same calls
# ---------------------------------------------------------------------------


@pytest.mark.benchmark(group="async-evalsha-wallclock")
class TestAsyncEvalshaWallClock:
    """Wall-clock time through ``redis.asyncio.Redis``, driven by ``loop.run_until_complete``."""

    def test_consume_empty_buffer(self, wall_benchmark, async_limiter):
        loop, limiter = async_limiter
        _run_async_consume_empty(wall_benchmark, loop, limiter)

    def test_consume_with_tasks(self, wall_benchmark, async_limiter, redis_client):
        loop, limiter = async_limiter
        _run_async_consume_with_tasks(wall_benchmark, loop, limiter, redis_client)

    def test_schedule_task(self, wall_benchmark, async_limiter):
        loop, limiter = async_limiter
        _run_async_schedule(wall_benchmark, loop, limiter)

    def test_acquire(self, wall_benchmark, async_limiter):
        loop, limiter = async_limiter
        _run_async_acquire(wall_benchmark, loop, limiter)

    def test_release(self, wall_benchmark, async_limiter, redis_client):
        loop, limiter = async_limiter
        _run_async_release(wall_benchmark, loop, limiter, redis_client)

    def test_set_nx(self, wall_benchmark, async_limiter):
        loop, limiter = async_limiter
        _run_async_set_nx(wall_benchmark, loop, limiter)

    def test_publish(self, wall_benchmark, async_limiter):
        loop, limiter = async_limiter
        _run_async_publish(wall_benchmark, loop, limiter)

    @pytest.mark.parametrize("buffer_depth", BUFFER_DEPTHS)
    def test_consume_vs_buffer_depth(self, wall_benchmark, async_limiter, redis_client, buffer_depth):
        loop, limiter = async_limiter
        _run_async_consume_at_depth(wall_benchmark, loop, limiter, buffer_depth, redis_client)
