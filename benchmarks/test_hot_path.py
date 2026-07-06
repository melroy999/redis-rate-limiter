"""Benchmarks for the core hot path: schedule_task() and consume().

These benchmarks measure the latency of the two most performance-critical
operations in the rate limiter. Both operations involve a Redis round trip
(EVALSHA of a Lua script), so the results reflect end-to-end Python-to-Redis
overhead rather than pure CPU cost.
"""

import itertools

import pytest

from benchmarks.helpers import bulk_fill_buffer

pytestmark = pytest.mark.benchmark(group="end-to-end")


# ---------------------------------------------------------------------------
# schedule_task() benchmarks
# ---------------------------------------------------------------------------


class TestScheduleTask:
    """Benchmark the task scheduling path (MD5 + SET NX + ZADD via Lua)."""

    def test_schedule_task(self, benchmark, limiter):
        """Measure single schedule_task() latency."""
        counter = itertools.count()

        def _schedule():
            i = next(counter)
            limiter.schedule_task("bench.module.func", {"seq": i})

        benchmark(_schedule)

    @pytest.mark.parametrize("priority", [1, 50, 100])
    def test_schedule_task_with_priority(self, benchmark, limiter, priority):
        """Measure schedule_task() latency across priority levels."""
        counter = itertools.count()

        def _schedule():
            i = next(counter)
            limiter.schedule_task("bench.module.func", {"seq": i}, priority=priority)

        benchmark(_schedule)


# ---------------------------------------------------------------------------
# consume() benchmarks
# ---------------------------------------------------------------------------


class TestConsume:
    """Benchmark the task consumption path (EVALSHA of consume.lua)."""

    def test_consume_empty_buffer(self, benchmark, limiter):
        """Measure consume() latency when the buffer is empty.

        This represents the fast path where no task is available.
        """
        benchmark(limiter.consume)

    def test_consume_with_tasks(self, benchmark, limiter):
        """Measure consume() + task_lifecycle latency with tasks available.

        The buffer is refilled outside the timer via a ``pedantic`` setup
        callback so that the measurement reflects only the consume and
        lifecycle cost, not the schedule path.
        """
        bulk_fill_buffer(limiter, 1)
        refill_counter = itertools.count(start=1)

        def _refill_one():
            i = next(refill_counter)
            bulk_fill_buffer(limiter, 1, start_id=i)

        def _consume_one():
            result = limiter.consume()
            if result["success"]:
                with limiter.task_lifecycle(result["task"]["id"]):
                    pass

        benchmark.pedantic(
            _consume_one,
            setup=_refill_one,
            rounds=2000,
            warmup_rounds=10,
        )

    @pytest.mark.parametrize("buffer_depth", [10, 100, 1000, 10000, 100000, 1000000])
    def test_consume_vs_buffer_depth(self, benchmark, limiter, buffer_depth):
        """Measure how buffer depth affects consume() latency.

        A larger sorted set (ZRANGE + ZREM) may increase per-call cost.
        Pre-fills the buffer once via a single bulk ZADD, then uses a
        ``pedantic`` setup callback to add one task back between every
        round so the buffer never deviates from ``buffer_depth``.
        """
        bulk_fill_buffer(limiter, buffer_depth)

        refill_counter = itertools.count(start=buffer_depth)

        def _refill_one():
            i = next(refill_counter)
            bulk_fill_buffer(limiter, 1, start_id=i)

        def _consume_one():
            result = limiter.consume()
            if result["success"]:
                with limiter.task_lifecycle(result["task"]["id"]):
                    pass

        benchmark.pedantic(
            _consume_one,
            setup=_refill_one,
            rounds=1000,
            warmup_rounds=10,
        )


# ---------------------------------------------------------------------------
# Combined schedule + consume round trip
# ---------------------------------------------------------------------------


class TestRoundTrip:
    """Benchmark the full schedule-then-consume cycle."""

    def test_schedule_and_consume(self, benchmark, limiter):
        """Measure one complete schedule + consume + lifecycle cycle."""
        counter = itertools.count()

        def _round_trip():
            i = next(counter)
            limiter.schedule_task("bench.module.func", {"seq": i})
            result = limiter.consume()
            if result["success"]:
                with limiter.task_lifecycle(result["task"]["id"]):
                    pass

        benchmark(_round_trip)


# ---------------------------------------------------------------------------
# Async mirrors of the above benchmarks
# ---------------------------------------------------------------------------


@pytest.mark.benchmark(group="async-end-to-end")
class TestAsyncScheduleTask:
    """Async mirror of ``TestScheduleTask`` driving ``redis.asyncio``."""

    def test_schedule_task(self, benchmark, async_limiter):
        loop, limiter = async_limiter
        counter = itertools.count()

        def _schedule():
            i = next(counter)
            loop.run_until_complete(
                limiter.schedule_task("bench.module.func", {"seq": i})
            )

        benchmark(_schedule)

    @pytest.mark.parametrize("priority", [1, 50, 100])
    def test_schedule_task_with_priority(self, benchmark, async_limiter, priority):
        loop, limiter = async_limiter
        counter = itertools.count()

        def _schedule():
            i = next(counter)
            loop.run_until_complete(
                limiter.schedule_task(
                    "bench.module.func", {"seq": i}, priority=priority
                )
            )

        benchmark(_schedule)


@pytest.mark.benchmark(group="async-end-to-end")
class TestAsyncConsume:
    """Async mirror of ``TestConsume``."""

    def test_consume_empty_buffer(self, benchmark, async_limiter):
        loop, limiter = async_limiter

        def _consume():
            loop.run_until_complete(limiter.consume())

        benchmark(_consume)

    def test_consume_with_tasks(self, benchmark, async_limiter, redis_client):
        loop, limiter = async_limiter
        bulk_fill_buffer(limiter, 1, redis_client=redis_client)
        refill_counter = itertools.count(start=1)

        def _refill_one():
            i = next(refill_counter)
            bulk_fill_buffer(limiter, 1, start_id=i, redis_client=redis_client)

        async def _consume_one_async():
            result = await limiter.consume()
            if result["success"]:
                async with limiter.task_lifecycle(result["task"]["id"]):
                    pass

        def _consume_one():
            loop.run_until_complete(_consume_one_async())

        benchmark.pedantic(
            _consume_one,
            setup=_refill_one,
            rounds=2000,
            warmup_rounds=10,
        )


@pytest.mark.benchmark(group="async-end-to-end")
class TestAsyncRoundTrip:
    """Async mirror of ``TestRoundTrip``."""

    def test_schedule_and_consume(self, benchmark, async_limiter, redis_client):
        loop, limiter = async_limiter
        counter = itertools.count()

        async def _round_trip_async():
            i = next(counter)
            await limiter.schedule_task("bench.module.func", {"seq": i})
            result = await limiter.consume()
            if result["success"]:
                async with limiter.task_lifecycle(result["task"]["id"]):
                    pass

        def _round_trip():
            loop.run_until_complete(_round_trip_async())

        benchmark(_round_trip)
