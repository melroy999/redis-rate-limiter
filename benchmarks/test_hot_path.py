"""Benchmarks for the core hot path: schedule_task() and consume().

These benchmarks measure the latency of the two most performance-critical
operations in the rate limiter. Both operations involve a Redis round trip
(EVALSHA of a Lua script), so the results reflect end-to-end Python-to-Redis
overhead rather than pure CPU cost.
"""

import itertools

import pytest


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
        """Measure consume() latency when a task is available.

        Pre-fills the buffer before each round so that consume() always
        finds work. The task lifecycle is completed immediately to free
        the concurrency slot.
        """

        def _consume_one():
            limiter.schedule_task("bench.module.func", {"seq": 0})
            result = limiter.consume()
            if result["success"]:
                with limiter.task_lifecycle(result["task"]["id"]):
                    pass

        benchmark(_consume_one)

    @pytest.mark.parametrize("buffer_depth", [10, 100, 1000])
    def test_consume_vs_buffer_depth(self, benchmark, limiter, buffer_depth):
        """Measure how buffer depth affects consume() latency.

        A larger sorted set (ZRANGE + ZREM) may increase per-call cost.
        """
        # Pre-fill the buffer to the target depth.
        for i in range(buffer_depth):
            limiter.schedule_task("bench.module.func", {"depth": i})

        def _consume_one():
            result = limiter.consume()
            if result["success"]:
                with limiter.task_lifecycle(result["task"]["id"]):
                    pass
                # Replenish so the buffer stays at target depth.
                limiter.schedule_task("bench.module.func", {"depth": 0})

        benchmark(_consume_one)


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
