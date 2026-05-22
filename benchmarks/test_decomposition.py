"""Per-iteration cost decomposition using instrumented limiters.

Each test runs a single end-to-end operation (consume, schedule, etc.)
through ``benchmark()`` or ``benchmark.pedantic()`` as normal. The
instrumented limiter records per-Redis-call wall-clock timings inline
(inside ``_eval_script``, ``_publish_drain_signal``, and ``redis.set``
for SET NX). After the benchmark completes, the decomposition is:

    RTT   = median of the per-iteration sum-of-call-timings
    Total = benchmark.stats.median (the benchmark's own e2e measurement)
    Py    = Total - RTT

Because the RTT samples are captured during the same iterations the
benchmark measures, both reflect identical environmental conditions.

Results are collected via ``record_property`` and printed in a summary
table by ``_print_instrumented_decomposition`` in ``conftest.py``.
"""

import itertools

import numpy as np
import pytest

from benchmarks.helpers import bulk_fill_buffer

ROUNDS = 2000


def _record_decomposition(
    request: pytest.FixtureRequest,
    variant: str,
    test_name: str,
    benchmark,
    rtt_samples: list[float],
) -> None:
    if not rtt_samples:
        return
    e2e_median = float(np.median(benchmark.stats.stats.data))
    rtt_median = float(np.median(rtt_samples))
    py_median = e2e_median - rtt_median

    request.node.user_properties.append(
        (
            "decomposition_result",
            {
                "variant": variant,
                "test_name": test_name,
                "e2e_median": e2e_median,
                "rtt_median": rtt_median,
                "py_median": py_median,
            },
        )
    )


# ---------------------------------------------------------------------------
# Sync instrumented tests
# ---------------------------------------------------------------------------


@pytest.mark.benchmark(group="decomposition-sync")
class TestSyncDecomposition:
    """Per-iteration cost decomposition through the sync Redis client."""

    def test_consume_empty_buffer(self, benchmark, request, instrumented_limiter):
        limiter = instrumented_limiter
        rtt_samples: list[float] = []

        def _run():
            limiter.pop_timings()
            limiter.consume()
            rtt_samples.append(sum(limiter.pop_timings()))

        benchmark(_run)
        _record_decomposition(
            request, "sync", "test_consume_empty_buffer", benchmark, rtt_samples
        )

    def test_consume_with_tasks(self, benchmark, request, instrumented_limiter):
        limiter = instrumented_limiter
        bulk_fill_buffer(limiter, 1)
        refill_counter = itertools.count(start=1)
        rtt_samples: list[float] = []

        def _refill():
            i = next(refill_counter)
            bulk_fill_buffer(limiter, 1, start_id=i)

        def _run():
            limiter.pop_timings()
            result = limiter.consume()
            if result["success"]:
                with limiter.task_lifecycle(result["task"]["id"]):
                    pass
            rtt_samples.append(sum(limiter.pop_timings()))

        benchmark.pedantic(_run, setup=_refill, rounds=ROUNDS, warmup_rounds=10)
        _record_decomposition(
            request, "sync", "test_consume_with_tasks", benchmark, rtt_samples
        )

    def test_schedule_task(self, benchmark, request, instrumented_limiter):
        limiter = instrumented_limiter
        counter = itertools.count()
        rtt_samples: list[float] = []

        def _run():
            limiter.pop_timings()
            i = next(counter)
            limiter.schedule_task("bench.module.func", {"seq": i})
            rtt_samples.append(sum(limiter.pop_timings()))

        benchmark(_run)
        _record_decomposition(
            request, "sync", "test_schedule_task", benchmark, rtt_samples
        )

    def test_schedule_and_consume(self, benchmark, request, instrumented_limiter):
        limiter = instrumented_limiter
        counter = itertools.count()
        rtt_samples: list[float] = []

        def _run():
            limiter.pop_timings()
            i = next(counter)
            limiter.schedule_task("bench.module.func", {"seq": i})
            result = limiter.consume()
            if result["success"]:
                with limiter.task_lifecycle(result["task"]["id"]):
                    pass
            rtt_samples.append(sum(limiter.pop_timings()))

        benchmark(_run)
        _record_decomposition(
            request, "sync", "test_schedule_and_consume", benchmark, rtt_samples
        )


# ---------------------------------------------------------------------------
# Async instrumented tests
# ---------------------------------------------------------------------------


@pytest.mark.benchmark(group="decomposition-async")
class TestAsyncDecomposition:
    """Per-iteration cost decomposition through the async Redis client."""

    def test_consume_empty_buffer(self, benchmark, request, async_instrumented_limiter):
        loop, limiter = async_instrumented_limiter
        rtt_samples: list[float] = []

        def _run():
            limiter.pop_timings()
            loop.run_until_complete(limiter.consume())
            rtt_samples.append(sum(limiter.pop_timings()))

        benchmark(_run)
        _record_decomposition(
            request, "async", "test_consume_empty_buffer", benchmark, rtt_samples
        )

    def test_consume_with_tasks(
        self, benchmark, request, async_instrumented_limiter, redis_client
    ):
        loop, limiter = async_instrumented_limiter
        bulk_fill_buffer(limiter, 1, redis_client=redis_client)
        refill_counter = itertools.count(start=1)
        rtt_samples: list[float] = []

        def _refill():
            i = next(refill_counter)
            bulk_fill_buffer(limiter, 1, start_id=i, redis_client=redis_client)

        async def _consume():
            result = await limiter.consume()
            if result["success"]:
                async with limiter.task_lifecycle(result["task"]["id"]):
                    pass

        def _run():
            limiter.pop_timings()
            loop.run_until_complete(_consume())
            rtt_samples.append(sum(limiter.pop_timings()))

        benchmark.pedantic(_run, setup=_refill, rounds=ROUNDS, warmup_rounds=10)
        _record_decomposition(
            request, "async", "test_consume_with_tasks", benchmark, rtt_samples
        )

    def test_schedule_task(self, benchmark, request, async_instrumented_limiter):
        loop, limiter = async_instrumented_limiter
        counter = itertools.count()
        rtt_samples: list[float] = []

        def _run():
            limiter.pop_timings()
            i = next(counter)
            loop.run_until_complete(
                limiter.schedule_task("bench.module.func", {"seq": i})
            )
            rtt_samples.append(sum(limiter.pop_timings()))

        benchmark(_run)
        _record_decomposition(
            request, "async", "test_schedule_task", benchmark, rtt_samples
        )

    def test_schedule_and_consume(self, benchmark, request, async_instrumented_limiter):
        loop, limiter = async_instrumented_limiter
        counter = itertools.count()
        rtt_samples: list[float] = []

        async def _op(i: int) -> None:
            await limiter.schedule_task("bench.module.func", {"seq": i})
            result = await limiter.consume()
            if result["success"]:
                async with limiter.task_lifecycle(result["task"]["id"]):
                    pass

        def _run():
            limiter.pop_timings()
            i = next(counter)
            loop.run_until_complete(_op(i))
            rtt_samples.append(sum(limiter.pop_timings()))

        benchmark(_run)
        _record_decomposition(
            request, "async", "test_schedule_and_consume", benchmark, rtt_samples
        )
