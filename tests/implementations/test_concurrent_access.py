"""Tests for concurrent access to shared rate limiter state.

These tests verify that the distributed coordination primitives (i.e., Lua scripts,
the distributed lock, and concurrency tracking) maintain their guarantees when
multiple clients contend for the same limiter simultaneously.

Multiple threads sharing a Redis connection accurately simulate distributed
workers (e.g., Celery, thread pool) hitting the same Redis instance: the GIL
is released during network I/O, hence Redis operations genuinely interleave.

Fixture dependencies:
    - ``redis_client``: from ``tests/conftest.py``.
    - ``make_limiter_pool``: from ``tests/implementations/conftest.py``.
"""

import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest

from tests.helpers.utils import schedule_n_tasks
from tests.implementations.conftest import TrackingRateLimiter

# The number of concurrent simulated workers (threads).
WORKERS = 8
FUNC_PATH = "myapp.tasks.work"


class SlowDispatchTrackingRateLimiter(TrackingRateLimiter):
    """Tracking limiter that intentionally holds the dispatch lock
    for an extended period.

    This keeps the critical section open long enough for concurrent
    contenders to encounter lock contention in a deterministic manner.
    """

    def _dispatch_task(self, func_path: str, payload: dict, task_id: str) -> None:
        """Record the dispatch after a brief delay to keep the lock held."""
        time.sleep(0.2)
        super()._dispatch_task(func_path, payload, task_id)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def run_concurrently(fn, args_list):
    r"""Run ``fn(*args)`` for every ``args`` in ``args_list`` with a synchronized start.

    A ``threading.Barrier`` is used to ensure that all threads begin their work at
    the same instant, thereby maximizing the probability of true contention on the
    Redis side.
    """
    barrier = threading.Barrier(len(args_list))
    results: list = []
    exceptions: list[Exception] = []

    def _wrapped(args):
        barrier.wait()
        return fn(*args)

    with ThreadPoolExecutor(max_workers=len(args_list)) as pool:
        futures = [pool.submit(_wrapped, args) for args in args_list]
        for future in as_completed(futures):
            try:
                results.append(future.result())
            except Exception as exc:
                exceptions.append(exc)

    if exceptions:
        raise ExceptionGroup("Concurrent execution failures", exceptions)

    return results


def complete_task(redis_client, limiter, task_id):
    """Simulate task completion by cleaning up the Redis state.

    This mirrors the cleanup performed by ``TaskLifecycle.__exit__``.
    """
    redis_client.zrem(limiter.concurrency_key, task_id)
    redis_client.delete(limiter.get_inflight_key(task_id))


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.concurrency
class TestConcurrentScheduling:
    """Tests for multiple clients scheduling tasks simultaneously."""

    @staticmethod
    def test_concurrent_unique_task_scheduling(make_limiter_pool, redis_client):
        """Verify that all unique tasks are scheduled without loss
        under concurrent access."""
        # Arrange
        tasks_per_worker = 10
        limiters = make_limiter_pool(
            WORKERS, limit=1000, window=60, max_concurrency=1000
        )

        def schedule_batch(limiter, worker_id):
            results = []
            for i in range(tasks_per_worker):
                scheduled, task_id = limiter.schedule_task(
                    FUNC_PATH, {"worker": worker_id, "task": i}
                )
                results.append((scheduled, task_id))
            return results

        # Act
        all_results = run_concurrently(
            schedule_batch,
            [(limiter, idx) for idx, limiter in enumerate(limiters)],
        )

        # Assert
        all_scheduled = [r for batch in all_results for r in batch]
        scheduled_count = sum(1 for scheduled, _ in all_scheduled if scheduled)
        assert scheduled_count == len(all_scheduled), (
            f"every unique task must be scheduled, "
            f"got {scheduled_count}/{len(all_scheduled)}"
        )

        buffer_size = redis_client.zcard(limiters[0].buffer_key)
        assert buffer_size == WORKERS * tasks_per_worker, (
            f"buffer should contain all "
            f"{WORKERS * tasks_per_worker} tasks, "
            f"got {buffer_size}"
        )

    @staticmethod
    def test_concurrent_duplicate_scheduling_produces_single_entry(
        make_limiter_pool, redis_client
    ):
        """Verify that SET NX ensures exactly one buffer entry
        for concurrent duplicate schedules."""
        # Arrange
        limiters = make_limiter_pool(
            WORKERS, limit=1000, window=60, max_concurrency=1000
        )
        payload = {"user_id": 42}

        def schedule_same(limiter):
            return limiter.schedule_task(FUNC_PATH, payload)

        # Act
        results = run_concurrently(
            schedule_same,
            [(limiter,) for limiter in limiters],
        )

        # Assert
        scheduled_count = sum(1 for scheduled, _ in results if scheduled)
        assert scheduled_count == 1, (
            f"exactly one thread should win the SET NX race, got {scheduled_count}"
        )

        buffer_size = redis_client.zcard(limiters[0].buffer_key)
        assert buffer_size == 1, (
            f"buffer should contain exactly 1 entry, got {buffer_size}"
        )


@pytest.mark.concurrency
class TestConcurrentConsumption:
    """Concurrent ``consume()`` calls testing Lua script atomicity directly.

    These tests bypass the distributed lock to verify the stronger claim that the
    Lua script alone enforces correctness.
    """

    @staticmethod
    def test_rate_limit_enforced_under_concurrent_consume(
        make_limiter_pool, redis_client
    ):
        """Verify that the total number of successful consumes
        never exceeds the rate limit."""
        # Arrange
        limit = 10
        limiters = make_limiter_pool(
            WORKERS, limit=limit, window=60, max_concurrency=1000
        )

        schedule_n_tasks(limiters[0], func_path=FUNC_PATH, n=limit * 5)

        # If the window is about to roll over (less than 2 seconds
        # remaining), wait for a fresh window so the concurrent
        # burst runs entirely within one window period.
        probe = limiters[0].consume()
        assert probe["success"], "probe consume should succeed on a full buffer"
        probe_consumed = 1

        if probe["reset_in_ms"] < 2000:
            time.sleep(probe["reset_in_ms"] / 1000 + 0.05)
            probe_consumed = 0

        def consume_greedily(limiter):
            """Continue consuming until rate-limited or the buffer is empty."""
            consumed = []
            for _ in range(limit * 2):
                result = limiter.consume()
                if result["success"] and result["task"]:
                    consumed.append(result["task"]["id"])
                else:
                    break
            return consumed

        # Act
        all_consumed = run_concurrently(
            consume_greedily,
            [(limiter,) for limiter in limiters],
        )

        # Assert
        # Under concurrent access, the sliding window counter may
        # be slightly conservative (one fewer than the limit)
        # because two workers can read the same count simultaneously
        # and both decide to reject.
        total = sum(len(batch) for batch in all_consumed) + probe_consumed
        assert total <= limit, (
            f"rate limit exceeded: consumed {total}, limit is {limit}"
        )
        assert total >= limit - 1, (
            f"consumed far fewer than expected: {total}, limit is {limit}"
        )

    @staticmethod
    def test_concurrency_limit_enforced_under_concurrent_consume(
        make_limiter_pool, redis_client
    ):
        """Verify that active concurrency never exceeds the max_concurrency limit."""
        # Arrange
        max_conc = 3
        limiters = make_limiter_pool(
            WORKERS, limit=1000, window=60, max_concurrency=max_conc
        )

        schedule_n_tasks(limiters[0], func_path=FUNC_PATH, n=50)

        def consume_once(limiter):
            return limiter.consume()

        # Act
        results = run_concurrently(
            consume_once,
            [(limiter,) for limiter in limiters],
        )

        # Assert
        for result in results:
            assert result["active_concurrency"] <= max_conc, (
                f"reported concurrency "
                f"{result['active_concurrency']} "
                f"exceeds max {max_conc}"
            )

        actual_concurrency = redis_client.zcard(limiters[0].concurrency_key)
        assert actual_concurrency == max_conc, (
            f"expected all {max_conc} concurrency slots filled, "
            f"got {actual_concurrency}"
        )

        successful = [r for r in results if r["success"]]
        assert len(successful) == max_conc, (
            f"exactly {max_conc} consumes should succeed, got {len(successful)}"
        )

    @staticmethod
    def test_each_task_consumed_exactly_once(make_limiter_pool, redis_client):
        """Verify that no task is consumed by more than one worker."""
        # Arrange
        num_tasks = 5
        limiters = make_limiter_pool(
            WORKERS, limit=1000, window=60, max_concurrency=1000
        )

        scheduled_ids = set(
            schedule_n_tasks(limiters[0], func_path=FUNC_PATH, n=num_tasks)
        )

        def consume_all(limiter):
            consumed = []
            for _ in range(num_tasks):
                result = limiter.consume()
                if result["success"] and result["task"]:
                    consumed.append(result["task"]["id"])
            return consumed

        # Act
        all_consumed = run_concurrently(
            consume_all,
            [(limiter,) for limiter in limiters],
        )

        # Assert
        all_task_ids = [tid for batch in all_consumed for tid in batch]
        assert len(all_task_ids) == len(set(all_task_ids)), (
            f"duplicate consumption detected: "
            f"{len(all_task_ids)} consumed "
            f"but only {len(set(all_task_ids))} unique"
        )

        assert set(all_task_ids) == scheduled_ids, (
            f"all scheduled tasks must be consumed. "
            f"missing: {scheduled_ids - set(all_task_ids)}"
        )


@pytest.mark.concurrency
class TestConcurrentDrain:
    """Tests for the full ``drain()`` path with the distributed
    lock under contention."""

    @staticmethod
    def test_concurrent_drains_produce_no_double_dispatches(make_limiter_pool, redis_client):
        """Verify that concurrent drainers never dispatch the same task twice."""
        # Arrange
        num_tasks = 5
        limiters = make_limiter_pool(
            WORKERS,
            limiter_cls=SlowDispatchTrackingRateLimiter,
            limit=1000,
            window=60,
            max_concurrency=1000,
        )

        schedule_n_tasks(limiters[0], func_path=FUNC_PATH, n=num_tasks)

        def drain_once(limiter):
            limiter.drain()

        # Act
        run_concurrently(drain_once, [(limiter,) for limiter in limiters])

        # Assert
        all_dispatched = []
        for limiter in limiters:
            all_dispatched.extend(limiter.dispatched_tasks)

        dispatched_ids = [t["task_id"] for t in all_dispatched]

        assert len(dispatched_ids) == len(set(dispatched_ids)), (
            f"double dispatch detected: "
            f"{len(dispatched_ids)} dispatched but "
            f"{len(set(dispatched_ids))} unique"
        )

    @staticmethod
    def test_all_tasks_eventually_consumed_under_contention(
        make_limiter_pool, redis_client
    ):
        """Verify that repeated concurrent drain rounds eventually
        consume every task."""
        # Arrange
        num_tasks = 8
        max_conc = 3
        limiters = make_limiter_pool(
            WORKERS,
            limiter_cls=TrackingRateLimiter,
            limit=1000,
            window=60,
            max_concurrency=max_conc,
        )

        scheduled_ids = set(
            schedule_n_tasks(limiters[0], func_path=FUNC_PATH, n=num_tasks)
        )

        consumed_ids: set[str] = set()

        # Act
        for _ in range(num_tasks * 3):

            def drain_once(limiter_arg):
                limiter_arg.drain()

            run_concurrently(drain_once, [(lim,) for lim in limiters])

            for limiter in limiters:
                for task in limiter.dispatched_tasks:
                    tid = task["task_id"]
                    if tid not in consumed_ids:
                        consumed_ids.add(tid)
                        complete_task(redis_client, limiter, tid)
                limiter.dispatched_tasks.clear()

            if consumed_ids == scheduled_ids:
                break

        # Assert
        assert consumed_ids == scheduled_ids, (
            f"not all tasks consumed. missing: {scheduled_ids - consumed_ids}"
        )

    @staticmethod
    def test_contention_aware_cooldown_distributes_drains(
        make_limiter_pool, redis_client
    ):
        """Verify that contention-aware cooldown distributes
        drain opportunities across workers."""
        # Arrange
        num_workers = 4
        num_tasks = 12
        limiters = make_limiter_pool(
            num_workers,
            limiter_cls=SlowDispatchTrackingRateLimiter,
            limit=50,
            window=10,
            max_concurrency=1000,
        )

        scheduled_ids = set(
            schedule_n_tasks(limiters[0], func_path=FUNC_PATH, n=num_tasks)
        )
        consumed_ids: set[str] = set()
        per_worker_dispatch_count = [0] * num_workers

        # Act
        for _ in range(num_tasks * 5):

            def drain_once(limiter_arg):
                limiter_arg.drain()

            run_concurrently(drain_once, [(lim,) for lim in limiters])

            for idx, limiter in enumerate(limiters):
                per_worker_dispatch_count[idx] += len(limiter.dispatched_tasks)
                for task in limiter.dispatched_tasks:
                    tid = task["task_id"]
                    if tid not in consumed_ids:
                        consumed_ids.add(tid)
                        complete_task(redis_client, limiter, tid)
                limiter.dispatched_tasks.clear()

            if consumed_ids == scheduled_ids:
                break

        # Assert
        assert consumed_ids == scheduled_ids, (
            f"not all tasks consumed. missing: {scheduled_ids - consumed_ids}"
        )

        # Dispatches must be distributed across multiple workers.
        workers_that_dispatched = sum(
            1 for count in per_worker_dispatch_count if count > 0
        )
        assert workers_that_dispatched >= 2, (
            f"expected at least 2 workers to dispatch tasks, but only "
            f"{workers_that_dispatched} did. "
            f"per-worker counts: {per_worker_dispatch_count}"
        )


@pytest.mark.concurrency
class TestConcurrentLifecycle:
    """Tests for ``TaskLifecycle`` cleanup under concurrent access."""

    @staticmethod
    def test_concurrent_lifecycle_cleanup_frees_slots(make_limiter_pool, redis_client):
        """Verify that all concurrency slots are freed when
        multiple lifecycles exit concurrently."""
        # Arrange
        max_conc = 5
        limiters = make_limiter_pool(1, limit=1000, window=60, max_concurrency=max_conc)
        limiter = limiters[0]

        schedule_n_tasks(limiter, func_path=FUNC_PATH, n=max_conc)
        consumed_task_ids: list[str] = []
        for _ in range(max_conc):
            result = limiter.consume()
            assert result["success"], (
                f"consume should succeed with {max_conc} slots available"
            )
            consumed_task_ids.append(result["task"]["id"])

        assert redis_client.zcard(limiter.concurrency_key) == max_conc, (
            "all concurrency slots should be filled after consuming"
        )

        lifecycles = [limiter.task_lifecycle(tid) for tid in consumed_task_ids]
        for lc in lifecycles:
            lc.__enter__()

        def exit_lifecycle(lifecycle):
            lifecycle.__exit__(None, None, None)

        # Act
        run_concurrently(exit_lifecycle, [(lc,) for lc in lifecycles])

        # Assert
        assert redis_client.zcard(limiter.concurrency_key) == 0, (
            "all concurrency slots must be freed after lifecycle exit"
        )

        for tid in consumed_task_ids:
            assert not redis_client.exists(limiter.get_inflight_key(tid)), (
                f"inflight key for task {tid} must be cleared after lifecycle exit"
            )


@pytest.mark.concurrency
class TestConcurrentFullPipeline:
    """End-to-end distributed coordination test."""

    @staticmethod
    def test_producers_and_consumers_under_contention(make_limiter_pool, redis_client):
        """Verify that no tasks are lost when producers and consumers
        operate concurrently.

        Producer threads schedule tasks while consumer threads drain.
        Completed tasks free their concurrency slots between rounds.
        """
        # Arrange
        limit = 20
        max_conc = 5
        num_tasks = 15
        num_producers = 4
        num_consumers = 4

        limiters = make_limiter_pool(
            num_producers + num_consumers,
            limiter_cls=TrackingRateLimiter,
            limit=limit,
            window=60,
            max_concurrency=max_conc,
        )

        producer_limiters = limiters[:num_producers]
        consumer_limiters = limiters[num_producers:]

        # Act
        # Phase 1: Concurrent scheduling.
        #
        # Tasks are distributed across producers. Each producer schedules a
        # disjoint range so that every (func_path, payload) pair is unique.
        tasks_per_producer = (num_tasks + num_producers - 1) // num_producers
        all_scheduled: set[str] = set()

        def produce(limiter, start_idx):
            ids = []
            for i in range(tasks_per_producer):
                idx = start_idx + i
                if idx >= num_tasks:
                    break
                scheduled, task_id = limiter.schedule_task(
                    FUNC_PATH, {"global_idx": idx}
                )
                if scheduled:
                    ids.append(task_id)
            return ids

        scheduled_results = run_concurrently(
            produce,
            [(lim, i * tasks_per_producer) for i, lim in enumerate(producer_limiters)],
        )
        for batch in scheduled_results:
            all_scheduled.update(batch)

        assert len(all_scheduled) == num_tasks, (
            f"all {num_tasks} tasks must be scheduled, got {len(all_scheduled)}"
        )

        # Phase 2: Concurrent drain and completion cycles.
        all_consumed: set[str] = set()
        for _ in range(num_tasks * 3):

            def drain_once(limiter):
                limiter.drain()

            run_concurrently(drain_once, [(lim,) for lim in consumer_limiters])

            for lim in consumer_limiters:
                for task in lim.dispatched_tasks:
                    tid = task["task_id"]
                    if tid not in all_consumed:
                        all_consumed.add(tid)
                        complete_task(redis_client, lim, tid)
                lim.dispatched_tasks.clear()

            if all_consumed == all_scheduled:
                break

        # Assert
        assert all_consumed == all_scheduled, (
            f"all scheduled tasks must be consumed. "
            f"missing: {all_scheduled - all_consumed}"
        )

        assert redis_client.zcard(limiters[0].concurrency_key) == 0, (
            "concurrency set must be empty after all tasks complete"
        )

        assert redis_client.zcard(limiters[0].buffer_key) == 0, (
            "buffer must be empty after all tasks are consumed"
        )
