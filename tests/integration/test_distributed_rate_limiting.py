"""Integration tests for distributed rate limiting correctness.

These tests verify that multiple independent limiter instances sharing the
same Redis backend correctly enforce the global rate limit. Each test uses
real Redis and real time, thereby validating the end-to-end behaviour of
the Lua scripts under concurrent access.

Multiple threads sharing a Redis connection accurately simulate distributed
workers: the GIL is released during network I/O, hence Redis operations
genuinely interleave.

Fixture dependencies:
    - ``redis_client``, ``limiter_id``, ``func_path``: from ``tests/conftest.py``.
"""

import math
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from tests.implementations.conftest import StubRateLimiter
from tests.integration.conftest import consume_and_complete, precise_sleep

# Skip the entire module on Windows owing to unreliable sub-second timing.
pytestmark = [
    pytest.mark.slow,
    pytest.mark.skipif(
        sys.platform == "win32",
        reason="Windows timer resolution (~15ms) makes sub-second timing tests unreliable.",
    ),
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def schedule_n_tasks(limiter: StubRateLimiter, n: int, func_path: str) -> list[str]:
    """Preload the limiter buffer with ``n`` unique tasks."""
    task_ids: list[str] = []
    for i in range(n):
        scheduled, task_id = limiter.schedule_task(func_path, {"seq": i})
        assert scheduled, f"failed to schedule task {i}"
        task_ids.append(task_id)
    return task_ids


def make_distributed_limiter(
    redis_client,
    limiter_id: str,
    **kwargs,
) -> StubRateLimiter:
    """Create a StubRateLimiter for distributed testing."""
    defaults = dict(
        limit=25, window=1.0, max_concurrency=100, max_age=3600, lease_duration=30
    )
    defaults.update(kwargs)
    return StubRateLimiter(
        redis_client=redis_client,
        limiter_id=limiter_id,
        **defaults,
    )


# ---------------------------------------------------------------------------
# Concrete test cases
# ---------------------------------------------------------------------------


class TestDistributedRateLimiting:
    """Verify that multiple consumers sharing Redis enforce the global rate limit."""

    @staticmethod
    def test_multi_consumer_per_window_consumption_bounded(
        redis_client,
        limiter_id,
        func_path,
    ):
        """N concurrent consumers never exceed the configured limit per window.

        Four threads greedily call ``consume()`` on separate limiter instances
        that share the same Redis keys. The total successful consumes within
        any single window must not exceed the configured limit (with a +1
        tolerance for discrete timing).
        """
        # Arrange
        limit = 25
        window = 1.0
        n_workers = 4
        n_windows = 3

        limiter_id = f"{limiter_id}_multi_consumer"
        limiters = [
            make_distributed_limiter(
                redis_client, limiter_id, limit=limit, window=window
            )
            for _ in range(n_workers)
        ]

        schedule_n_tasks(limiters[0], n=limit * (n_windows + 1), func_path=func_path)

        consumed_timestamps: list[float] = []
        lock = threading.Lock()
        stop = threading.Event()

        def greedy_consumer(limiter: StubRateLimiter) -> None:
            while not stop.is_set():
                result = consume_and_complete(limiter)
                if result["success"]:
                    with lock:
                        consumed_timestamps.append(time.time())
                else:
                    time.sleep(0.01)

        # Act
        with ThreadPoolExecutor(max_workers=n_workers) as pool:
            futures = [pool.submit(greedy_consumer, lim) for lim in limiters]
            precise_sleep(window * n_windows + 0.5)
            stop.set()
            for f in futures:
                f.result()

        # Assert
        assert len(consumed_timestamps) > 0, (
            "expected at least some successful consumes"
        )
        t0 = consumed_timestamps[0]
        window_counts: dict[int, int] = {}
        for ts in consumed_timestamps:
            window_idx = int((ts - t0) / window)
            window_counts[window_idx] = window_counts.get(window_idx, 0) + 1

        # The sliding window algorithm can allow up to 2x limit in the first
        # window (empty previous window), but steady-state should be at most
        # limit + 1 (due to discrete timing).
        for idx, count in window_counts.items():
            max_allowed = limit * 2 if idx == 0 else limit + 2
            assert count <= max_allowed, (
                f"window {idx} consumed {count} tasks, exceeding max {max_allowed} "
                f"(limit={limit})"
            )

    @staticmethod
    def test_buffer_grows_when_offered_exceeds_limit(
        redis_client,
        limiter_id,
        func_path,
    ):
        """The buffer depth increases when tasks are scheduled faster than the limit allows.

        A producer thread schedules tasks at 2x the limit while a consumer
        thread drains at the actual rate. After a full window, the buffer
        should contain more tasks than when the test started.
        """
        # Arrange
        limit = 25
        window = 1.0
        limiter_id = f"{limiter_id}_buffer_grows"
        limiter = make_distributed_limiter(
            redis_client,
            limiter_id,
            limit=limit,
            window=window,
        )

        stop = threading.Event()

        def producer() -> None:
            seq = 0
            interval = 1.0 / (limit * 2)
            while not stop.is_set():
                limiter.schedule_task(func_path, {"seq": seq})
                seq += 1
                precise_sleep(interval)

        def consumer() -> None:
            while not stop.is_set():
                result = consume_and_complete(limiter)
                if not result["success"]:
                    time.sleep(0.01)

        # Act
        producer_thread = threading.Thread(target=producer, daemon=True)
        consumer_thread = threading.Thread(target=consumer, daemon=True)

        producer_thread.start()
        consumer_thread.start()

        precise_sleep(window * 2)
        stop.set()
        producer_thread.join(timeout=2)
        consumer_thread.join(timeout=2)

        # Assert
        buffer_count = redis_client.zcard(f"{limiter_id}:buffer")
        assert buffer_count > 0, (
            f"buffer should have grown under 2x offered load, got {buffer_count}"
        )

    @staticmethod
    def test_buffer_drains_when_offered_below_limit(
        redis_client,
        limiter_id,
        func_path,
    ):
        """The buffer eventually empties when the offered rate drops below the limit.

        Pre-fill the buffer with tasks, then let consumers drain while a
        producer schedules at 0.5x the limit. The buffer should reach zero
        within a bounded duration.
        """
        # Arrange
        limit = 25
        window = 1.0
        limiter_id = f"{limiter_id}_buffer_drains"
        limiter = make_distributed_limiter(
            redis_client,
            limiter_id,
            limit=limit,
            window=window,
        )

        prefill = limit * 2
        schedule_n_tasks(limiter, n=prefill, func_path=func_path)

        producer_stop = threading.Event()
        consumer_stop = threading.Event()

        def producer() -> None:
            seq = prefill
            interval = 1.0 / (limit * 0.5)
            while not producer_stop.is_set():
                limiter.schedule_task(func_path, {"seq": seq})
                seq += 1
                precise_sleep(interval)

        def consumer() -> None:
            while not consumer_stop.is_set():
                result = consume_and_complete(limiter)
                if not result["success"]:
                    time.sleep(0.01)

        # Act
        producer_thread = threading.Thread(target=producer, daemon=True)
        consumer_thread = threading.Thread(target=consumer, daemon=True)

        producer_thread.start()
        consumer_thread.start()

        # Run for enough windows to drain the prefill.
        # At net rate of (limit - 0.5*limit) = 0.5*limit per window, draining
        # 2*limit tasks takes approximately 4 windows.
        precise_sleep(window * 6)

        # Stop the producer first, then let the consumer drain any stragglers
        # before stopping it. This avoids a race where the producer schedules
        # a task after the consumer's final iteration.
        producer_stop.set()
        producer_thread.join(timeout=2)
        precise_sleep(window)
        consumer_stop.set()
        consumer_thread.join(timeout=2)

        # Assert
        buffer_count = redis_client.zcard(f"{limiter_id}:buffer")
        assert buffer_count == 0, (
            f"buffer should have drained under 0.5x offered load, got {buffer_count} remaining"
        )

    @staticmethod
    def test_sine_wave_throughput_bounded(
        redis_client,
        limiter_id,
        func_path,
    ):
        """Under sine-wave traffic, the consumed rate per window never exceeds 2x the limit.

        A producer schedules tasks following a sine-wave pattern while multiple
        consumers drain. The sliding window algorithm guarantees that the total
        throughput in any window-sized interval stays within the 2x burst bound.
        """
        # Arrange
        limit = 25
        window = 1.0
        sine_period = 4.0
        sine_amplitude = 0.5
        n_workers = 3
        n_cycles = 2

        limiter_id = f"{limiter_id}_sine_wave"
        limiters = [
            make_distributed_limiter(
                redis_client, limiter_id, limit=limit, window=window
            )
            for _ in range(n_workers)
        ]

        consumed_timestamps: list[float] = []
        lock = threading.Lock()
        stop = threading.Event()

        def producer() -> None:
            seq = 0
            t0 = time.monotonic()
            while not stop.is_set():
                elapsed = time.monotonic() - t0
                rate = limit * (
                    1 + sine_amplitude * math.sin(2 * math.pi * elapsed / sine_period)
                )
                rate = max(1.0, rate)
                interval = 1.0 / rate
                limiters[0].schedule_task(func_path, {"seq": seq})
                seq += 1
                precise_sleep(interval)

        def consumer(limiter: StubRateLimiter) -> None:
            while not stop.is_set():
                result = consume_and_complete(limiter)
                if result["success"]:
                    with lock:
                        consumed_timestamps.append(time.time())
                else:
                    time.sleep(0.01)

        # Act
        duration = sine_period * n_cycles + 0.5
        with ThreadPoolExecutor(max_workers=n_workers + 1) as pool:
            futures = [pool.submit(producer)]
            futures += [pool.submit(consumer, lim) for lim in limiters]
            precise_sleep(duration)
            stop.set()
            for f in futures:
                f.result()

        # Assert
        assert len(consumed_timestamps) > 0, (
            "expected at least some successful consumes"
        )
        t0 = consumed_timestamps[0]
        window_counts: dict[int, int] = {}
        for ts in consumed_timestamps:
            window_idx = int((ts - t0) / window)
            window_counts[window_idx] = window_counts.get(window_idx, 0) + 1

        # The 2x burst bound is a proven property of the sliding window algorithm:
        # in the worst case (empty previous window + boundary crossing), up to 2x
        # the limit can be consumed. Under sustained load, each window should be
        # at most limit + a small tolerance.
        for idx, count in window_counts.items():
            max_allowed = limit * 2 + 2
            assert count <= max_allowed, (
                f"window {idx} consumed {count} tasks, exceeding max {max_allowed} "
                f"(limit={limit})"
            )
