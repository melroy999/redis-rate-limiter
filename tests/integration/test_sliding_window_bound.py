"""Experimental: Sliding window measurement bound assertion.

This test exists to determine whether the sliding window steady-state bound
(tighter than 2x limit) holds reliably on native Linux.  It was removed from
the main integration test suite because it fails intermittently on Docker
Desktop for Windows (WSL2), where scheduling hiccups cause under-saturated
windows and compensating bursts that push the measurement window count well
above ``limit``.

Run on native Linux to see if the bound holds there:
    pytest tests/integration/test_sliding_window_bound.py -v -s --count=10

If it passes reliably on native Linux, consider re-integrating it into the
main test suite with a platform guard.  If not, delete this file.

See ``tests/integration/README.md`` for background on the measurement window
bound and why it is not provable across environments.
"""

import math
import sys
import time
from collections import Counter

import pytest

from tests.implementations.conftest import MinimalRateLimiter

# Same configs as the parameterized suite.
CONFIGS = [
    (3, 0.5),
    (5, 0.5),
    (5, 1),
    (10, 1),
    (10, 2),
    (20, 2),
    (5, 5),
    (15, 5),
]


def consume_and_complete(limiter) -> dict:
    """Consume a task and immediately complete its lifecycle."""
    result = limiter.consume()
    if result["success"]:
        task_id = result["task"]["id"]
        with limiter.task_lifecycle(task_id):
            pass
    return result


def precise_sleep(duration_seconds: float) -> None:
    """Sleep using active polling for sub-second precision."""
    target_time = time.time() + duration_seconds
    while time.time() < target_time:
        time.sleep(0.001)


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Requires sub-second timing precision; run in Docker or on Linux.",
)
class TestSlidingWindowBound:
    """Assert that the steady-state sliding window count stays within a
    tighter bound than 2x limit.

    This is the assertion that was removed from the main suite because
    it could not be made reliable on Docker Desktop / WSL2.
    """

    @pytest.fixture
    def limiter(self, request, redis_client, default_limiter_id):
        limit, window = request.param
        limiter_id = (
            f"{default_limiter_id}_sw_bound_{limit}_{window}".replace(".", "_")
        )
        limiter = MinimalRateLimiter(
            redis_client=redis_client,
            limiter_id=limiter_id,
            limit=limit,
            window=window,
            max_concurrency=100,
            max_age=3600,
            lease_duration=30,
        )
        yield limiter
        keys = redis_client.keys(f"{limiter.id}:*")
        if keys:
            redis_client.delete(*keys)

    @pytest.mark.parametrize(
        "limiter",
        CONFIGS,
        indirect=True,
        ids=[f"limit={l}_window={w}s" for l, w in CONFIGS],
    )
    def test_steady_state_sliding_window_bound(self, limiter, redis_client, func_path):
        """Assert measurement window count stays within a derived bound.

        The bound uses ``jitter_multiplier = 5`` to account for accumulated
        timing imprecision across retry cycles.  This was empirically
        insufficient on WSL2 but may hold on native Linux.
        """
        limit = limiter.limit
        window = limiter.window
        window_ms = int(window * 1000)
        sleep_fraction = 0.05

        # Scale up for short windows.
        num_windows = 4
        num_windows = max(num_windows, math.ceil(num_windows / window))
        total_duration = num_windows * window

        for i in range(2 * limit * num_windows):
            limiter.schedule_task(func_path, {"index": i})

        # Calibrate clock offset.
        redis_time = redis_client.time()
        redis_now_s = redis_time[0] + redis_time[1] / 1_000_000
        clock_offset = redis_now_s - time.time()

        # Consume continuously.
        start_time = time.time()
        timestamps = []

        while time.time() - start_time < total_duration:
            result = consume_and_complete(limiter)
            if result["success"]:
                timestamps.append(time.time())
            else:
                precise_sleep(window * sleep_fraction)

        total_consumed = len(timestamps)
        actual_duration = time.time() - start_time

        # Steady-state sliding window max (skip first 2W).
        steady_state_start = start_time + window * 2
        post_burst = [ts for ts in timestamps if ts >= steady_state_start]
        observed_steady_state_max = 0
        for ts in post_burst:
            count = sum(1 for t in timestamps if ts <= t < ts + window)
            observed_steady_state_max = max(observed_steady_state_max, count)

        # Per-fixed-window counts.
        def to_redis_window(ts: float) -> int:
            redis_ms = (ts + clock_offset) * 1000
            return int(redis_ms // window_ms) * window_ms

        fixed_window_counts = Counter(to_redis_window(ts) for ts in timestamps)
        sorted_windows = sorted(fixed_window_counts.keys())
        steady_windows = sorted_windows[2:] if len(sorted_windows) > 2 else []
        fixed_window_max = max(
            (fixed_window_counts[w] for w in steady_windows), default=0
        )

        # Derive the sliding window bound.
        jitter_multiplier = 5
        max_approx_error = math.ceil(limit * sleep_fraction * jitter_multiplier) + 2
        sliding_window_bound = limit + max_approx_error

        # Report.
        print(f"\n  Config: limit={limit}, window={window}s")
        print(f"    Duration: {actual_duration:.2f}s ({num_windows} windows)")
        print(f"    Total consumed: {total_consumed}")
        print(f"    Steady-state sliding window max: {observed_steady_state_max}")
        print(f"    Sliding window bound: {sliding_window_bound}")
        print(f"    Fixed window max: {fixed_window_max} (max allowed: {limit})")
        print(
            f"    Per-fixed-window counts: {dict(sorted(fixed_window_counts.items()))}"
        )

        # Assert per-fixed-window invariant (provable, should always hold).
        for win_start in steady_windows:
            cnt = fixed_window_counts[win_start]
            assert cnt <= limit, (
                f"fixed window {win_start} had {cnt} requests (limit={limit})"
            )

        # Assert the derived sliding window bound (the one under test).
        if post_burst:
            assert observed_steady_state_max <= sliding_window_bound, (
                f"exceeded derived bound: {observed_steady_state_max} requests "
                f"in {window}s window (limit={limit}, "
                f"bound={sliding_window_bound})"
            )
