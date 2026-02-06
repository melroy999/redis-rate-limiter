"""Integration tests for rate limiting behavior.

End-to-end tests verifying rate limiter correctly limits requests
and handles burst scenarios using Redis and Lua scripts.
"""

import time

import pytest

from celery_rate_limiter.limiters import CeleryRateLimiter


@pytest.fixture
def integration_limiter(redis_client, celery_app):
    """Create a limiter with explicit configuration for integration tests.

    Config:
        - limit: 5 requests
        - window: 60 seconds
        - max_concurrency: 2 simultaneous tasks
        - max_age: 3600 seconds (1 hour)
        - lease_duration: 30 seconds
    """
    limiter = CeleryRateLimiter(
        redis_client=redis_client,
        celery_app=celery_app,
        limiter_id="integration_test_limiter",
        limit=5,
        window=60,
        max_concurrency=2,
        max_age=3600,
        lease_duration=30,
    )

    yield limiter

    # Cleanup
    # Delete all keys associated with the limiter to ensure all tests get a clean slate.
    keys = redis_client.keys(f"{limiter.id}:*")
    if keys:
        redis_client.delete(*keys)


def consume_and_complete(limiter) -> dict:
    """Consume a task and immediately complete its lifecycle.

    This simulates a task being consumed and executed instantly, releasing
    the concurrency slot. Useful for tests that need to isolate rate limiting
    behavior from concurrency limiting.

    Args:
        limiter: The rate limiter instance.

    Returns:
        The consume result dict.
    """
    result = limiter.consume()
    if result["success"]:
        task_id = result["task"]["id"]
        with limiter.task_lifecycle(task_id):
            pass
    return result


class TestRateLimitingIntegration:
    """Integration tests for rate limiting with Redis."""

    def test_basic_rate_limit_enforcement(self, integration_limiter, func_path):
        """Verify rate limiter enforces the configured limit.

        Limiter config: limit=5, window=60.
        Schedule 10 tasks, consume up to limit (releasing concurrency slots
        after each consume to isolate rate limit behavior), verify queueing.
        """
        # Arrange
        for i in range(10):
            success, _ = integration_limiter.schedule_task(func_path, {"index": i})
            assert success is True

        # Act
        # Concurrency slots are released after each consume to isolate rate limit behavior.
        results = [consume_and_complete(integration_limiter) for _ in range(10)]
        consumed_count = sum(1 for r in results if r["success"])

        # Assert
        assert consumed_count == 5, "should consume exactly 5 tasks"
        last_successful = [r for r in results if r["success"]][-1]
        assert last_successful["remaining_tokens"] == 0
        assert results[-1]["remaining_tasks"] == 5

    def test_concurrency_limit_enforcement(self, integration_limiter, func_path):
        """Verify concurrency limits are enforced independently of rate limit.

        Limiter config: max_concurrency=2.
        Verify only 2 tasks consumed simultaneously even if rate limit allows more.
        """
        # Arrange
        for i in range(5):
            integration_limiter.schedule_task(func_path, {"index": i})

        # Act
        results = []
        for _ in range(3):
            result = integration_limiter.consume()
            results.append(result)

        # Assert
        assert results[0]["success"] is True
        assert results[1]["success"] is True
        assert results[0]["active_concurrency"] == 1
        assert results[1]["active_concurrency"] == 2
        assert results[2]["success"] is False
        assert results[2]["active_concurrency"] == 2

    @pytest.mark.parametrize("num_tasks", [3, 5, 10, 20])
    def test_accurate_telemetry_tracking(self, integration_limiter, num_tasks, func_path):
        """Verify telemetry accurately tracks remaining tokens and tasks."""
        # Arrange
        for i in range(num_tasks):
            integration_limiter.schedule_task(func_path, {"index": i})

        # Act
        # Concurrency slots are released after each consume to isolate rate limit behavior.
        results = [consume_and_complete(integration_limiter) for _ in range(num_tasks)]

        # Assert
        consumed = sum(1 for r in results if r["success"])
        expected_consumed = min(num_tasks, 5)
        assert consumed == expected_consumed

        final_result = results[-1]
        expected_remaining = max(0, num_tasks - consumed)
        assert final_result["remaining_tasks"] == expected_remaining

        successful_results = [r for r in results if r["success"]]
        if successful_results:
            expected_tokens = [4, 3, 2, 1, 0]
            actual_tokens = [r["remaining_tokens"] for r in successful_results]
            assert actual_tokens == expected_tokens[:len(successful_results)]

    def test_empty_buffer_returns_no_task(self, integration_limiter):
        """Verify consuming from empty buffer returns unsuccessful result."""
        # Act
        result = integration_limiter.consume()

        # Assert
        assert result["success"] is False
        assert result["task"] is None
        assert result["remaining_tasks"] == 0
        assert result["remaining_tokens"] == 5


class TestSlidingWindowBehavior:
    """Tests for sliding window counter algorithm behavior.

    The sliding window counter algorithm approximates a true sliding window
    by weighting the previous and current fixed windows. This has important
    implications:

    1. Burst behavior: At window boundaries, up to 2x the limit may be
       consumed in a short period. This occurs when the previous window is
       empty and requests arrive at the boundary--the algorithm allows a full
       limit from each adjacent window.

    2. Steady-state approximation: Once past the initial window (where the
       burst can occur), the algorithm approximates the configured limit.
       The counter's uniform-distribution assumption and timing jitter mean
       the true count in a measurement window can exceed the limit by 1,
       but not more.

    3. Long-term convergence: Despite short-term bursts, the average
       consumption rate over multiple windows converges to the configured
       limit.

    These tests verify long-term rate convergence and opportunistically check
    the 2x burst bound. The burst bound check is not guaranteed to catch all
    violations (it depends on timing we don't control), but will fail if the
    implementation is fundamentally broken.

    Note: The 2x burst bound property is formally verified in
    tests/properties/test_sliding_window_counter.py using the pure algorithm
    extracted from the Lua implementation.
    """

    @pytest.fixture
    def sliding_window_limiter(self, redis_client, celery_app):
        """Limiter with short window for sliding window behavior tests.

        Config:
            - limit: 10 requests per window
            - window: 0.5 seconds (short for fast testing)
            - max_concurrency: 50 (high to isolate rate limiting behavior)
        """
        limiter = CeleryRateLimiter(
            redis_client=redis_client,
            celery_app=celery_app,
            limiter_id="sliding_window_test_limiter",
            limit=10,
            window=0.5,
            max_concurrency=50,
            max_age=3600,
            lease_duration=30,
        )

        yield limiter

        keys = redis_client.keys(f"{limiter.id}:*")
        if keys:
            redis_client.delete(*keys)

    def test_long_term_rate_converges_to_limit(
        self, sliding_window_limiter, func_path
    ):
        """Verify average consumption rate converges to configured limit.

        Over multiple windows, total successful consumptions should approximate
        `num_windows x limit`. This tests the fundamental property of rate
        limiting: controlling throughput over time.

        Additionally, we opportunistically verify that no window-sized period
        exceeds 2x the limit. This check is not exhaustive--the 2x bound depends
        on specific timing scenarios (empty previous window + boundary burst)
        that we cannot reliably trigger. However, it will catch grossly broken
        implementations that allow unbounded throughput.

        Note on the 2x bound: The sliding window counter algorithm can allow
        up to 2x limit in edge cases. This is a known algorithmic property,
        not a bug. A true sliding window would enforce exactly 1x limit, but
        the counter approximation trades this for O(1) space complexity.
        """
        # Arrange
        # Infer configuration from limiter for consistency.
        limit = sliding_window_limiter.limit
        window = sliding_window_limiter.window
        num_windows = 4
        total_duration = num_windows * window

        # Schedule more tasks than we expect to consume.
        for i in range(2 * num_windows * limit):
            sliding_window_limiter.schedule_task(func_path, {"index": i})

        # Act
        # Consume continuously, recording timestamps.
        # Only sleep on failure (rate limited) to maximize burst potential.
        start_time = time.time()
        timestamps = []

        while time.time() - start_time < total_duration:
            result = consume_and_complete(sliding_window_limiter)
            if result["success"]:
                timestamps.append(time.time())
            else:
                # Rate limited--wait briefly for tokens to recover.
                time.sleep(0.01)

        total_consumed = len(timestamps)
        actual_duration = time.time() - start_time

        # Calculate observed metrics.
        observed_max_burst = 0
        for ts in timestamps:
            count_in_window = sum(1 for t in timestamps if ts <= t < ts + window)
            observed_max_burst = max(observed_max_burst, count_in_window)

        observed_rate = total_consumed / actual_duration * window  # requests per window

        # Steady-state max: skip the first 2W (burst + recovery), then check
        # that no window-sized period exceeds limit+1.
        #
        # This bound holds here because sustained, greedy consumption from a
        # single worker keeps fixed windows roughly uniformly filled. It is NOT
        # a general property of the sliding window counter--arbitrary traffic
        # patterns (multiple workers, bursty arrivals, non-greedy consumers)
        # can violate it. The +1 accounts for timing jitter.
        steady_state_start = start_time + window * 2
        post_burst_timestamps = [ts for ts in timestamps if ts >= steady_state_start]
        observed_steady_state_max = 0
        for ts in post_burst_timestamps:
            count_in_window = sum(1 for t in timestamps if ts <= t < ts + window)
            observed_steady_state_max = max(observed_steady_state_max, count_in_window)

        # Report observed metrics.
        print(f"\n  Sliding window test results:")
        print(f"    Config: limit={limit}, window={window}s")
        print(f"    Duration: {actual_duration:.2f}s ({num_windows} windows)")
        print(f"    Total consumed: {total_consumed}")
        print(f"    Observed rate: {observed_rate:.2f} requests/window (expected: {limit})")
        print(f"    Max burst in any {window}s window: {observed_max_burst} (max allowed: {2 * limit})")
        print(f"    Steady-state max (after {window * 2:.2f}s): {observed_steady_state_max} (max allowed: {limit + 1})")

        # Assert
        # The sliding window algorithm can burst up to 2x limit at the start if we
        # happen to hit a window boundary with empty history. After that initial
        # burst, the algorithm spreads requests properly across successive windows.
        # Account for this by allowing up to 1 extra window's worth on the upper bound.
        expected = num_windows * limit
        
        # 20% tolerance for timing variance.
        lower_bound = expected * 0.80
        
        # Possible initial boundary burst.
        upper_bound = (num_windows + 1) * limit

        assert lower_bound <= total_consumed <= upper_bound, (
            f"expected ~{expected} consumed over {num_windows} windows, "
            f"got {total_consumed} (allowed: {lower_bound:.0f}-{upper_bound})"
        )

        # Verify no window-sized period exceeded 2x limit.
        # This is not guaranteed to catch all violations: it depends on timing we
        # don't control, but catches fundamentally broken implementations.
        max_burst = 2 * limit
        assert observed_max_burst <= max_burst, (
            f"exceeded 2x limit: {observed_max_burst} requests in {window}s window "
            f"(limit={limit}, max_allowed={max_burst}). "
            f"this indicates a bug in the sliding window implementation."
        )

        # Verify steady-state max under sustained load (see comment above).
        if post_burst_timestamps:
            assert observed_steady_state_max <= limit + 1, (
                f"exceeded limit+1 in steady state: {observed_steady_state_max} requests "
                f"in {window}s window (limit={limit}, max_allowed={limit + 1}). "
                f"the sliding window counter should approximate the limit after the initial burst phase."
            )

    def test_burst_at_window_boundary_after_empty_window(
        self, sliding_window_limiter, func_path
    ):
        """Verify burst behavior when consuming across a boundary after an empty window.

        The sliding window counter allows up to 2x limit when:
        1. The previous window is empty (no consumption)
        2. Consumption starts near the end of the current (empty) window
        3. Consumption continues into the next window

        This test uses `reset_in_ms` to calculate a single wait that positions
        us at the end of a window with an empty previous window:
        - Wait for reset_in_ms (finish current window)
        - Plus one full window (ensure an empty window passes)
        - Plus 80% of another window (position near the end)
        """
        # Arrange
        # Infer configuration from limiter.
        limit = sliding_window_limiter.limit
        window = sliding_window_limiter.window
        
        # Fraction of window to use as "near the end."
        window_tail = 0.05

        # Schedule enough tasks for a potential 2x burst.
        for i in range(limit * 3):
            sliding_window_limiter.schedule_task(func_path, {"index": i})

        # Get reset_in_ms to calculate wait time.
        # This consume may succeed, but we only need the timing info.
        result = sliding_window_limiter.consume()
        if result["success"]:
            with sliding_window_limiter.task_lifecycle(result["task"]["id"]):
                pass

        reset_ms = result["reset_in_ms"]

        # Calculate wait: finish current window + skip one empty window + position near end.
        # This ensures the previous window is empty when we start consuming.
        wait_seconds = (reset_ms / 1000) + window + (window * (1 - window_tail))
        time.sleep(wait_seconds)

        # Consume rapidly across the window boundary.
        # Use a time-based loop: tail of current window + enough windows to consume all tasks.
        # This ensures we capture the burst and verify subsequent windows don't cause issues.
        timestamps = []
        num_task_windows = 3  # We scheduled limit * 3 tasks
        burst_window = window * (num_task_windows + window_tail)
        start_time = time.time()

        while time.time() - start_time < burst_window:
            result = consume_and_complete(sliding_window_limiter)
            if result["success"]:
                timestamps.append(time.time())
            # No sleep--consume as fast as possible to maximize burst.

        total_consumed = len(timestamps)
        burst_duration = timestamps[-1] - timestamps[0] if len(timestamps) > 1 else 0

        # Calculate max burst in any window-sized period.
        max_burst_in_window = 0
        for ts in timestamps:
            count_in_window = sum(1 for t in timestamps if ts <= t < ts + window)
            max_burst_in_window = max(max_burst_in_window, count_in_window)

        # Report observed burst.
        print(f"\n  Window boundary burst test results:")
        print(f"    Config: limit={limit}, window={window}s")
        print(f"    Burst duration: {burst_duration:.3f}s")
        print(f"    Total consumed: {total_consumed}")
        print(f"    Max in any {window}s window: {max_burst_in_window}")
        print(f"    Expected range: {limit} < max_burst <= {2 * limit}")

        # Assert
        # Max burst in any window should exceed limit (demonstrating burst) but
        # never exceed 2x limit (the algorithmic upper bound).
        assert max_burst_in_window > limit, (
            f"expected burst to exceed limit ({limit}), got {max_burst_in_window}. "
            f"this may indicate the test didn't trigger the burst scenario."
        )
        assert max_burst_in_window <= 2 * limit, (
            f"burst exceeded 2x limit: {max_burst_in_window} > {2 * limit}. "
            f"this indicates a bug in the sliding window implementation."
        )


# Configuration matrix for slow tests: (limit, window_seconds).
# These cover edge cases and various realistic configurations.
SLIDING_WINDOW_CONFIGS = [
    (3, 0.5),   # Small limit, very short window.
    (5, 0.5),   # Medium limit, very short window.
    (5, 1),     # Medium limit, short window.
    (10, 1),    # Default-ish limit, short window.
    (10, 2),    # Default config (matches fast test).
    (20, 2),    # Higher limit, short window.
    (5, 5),     # Medium limit, longer window.
    (15, 5),    # Higher limit, longer window.
]


@pytest.mark.slow
class TestSlidingWindowBehaviorParametrized:
    """Parameterized sliding window tests across multiple configurations.

    These tests are marked as slow because they use real time.sleep() calls
    and run across multiple window/limit configurations. They verify that
    the sliding window algorithm properties hold regardless of configuration.

    Run with: pytest -m slow
    Skip with: pytest -m "not slow"
    """

    @pytest.fixture
    def sliding_window_limiter(self, request, redis_client, celery_app):
        """Parameterized limiter fixture for sliding window behavior tests.

        The limit and window are injected via indirect parametrization,
        allowing the same test logic to run across multiple configurations.
        """
        limit, window = request.param
        limiter_id = f"sliding_window_param_{limit}_{window}".replace(".", "_")

        limiter = CeleryRateLimiter(
            redis_client=redis_client,
            celery_app=celery_app,
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
        "sliding_window_limiter",
        SLIDING_WINDOW_CONFIGS,
        indirect=True,
        ids=[f"limit={l}, window={w}s" for l, w in SLIDING_WINDOW_CONFIGS],
    )
    def test_long_term_rate_converges_to_limit(
        self, sliding_window_limiter : CeleryRateLimiter, func_path
    ):
        """Verify average consumption rate converges to configured limit.

        This parameterized version runs the convergence test across multiple
        configurations to ensure the algorithm behaves correctly regardless
        of specific limit/window values.
        """
        # Arrange
        # Infer configuration from limiter.
        limit = sliding_window_limiter.limit
        window = sliding_window_limiter.window
        num_windows = 4
        total_duration = num_windows * window

        # Schedule more tasks than we expect to consume.
        for i in range(2 * limit * num_windows):
            sliding_window_limiter.schedule_task(func_path, {"index": i})

        # Act
        # Consume continuously, recording timestamps.
        start_time = time.time()
        timestamps = []

        while time.time() - start_time < total_duration:
            result = consume_and_complete(sliding_window_limiter)
            if result["success"]:
                timestamps.append(time.time())
            else:
                # Rate limited--wait briefly for tokens to recover.
                time.sleep(0.01)

        total_consumed = len(timestamps)
        actual_duration = time.time() - start_time

        # Calculate observed metrics.
        observed_max_burst = 0
        for ts in timestamps:
            count_in_window = sum(1 for t in timestamps if ts <= t < ts + window)
            observed_max_burst = max(observed_max_burst, count_in_window)

        observed_rate = total_consumed / actual_duration * window

        # Steady-state max: see the non-parameterized test for why this bound
        # holds under sustained single-worker load but not in general.
        steady_state_start = start_time + window * 2
        post_burst_timestamps = [ts for ts in timestamps if ts >= steady_state_start]
        observed_steady_state_max = 0
        for ts in post_burst_timestamps:
            count_in_window = sum(1 for t in timestamps if ts <= t < ts + window)
            observed_steady_state_max = max(observed_steady_state_max, count_in_window)

        # Report observed metrics.
        print(f"\n  Parameterized sliding window test results:")
        print(f"    Config: limit={limit}, window={window}s")
        print(f"    Duration: {actual_duration:.2f}s ({num_windows} windows)")
        print(f"    Total consumed: {total_consumed}")
        print(f"    Observed rate: {observed_rate:.2f} requests/window (expected: {limit})")
        print(f"    Max burst in any {window}s window: {observed_max_burst} (max allowed: {2 * limit})")
        print(f"    Steady-state max (after {window * 2:.2f}s): {observed_steady_state_max} (max allowed: {limit + 1})")

        # Assert
        # Account for possible initial boundary burst (up to 1 extra window's worth).
        expected = num_windows * limit
        
        # 25% tolerance for timing variance (slightly more lenient for short windows).
        lower_bound = expected * 0.75
        
        # Possible initial boundary burst.
        upper_bound = (num_windows + 1) * limit

        assert lower_bound <= total_consumed <= upper_bound, (
            f"expected ~{expected} consumed over {num_windows} windows, "
            f"got {total_consumed} (allowed: {lower_bound:.0f}-{upper_bound})"
        )

        # Verify 2x bound.
        max_burst = 2 * limit
        assert observed_max_burst <= max_burst, (
            f"exceeded 2x limit: {observed_max_burst} requests in {window}s window "
            f"(limit={limit}, max_allowed={max_burst})"
        )

        # Verify steady-state max under sustained load (see non-parameterized test).
        if post_burst_timestamps:
            assert observed_steady_state_max <= limit + 1, (
                f"exceeded limit+1 in steady state: {observed_steady_state_max} requests "
                f"in {window}s window (limit={limit}, max_allowed={limit + 1}). "
                f"the sliding window counter should approximate the limit after the initial burst phase."
            )

    @pytest.mark.parametrize(
        "sliding_window_limiter",
        SLIDING_WINDOW_CONFIGS,
        indirect=True,
        ids=[f"limit={l}_window={w}s" for l, w in SLIDING_WINDOW_CONFIGS],
    )
    def test_burst_at_window_boundary_after_empty_window(
        self, sliding_window_limiter: CeleryRateLimiter, func_path
    ):
        """Verify burst behavior across multiple configurations.

        This parameterized version ensures the 2x burst bound holds for
        various limit/window combinations.
        """
        # Arrange
        # Infer configuration from limiter.
        limit = sliding_window_limiter.limit
        window = sliding_window_limiter.window
        
        # Fraction of window to use as "near the end."
        window_tail = 0.05

        # Schedule enough tasks for a potential 2x burst.
        for i in range(limit * 3):
            sliding_window_limiter.schedule_task(func_path, {"index": i})

        # Get reset_in_ms to calculate wait time.
        result = sliding_window_limiter.consume()
        if result["success"]:
            with sliding_window_limiter.task_lifecycle(result["task"]["id"]):
                pass

        reset_ms = result["reset_in_ms"]

        # Calculate wait: finish current window + skip one empty window + position near end.
        wait_seconds = (reset_ms / 1000) + window + (window * (1 - window_tail))
        time.sleep(wait_seconds)

        # Consume rapidly across the window boundary.
        timestamps = []
        num_task_windows = 3
        burst_window = window * (num_task_windows + window_tail)
        start_time = time.time()

        while time.time() - start_time < burst_window:
            result = consume_and_complete(sliding_window_limiter)
            if result["success"]:
                timestamps.append(time.time())

        total_consumed = len(timestamps)
        burst_duration = timestamps[-1] - timestamps[0] if len(timestamps) > 1 else 0

        # Calculate max burst in any window-sized period.
        max_burst_in_window = 0
        for ts in timestamps:
            count_in_window = sum(1 for t in timestamps if ts <= t < ts + window)
            max_burst_in_window = max(max_burst_in_window, count_in_window)

        # Report observed burst.
        print(f"\n  Parameterized window boundary burst test results:")
        print(f"    Config: limit={limit}, window={window}s")
        print(f"    Burst duration: {burst_duration:.3f}s")
        print(f"    Total consumed: {total_consumed}")
        print(f"    Max in any {window}s window: {max_burst_in_window}")
        print(f"    Expected range: {limit} < max_burst <= {2 * limit}")

        # Assert
        # Max burst should exceed limit (demonstrating burst capability) but never
        # exceed 2x limit (the algorithmic upper bound).
        assert max_burst_in_window > limit, (
            f"expected burst to exceed limit ({limit}), got {max_burst_in_window}. "
            f"this may indicate the test didn't trigger the burst scenario."
        )
        assert max_burst_in_window <= 2 * limit, (
            f"burst exceeded 2x limit: {max_burst_in_window} > {2 * limit}. "
            f"this indicates a bug in the sliding window implementation."
        )
