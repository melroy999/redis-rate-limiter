"""Integration tests for rate limiting behavior.

End-to-end tests verifying rate limiter correctly limits requests
and handles burst scenarios using Redis and Lua scripts.
"""

import json
import sys
import time

import pytest

from celery_rate_limiter import AbstractDistributedRateLimiter
from tests.implementations.conftest import MinimalRateLimiter


@pytest.fixture
def integration_limiter(redis_client, default_limiter_id):
    """Create a limiter with explicit configuration for integration tests.

    Config:
        - limit: 5 requests
        - window: 60 seconds
        - max_concurrency: 2 simultaneous tasks
        - max_age: 3600 seconds (1 hour)
        - lease_duration: 30 seconds
    """
    limiter = MinimalRateLimiter(
        redis_client=redis_client,
        limiter_id=f"{default_limiter_id}_integration_default",
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


def precise_sleep(duration_seconds: float) -> None:
    """Sleep for a precise duration using active polling.

    Works around Windows time.sleep() unreliability for sub-second sleeps.
    On Windows, time.sleep() can be off by 10x for small durations due to
    poor timer resolution (~15ms).

    Args:
        duration_seconds: Duration to sleep in seconds.
    """
    target_time = time.time() + duration_seconds
    # Use 1ms sleep intervals to avoid busy-waiting while maintaining precision.
    while time.time() < target_time:
        time.sleep(0.001)


def wait_until_task_is_expired(
    redis_client, limiter: AbstractDistributedRateLimiter
) -> None:
    """Wait until the oldest queued task is guaranteed expired by Redis time.

    consume.lua computes age in integer seconds using Redis server time and
    expires only when age > max_age. This helper waits against that same clock
    to avoid fixed-sleep flakiness under variable CI/container load.
    """
    members = redis_client.zrange(limiter.buffer_key, 0, 0)
    assert len(members) == 1, "expected one queued task before expiry wait"
    task_data = json.loads(members[0])

    arrived_sec = int(task_data["__meta_arrived_at"]) // 1000
    effective_max_age = int(task_data.get("__meta_max_age", limiter.max_age))
    target_expired_sec = arrived_sec + effective_max_age + 1

    deadline = time.time() + 8.0
    while time.time() < deadline:
        redis_sec = int(redis_client.time()[0])
        if redis_sec >= target_expired_sec:
            return
        time.sleep(0.05)

    pytest.fail("timed out waiting for task to become expired by redis server clock")


def position_at_window_percentage(
    limiter, target_pct: float, verbose: bool = False
) -> tuple[dict, float]:
    """Position precisely at a target percentage through a rate limit window.

    Uses two-phase positioning:
    1. Coarse: Wait 2 windows to ensure clean state (empty previous window).
    2. Fine-tune: Measure current position and wait to reach target.

    Args:
        limiter: The rate limiter instance.
        target_pct: Target position as fraction (0.0-1.0, e.g., 0.8 for 80%).
        verbose: Whether to print debug information.

    Returns:
        Tuple of (consume_result, actual_position_pct) at target position.
    """
    window = limiter.window

    # Phase 1: Coarse positioning - wait 2 windows for clean state.
    if verbose:
        print("\n  [DEBUG] Initial positioning:")
        print(
            f"    Waiting {window * 2:.1f}s (2 windows) to ensure empty previous window..."
        )

    result = limiter.consume()
    if result["success"]:
        with limiter.task_lifecycle(result["task"]["id"]):
            pass

    precise_sleep(window * 2)

    # Phase 2: Fine-tune positioning to target percentage.
    check_result = limiter.consume()
    if check_result["success"]:
        with limiter.task_lifecycle(check_result["task"]["id"]):
            pass

    reset_ms = check_result["reset_in_ms"]
    current_pct = (window * 1000 - reset_ms) / (window * 1000)

    if verbose:
        print("\n  [DEBUG] Fine-tuning position:")
        print(f"    Current position: {current_pct * 100:.1f}% through window")
        print(f"    Target position: {target_pct * 100:.1f}% through window")
        print(f"    reset_in_ms: {reset_ms}ms")

    # Calculate wait to reach target.
    if current_pct < target_pct:
        wait_time = (target_pct - current_pct) * window
        if verbose:
            print(f"    Waiting additional {wait_time:.3f}s to reach target...")
        precise_sleep(wait_time)
    else:
        # Passed target, wait for next window + position in that one.
        wait_for_next = reset_ms / 1000
        wait_to_position = target_pct * window
        total_wait = wait_for_next + wait_to_position
        if verbose:
            print(
                f"    Already past target, waiting {total_wait:.3f}s for next window + position..."
            )
        precise_sleep(total_wait)

    # Verify final position.
    final_result = limiter.consume()
    actual_pct = (window * 1000 - final_result["reset_in_ms"]) / (window * 1000)

    if verbose:
        print("\n  [DEBUG] State at target position:")
        print(f"    remaining_tokens: {final_result['remaining_tokens']}")
        print(f"    reset_in_ms: {final_result['reset_in_ms']}ms")
        print(
            f"    Position in window: {actual_pct * 100:.1f}% (target: {target_pct * 100:.0f}%)"
        )

    return final_result, actual_pct


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
    def test_accurate_telemetry_tracking(
        self, integration_limiter, num_tasks, func_path
    ):
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
            assert actual_tokens == expected_tokens[: len(successful_results)]

    def test_empty_buffer_returns_no_task(self, integration_limiter):
        """Verify consuming from empty buffer returns unsuccessful result."""
        # Act
        result = integration_limiter.consume()

        # Assert
        assert result["success"] is False
        assert result["task"] is None
        assert result["remaining_tasks"] == 0
        assert result["remaining_tokens"] == 5

    def test_bulk_deduplication_only_buffers_one_task(
        self, integration_limiter, redis_client, func_path
    ):
        """Verify deduplication holds under repeated scheduling pressure.

        Schedule 50 identical tasks (same func_path and payload). Only the
        first should succeed; the remaining 49 should be rejected as
        duplicates, and the buffer should contain exactly 1 task.
        """
        # Arrange
        payload = {"user_id": 1}
        num_duplicates = 50

        # Act
        results = [
            integration_limiter.schedule_task(func_path, payload)
            for _ in range(num_duplicates)
        ]

        # Assert
        successes = [r for r in results if r[0] is True]
        failures = [r for r in results if r[0] is False]
        assert len(successes) == 1, (
            f"exactly 1 scheduling should succeed, got {len(successes)}"
        )
        assert len(failures) == num_duplicates - 1, (
            f"remaining {num_duplicates - 1} should be rejected as duplicates"
        )
        task_ids = {r[1] for r in results}
        assert len(task_ids) == 1, "all duplicates should produce the same task ID"
        buffer_size = redis_client.zcard(integration_limiter.buffer_key)
        assert buffer_size == 1, f"buffer should contain 1 task, got {buffer_size}"

    def test_bulk_scheduling_unique_tasks(
        self, integration_limiter, redis_client, func_path
    ):
        """Verify bulk scheduling of unique tasks at scale.

        Schedule 100 tasks with unique payloads. All should succeed, produce
        unique task IDs, and the buffer should contain all 100 tasks.
        """
        # Arrange
        num_tasks = 100

        # Act
        results = [
            integration_limiter.schedule_task(func_path, {"index": i})
            for i in range(num_tasks)
        ]

        # Assert
        successes = [r for r in results if r[0] is True]
        assert len(successes) == num_tasks, (
            f"all {num_tasks} tasks should schedule successfully, got {len(successes)}"
        )
        task_ids = [r[1] for r in results]
        assert len(set(task_ids)) == num_tasks, (
            f"all task IDs should be unique, got {len(set(task_ids))} unique out of {num_tasks}"
        )
        buffer_size = redis_client.zcard(integration_limiter.buffer_key)
        assert buffer_size == num_tasks, (
            f"buffer should contain {num_tasks} tasks, got {buffer_size}"
        )

    def test_task_lifecycle_releases_slot_on_error(
        self, integration_limiter, redis_client, func_path
    ):
        """Verify concurrency slot is released when a task errors during execution.

        The full chain: schedule -> consume -> lifecycle error -> cleanup ->
        slot available for next consume.
        """
        # Arrange
        integration_limiter.schedule_task(func_path, {"index": 0})
        integration_limiter.schedule_task(func_path, {"index": 1})

        # Act
        # Consume a task and simulate an error within its lifecycle.
        result = integration_limiter.consume()
        assert result["success"] is True
        task_id = result["task"]["id"]

        with pytest.raises(RuntimeError):
            with integration_limiter.task_lifecycle(task_id):
                raise RuntimeError("simulated task failure")

        # Assert
        active_slots = redis_client.zcard(integration_limiter.concurrency_key)
        assert active_slots == 0, (
            f"concurrency slot should be released after error, got {active_slots} active"
        )
        next_result = consume_and_complete(integration_limiter)
        assert next_result["success"] is True, (
            "next consume should succeed after error cleanup released the slot"
        )

    def test_expired_task_moved_to_dlq(
        self, redis_client, func_path, default_limiter_id
    ):
        """Verify expired queued tasks are moved to DLQ and reported as expired."""
        # Arrange
        limiter = MinimalRateLimiter(
            redis_client=redis_client,
            limiter_id=f"{default_limiter_id}_integration_expired_dlq",
            limit=5,
            window=60,
            max_concurrency=2,
            max_age=1,
            lease_duration=30,
        )
        success, task_id = limiter.schedule_task(func_path, {"index": 0})
        assert success is True, "task should be scheduled successfully"
        wait_until_task_is_expired(redis_client, limiter)

        # Act
        result = limiter.consume()

        # Assert
        assert result["success"] is False, (
            "expired task should not be consumed as success"
        )
        assert result["expired"] is True, "expired task should be flagged as expired"
        assert result["task"] is None, (
            "expired task should not be returned in consume result"
        )
        assert redis_client.llen(limiter.dlq_key) == 1, (
            "expired task should be pushed to dlq"
        )
        assert redis_client.exists(limiter.get_inflight_key(task_id)) == 0, (
            "expired task should clear its inflight marker so it can be rescheduled"
        )

        # Verify DLQ entry contains the original task data.
        dlq_entry = json.loads(redis_client.lindex(limiter.dlq_key, 0))
        assert dlq_entry["func_path"] == func_path, (
            "dlq entry should preserve original func_path"
        )
        assert dlq_entry["payload"] == {"index": 0}, (
            "dlq entry should preserve original payload"
        )

    def test_per_task_max_age_override_expires_sooner(
        self, redis_client, func_path, default_limiter_id
    ):
        """Verify per-task max_age override can expire earlier than global max_age."""
        # Arrange
        limiter = MinimalRateLimiter(
            redis_client=redis_client,
            limiter_id=f"{default_limiter_id}_integration_per_task_max_age",
            limit=5,
            window=60,
            max_concurrency=2,
            max_age=3600,
            lease_duration=30,
        )
        success, task_id = limiter.schedule_task(func_path, {"index": 1}, max_age=1)
        assert success is True, "task should be scheduled successfully"
        wait_until_task_is_expired(redis_client, limiter)

        # Act
        result = limiter.consume()

        # Assert
        assert result["success"] is False, (
            "task should not be consumed after max_age override expiry"
        )
        assert result["expired"] is True, (
            "task should be marked expired by per-task max_age override"
        )
        assert redis_client.llen(limiter.dlq_key) == 1, (
            "expired override task should be moved to dlq"
        )
        assert redis_client.exists(limiter.get_inflight_key(task_id)) == 0, (
            "expired override task should clear its inflight marker"
        )

    def test_per_task_max_age_stored_in_buffer(
        self, redis_client, func_path, default_limiter_id
    ):
        """Verify schedule_task(max_age=...) stores __meta_max_age in buffered payload."""
        # Arrange
        limiter = MinimalRateLimiter(
            redis_client=redis_client,
            limiter_id=f"{default_limiter_id}_integration_meta_max_age",
            limit=5,
            window=60,
            max_concurrency=2,
            max_age=3600,
            lease_duration=30,
        )
        expected_max_age = 7

        # Act
        success, _ = limiter.schedule_task(
            func_path, {"index": 2}, max_age=expected_max_age
        )

        # Assert
        assert success is True, "task should be scheduled successfully"
        members = redis_client.zrange(limiter.buffer_key, 0, -1)
        assert len(members) == 1, "buffer should contain exactly one task"
        task_data = json.loads(members[0])
        assert task_data["__meta_max_age"] == expected_max_age, (
            "buffered task should store per-task max age override"
        )

    def test_expired_lease_cleaned_up_on_consume(
        self, redis_client, func_path, default_limiter_id
    ):
        """Verify stale concurrency lease entries are cleaned during consume."""
        # Arrange
        limiter = MinimalRateLimiter(
            redis_client=redis_client,
            limiter_id=f"{default_limiter_id}_integration_stale_lease",
            limit=5,
            window=60,
            max_concurrency=2,
            max_age=3600,
            lease_duration=30,
        )
        stale_task_id = "stale-task"
        redis_client.zadd(
            limiter.concurrency_key, {stale_task_id: int(time.time()) - 100}
        )
        success, _ = limiter.schedule_task(func_path, {"index": 3})
        assert success is True, "task should be scheduled successfully"

        # Act
        result = limiter.consume()

        # Assert
        assert result["success"] is True, (
            "consume should succeed after stale lease cleanup"
        )
        assert redis_client.zscore(limiter.concurrency_key, stale_task_id) is None, (
            "stale lease entry should be removed during consume"
        )
        assert result["task"] is not None, "consume should return a task after cleanup"
        consumed_task_id = result["task"]["id"]
        assert (
            redis_client.zscore(limiter.concurrency_key, consumed_task_id) is not None
        ), "newly consumed task should be present in concurrency set"

    def test_get_status_reflects_live_state(
        self, integration_limiter, redis_client, func_path
    ):
        """Verify get_status() mirrors current Redis-backed limiter state."""
        # Arrange
        for idx in range(3):
            integration_limiter.schedule_task(func_path, {"index": idx})
        integration_limiter.consume()

        # Act
        status = integration_limiter.get_status()

        # Assert
        current_concurrency = int(status["concurrency"]["current"])
        assert status["limiter_id"] == integration_limiter.id, (
            "status limiter id should match limiter instance"
        )
        assert int(status["buffer"]["count"]) == redis_client.zcard(
            integration_limiter.buffer_key
        ), "status buffer count should match redis zcard"
        assert current_concurrency == redis_client.zcard(
            integration_limiter.concurrency_key
        ), "status concurrency current should match redis zcard"
        assert status["concurrency"]["max"] == integration_limiter.max_concurrency, (
            "status concurrency max should match limiter configuration"
        )
        assert status["concurrency"]["available"] == max(
            0, integration_limiter.max_concurrency - current_concurrency
        ), "status concurrency available should match computed capacity"
        assert status["rate_limit"]["limit"] == integration_limiter.limit, (
            "status rate-limit limit should match limiter configuration"
        )
        assert status["dispatcher"]["is_locked"] == redis_client.exists(
            integration_limiter.lock_key
        ), "status lock state should match redis lock key presence"


@pytest.mark.skipif(
    sys.platform == "win32",
    reason=(
        "Sliding window tests require sub-second timing precision. "
        "Windows time.sleep() and timer resolution (~15ms) are too coarse "
        "for reliable results. Run in Docker instead: "
        "docker compose --profile test up"
    ),
)
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
    def sliding_window_limiter(self, redis_client, default_limiter_id):
        """Limiter with production-realistic settings for sliding window behavior tests.

        Config:
            - limit: 25 requests per window (production setting)
            - window: 1.0 seconds (production setting, less timing-sensitive than 0.5s)
            - max_concurrency: 50 (high to isolate rate limiting behavior)
        """
        limiter = MinimalRateLimiter(
            redis_client=redis_client,
            limiter_id=f"{default_limiter_id}_sliding_window",
            limit=25,
            window=1.0,
            max_concurrency=100,
            max_age=3600,
            lease_duration=30,
        )

        yield limiter

        keys = redis_client.keys(f"{limiter.id}:*")
        if keys:
            redis_client.delete(*keys)

    def test_long_term_rate_converges_to_limit(self, sliding_window_limiter, func_path):
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
        # Sleep when rate-limited to allow more even distribution across windows.
        start_time = time.time()
        timestamps = []

        while time.time() - start_time < total_duration:
            result = consume_and_complete(sliding_window_limiter)
            if result["success"]:
                timestamps.append(time.time())
            else:
                # Rate limited--wait for tokens to recover.
                # Use 5% of window duration for realistic pacing.
                precise_sleep(window * 0.05)

        total_consumed = len(timestamps)
        actual_duration = time.time() - start_time

        # Calculate observed metrics.
        observed_max_burst = 0
        for ts in timestamps:
            count_in_window = sum(1 for t in timestamps if ts <= t < ts + window)
            observed_max_burst = max(observed_max_burst, count_in_window)

        # Requests per window.
        observed_rate = total_consumed / actual_duration * window

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
        print("\n  Sliding window test results:")
        print(f"    Config: limit={limit}, window={window}s")
        print(f"    Duration: {actual_duration:.2f}s ({num_windows} windows)")
        print(f"    Total consumed: {total_consumed}")
        print(
            f"    Observed rate: {observed_rate:.2f} requests/window (expected: {limit})"
        )
        print(
            f"    Max burst in any {window}s window: {observed_max_burst} (max allowed: {2 * limit})"
        )
        print(
            f"    Steady-state max (after {window * 2:.2f}s): {observed_steady_state_max} (max allowed: {limit + 1})"
        )

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
        self, sliding_window_limiter, func_path, request
    ):
        """Verify burst behavior when consuming across a boundary after an empty window.

        The sliding window counter allows up to 2x limit when:
        1. The previous window is empty (no consumption)
        2. Consumption starts near the end of the current (empty) window
        3. Consumption continues into the next window
        """
        # Arrange
        limit = sliding_window_limiter.limit
        window = sliding_window_limiter.window

        # Position at 80% through window.
        window_tail = 0.2
        verbose = request.config.getoption("verbose") > 0

        # Schedule enough tasks for a potential 2x burst.
        for i in range(limit * 3):
            sliding_window_limiter.schedule_task(func_path, {"index": i})

        # Position at 80% through window with empty previous window.
        pre_burst_result, actual_pct = position_at_window_percentage(
            sliding_window_limiter, target_pct=1 - window_tail, verbose=verbose
        )

        # Complete the positioning consume and start timestamp tracking.
        if pre_burst_result["success"]:
            with sliding_window_limiter.task_lifecycle(pre_burst_result["task"]["id"]):
                pass
            timestamps = [time.time()]
        else:
            timestamps = []

        # Consume rapidly across the window boundary.
        # Use a time-based loop: tail of current window + enough windows to consume all tasks.
        # This ensures we capture the burst and verify subsequent windows don't cause issues.
        # We scheduled limit * 3 tasks.
        num_task_windows = 3
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
        max_burst_start_idx = 0
        for i, ts in enumerate(timestamps):
            count_in_window = sum(1 for t in timestamps if ts <= t < ts + window)
            if count_in_window > max_burst_in_window:
                max_burst_in_window = count_in_window
                max_burst_start_idx = i

        # Verbose debug output.
        if verbose:
            # Analyze consumption gaps.
            if len(timestamps) >= 2:
                first_10_gaps = [
                    timestamps[i + 1] - timestamps[i]
                    for i in range(min(9, len(timestamps) - 1))
                ]
                print(
                    f"\n  [DEBUG] First 10 consumption gaps (ms): {[f'{g * 1000:.1f}' for g in first_10_gaps]}"
                )
                print(f"    Fastest gap: {min(first_10_gaps) * 1000:.1f}ms")
                print(f"    Slowest gap in first 10: {max(first_10_gaps) * 1000:.1f}ms")

            # Analyze the max burst window.
            if max_burst_in_window > 0:
                burst_start = timestamps[max_burst_start_idx]
                burst_end = burst_start + window
                burst_timestamps = [
                    t for t in timestamps if burst_start <= t < burst_end
                ]
                print("\n  [DEBUG] Max burst window analysis:")
                print(
                    f"    Started at index {max_burst_start_idx}, consumed {max_burst_in_window} tokens"
                )
                print(
                    f"    Time span: {burst_timestamps[0] - timestamps[0]:.3f}s to {burst_timestamps[-1] - timestamps[0]:.3f}s into test"
                )
                if len(burst_timestamps) >= 2:
                    burst_gaps = [
                        burst_timestamps[i + 1] - burst_timestamps[i]
                        for i in range(len(burst_timestamps) - 1)
                    ]
                    print(
                        f"    Average gap in burst window: {sum(burst_gaps) / len(burst_gaps) * 1000:.1f}ms"
                    )
                    print(
                        f"    Burst window duration: {burst_timestamps[-1] - burst_timestamps[0]:.3f}s"
                    )

        # Always report summary (visible even without -v).
        print("\n  Window boundary burst test results:")
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
    (3, 0.5),  # Small limit, very short window.
    (5, 0.5),  # Medium limit, very short window.
    (5, 1),  # Medium limit, short window.
    (10, 1),  # Default-ish limit, short window.
    (10, 2),  # Default config (matches fast test).
    (20, 2),  # Higher limit, short window.
    (5, 5),  # Medium limit, longer window.
    (15, 5),  # Higher limit, longer window.
]


@pytest.mark.slow
@pytest.mark.skipif(
    sys.platform == "win32",
    reason=(
        "Sliding window tests require sub-second timing precision. "
        "Windows time.sleep() and timer resolution (~15ms) are too coarse "
        "for reliable results. Run in Docker instead: "
        "docker compose --profile test-all up"
    ),
)
class TestSlidingWindowBehaviorParametrized:
    """Parameterized sliding window tests across multiple configurations.

    These tests are marked as slow because they use real time.sleep() calls
    and run across multiple window/limit configurations. They verify that
    the sliding window algorithm properties hold regardless of configuration.

    Run with: pytest -m slow
    Skip with: pytest -m "not slow"
    """

    @pytest.fixture
    def sliding_window_limiter(self, request, redis_client, default_limiter_id):
        """Parameterized limiter fixture for sliding window behavior tests.

        The limit and window are injected via indirect parametrization,
        allowing the same test logic to run across multiple configurations.
        """
        limit, window = request.param
        limiter_id = (
            f"{default_limiter_id}_sliding_window_param_{limit}_{window}".replace(
                ".", "_"
            )
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
        "sliding_window_limiter",
        SLIDING_WINDOW_CONFIGS,
        indirect=True,
        ids=[
            f"limit={limit_value}, window={window_value}s"
            for limit_value, window_value in SLIDING_WINDOW_CONFIGS
        ],
    )
    def test_long_term_rate_converges_to_limit(
        self, sliding_window_limiter: MinimalRateLimiter, func_path
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
                # Rate limited--wait for tokens to recover.
                # Use 5% of window duration for realistic pacing.
                precise_sleep(window * 0.05)

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
        print("\n  Parameterized sliding window test results:")
        print(f"    Config: limit={limit}, window={window}s")
        print(f"    Duration: {actual_duration:.2f}s ({num_windows} windows)")
        print(f"    Total consumed: {total_consumed}")
        print(
            f"    Observed rate: {observed_rate:.2f} requests/window (expected: {limit})"
        )
        print(
            f"    Max burst in any {window}s window: {observed_max_burst} (max allowed: {2 * limit})"
        )
        print(
            f"    Steady-state max (after {window * 2:.2f}s): {observed_steady_state_max} (max allowed: {limit + 1})"
        )

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
                f"in {window}s window (limit={limit}, max_allowed={limit + 1}), "
                f"the sliding window counter should approximate the limit after the initial burst phase"
            )

    @pytest.mark.parametrize(
        "sliding_window_limiter",
        SLIDING_WINDOW_CONFIGS,
        indirect=True,
        ids=[
            f"limit={limit_value}_window={window_value}s"
            for limit_value, window_value in SLIDING_WINDOW_CONFIGS
        ],
    )
    def test_burst_at_window_boundary_after_empty_window(
        self, sliding_window_limiter: MinimalRateLimiter, func_path, request
    ):
        """Verify burst behavior across multiple configurations.

        This parameterized version ensures the 2x burst bound holds for
        various limit/window combinations.
        """
        # Arrange
        limit = sliding_window_limiter.limit
        window = sliding_window_limiter.window

        # Position at 80% through window.
        window_tail = 0.2
        verbose = request.config.getoption("verbose") > 0

        # Schedule enough tasks for a potential 2x burst.
        for i in range(limit * 3):
            sliding_window_limiter.schedule_task(func_path, {"index": i})

        # Position at 80% through window with empty previous window.
        pre_burst_result, actual_pct = position_at_window_percentage(
            sliding_window_limiter, target_pct=1 - window_tail, verbose=verbose
        )

        # Complete the positioning consume and start timestamp tracking.
        if pre_burst_result["success"]:
            with sliding_window_limiter.task_lifecycle(pre_burst_result["task"]["id"]):
                pass
            timestamps = [time.time()]
        else:
            timestamps = []

        # Consume rapidly across the window boundary.
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
        print("\n  Parameterized window boundary burst test results:")
        print(f"    Config: limit={limit}, window={window}s")
        print(f"    Burst duration: {burst_duration:.3f}s")
        print(f"    Total consumed: {total_consumed}")
        print(f"    Max in any {window}s window: {max_burst_in_window}")
        print(f"    Expected range: {limit} < max_burst <= {2 * limit}")

        # Assert
        # Max burst should exceed limit (demonstrating burst capability) but never
        # exceed 2x limit (the algorithmic upper bound).
        assert max_burst_in_window > limit, (
            f"expected burst to exceed limit ({limit}), got {max_burst_in_window}, "
            f"this may indicate the test didn't trigger the burst scenario"
        )
        assert max_burst_in_window <= 2 * limit, (
            f"burst exceeded 2x limit: {max_burst_in_window} > {2 * limit}, "
            f"this indicates a bug in the sliding window implementation"
        )
