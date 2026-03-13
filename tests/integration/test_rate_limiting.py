"""Integration tests for rate limiting behaviour.

This module contains end-to-end tests that verify the rate limiter
correctly enforces request limits and handles burst scenarios using
Redis and Lua scripts.

Fixture dependencies from the root ``tests/conftest.py``:
    - ``redis_client``: sync Redis client (namespace-isolated).
    - ``limiter_id``: unique per-test limiter identifier.
    - ``func_path``: static function path string.

Test helpers from ``tests/implementations/conftest``:
    - ``StubRateLimiter``: minimal concrete rate limiter for testing.
    - ``TrackingRateLimiter``: rate limiter that records dispatch and drain calls.

Test helpers from ``tests/integration/conftest``:
    - ``consume_and_complete``: consume and release concurrency slot in one step.
    - ``precise_sleep``: active-polling sleep for sub-second timing precision.
"""

import json
import sys
import time

import pytest

from redis_rate_limiter import AbstractDistributedRateLimiter
from tests.implementations.conftest import StubRateLimiter, TrackingRateLimiter
from tests.integration.conftest import consume_and_complete, precise_sleep

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def integration_limiter(redis_client, limiter_id):
    """Create a rate limiter for integration tests."""
    # Setup
    limiter = StubRateLimiter(
        redis_client=redis_client,
        limiter_id=f"{limiter_id}_integration_default",
        limit=5,
        window=60,
        max_concurrency=2,
        max_age=3600,
        lease_duration=30,
    )

    yield limiter

    # Teardown
    limiter.shutdown()


def wait_until_task_is_expired(
    redis_client, limiter: AbstractDistributedRateLimiter
) -> None:
    """Wait until the oldest queued task is guaranteed to
    have expired according to Redis server time.

    The ``consume.lua`` script computes task age in integer seconds using
    the Redis server clock and expires a task only when its age exceeds
    ``max_age``. This helper polls against that same clock, thereby
    avoiding fixed-sleep flakiness under variable CI or container load.
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
) -> float:
    """Sleep until the current time is positioned at
    ``target_pct`` through a rate limit window.

    An empty-window wait is not required because each
    test uses a unique limiter ID (uuid4) with a flushed
    Redis instance; as such, ``previous_count`` is
    always 0.
    """
    window = limiter.window
    window_ms = int(window * 1000)

    # Determine the Redis server time and compute the fixed window boundaries.
    redis_time = limiter.redis.time()
    redis_now_ms = redis_time[0] * 1000 + redis_time[1] // 1000

    current_window_start_ms = (redis_now_ms // window_ms) * window_ms
    elapsed_ms = redis_now_ms - current_window_start_ms
    current_pct = elapsed_ms / window_ms

    # Sleep until the nearest occurrence of target_pct is reached.
    if current_pct < target_pct:
        # The target lies ahead within the current window.
        target_window_start_ms = current_window_start_ms
    else:
        # The target has already been passed; wait for the next window.
        target_window_start_ms = current_window_start_ms + window_ms

    target_redis_ms = target_window_start_ms + target_pct * window_ms
    wait_s = (target_redis_ms - redis_now_ms) / 1000

    if verbose:
        print("\n  [DEBUG] Positioning via Redis TIME:")
        print(f"    Current position: {current_pct * 100:.1f}% through window")
        print(f"    Target position: {target_pct * 100:.0f}% through window")
        print(f"    Waiting {wait_s:.3f}s to reach target...")

    precise_sleep(wait_s)

    # Verify the position via Redis TIME (no consumption, no side effects).
    redis_time_after = limiter.redis.time()
    redis_after_ms = redis_time_after[0] * 1000 + redis_time_after[1] // 1000
    actual_pct = (redis_after_ms - target_window_start_ms) / window_ms

    if verbose:
        print("\n  [DEBUG] Position after sleep:")
        print(
            f"    Position in window: {actual_pct * 100:.1f}% "
            f"(target: {target_pct * 100:.0f}%)"
        )

    return actual_pct


# ---------------------------------------------------------------------------
# Concrete test cases
# ---------------------------------------------------------------------------


@pytest.mark.behavior
class TestRateLimitingIntegration:
    """Integration tests for rate limiting behaviour with Redis."""

    @staticmethod
    def test_basic_rate_limit_enforcement(integration_limiter, func_path):
        """Verify that the rate limiter enforces the
        configured limit.

        Limiter config: limit=5, window=60.
        """
        # Arrange
        for i in range(10):
            success, _ = integration_limiter.schedule_task(func_path, {"index": i})
            assert success is True, f"task {i} should be scheduled successfully"

        # Act
        results = [consume_and_complete(integration_limiter) for _ in range(10)]
        consumed_count = sum(1 for r in results if r["success"])

        # Assert
        assert consumed_count == 5, "should consume exactly 5 tasks"
        last_successful = [r for r in results if r["success"]][-1]
        assert last_successful["remaining_tokens"] == 0, (
            "last successful consume should exhaust all tokens"
        )
        assert results[-1]["remaining_tasks"] == 5, (
            "five tasks should remain in the buffer after rate limit is reached"
        )

    @staticmethod
    def test_concurrency_limit_enforcement(integration_limiter, func_path):
        """Verify that concurrency limits are enforced
        independently of the rate limit.

        Limiter config: max_concurrency=2.
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
        assert results[0]["success"] is True, "first consume should succeed"
        assert results[1]["success"] is True, "second consume should succeed"
        assert results[0]["active_concurrency"] == 1, (
            "first consume should show 1 active concurrency slot"
        )
        assert results[1]["active_concurrency"] == 2, (
            "second consume should show 2 active concurrency slots"
        )
        assert results[2]["success"] is False, (
            "third consume should fail due to concurrency limit"
        )
        assert results[2]["active_concurrency"] == 2, (
            "concurrency should remain at max after failed consume"
        )

    @staticmethod
    @pytest.mark.parametrize(
        "num_tasks",
        [3, 5, 10, 20],
        ids=["below_limit", "at_limit", "above_limit_10", "above_limit_20"],
    )
    def test_accurate_telemetry_tracking(integration_limiter, num_tasks, func_path):
        """Verify that telemetry accurately tracks the remaining tokens and tasks."""
        # Arrange
        for i in range(num_tasks):
            integration_limiter.schedule_task(func_path, {"index": i})

        # Act
        results = [consume_and_complete(integration_limiter) for _ in range(num_tasks)]

        # Assert
        consumed = sum(1 for r in results if r["success"])
        expected_consumed = min(num_tasks, 5)
        assert consumed == expected_consumed, (
            f"should consume {expected_consumed} of {num_tasks} tasks"
        )

        final_result = results[-1]
        expected_remaining = max(0, num_tasks - consumed)
        assert final_result["remaining_tasks"] == expected_remaining, (
            f"remaining tasks should be {expected_remaining} after consuming {consumed}"
        )

        successful_results = [r for r in results if r["success"]]
        if successful_results:
            expected_tokens = [4, 3, 2, 1, 0]
            actual_tokens = [r["remaining_tokens"] for r in successful_results]
            assert actual_tokens == expected_tokens[: len(successful_results)], (
                "remaining tokens should decrement from 4 to 0"
            )

    @staticmethod
    def test_empty_buffer_returns_no_task(integration_limiter):
        """Verify that consuming from an empty buffer returns an unsuccessful result."""
        # Act
        result = integration_limiter.consume()

        # Assert
        assert result["success"] is False, "consume should fail on empty buffer"
        assert result["task"] is None, "no task should be returned from empty buffer"
        assert result["remaining_tasks"] == 0, (
            "remaining tasks should be 0 on empty buffer"
        )
        assert result["remaining_tokens"] == 5, (
            "all tokens should be available on empty buffer"
        )

    @staticmethod
    def test_bulk_deduplication_only_buffers_one_task(
        integration_limiter, redis_client, func_path
    ):
        """Verify that deduplication is maintained under
        repeated scheduling pressure."""
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

    @staticmethod
    def test_bulk_scheduling_unique_tasks(integration_limiter, redis_client, func_path):
        """Verify the bulk scheduling of unique tasks at
        scale."""
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
            "all task IDs should be unique, "
            f"got {len(set(task_ids))} unique out of {num_tasks}"
        )
        buffer_size = redis_client.zcard(integration_limiter.buffer_key)
        assert buffer_size == num_tasks, (
            f"buffer should contain {num_tasks} tasks, got {buffer_size}"
        )

    @staticmethod
    def test_task_lifecycle_releases_slot_on_error(
        integration_limiter, redis_client, func_path
    ):
        """Verify that the concurrency slot is released
        when a task encounters an error during
        execution."""
        # Arrange
        integration_limiter.schedule_task(func_path, {"index": 0})
        integration_limiter.schedule_task(func_path, {"index": 1})

        # Act
        result = integration_limiter.consume()
        assert result["success"] is True, "first consume should succeed"
        task_id = result["task"]["id"]

        with pytest.raises(RuntimeError, match="simulated task failure"):
            with integration_limiter.task_lifecycle(task_id):
                raise RuntimeError("simulated task failure")

        # Assert
        active_slots = redis_client.zcard(integration_limiter.concurrency_key)
        assert active_slots == 0, (
            "concurrency slot should be released after error, "
            f"got {active_slots} active"
        )
        next_result = consume_and_complete(integration_limiter)
        assert next_result["success"] is True, (
            "next consume should succeed after error cleanup released the slot"
        )

    @staticmethod
    @pytest.mark.slow
    def test_expired_task_moved_to_dlq(redis_client, func_path, limiter_id):
        """Verify that expired queued tasks are moved to the
        DLQ and reported as expired."""
        # Arrange
        limiter = StubRateLimiter(
            redis_client=redis_client,
            limiter_id=f"{limiter_id}_integration_expired_dlq",
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

        # Verify that the DLQ entry contains the original task data.
        dlq_entry = json.loads(redis_client.lindex(limiter.dlq_key, 0))
        assert dlq_entry["func_path"] == func_path, (
            "dlq entry should preserve original func_path"
        )
        assert dlq_entry["payload"] == {"index": 0}, (
            "dlq entry should preserve original payload"
        )

    @staticmethod
    @pytest.mark.slow
    def test_per_task_max_age_override_expires_sooner(
        redis_client, func_path, limiter_id
    ):
        """Verify that a per-task max_age override can cause
        expiration earlier than the global max_age."""
        # Arrange
        limiter = StubRateLimiter(
            redis_client=redis_client,
            limiter_id=f"{limiter_id}_integration_per_task_max_age",
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

    @staticmethod
    def test_per_task_max_age_stored_in_buffer(redis_client, func_path, limiter_id):
        """Verify that ``schedule_task()`` with ``max_age``
        stores the ``__meta_max_age`` field in the
        buffered payload."""
        # Arrange
        limiter = StubRateLimiter(
            redis_client=redis_client,
            limiter_id=f"{limiter_id}_integration_meta_max_age",
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

    @staticmethod
    @pytest.mark.slow
    def test_expired_lease_cleaned_up_on_consume(redis_client, func_path, limiter_id):
        """Verify that stale concurrency lease entries are
        cleaned during consumption."""
        # Arrange
        limiter = StubRateLimiter(
            redis_client=redis_client,
            limiter_id=f"{limiter_id}_integration_stale_lease",
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

    @staticmethod
    def test_get_status_reflects_live_state(
        integration_limiter, redis_client, func_path
    ):
        """Verify that ``get_status()`` mirrors the current
        Redis-backed limiter state."""
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


@pytest.mark.behavior
@pytest.mark.slow
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
    """Tests for the sliding window counter algorithm behaviour.

    The sliding window counter algorithm approximates a true sliding window
    by weighting the previous and current fixed windows. This approach has
    several important implications:

    1. Burst behaviour: At window boundaries, up to 2x the limit may be
       consumed within a short period. This occurs when the previous window
       is empty and requests arrive at the boundary, the algorithm permits
       a full limit from each adjacent window.

    2. Steady-state approximation: Once past the initial window (in which
       the burst can occur), the algorithm approximates the configured limit.
       The uniform-distribution assumption of the counter, combined with
       timing jitter, means the true count in a measurement window may
       exceed the limit by 1, but not more.

    3. Long-term convergence: Despite short-term bursts, the average
       consumption rate over multiple windows converges to the configured
       limit.

    These tests verify long-term rate convergence and opportunistically
    check the 2x burst bound. The burst bound check is not guaranteed to
    detect all violations (as it depends on timing that is not under test
    control), but it will fail if the implementation is fundamentally
    incorrect.

    Note: The 2x burst bound property is formally verified in
    ``tests/properties/test_sliding_window_counter.py`` using the pure
    algorithm extracted from the Lua implementation.
    """

    @pytest.fixture
    def sliding_window_limiter(self, redis_client, limiter_id):
        """Create a rate limiter for sliding window
        behaviour tests."""
        # Setup
        limiter = StubRateLimiter(
            redis_client=redis_client,
            limiter_id=f"{limiter_id}_sliding_window",
            limit=25,
            # 1.0s is less timing-sensitive than 0.5s.
            window=1.0,
            # Set high to isolate rate limiting behaviour.
            max_concurrency=100,
            max_age=3600,
            lease_duration=30,
        )

        yield limiter

        # Teardown
        limiter.shutdown()

    @staticmethod
    def test_long_term_rate_converges_to_limit(sliding_window_limiter, func_path):
        """Verify that the average consumption rate converges to the configured limit.

        Refer to ``tests/integration/README.md`` for the full derivation
        and rationale behind each assertion.
        """
        # Arrange
        # Infer the configuration from the limiter for consistency.
        limit = sliding_window_limiter.limit
        window = sliding_window_limiter.window
        num_windows = 4
        total_duration = num_windows * window

        # The sleep fraction used when the consumer is rate-limited.
        sleep_fraction = 0.05

        # Schedule more tasks than are expected to be consumed.
        for i in range(2 * num_windows * limit):
            sliding_window_limiter.schedule_task(func_path, {"index": i})

        # Act
        # Consume continuously, recording timestamps. A brief sleep is
        # introduced when rate-limited to permit a more even distribution
        # across windows.
        start_time = time.time()
        timestamps = []

        while time.time() - start_time < total_duration:
            result = consume_and_complete(sliding_window_limiter)
            if result["success"]:
                timestamps.append(time.time())
            else:
                # Rate limited: wait for tokens to recover.
                precise_sleep(window * sleep_fraction)

        total_consumed = len(timestamps)
        actual_duration = time.time() - start_time

        # Calculate the observed metrics.
        observed_max_burst = 0
        for ts in timestamps:
            count_in_window = sum(1 for t in timestamps if ts <= t < ts + window)
            observed_max_burst = max(observed_max_burst, count_in_window)

        # Observed rate, expressed in requests per window.
        observed_rate = total_consumed / actual_duration * window

        # Steady-state maximum: skip the first 2W (burst + recovery), then verify
        # that no window-sized period exceeds the derived analytical bound.
        steady_state_start = start_time + window * 2
        post_burst_timestamps = [ts for ts in timestamps if ts >= steady_state_start]
        observed_steady_state_max = 0
        for ts in post_burst_timestamps:
            count_in_window = sum(1 for t in timestamps if ts <= t < ts + window)
            observed_steady_state_max = max(observed_steady_state_max, count_in_window)

        # Report the observed metrics.
        print("\n  Sliding window convergence test results:")
        print(f"    Config: limit={limit}, window={window}s")
        print(f"    Duration: {actual_duration:.2f}s ({num_windows} windows)")
        print(f"    Total consumed: {total_consumed}")
        print(
            f"    Observed rate: {observed_rate:.1f} "
            f"requests/window (expected: {limit})"
        )
        print(
            f"    Max burst in any {window}s window: "
            f"{observed_max_burst} (max allowed: {2 * limit})"
        )
        print(
            f"    Steady-state sliding window max (after {window * 2:.1f}s): "
            f"{observed_steady_state_max}"
        )

        # Assert
        expected = num_windows * limit

        # A 20% tolerance is applied to account for timing variance.
        lower_bound = expected * 0.80

        # The initial burst (empty history + boundary crossing) may contribute
        # up to one additional window's worth of consumption.
        upper_bound = (num_windows + 1) * limit

        assert lower_bound <= total_consumed <= upper_bound, (
            f"expected ~{expected} consumed over {num_windows} windows, "
            f"got {total_consumed} (allowed: {lower_bound:.0f}-{upper_bound})"
        )

        # Verify that no window-sized period exceeded 2x the limit.
        max_burst = 2 * limit
        assert observed_max_burst <= max_burst, (
            f"exceeded 2x limit: {observed_max_burst} requests in {window}s window "
            f"(limit={limit}, max_allowed={max_burst}). "
            f"this indicates a bug in the sliding window implementation."
        )

    @staticmethod
    def test_burst_at_window_boundary_after_empty_window(
        sliding_window_limiter, func_path, request
    ):
        """Verify burst behaviour when consuming across a
        boundary following an empty window.

        The sliding window counter permits up to 2x the limit when:
        1. The previous window is empty (i.e., no consumption occurred).
        2. Consumption begins near the end of the current (empty) window.
        3. Consumption continues into the subsequent window.
        """
        # Arrange
        limit = sliding_window_limiter.limit
        window = sliding_window_limiter.window

        # Position at 80% through the window.
        window_tail = 0.2
        verbose = request.config.getoption("verbose") > 0

        # Schedule a sufficient number of tasks for a potential 2x burst.
        for i in range(limit * 3):
            sliding_window_limiter.schedule_task(func_path, {"index": i})

        # Position at 80% through the window, with an empty previous window.
        position_at_window_percentage(
            sliding_window_limiter, target_pct=1 - window_tail, verbose=verbose
        )

        # Consume rapidly across the window boundary.
        # A time-based loop is used, spanning the tail of the current window plus
        # sufficient additional windows to consume all tasks. This ensures that the
        # burst is captured and that subsequent windows do not introduce anomalies.
        timestamps = []
        num_task_windows = 3
        burst_window = window * (num_task_windows + window_tail)
        start_time = time.time()

        while time.time() - start_time < burst_window:
            result = consume_and_complete(sliding_window_limiter)
            if result["success"]:
                timestamps.append(time.time())
            # No sleep: consume as rapidly as possible to maximise the burst.

        total_consumed = len(timestamps)
        burst_duration = timestamps[-1] - timestamps[0] if len(timestamps) > 1 else 0

        # Calculate the maximum burst observed in any window-sized period.
        max_burst_in_window = 0
        max_burst_start_idx = 0
        for i, ts in enumerate(timestamps):
            count_in_window = sum(1 for t in timestamps if ts <= t < ts + window)
            if count_in_window > max_burst_in_window:
                max_burst_in_window = count_in_window
                max_burst_start_idx = i

        # Verbose debug output (displayed only when the verbose flag is set).
        if verbose:
            # Analyse the consumption gaps.
            if len(timestamps) >= 2:
                first_10_gaps = [
                    timestamps[i + 1] - timestamps[i]
                    for i in range(min(9, len(timestamps) - 1))
                ]
                print(
                    "\n  [DEBUG] First 10 consumption gaps (ms): "
                    f"{[f'{g * 1000:.1f}' for g in first_10_gaps]}"
                )
                print(f"    Fastest gap: {min(first_10_gaps) * 1000:.1f}ms")
                print(f"    Slowest gap in first 10: {max(first_10_gaps) * 1000:.1f}ms")

            # Analyse the maximum burst window.
            if max_burst_in_window > 0:
                burst_start = timestamps[max_burst_start_idx]
                burst_end = burst_start + window
                burst_timestamps = [
                    t for t in timestamps if burst_start <= t < burst_end
                ]
                print("\n  [DEBUG] Max burst window analysis:")
                print(
                    f"    Started at index {max_burst_start_idx}, "
                    f"consumed {max_burst_in_window} tokens"
                )
                print(
                    f"    Time span: "
                    f"{burst_timestamps[0] - timestamps[0]:.3f}s to "
                    f"{burst_timestamps[-1] - timestamps[0]:.3f}s "
                    f"into test"
                )
                if len(burst_timestamps) >= 2:
                    burst_gaps = [
                        burst_timestamps[i + 1] - burst_timestamps[i]
                        for i in range(len(burst_timestamps) - 1)
                    ]
                    print(
                        f"    Average gap in burst window: "
                        f"{sum(burst_gaps) / len(burst_gaps) * 1000:.1f}ms"
                    )
                    print(
                        f"    Burst window duration: "
                        f"{burst_timestamps[-1] - burst_timestamps[0]:.3f}s"
                    )

        # Always report a summary (visible even without the -v flag).
        print("\n  Window boundary burst test results:")
        print(f"    Config: limit={limit}, window={window}s")
        print(f"    Burst duration: {burst_duration:.3f}s")
        print(f"    Total consumed: {total_consumed}")
        print(f"    Max in any {window}s window: {max_burst_in_window}")
        print(f"    Expected range: {limit} < max_burst <= {2 * limit}")

        # Assert
        # The maximum burst in any window should exceed the limit (demonstrating
        # burst behaviour) but must never exceed 2x the limit (the algorithmic
        # upper bound).
        assert max_burst_in_window > limit, (
            f"expected burst to exceed limit ({limit}), got {max_burst_in_window}. "
            f"this may indicate the test did not trigger the burst scenario."
        )
        assert max_burst_in_window <= 2 * limit, (
            f"burst exceeded 2x limit: {max_burst_in_window} > {2 * limit}. "
            f"this indicates a bug in the sliding window implementation."
        )

    @staticmethod
    def test_drain_retry_delay_reflects_token_recovery_not_window_reset(
        redis_client, limiter_id, func_path
    ):
        """Verify that the drain schedules its retry at the
        token recovery interval, not at the window
        reset time.

        After the rate limit is exhausted in window N and the boundary
        into window N+1 is crossed, the previous window's count
        (val_previous=limit) decays linearly. The drain should schedule
        its retry for the point at which the first token becomes available
        via that decay (~window/limit), rather than for the full window
        reset time (~window).

        With limit=25, window=1.0s:
        - Token recovery interval: 1.0/25 = 40ms
        - Window reset time (reset_in_ms): up to ~1000ms
        """
        # Arrange
        limit = 25
        window = 1.0
        token_interval = window / limit  # 0.04s

        limiter = TrackingRateLimiter(
            redis_client=redis_client,
            limiter_id=f"{limiter_id}_drain_delay",
            limit=limit,
            window=window,
            max_concurrency=100,
            max_age=3600,
            lease_duration=30,
            jitter_enabled=False,  # Isolate the base delay calculation from jitter.
        )

        for i in range(limit * 3):
            limiter.schedule_task(func_path, {"index": i})

        for _ in range(limit):
            result = consume_and_complete(limiter)
            assert result["success"] is True, (
                "each consume within the rate limit should succeed"
            )

        # Verify that the rate limit is exhausted within the current window.
        probe = limiter.consume()
        assert probe["success"] is False, "rate limit should be exhausted"
        assert probe["remaining_tokens"] == 0, (
            "all tokens should be exhausted after consuming the full limit"
        )

        # Cross the window boundary such that the current window's count becomes
        # the previous window's count. At this point val_previous=25 and
        # val_current=0, and the sliding window decay is the sole path to
        # token recovery.
        precise_sleep(probe["reset_in_ms"] / 1000 + 0.01)

        # Position early in the new window so that reset_in_ms is large.
        position_at_window_percentage(limiter, target_pct=0.05)

        # Act
        # drain() will invoke consume(), which will be rate-limited, and
        # subsequently call _schedule_drain(delay).
        limiter.scheduled_drains.clear()
        limiter.drain()

        # Assert
        assert len(limiter.scheduled_drains) == 1, (
            "drain should schedule exactly one retry when rate limited"
        )

        scheduled_delay = limiter.scheduled_drains[0]

        # The delay should reflect the token recovery time (~40ms), not the
        # window reset time (~950ms after positioning at 5% of the window).
        # A generous 10x tolerance (400ms) is applied.
        max_acceptable = token_interval * 10

        print("\n  Drain retry delay test results:")
        print(f"    Config: limit={limit}, window={window}s")
        print(f"    Token recovery interval: {token_interval * 1000:.1f}ms")
        print(f"    Scheduled drain delay: {scheduled_delay * 1000:.1f}ms")
        print(f"    Max acceptable: {max_acceptable * 1000:.1f}ms")

        assert scheduled_delay < max_acceptable, (
            f"drain delay {scheduled_delay * 1000:.1f}ms exceeds "
            f"{max_acceptable * 1000:.1f}ms (= 10 * token_interval of "
            f"{token_interval * 1000:.1f}ms). "
            f"the drain should calculate token recovery time from the "
            f"sliding window, not use the full window reset time."
        )
