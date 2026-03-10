"""Generic implementation tests for the AbstractDistributedRateLimiter.

This module validates backend-agnostic behavior by exercising both sync
and async concrete limiter implementations. Tests are written once in
async form; the sync implementation participates via the
``SyncToAsyncLimiterAdapter``, while the async implementation runs natively.

Fixture dependencies:
    - ``stub_limiter``, ``async_stub_limiter``:
      from ``tests/implementations/conftest.py``.
    - ``async_redis_client``, ``func_path``, ``payload``: from ``tests/conftest.py``.
"""

import inspect
import json
import logging
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import redis

from redis_rate_limiter.core.limiters import AbstractDistributedRateLimiter
from tests.contracts.test_rate_limiter import RateLimiterContractTest
from tests.helpers.adapters import SyncToAsyncLimiterAdapter
from tests.helpers.utils import assert_log_emitted, is_subset

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def assert_task_existence(
    limiter, async_redis_client, func_path: str, payload: dict, task_id: str
) -> None:
    """Verify that a task exists in Redis with the correct associated data."""
    full_data = limiter._get_task_data(task_id, func_path, payload)
    inflight_key = limiter.get_inflight_key(task_id)

    assert await async_redis_client.exists(inflight_key) == 1, (
        f"task {task_id} must be marked as in-flight"
    )

    all_members = await async_redis_client.zrange(limiter.buffer_key, 0, -1)
    results = [m for m in all_members if f'"{task_id}"' in m]
    assert len(results) > 0, f"task with ID {task_id} not found in buffer"
    assert len(results) == 1, (
        f"task with ID {task_id} has been found more than once in the buffer"
    )

    full_data_server_str = results[0]
    full_data_server = json.loads(full_data_server_str)

    assert is_subset(full_data, full_data_server), (
        f"task with ID {task_id} has a data mismatch"
    )
    assert full_data_server.get("inflight_key") == inflight_key, (
        f"task with ID {task_id} should persist inflight_key for Lua-side cleanup"
    )
    assert "__meta_arrived_at" in full_data_server, (
        f"the __meta_arrived_at tag is missing for task with ID {task_id}"
    )
    assert isinstance(full_data_server["__meta_arrived_at"], int), (
        f"the __meta_arrived_at tag is not an int for task with ID {task_id}"
    )
    assert full_data_server["__meta_arrived_at"] > 0, (
        "arrival timestamp should be positive"
    )


@pytest.mark.contract
class TestRateLimiterContracts(RateLimiterContractTest):
    """Contract compliance for the backend-agnostic rate limiter implementation."""

    @pytest.fixture
    def limiter(self, stub_limiter):
        """Wrap the sync limiter in an async adapter for the unified contracts."""
        return SyncToAsyncLimiterAdapter(stub_limiter)


# ---------------------------------------------------------------------------
# Unified implementation tests
# ---------------------------------------------------------------------------


class RateLimiterImplementationTests:
    """Backend-agnostic implementation tests for scheduling,
    Lua script recovery, and buffer bookkeeping.

    Subclasses must provide a ``limiter`` fixture that returns either a
    ``SyncToAsyncLimiterAdapter``-wrapped sync limiter or a native async
    limiter. All Redis verification uses the ``async_redis_client`` fixture.
    """

    @staticmethod
    def _mock_eval_script(limiter, return_value):
        """Patch ``_eval_script`` on the underlying limiter,
        selecting the correct mock class for sync/async."""
        actual = getattr(limiter, "_inner", limiter)
        mock_cls = (
            AsyncMock if inspect.iscoroutinefunction(actual._eval_script) else MagicMock
        )
        return patch.object(
            actual,
            "_eval_script",
            mock_cls(return_value=return_value),
        )

    @staticmethod
    async def test_schedule_single_task_stores_correctly(
        limiter, async_redis_client, func_path, payload
    ):
        """Verify that a single task is stored with all required metadata."""
        # Act
        _, task_id = await limiter.schedule_task(func_path, payload)

        # Assert
        await assert_task_existence(
            limiter, async_redis_client, func_path, payload, task_id
        )
        assert await async_redis_client.zcard(limiter.buffer_key) == 1, (
            "buffer should contain exactly one task"
        )

    @staticmethod
    async def test_schedule_duplicate_task_skips_second(
        limiter, async_redis_client, func_path, payload
    ):
        """Verify that duplicate tasks are not scheduled twice."""
        # Act
        success_1, task_id_1 = await limiter.schedule_task(func_path, payload)
        success_2, task_id_2 = await limiter.schedule_task(func_path, payload)

        # Assert
        assert success_1 is True, "first task should be scheduled successfully"
        assert success_2 is False, "duplicate task should not be scheduled"
        assert task_id_1 == task_id_2, "duplicate task should have same ID"
        await assert_task_existence(
            limiter, async_redis_client, func_path, payload, task_id_1
        )

    @staticmethod
    async def test_schedule_multiple_tasks_with_one_duplicate(
        limiter, async_redis_client, func_path
    ):
        """Verify that multiple distinct tasks can be scheduled
        with duplicate detection."""
        # Arrange
        payload_1 = {"user_id": 123}
        payload_2 = {"user_id": 456}

        # Act
        await limiter.schedule_task(func_path, payload_1)
        success_duplicate, task_id_duplicate = await limiter.schedule_task(
            func_path, payload_1
        )
        success_new, task_id_new = await limiter.schedule_task(func_path, payload_2)

        # Assert
        assert success_duplicate is False, "duplicate should not be scheduled"
        assert success_new is True, "new task should be scheduled"

        await assert_task_existence(
            limiter, async_redis_client, func_path, payload_1, task_id_duplicate
        )
        await assert_task_existence(
            limiter, async_redis_client, func_path, payload_2, task_id_new
        )
        assert await async_redis_client.zcard(limiter.buffer_key) == 2, (
            "buffer should contain exactly two tasks"
        )

    @staticmethod
    async def test_schedule_task_default_priority_is_100(
        limiter, async_redis_client, func_path, payload
    ):
        """Verify that tasks scheduled without an explicit priority
        use the default value of 100."""
        # Act
        success, _ = await limiter.schedule_task(func_path, payload)

        # Assert
        assert success is True, "scheduling should succeed"
        members = await async_redis_client.zrange(
            limiter.buffer_key, 0, -1, withscores=True
        )
        assert len(members) == 1, "buffer should contain exactly one task"
        _, score = members[0]
        assert score == pytest.approx(100.0), (
            f"default priority should be 100, got {score}"
        )

    @staticmethod
    async def test_schedule_task_uses_max_age_to_set_inflight_ttl(
        limiter, async_redis_client, func_path, payload
    ):
        """Verify that the in-flight key TTL is derived from the effective max_age."""
        # Arrange
        # ceil(max(1, 17) + max(1, 30) + max(1, 60)) = 107
        per_task_max_age = 17
        expected_ttl = 107

        # Act
        success, task_id = await limiter.schedule_task(
            func_path, payload, max_age=per_task_max_age
        )

        # Assert
        assert success is True, "task should be scheduled successfully"
        inflight_key = limiter.get_inflight_key(task_id)
        actual_ttl = await async_redis_client.ttl(inflight_key)
        assert expected_ttl - 2 <= actual_ttl <= expected_ttl, (
            f"inflight key TTL should be ~{expected_ttl}s, got {actual_ttl}s"
        )

    @staticmethod
    async def test_schedule_task_stores_custom_priority_as_score(
        limiter, async_redis_client, func_path, payload
    ):
        """Verify that tasks scheduled with a custom priority
        store it as the ZSET score."""
        # Arrange
        priority = 42

        # Act
        success, _ = await limiter.schedule_task(func_path, payload, priority=priority)

        # Assert
        assert success is True, "scheduling should succeed"
        members = await async_redis_client.zrange(
            limiter.buffer_key, 0, -1, withscores=True
        )
        assert len(members) == 1, "buffer should contain exactly one task"
        _, score = members[0]
        assert score == float(priority), (
            f"priority score should be {priority}, got {score}"
        )

    @staticmethod
    async def test_schedule_task_priority_determines_buffer_ordering(
        limiter, async_redis_client
    ):
        """Verify that tasks are ordered by priority in the buffer,
        with the lowest score consumed first."""
        # Arrange
        tasks = [
            ("myapp.tasks.medium", {"id": "medium"}, 50),
            ("myapp.tasks.high", {"id": "high"}, 10),
            ("myapp.tasks.low", {"id": "low"}, 200),
        ]

        # Act & Assert
        for func_path, payload, priority in tasks:
            success, _ = await limiter.schedule_task(
                func_path, payload, priority=priority
            )
            assert success is True, f"task {func_path} should be scheduled"

        # Assert
        members = await async_redis_client.zrange(
            limiter.buffer_key, 0, -1, withscores=True
        )
        scores = [score for _, score in members]
        assert scores == [10.0, 50.0, 200.0], (
            f"tasks should be ordered by priority ascending, got scores {scores}"
        )

    @staticmethod
    async def test_schedule_task_equal_priorities_coexist(limiter, async_redis_client):
        """Verify that multiple tasks with the same priority
        are all stored in the buffer."""
        # Arrange
        priority = 50
        tasks = [
            ("myapp.tasks.task_a", {"id": "a"}),
            ("myapp.tasks.task_b", {"id": "b"}),
        ]

        # Act & Assert
        for func_path, payload in tasks:
            success, _ = await limiter.schedule_task(
                func_path, payload, priority=priority
            )
            assert success is True, f"task {func_path} should be scheduled"

        # Assert
        members = await async_redis_client.zrange(
            limiter.buffer_key, 0, -1, withscores=True
        )
        assert len(members) == len(tasks), (
            f"buffer should contain all {len(tasks)} tasks"
        )
        scores = [score for _, score in members]
        assert all(s == float(priority) for s in scores), (
            f"all tasks should have priority {priority}, got {scores}"
        )

    @staticmethod
    @pytest.mark.parametrize(
        "payload",
        [
            {"user_id": 123},
            {},
            {"a": [1, 2], "b": {"c": 3}},
            {"msg": "\u2705 unicode"},
        ],
        ids=["simple_dict", "empty_dict", "nested_dict", "unicode_content"],
    )
    async def test_payload_serialization_preserves_data(
        limiter, async_redis_client, payload, func_path
    ):
        """Property: any JSON-serializable payload should survive
        a Redis round-trip intact."""
        # Act
        success, task_id = await limiter.schedule_task(func_path, payload)

        # Assert
        assert success is True, "task should be scheduled successfully"
        await assert_task_existence(
            limiter, async_redis_client, func_path, payload, task_id
        )

    @staticmethod
    async def test_lua_script_permanent_failure_raises_error(
        limiter, async_redis_client
    ):
        """Verify that a permanent Lua script failure raises a RuntimeError."""
        # Arrange
        with patch.object(
            limiter.redis,
            "evalsha",
            side_effect=redis.exceptions.NoScriptError("Permanent Failure"),
        ) as mock_eval:
            # Act & Assert
            with pytest.raises(
                RuntimeError, match="Redis failed to retain the Lua script"
            ):
                await limiter.schedule_task("path", {})

            assert mock_eval.call_count == 2, "should attempt retry before failing"

        task_wildcard = limiter.get_inflight_key("*")
        inflight_keys = await async_redis_client.keys(task_wildcard)
        assert len(inflight_keys) == 0, "no inflight keys should remain after failure"
        assert await async_redis_client.zcard(limiter.buffer_key) == 0, (
            "buffer should be empty after failure"
        )

    @staticmethod
    async def test_schedule_non_noscript_failure_cleans_inflight_and_reraises(
        limiter, async_redis_client, func_path, payload
    ):
        """Verify that non-NoScript schedule failures clean
        the in-flight marker before re-raising."""
        # Arrange
        with patch.object(
            limiter.redis,
            "evalsha",
            side_effect=redis.exceptions.ConnectionError("redis down"),
        ):
            # Act
            with pytest.raises(redis.exceptions.ConnectionError, match="redis down"):
                await limiter.schedule_task(func_path, payload)

        # Assert
        inflight_keys = await async_redis_client.keys(limiter.get_inflight_key("*"))
        assert inflight_keys == [], (
            "inflight marker must be cleared on non-NoScript schedule failure"
        )
        assert await async_redis_client.zcard(limiter.buffer_key) == 0, (
            "failed schedule should not leave buffered tasks behind"
        )

    @staticmethod
    async def test_get_buffer_count_reflects_scheduled_tasks(limiter, func_path):
        """Verify that ``get_buffer_count()`` reflects the number of scheduled tasks."""
        # Arrange
        for idx in range(3):
            await limiter.schedule_task(func_path, {"idx": idx})

        # Act
        count = await limiter.get_buffer_count()

        # Assert
        assert count == 3, "buffer count should match number of scheduled tasks"

    @staticmethod
    async def test_execution_lock_forwards_all_attributes(limiter):
        """Verify that ``execution_lock()`` forwards all limiter
        attributes to the lock."""
        # Act
        lock = limiter.execution_lock()

        # Assert
        # cooldown_ms = min(int((window / limit) * 1000), 1000)
        expected_cooldown = min(
            int((limiter.window / limiter.limit) * 1000),
            1000,
        )
        assert lock.cooldown_ms == expected_cooldown, (
            f"lock cooldown_ms should be {expected_cooldown}, got {lock.cooldown_ms}"
        )
        assert lock.lock_key == limiter.lock_key, (
            "lock_key must be forwarded from the limiter"
        )
        assert lock.worker_id == limiter._worker_id, (
            "worker_id must be forwarded from the limiter"
        )
        assert lock.contention_key == limiter.contention_key, (
            "contention_key must be forwarded from the limiter"
        )
        assert lock.timeout_ms == 5000, (
            "timeout_ms must default to 5000 when not explicitly provided"
        )

    @staticmethod
    async def test_contention_key_follows_redis_key_convention(limiter):
        """Verify that ``contention_key`` is derived from the
        limiter id with the expected suffix."""
        # Assert
        assert limiter.contention_key == f"{limiter.id}:dispatch_lock:contention", (
            "contention_key must follow the {id}:dispatch_lock:contention format"
        )

    @staticmethod
    async def test_execution_lock_cooldown_below_cap_reflects_multiplier(limiter):
        """Verify that cooldown_ms reflects the ``* 1000``
        multiplier when below the cap."""
        # Arrange
        original_window, original_limit = limiter.window, limiter.limit
        limiter.window, limiter.limit = 2, 3

        try:
            # Act
            lock = limiter.execution_lock()

            # Assert
            assert lock.cooldown_ms == 666, (
                f"cooldown_ms should be int((2/3)*1000)=666, got {lock.cooldown_ms}"
            )
        finally:
            limiter.window, limiter.limit = original_window, original_limit

    @staticmethod
    async def test_execution_lock_cooldown_is_zero_when_limit_is_zero(limiter):
        """Verify that ``execution_lock()`` sets ``cooldown_ms``
        to zero when ``limit`` is zero."""
        # Arrange
        original_limit = limiter.limit
        limiter.limit = 0

        try:
            # Act
            lock = limiter.execution_lock()

            # Assert
            assert lock.cooldown_ms == 0, (
                "cooldown_ms should be zero when limit is zero "
                "to avoid division by zero"
            )
        finally:
            limiter.limit = original_limit

    @staticmethod
    async def test_consume_lease_expiry_reflects_configured_duration(
        limiter, async_redis_client, func_path, payload
    ):
        """Verify that ``consume()`` passes ``lease_duration``
        through to the Lua script."""
        # Arrange
        original_lease_duration = limiter.lease_duration
        limiter.lease_duration = 45

        try:
            # Act
            _, task_id = await limiter.schedule_task(func_path, payload)
            result = await limiter.consume()

            # Assert
            assert result["success"] is True, (
                "consume should succeed when a task is available"
            )

            lease_expiry = await async_redis_client.zscore(
                limiter.concurrency_key, task_id
            )
            redis_time = await async_redis_client.time()
            redis_timestamp = redis_time[0]

            assert lease_expiry is not None, (
                "consumed task must appear in the concurrency sorted set"
            )

            # The key distinction is 45 (configured) vs 30 (Lua default).
            lease_remaining = lease_expiry - redis_timestamp
            assert 40 <= lease_remaining <= 50, (
                f"lease remaining should be ~45s (configured), got {lease_remaining}s; "
                "if ~30s, the Lua default is being used instead of the configured value"
            )
        finally:
            limiter.lease_duration = original_lease_duration

    @staticmethod
    async def test_consume_result_index_mapping_is_correct(limiter):
        """Verify that ``consume()`` maps each Lua return index
        to the correct result field."""
        # Arrange
        task_json = json.dumps(
            {
                "id": "sentinel-task",
                "func_path": "tests.helpers.tasks.noop_task",
                "payload": {"sentinel": True},
                "inflight_key": "test:inflight:sentinel-task",
            }
        )
        sentinel_result = [
            "1",  # [0] success flag
            task_json,  # [1] task data
            "100",  # [2] remaining_tokens
            "200",  # [3] active_concurrency
            "300",  # [4] reset_in_ms
            "400",  # [5] remaining_tasks
            "500",  # [6] val_previous
            "600",  # [7] val_current
        ]

        # Act
        with RateLimiterImplementationTests._mock_eval_script(limiter, sentinel_result):
            result = await limiter.consume()

        # Assert
        assert result["success"] is True, "success should be True when result[0] is '1'"
        assert result["remaining_tokens"] == 100, (
            "remaining_tokens must map to result[2]"
        )
        assert result["active_concurrency"] == 200, (
            "active_concurrency must map to result[3]"
        )
        assert result["reset_in_ms"] == 300, "reset_in_ms must map to result[4]"
        assert result["remaining_tasks"] == 400, "remaining_tasks must map to result[5]"
        assert result["val_previous"] == 500, "val_previous must map to result[6]"
        assert result["val_current"] == 600, "val_current must map to result[7]"

    @staticmethod
    async def test_consume_expired_result_sets_correct_flags(limiter):
        """Verify that ``consume()`` correctly parses the expired
        indicator (``result[0]="-1"``)."""
        # Arrange
        task_json = json.dumps(
            {
                "id": "expired-task",
                "func_path": "tests.helpers.tasks.noop_task",
                "payload": {"expired": True},
                "inflight_key": "test:inflight:expired-task",
            }
        )
        sentinel_result = [
            "-1",  # [0] expired flag
            task_json,  # [1] task data (Lua returns task data even for expired tasks)
            "10",  # [2] remaining_tokens
            "0",  # [3] active_concurrency
            "500",  # [4] reset_in_ms
            "3",  # [5] remaining_tasks
            "5",  # [6] val_previous
            "2",  # [7] val_current
        ]

        # Act
        with RateLimiterImplementationTests._mock_eval_script(limiter, sentinel_result):
            result = await limiter.consume()

        # Assert
        assert result["expired"] is True, (
            "expired should be True when result[0] is '-1'"
        )
        assert result["success"] is False, (
            "success should be False when result[0] is '-1'"
        )

    @staticmethod
    async def test_consume_denied_result_sets_correct_flags(limiter):
        """Verify that ``consume()`` correctly parses the denied
        indicator (``result[0]="0"``)."""
        # Arrange
        sentinel_result = [
            "0",  # [0] denied flag
            "",  # [1] no task data
            "0",  # [2] remaining_tokens
            "2",  # [3] active_concurrency
            "100",  # [4] reset_in_ms
            "5",  # [5] remaining_tasks
            "10",  # [6] val_previous
            "5",  # [7] val_current
        ]

        # Act
        with RateLimiterImplementationTests._mock_eval_script(limiter, sentinel_result):
            result = await limiter.consume()

        # Assert
        assert result["success"] is False, (
            "success should be False when result[0] is '0'"
        )
        assert result["expired"] is False, (
            "expired should be False when result[0] is '0'"
        )
        assert result["task"] is None, (
            "task should be None when result[1] is an empty string"
        )

    @staticmethod
    async def test_execution_lock_cooldown_caps_at_1000ms(limiter):
        """Verify that the execution lock cooldown is capped
        at 1000ms for large window/limit ratios."""
        # Arrange
        actual_limiter = getattr(limiter, "_inner", limiter)
        original_window = actual_limiter.window
        original_limit = actual_limiter.limit
        actual_limiter.window = 60
        actual_limiter.limit = 1

        try:
            # Act
            lock = actual_limiter.execution_lock()

            # Assert
            # Without cap: int((60/1) * 1000) = 60000
            # With cap: min(60000, 1000) = 1000
            assert lock.cooldown_ms == 1000, (
                "cooldown should be capped at 1000ms, not the raw value of 60000ms"
            )
        finally:
            actual_limiter.window = original_window
            actual_limiter.limit = original_limit


# ---------------------------------------------------------------------------
# Unified observability tests
# ---------------------------------------------------------------------------


class RateLimiterObservabilityTests:
    """Observability tests for rate limiter scheduling operations.

    These tests verify logging behavior and are separated from the behavioral
    tests in ``RateLimiterImplementationTests`` per the separation of concerns
    guideline (Section 4.1).
    """

    @staticmethod
    async def test_schedule_single_task_emits_expected_logs(
        limiter, func_path, payload, caplog
    ):
        """Verify that scheduling a single task emits the expected
        debug and info logs."""
        # Act
        with caplog.at_level(logging.DEBUG, logger="redis_rate_limiter"):
            _, task_id = await limiter.schedule_task(func_path, payload)

        # Assert
        assert_log_emitted(
            caplog.records,
            "DEBUG",
            [
                f"limiter={limiter.id}",
                f"task_id={task_id}",
                f"func_path={func_path}",
                "priority=100",
            ],
            "should emit a debug log for the scheduling attempt "
            "with limiter id, task id, func path, and priority",
        )
        assert_log_emitted(
            caplog.records,
            "INFO",
            [f"limiter={limiter.id}", f"task_id={task_id}", f"func_path={func_path}"],
            "should emit an info log for the successfully scheduled task",
        )

    @staticmethod
    async def test_schedule_duplicate_task_emits_debug_log(
        limiter, func_path, payload, caplog
    ):
        """Verify that scheduling a duplicate task emits a debug
        log indicating the skip."""
        # Arrange
        await limiter.schedule_task(func_path, payload)

        # Act
        with caplog.at_level(logging.DEBUG, logger="redis_rate_limiter"):
            _, task_id = await limiter.schedule_task(func_path, payload)

        # Assert
        assert_log_emitted(
            caplog.records,
            "DEBUG",
            [f"limiter={limiter.id}", f"task_id={task_id}", "already in-flight"],
            "should emit a debug log for the skipped duplicate "
            "with limiter id and task id",
        )


# ---------------------------------------------------------------------------
# Concrete test cases
# ---------------------------------------------------------------------------


class _SyncLimiterFixture:
    """Shared fixture mixin that provides the sync limiter via the async adapter."""

    @pytest.fixture
    def limiter(self, stub_limiter):
        """Wrap the sync stub limiter in an async adapter."""
        return SyncToAsyncLimiterAdapter(stub_limiter)


class _AsyncLimiterFixture:
    """Shared fixture mixin that provides the async limiter directly."""

    @pytest.fixture
    def limiter(self, async_stub_limiter):
        """Provide the async stub limiter directly."""
        return async_stub_limiter


@pytest.mark.behavior
class TestSyncRateLimiterImplementation(
    _SyncLimiterFixture, RateLimiterImplementationTests
):
    """Sync rate limiter behavior exercised through the async adapter."""


@pytest.mark.observability
class TestSyncRateLimiterObservability(
    _SyncLimiterFixture, RateLimiterObservabilityTests
):
    """Sync rate limiter observability exercised through the async adapter."""


@pytest.mark.behavior
class TestAsyncRateLimiterImplementation(
    _AsyncLimiterFixture, RateLimiterImplementationTests
):
    """Async rate limiter behavior exercised natively."""


@pytest.mark.observability
class TestAsyncRateLimiterObservability(
    _AsyncLimiterFixture, RateLimiterObservabilityTests
):
    """Async rate limiter observability exercised natively."""


# ---------------------------------------------------------------------------
# Signature tests
# ---------------------------------------------------------------------------


@pytest.mark.signature
class TestScheduleTaskSignatures:
    """Signature tests for ``schedule_task()`` default parameter values."""

    @staticmethod
    def test_schedule_task_default_parameters():
        """Verify that ``priority`` and ``max_age`` have the expected defaults.

        Mutation target: ``priority`` and ``max_age`` default values in
        ``AbstractDistributedRateLimiter.schedule_task``.
        """
        # Arrange & Act
        sig = inspect.signature(AbstractDistributedRateLimiter.schedule_task)

        # Assert
        assert sig.parameters["priority"].default == 100, "priority default must be 100"
        assert sig.parameters["max_age"].default is None, "max_age default must be None"
