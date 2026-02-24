"""Generic implementation tests for the AbstractDistributedRateLimiter.

This module validates backend-agnostic behavior by exercising both sync
and async concrete limiter implementations. Tests are written once in
async form; the sync implementation participates via the
``SyncToAsyncLimiterAdapter``, while the async implementation runs natively.
"""

import inspect
import json
import logging
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import redis

from tests.contracts.test_rate_limiter import RateLimiterContractTest
from tests.helpers.adapters import SyncToAsyncLimiterAdapter
from tests.helpers.utils import is_subset

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def assert_task_existence(
    limiter, async_redis_client, func_path: str, payload: dict, task_id: str
) -> None:
    """Verify that a task exists in Redis with the correct associated data.

    Args:
        limiter: The rate limiter instance under test (sync adapter or async native).
        async_redis_client: The async Redis client used for verification.
        func_path: The function path of the scheduled task.
        payload: The payload data associated with the task.
        task_id: The identifier of the task to verify.
    """
    # Gather the data required to verify the assertions.
    full_data = limiter._get_task_data(task_id, func_path, payload)
    inflight_key = limiter.get_inflight_key(task_id)

    # Assert that the task is marked as in-flight.
    assert await async_redis_client.exists(inflight_key) == 1, (
        f"task {task_id} must be marked as in-flight"
    )

    # Assert that the task appears in the buffer exactly once.
    _, results = await async_redis_client.zscan(
        limiter.buffer_key, match=f'*"{task_id}"*'
    )
    assert len(results) > 0, f"task with ID {task_id} not found in buffer"
    assert len(results) == 1, (
        f"task with ID {task_id} has been found more than once in the buffer"
    )

    # Assert that the persisted task data remains correct.
    full_data_server_str, _score = results[0]
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


class TestRateLimiterContracts(RateLimiterContractTest):
    """Contract compliance for the backend-agnostic rate limiter implementation."""

    @pytest.fixture
    def limiter(self, generic_limiter):
        """Wrap the sync generic limiter in an async adapter for the unified contracts."""
        return SyncToAsyncLimiterAdapter(generic_limiter)


# ---------------------------------------------------------------------------
# Unified implementation tests
# ---------------------------------------------------------------------------


class RateLimiterImplementationTests:
    """Backend-agnostic implementation tests for scheduling, Lua script recovery, and buffer bookkeeping.

    Subclasses must provide a ``limiter`` fixture that returns either a
    ``SyncToAsyncLimiterAdapter``-wrapped sync limiter or a native async
    limiter. All Redis verification uses the ``async_redis_client`` fixture.
    """

    @staticmethod
    async def test_schedule_single_task_stores_correctly(
        limiter, async_redis_client, func_path, payload, caplog
    ):
        """Verify that a single task is stored with all required metadata."""
        # Act
        with caplog.at_level(logging.DEBUG, logger="celery_rate_limiter"):
            _, task_id = await limiter.schedule_task(func_path, payload)

        # Assert
        await assert_task_existence(
            limiter, async_redis_client, func_path, payload, task_id
        )
        assert await async_redis_client.zcard(limiter.buffer_key) == 1, (
            "buffer should contain exactly one task"
        )

        # Verify that the scheduling attempt debug log was emitted.
        assert any(
            record.levelname == "DEBUG"
            and f"limiter={limiter.id}" in record.message
            and f"task_id={task_id}" in record.message
            and f"func_path={func_path}" in record.message
            and "priority=100" in record.message
            for record in caplog.records
        ), (
            "should emit a debug log for the scheduling attempt with limiter id, task id, func path, and priority"
        )

        # Verify that the task scheduled info log was emitted.
        assert any(
            record.levelname == "INFO"
            and f"limiter={limiter.id}" in record.message
            and f"task_id={task_id}" in record.message
            and f"func_path={func_path}" in record.message
            for record in caplog.records
        ), "should emit an info log for the successfully scheduled task"

    @staticmethod
    async def test_schedule_duplicate_task_skips_second(
        limiter, async_redis_client, func_path, payload, caplog
    ):
        """Verify that duplicate tasks are not scheduled twice."""
        # Act
        success_1, task_id_1 = await limiter.schedule_task(func_path, payload)
        with caplog.at_level(logging.DEBUG, logger="celery_rate_limiter"):
            success_2, task_id_2 = await limiter.schedule_task(func_path, payload)

        # Assert
        assert success_1 is True, "first task should be scheduled successfully"
        assert success_2 is False, "duplicate task should not be scheduled"
        assert task_id_1 == task_id_2, "duplicate task should have same ID"
        await assert_task_existence(
            limiter, async_redis_client, func_path, payload, task_id_1
        )

        # Verify that the duplicate-skip debug log was emitted.
        assert any(
            record.levelname == "DEBUG"
            and f"limiter={limiter.id}" in record.message
            and f"task_id={task_id_1}" in record.message
            and "already in-flight" in record.message
            for record in caplog.records
        ), (
            "should emit a debug log for the skipped duplicate with limiter id and task id"
        )

    @staticmethod
    async def test_schedule_multiple_tasks_with_one_duplicate(
        limiter, async_redis_client, func_path
    ):
        """Verify that multiple distinct tasks can be scheduled with duplicate detection."""
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
        """Verify that tasks scheduled without an explicit priority use the default value of 100."""
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
        limiter, func_path, payload
    ):
        """Verify that the in-flight key TTL is derived from the effective max_age."""
        # Arrange
        per_task_max_age = 17
        expected_ttl = per_task_max_age + limiter.lease_duration + limiter.window

        # Act
        with patch.object(limiter.redis, "set", wraps=limiter.redis.set) as mocked_set:
            success, _ = await limiter.schedule_task(
                func_path, payload, max_age=per_task_max_age
            )

        # Assert
        assert success is True, "task should be scheduled successfully"
        mocked_set.assert_called_once()
        assert mocked_set.call_args.kwargs["ex"] == expected_ttl, (
            "inflight key TTL should be derived from max_age + lease_duration + window"
        )

    @staticmethod
    async def test_schedule_task_stores_custom_priority_as_score(
        limiter, async_redis_client, func_path, payload
    ):
        """Verify that tasks scheduled with a custom priority store it as the ZSET score."""
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
        """Verify that tasks are ordered by priority in the buffer, with the lowest score consumed first."""
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
        """Verify that multiple tasks with the same priority are all stored in the buffer."""
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
    @staticmethod
    async def test_payload_serialization_preserves_data(
        limiter, async_redis_client, payload, func_path
    ):
        """Property: any JSON-serializable payload should survive a Redis round-trip intact."""
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
        # Force evalsha to fail on every invocation.
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

            # Verify that a retry attempt was made.
            assert mock_eval.call_count == 2, "should attempt retry before failing"

        # Verify cleanup: no tasks should have been added and no in-flight markers should remain.
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
        """Verify that non-NoScript schedule failures clean the in-flight marker before re-raising."""
        # Arrange
        with patch.object(
            limiter.redis,
            "evalsha",
            side_effect=redis.exceptions.ConnectionError("redis down"),
        ):
            # Act
            with pytest.raises(
                redis.exceptions.ConnectionError, match="redis down"
            ) as exc_info:
                await limiter.schedule_task(func_path, payload)

        # Assert
        assert "redis down" in str(exc_info.value), (
            "schedule should re-raise the original redis connection error"
        )
        inflight_keys = await async_redis_client.keys(limiter.get_inflight_key("*"))
        assert inflight_keys == [], (
            "inflight marker must be cleared on non-NoScript schedule failure"
        )
        assert await async_redis_client.zcard(limiter.buffer_key) == 0, (
            "failed schedule should not leave buffered tasks behind"
        )

    @staticmethod
    async def test_get_buffer_count_returns_zero_when_empty(limiter):
        """Verify that ``get_buffer_count()`` returns zero when no tasks are scheduled."""
        # Act
        count = await limiter.get_buffer_count()

        # Assert
        assert count == 0, "empty buffer should report zero tasks"

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
        """Verify that ``execution_lock()`` forwards all limiter attributes to the lock."""
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
        """Verify that ``contention_key`` is derived from the limiter id with the expected suffix."""
        # Assert
        assert limiter.contention_key == f"{limiter.id}:dispatch_lock:contention", (
            "contention_key must follow the {id}:dispatch_lock:contention format"
        )

    @staticmethod
    async def test_execution_lock_cooldown_below_cap_reflects_multiplier(limiter):
        """Verify that cooldown_ms reflects the ``* 1000`` multiplier when below the cap.

        With ``window=2, limit=3`` the uncapped cooldown is ``int((2/3) * 1000) = 666``,
        well below the 1000ms cap. A mutation that changes ``* 1000`` to ``* 1001``
        would produce 667 instead of 666, which this assertion catches.
        """
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
        """Verify that ``execution_lock()`` sets ``cooldown_ms`` to zero when ``limit`` is zero."""
        # Arrange
        original_limit = limiter.limit
        limiter.limit = 0

        try:
            # Act
            lock = limiter.execution_lock()

            # Assert
            assert lock.cooldown_ms == 0, (
                "cooldown_ms should be zero when limit is zero to avoid division by zero"
            )
        finally:
            limiter.limit = original_limit

    @staticmethod
    async def test_consume_lease_expiry_reflects_configured_duration(
        limiter, async_redis_client, func_path, payload
    ):
        """Verify that ``consume()`` passes ``lease_duration`` through to the Lua script."""
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
        """Verify that ``consume()`` maps each Lua return index to the correct result field.

        Patching ``_eval_script`` with unique sentinel values per index ensures
        that any index-swap mutation (e.g., ``result[5]`` to ``result[6]``) is
        immediately caught.
        """
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

        actual_limiter = getattr(limiter, "_inner", limiter)
        mock_cls = (
            AsyncMock
            if inspect.iscoroutinefunction(actual_limiter._eval_script)
            else MagicMock
        )

        # Act
        with patch.object(
            actual_limiter, "_eval_script", mock_cls(return_value=sentinel_result)
        ):
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


# ---------------------------------------------------------------------------
# Concrete test cases
# ---------------------------------------------------------------------------


class TestSyncRateLimiterImplementation(RateLimiterImplementationTests):
    """Sync rate limiter implementation exercised through the async adapter."""

    @pytest.fixture
    def limiter(self, generic_limiter):
        """Wrap the sync generic limiter in an async adapter."""
        return SyncToAsyncLimiterAdapter(generic_limiter)


class TestAsyncRateLimiterImplementation(RateLimiterImplementationTests):
    """Async rate limiter implementation exercised natively."""

    @pytest.fixture
    def limiter(self, async_generic_limiter):
        """Provide the async generic limiter directly."""
        return async_generic_limiter
