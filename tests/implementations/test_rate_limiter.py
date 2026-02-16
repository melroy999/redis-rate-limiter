"""Generic implementation tests for the AbstractDistributedRateLimiter.

This module validates backend-agnostic behavior by exercising a minimal concrete
limiter implementation.
"""

import json
from unittest.mock import patch

import pytest
import redis

from tests.contracts.test_rate_limiter import RateLimiterContractTest
from tests.helpers.utils import is_subset


class TestRateLimiterImplementation(RateLimiterContractTest):
    """Backend-agnostic implementation tests.

    This class inherits the shared contract suite and supplements it with
    implementation-level checks for scheduling, Lua script recovery, and
    buffer bookkeeping.
    """

    @pytest.fixture
    def limiter(self, generic_limiter):
        """Provide the generic limiter instance under the contract fixture name."""
        return generic_limiter

    # ==================== Helper Methods ====================

    @staticmethod
    def assert_task_existence(
        limiter, redis_client, func_path: str, payload: dict, task_id: str
    ) -> None:
        """Verify that a task exists in Redis with the correct associated data.

        Args:
            limiter: The rate limiter instance under test.
            redis_client: The Redis client used for verification.
            func_path: The function path of the scheduled task.
            payload: The payload data associated with the task.
            task_id: The identifier of the task to verify.
        """
        # Gather the data required to verify the assertions.
        full_data = limiter._get_task_data(task_id, func_path, payload)
        inflight_key = limiter.get_inflight_key(task_id)

        # Assert that the task is marked as in-flight.
        assert redis_client.exists(inflight_key) == 1, (
            f"task {task_id} must be marked as in-flight"
        )

        # Assert that the task appears in the buffer exactly once.
        _, results = redis_client.zscan(limiter.buffer_key, match=f'*"{task_id}"*')
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

    # ==================== Implementation-Specific Tests ====================

    def test_schedule_single_task_stores_correctly(
        self, limiter, redis_client, func_path, default_payload
    ):
        """Verify that a single task is stored with all required metadata."""
        # Act
        _, task_id = limiter.schedule_task(func_path, default_payload)

        # Assert
        self.assert_task_existence(
            limiter, redis_client, func_path, default_payload, task_id
        )
        assert redis_client.zcard(limiter.buffer_key) == 1, (
            "buffer should contain exactly one task"
        )

    def test_schedule_duplicate_task_skips_second(
        self, limiter, redis_client, func_path, default_payload
    ):
        """Verify that duplicate tasks are not scheduled twice."""
        # Act
        success_1, task_id_1 = limiter.schedule_task(func_path, default_payload)
        success_2, task_id_2 = limiter.schedule_task(func_path, default_payload)

        # Assert
        assert success_1 is True, "first task should be scheduled successfully"
        assert success_2 is False, "duplicate task should not be scheduled"
        assert task_id_1 == task_id_2, "duplicate task should have same ID"
        self.assert_task_existence(
            limiter, redis_client, func_path, default_payload, task_id_1
        )

    def test_schedule_multiple_tasks_with_one_duplicate(
        self, limiter, redis_client, func_path
    ):
        """Verify that multiple distinct tasks can be scheduled with duplicate detection."""
        # Arrange
        payload_1 = {"user_id": 123}
        payload_2 = {"user_id": 456}

        # Act
        limiter.schedule_task(func_path, payload_1)
        success_duplicate, task_id_duplicate = limiter.schedule_task(
            func_path, payload_1
        )
        success_new, task_id_new = limiter.schedule_task(func_path, payload_2)

        # Assert
        assert success_duplicate is False, "duplicate should not be scheduled"
        assert success_new is True, "new task should be scheduled"

        self.assert_task_existence(
            limiter, redis_client, func_path, payload_1, task_id_duplicate
        )
        self.assert_task_existence(
            limiter, redis_client, func_path, payload_2, task_id_new
        )
        assert redis_client.zcard(limiter.buffer_key) == 2, (
            "buffer should contain exactly two tasks"
        )

    def test_schedule_task_default_priority_is_100(
        self, limiter, redis_client, func_path, default_payload
    ):
        """Verify that tasks scheduled without an explicit priority use the default value of 100."""
        # Act
        success, _ = limiter.schedule_task(func_path, default_payload)

        # Assert
        assert success is True, "scheduling should succeed"
        members = redis_client.zrange(limiter.buffer_key, 0, -1, withscores=True)
        assert len(members) == 1, "buffer should contain exactly one task"
        _, score = members[0]
        assert score == 100.0, f"default priority should be 100, got {score}"

    def test_schedule_task_uses_max_age_to_set_inflight_ttl(
        self, limiter, redis_client, func_path, default_payload
    ):
        """Verify that the in-flight key TTL is derived from the effective max_age."""
        # Arrange
        per_task_max_age = 17
        expected_ttl = per_task_max_age + limiter.lease_duration + limiter.window

        # Act
        with patch.object(limiter.redis, "set", wraps=redis_client.set) as mocked_set:
            success, _ = limiter.schedule_task(
                func_path, default_payload, max_age=per_task_max_age
            )

        # Assert
        assert success is True, "task should be scheduled successfully"
        mocked_set.assert_called_once()
        assert mocked_set.call_args.kwargs["ex"] == expected_ttl, (
            "inflight key TTL should be derived from max_age + lease_duration + window"
        )

    def test_schedule_task_stores_custom_priority_as_score(
        self, limiter, redis_client, func_path, default_payload
    ):
        """Verify that tasks scheduled with a custom priority store it as the ZSET score."""
        # Arrange
        priority = 42

        # Act
        success, _ = limiter.schedule_task(
            func_path, default_payload, priority=priority
        )

        # Assert
        assert success is True, "scheduling should succeed"
        members = redis_client.zrange(limiter.buffer_key, 0, -1, withscores=True)
        assert len(members) == 1, "buffer should contain exactly one task"
        _, score = members[0]
        assert score == float(priority), (
            f"priority score should be {priority}, got {score}"
        )

    def test_schedule_task_priority_determines_buffer_ordering(
        self, limiter, redis_client
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
            success, _ = limiter.schedule_task(func_path, payload, priority=priority)
            assert success is True, f"task {func_path} should be scheduled"

        # Assert
        members = redis_client.zrange(limiter.buffer_key, 0, -1, withscores=True)
        scores = [score for _, score in members]
        assert scores == [10.0, 50.0, 200.0], (
            f"tasks should be ordered by priority ascending, got scores {scores}"
        )

    def test_schedule_task_equal_priorities_coexist(self, limiter, redis_client):
        """Verify that multiple tasks with the same priority are all stored in the buffer."""
        # Arrange
        priority = 50
        tasks = [
            ("myapp.tasks.task_a", {"id": "a"}),
            ("myapp.tasks.task_b", {"id": "b"}),
        ]

        # Act & Assert
        for func_path, payload in tasks:
            success, _ = limiter.schedule_task(func_path, payload, priority=priority)
            assert success is True, f"task {func_path} should be scheduled"

        # Assert
        members = redis_client.zrange(limiter.buffer_key, 0, -1, withscores=True)
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
            {"msg": "✅ unicode"},
        ],
        ids=["simple_dict", "empty_dict", "nested_dict", "unicode_content"],
    )
    def test_payload_serialization_preserves_data(
        self, limiter, redis_client, payload, func_path
    ):
        """Property: any JSON-serializable payload should survive a Redis round-trip intact."""
        # Act
        success, task_id = limiter.schedule_task(func_path, payload)

        # Assert
        assert success is True, "task should be scheduled successfully"
        self.assert_task_existence(limiter, redis_client, func_path, payload, task_id)

    def test_lua_script_recovery_on_noscript_error(
        self, limiter, redis_client, func_path, default_payload
    ):
        """Verify that the limiter recovers from a ``NoScriptError`` by reloading the Lua script."""
        # Arrange
        real_evalsha = redis_client.evalsha
        real_script_load = redis_client.script_load

        # Create a function that raises NoScriptError only on the first invocation.
        def mocked_evalsha_func(*args, **kwargs):
            if mocked_evalsha_func.call_count == 0:
                mocked_evalsha_func.call_count += 1
                raise redis.exceptions.NoScriptError("NOSCRIPT")
            return real_evalsha(*args, **kwargs)

        mocked_evalsha_func.call_count = 0

        # Act
        with (
            patch.object(
                limiter.redis, "evalsha", side_effect=mocked_evalsha_func
            ) as mock_eval,
            patch.object(
                limiter.redis, "script_load", side_effect=real_script_load
            ) as mock_load,
        ):
            success, task_id = limiter.schedule_task(func_path, default_payload)

            # Assert
            assert success is True, "task should be scheduled after recovery"
            self.assert_task_existence(
                limiter, redis_client, func_path, default_payload, task_id
            )

            # Verify that the recovery path was taken.
            assert mock_eval.call_count == 2, (
                "evalsha should be called twice (fail then retry)"
            )
            assert mock_load.call_count == 1, "script_load should be called to recover"

        # Verify that the script SHA was reloaded and cached.
        assert limiter.schedule_script_sha is not None, (
            "script SHA should be cached after reload"
        )

    def test_lua_script_permanent_failure_raises_error(self, limiter, redis_client):
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
                limiter.schedule_task("path", {})

            # Verify that a retry attempt was made.
            assert mock_eval.call_count == 2, "should attempt retry before failing"

        # Verify cleanup: no tasks should have been added and no in-flight markers should remain.
        task_wildcard = limiter.get_inflight_key("*")
        inflight_keys = redis_client.keys(task_wildcard)
        assert len(inflight_keys) == 0, "no inflight keys should remain after failure"
        assert redis_client.zcard(limiter.buffer_key) == 0, (
            "buffer should be empty after failure"
        )

    def test_schedule_non_noscript_failure_cleans_inflight_and_reraises(
        self, limiter, redis_client, func_path, default_payload
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
                limiter.schedule_task(func_path, default_payload)

        # Assert
        assert "redis down" in str(exc_info.value), (
            "schedule should re-raise the original redis connection error"
        )
        inflight_keys = redis_client.keys(limiter.get_inflight_key("*"))
        assert inflight_keys == [], (
            "inflight marker must be cleared on non-NoScript schedule failure"
        )
        assert redis_client.zcard(limiter.buffer_key) == 0, (
            "failed schedule should not leave buffered tasks behind"
        )

    def test_consume_lua_script_recovery_on_noscript_error(self, limiter, redis_client):
        """Verify that ``consume()`` recovers from a ``NoScriptError`` by reloading the Lua script."""
        # Arrange
        real_evalsha = redis_client.evalsha
        real_script_load = redis_client.script_load

        def mocked_evalsha_func(*args, **kwargs):
            if mocked_evalsha_func.call_count == 0:
                mocked_evalsha_func.call_count += 1
                raise redis.exceptions.NoScriptError("NOSCRIPT")
            return real_evalsha(*args, **kwargs)

        mocked_evalsha_func.call_count = 0

        # Act
        with (
            patch.object(
                limiter.redis, "evalsha", side_effect=mocked_evalsha_func
            ) as mock_eval,
            patch.object(
                limiter.redis, "script_load", side_effect=real_script_load
            ) as mock_load,
        ):
            result = limiter.consume()

            # Assert
            assert result["success"] is False, (
                "consume should return unsuccessful on empty buffer"
            )
            assert mock_eval.call_count == 2, (
                "evalsha should be called twice (fail then retry)"
            )
            assert mock_load.call_count == 1, "script_load should be called to recover"

        assert limiter.consume_script_sha is not None, (
            "script sha should be cached after reload"
        )

    def test_consume_lua_script_permanent_failure_raises_error(self, limiter):
        """Verify that a permanent ``NoScriptError`` during ``consume()`` raises a RuntimeError."""
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
                limiter.consume()

            assert mock_eval.call_count == 2, (
                "consume should attempt one retry before failing"
            )

    def test_consume_connection_error_propagates(self, limiter):
        """Verify that a non-NoScript Redis error during ``consume()`` propagates to the caller."""
        # Arrange
        with patch.object(
            limiter.redis,
            "evalsha",
            side_effect=redis.exceptions.ConnectionError("redis unreachable"),
        ):
            # Act & Assert
            with pytest.raises(
                redis.exceptions.ConnectionError, match="redis unreachable"
            ):
                limiter.consume()

    def test_get_buffer_count_returns_zero_when_empty(self, limiter):
        """Verify that ``get_buffer_count()`` returns zero when no tasks are scheduled."""
        # Act
        count = limiter.get_buffer_count()

        # Assert
        assert count == 0, "empty buffer should report zero tasks"

    def test_get_buffer_count_reflects_scheduled_tasks(self, limiter, func_path):
        """Verify that ``get_buffer_count()`` reflects the number of scheduled tasks."""
        # Arrange
        for idx in range(3):
            limiter.schedule_task(func_path, {"idx": idx})

        # Act
        count = limiter.get_buffer_count()

        # Assert
        assert count == 3, "buffer count should match number of scheduled tasks"
