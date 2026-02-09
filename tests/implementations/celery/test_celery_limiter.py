"""Tests for the CeleryRateLimiter implementation.

This module tests the Celery-specific rate limiter implementation. It inherits
contract tests and adds implementation-specific tests for scheduling, Lua scripts,
and payload handling.
"""

import json
from unittest.mock import patch

import pytest
import redis
from helpers.utils import is_subset

from tests.contracts.test_rate_limiter import RateLimiterContractTest


class TestCeleryRateLimiter(RateLimiterContractTest):
    """Test CeleryRateLimiter implementation.

    Inherits all contract tests from RateLimiterContractTest and adds
    Celery-specific tests for task scheduling, Lua scripts, and error handling.
    """

    # ==================== Helper Methods ====================

    @staticmethod
    def assert_task_existence(
        limiter, redis_client, func_path: str, payload: dict, task_id: str
    ) -> None:
        """Verify that a task exists in Redis with correct data.

        Args:
            limiter: The limiter instance.
            redis_client: Redis client for verification.
            func_path: Function path of the scheduled task.
            payload: Payload data of the task.
            task_id: ID of the task to verify.
        """
        # Gather data needed to verify assertions.
        enhanced_payload = limiter._get_enhanced_payload(payload, True)
        full_data = limiter._get_task_data(task_id, func_path, enhanced_payload)
        active_key = limiter.get_active_key(task_id)

        # Assert that the task is marked as active.
        assert redis_client.exists(active_key) == 1, (
            f"task {task_id} must be marked as active"
        )

        # Assert that the task is in the buffer only once.
        _, results = redis_client.zscan(limiter.buffer_key, match=f'*"{task_id}"*')
        assert len(results) > 0, f"task with ID {task_id} not found in buffer"
        assert len(results) == 1, (
            f"task with ID {task_id} has been found more than once in the buffer"
        )

        # Assert that the task data remains correct.
        full_data_server_str, score = results[0]
        full_data_server = json.loads(full_data_server_str)

        assert is_subset(full_data, full_data_server), (
            f"task with ID {task_id} has a data mismatch"
        )
        assert "__meta_arrived_at" in full_data_server, (
            f"the __meta_arrived_at tag is missing for task with ID {task_id}"
        )
        assert isinstance(full_data_server["__meta_arrived_at"], int), (
            f"the __meta_arrived_at tag is not an int for task with ID {task_id}"
        )

    # ==================== Implementation-Specific Tests ====================

    def test_schedule_single_task_stores_correctly(self, limiter, redis_client, func_path, default_payload):
        """Verify a single task is stored with all required metadata."""
        # Act
        _, task_id = limiter.schedule_task(func_path, default_payload)

        # Assert
        self.assert_task_existence(limiter, redis_client, func_path, default_payload, task_id)
        assert redis_client.zcard(limiter.buffer_key) == 1, "buffer should contain exactly one task"

    def test_schedule_duplicate_task_skips_second(self, limiter, redis_client, func_path, default_payload):
        """Verify duplicate tasks are not scheduled twice."""
        # Act
        success_1, task_id_1 = limiter.schedule_task(func_path, default_payload)
        success_2, task_id_2 = limiter.schedule_task(func_path, default_payload)

        # Assert
        assert success_1 is True, "first task should be scheduled successfully"
        assert success_2 is False, "duplicate task should not be scheduled"
        assert task_id_1 == task_id_2, "duplicate task should have same ID"
        self.assert_task_existence(limiter, redis_client, func_path, default_payload, task_id_1)

    def test_schedule_multiple_tasks_with_one_duplicate(self, limiter, redis_client, func_path):
        """Verify multiple different tasks can be scheduled with duplicate detection."""
        # Arrange
        payload_1 = {"user_id": 123}
        payload_2 = {"user_id": 456}

        # Act
        limiter.schedule_task(func_path, payload_1)
        success_duplicate, task_id_duplicate = limiter.schedule_task(func_path, payload_1)
        success_new, task_id_new = limiter.schedule_task(func_path, payload_2)

        # Assert
        assert success_duplicate is False, "duplicate should not be scheduled"
        assert success_new is True, "new task should be scheduled"

        self.assert_task_existence(limiter, redis_client, func_path, payload_1, task_id_duplicate)
        self.assert_task_existence(limiter, redis_client, func_path, payload_2, task_id_new)
        assert redis_client.zcard(limiter.buffer_key) == 2, "buffer should contain exactly two tasks"

    def test_schedule_task_default_priority_is_100(self, limiter, redis_client, func_path, default_payload):
        """Verify tasks scheduled without explicit priority use the default priority of 100."""
        # Act
        success, _ = limiter.schedule_task(func_path, default_payload)

        # Assert
        assert success is True, "scheduling should succeed"
        members = redis_client.zrange(limiter.buffer_key, 0, -1, withscores=True)
        assert len(members) == 1, "buffer should contain exactly one task"
        _, score = members[0]
        assert score == 100.0, f"default priority should be 100, got {score}"

    def test_schedule_task_stores_custom_priority_as_score(self, limiter, redis_client, func_path, default_payload):
        """Verify tasks scheduled with a custom priority store it as the ZSET score."""
        # Arrange
        priority = 42

        # Act
        success, _ = limiter.schedule_task(func_path, default_payload, priority=priority)

        # Assert
        assert success is True, "scheduling should succeed"
        members = redis_client.zrange(limiter.buffer_key, 0, -1, withscores=True)
        assert len(members) == 1, "buffer should contain exactly one task"
        _, score = members[0]
        assert score == float(priority), f"priority score should be {priority}, got {score}"

    def test_schedule_task_priority_determines_buffer_ordering(self, limiter, redis_client):
        """Verify tasks are ordered by priority in the buffer (lowest score consumed first)."""
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
        """Verify multiple tasks with the same priority are all stored in the buffer."""
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
    def test_payload_serialization_preserves_data(self, limiter, redis_client, payload, func_path):
        """Property: any JSON-serializable payload should survive Redis round-trip."""
        # Act
        success, task_id = limiter.schedule_task(func_path, payload)

        # Assert
        assert success is True, "task should be scheduled successfully"
        self.assert_task_existence(limiter, redis_client, func_path, payload, task_id)

    def test_lua_script_recovery_on_noscript_error(self, limiter, redis_client, func_path, default_payload):
        """Verify limiter recovers from NoScriptError by reloading Lua script."""
        # Arrange
        real_evalsha = redis_client.evalsha
        real_script_load = redis_client.script_load

        # Create a function that raises NoScriptError only on the first call.
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
            self.assert_task_existence(limiter, redis_client, func_path, default_payload, task_id)

            # Verify recovery path was taken.
            assert mock_eval.call_count == 2, "evalsha should be called twice (fail then retry)"
            assert mock_load.call_count == 1, "script_load should be called to recover"

        # Verify script was reloaded
        assert limiter.schedule_script_sha is not None, "script SHA should be cached after reload"

    def test_lua_script_permanent_failure_raises_error(self, limiter, redis_client):
        """Verify permanent Lua script failure raises RuntimeError."""
        # Arrange
        # Force evalsha to always fail.
        with patch.object(
            limiter.redis,
            "evalsha",
            side_effect=redis.exceptions.NoScriptError("Permanent Failure"),
        ) as mock_eval:
            # Act & Assert
            with pytest.raises(RuntimeError, match="Redis failed to retain the Lua script"):
                limiter.schedule_task("path", {})

            # Verify retry attempt was made
            assert mock_eval.call_count == 2, "should attempt retry before failing"

        # Verify cleanup: no tasks added and no active markers
        task_wildcard = limiter.get_active_key("*")
        active_keys = redis_client.keys(task_wildcard)
        assert len(active_keys) == 0, "no active keys should remain after failure"
        assert redis_client.zcard(limiter.buffer_key) == 0, "buffer should be empty after failure"

    def test_consume_lua_script_recovery_on_noscript_error(self, limiter, redis_client):
        """Verify consume() recovers from NoScriptError by reloading Lua script."""
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
            assert result["success"] is False, "consume should return unsuccessful on empty buffer"
            assert mock_eval.call_count == 2, "evalsha should be called twice (fail then retry)"
            assert mock_load.call_count == 1, "script_load should be called to recover"

        assert limiter.consume_script_sha is not None, "script sha should be cached after reload"

    def test_consume_lua_script_permanent_failure_raises_error(self, limiter):
        """Verify permanent NoScriptError during consume() raises RuntimeError."""
        # Arrange
        with patch.object(
            limiter.redis,
            "evalsha",
            side_effect=redis.exceptions.NoScriptError("Permanent Failure"),
        ) as mock_eval:
            # Act & Assert
            with pytest.raises(RuntimeError, match="Redis failed to retain the Lua script"):
                limiter.consume()

            assert mock_eval.call_count == 2, "consume should attempt one retry before failing"

    def test_get_buffer_count_returns_zero_when_empty(self, limiter):
        """Verify get_buffer_count() returns zero when no tasks are scheduled."""
        # Act
        count = limiter.get_buffer_count()

        # Assert
        assert count == 0, "empty buffer should report zero tasks"

    def test_get_buffer_count_reflects_scheduled_tasks(self, limiter, func_path):
        """Verify get_buffer_count() reflects number of scheduled tasks."""
        # Arrange
        for idx in range(3):
            limiter.schedule_task(func_path, {"idx": idx})

        # Act
        count = limiter.get_buffer_count()

        # Assert
        assert count == 3, "buffer count should match number of scheduled tasks"

    def test_schedule_task_with_use_executor_false_stores_meta(
        self, limiter, redis_client, func_path, default_payload
    ):
        """Verify schedule_task stores use_executor=False in task payload metadata."""
        # Act
        success, task_id = limiter.schedule_task(
            func_path, default_payload, use_executor=False
        )

        # Assert
        assert success is True, "scheduling should succeed"
        _, results = redis_client.zscan(limiter.buffer_key, match=f'*"{task_id}"*')
        assert len(results) == 1, "scheduled task should exist in buffer exactly once"
        task_data = json.loads(results[0][0])
        assert task_data["payload"]["meta"]["use_executor"] is False, (
            "task metadata should store use_executor as false"
        )

    def test_dispatch_task_use_executor_true_sends_generic_worker(
        self, limiter, default_payload
    ):
        """Verify _dispatch_task sends generic worker when use_executor is true."""
        # Arrange
        task_id = "task-id-generic"
        payload = limiter._get_enhanced_payload(default_payload, use_executor=True)

        # Act
        with patch.object(limiter.app, "send_task") as mock_send_task:
            limiter._dispatch_task("myapp.tasks.process", payload, task_id)

            # Assert
            mock_send_task.assert_called_once_with(
                "celery_rate_limiter.generic_worker",
                kwargs={
                    "limiter_id": limiter.id,
                    "func_path": "myapp.tasks.process",
                    "payload": default_payload,
                    "_rate_limit_task_id": task_id,
                },
            )

    def test_dispatch_task_use_executor_false_sends_custom_task(
        self, limiter, default_payload
    ):
        """Verify _dispatch_task sends custom task when use_executor is false."""
        # Arrange
        task_id = "task-id-custom"
        func_path = "myapp.tasks.custom"
        payload = limiter._get_enhanced_payload(default_payload, use_executor=False)

        # Act
        with patch.object(limiter.app, "send_task") as mock_send_task:
            limiter._dispatch_task(func_path, payload, task_id)

            # Assert
            mock_send_task.assert_called_once_with(
                func_path,
                args=[default_payload],
                kwargs={"_rate_limit_task_id": task_id},
            )

    def test_schedule_drain_sends_celery_task_with_correct_args(self, limiter):
        """Verify _schedule_drain sends attempt_consume with expected args."""
        # Arrange
        delay = 1.75

        # Act
        with patch.object(limiter.app, "send_task") as mock_send_task:
            limiter._schedule_drain(delay=delay)

            # Assert
            mock_send_task.assert_called_once_with(
                "celery_rate_limiter.attempt_consume",
                args=[limiter.id],
                countdown=delay,
            )

    def test_enhanced_payload_structure(self, limiter, default_payload):
        """Verify _get_enhanced_payload wraps payload in data/meta structure."""
        # Act
        enhanced_payload = limiter._get_enhanced_payload(
            default_payload, use_executor=False
        )

        # Assert
        assert enhanced_payload["data"] == default_payload, (
            "enhanced payload should preserve original data"
        )
        assert enhanced_payload["meta"] == {"use_executor": False}, (
            "enhanced payload meta should contain use_executor flag"
        )
