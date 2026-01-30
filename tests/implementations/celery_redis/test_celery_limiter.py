"""Tests for the CeleryRateLimiter implementation.

This module tests the Celery-specific rate limiter implementation. It inherits
contract tests and adds implementation-specific tests for scheduling, Lua scripts,
and payload handling.
"""

import json
from unittest.mock import patch

import pytest
import redis

from tests.contracts.test_rate_limiter_contract import RateLimiterContractTest
from tests.test_utils import is_subset


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
        cursor, results = redis_client.zscan(limiter.buffer_key, match=f'*"{task_id}"*')
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

    def test_schedule_single_task_stores_correctly(self, limiter, redis_client):
        """Verify a single task is stored with all required metadata."""
        # Arrange
        payload = {"user_id": 123}
        func_path = "myapp.tasks.process_data"

        # Act
        success, task_id = limiter.schedule_task(func_path, payload)

        # Assert
        self.assert_task_existence(limiter, redis_client, func_path, payload, task_id)
        assert redis_client.zcard(limiter.buffer_key) == 1, "buffer should contain exactly one task"

    def test_schedule_duplicate_task_skips_second(self, limiter, redis_client):
        """Verify duplicate tasks are not scheduled twice."""
        # Arrange
        payload = {"user_id": 123}
        func_path = "myapp.tasks.process_data"

        # Act
        success_1, task_id_1 = limiter.schedule_task(func_path, payload)
        success_2, task_id_2 = limiter.schedule_task(func_path, payload)

        # Assert
        assert success_1 is True, "first task should be scheduled successfully"
        assert success_2 is False, "duplicate task should not be scheduled"
        assert task_id_1 == task_id_2, "duplicate task should have same ID"
        self.assert_task_existence(limiter, redis_client, func_path, payload, task_id_1)

    def test_schedule_multiple_tasks_with_one_duplicate(self, limiter, redis_client):
        """Verify multiple different tasks can be scheduled with duplicate detection."""
        # Arrange
        payload_1 = {"user_id": 123}
        payload_2 = {"user_id": 456}
        func_path = "myapp.tasks.process_data"

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
    def test_payload_serialization_preserves_data(self, limiter, redis_client, payload):
        """Property: any JSON-serializable payload should survive Redis round-trip."""
        # Arrange
        func_path = "myapp.tasks.process_data"

        # Act
        success, task_id = limiter.schedule_task(func_path, payload)

        # Assert
        assert success is True, "task should be scheduled successfully"
        self.assert_task_existence(limiter, redis_client, func_path, payload, task_id)

    def test_lua_script_recovery_on_noscript_error(self, limiter, redis_client):
        """Verify limiter recovers from NoScriptError by reloading Lua script."""
        # Arrange
        payload = {"user_id": 123}
        func_path = "myapp.tasks.process_data"
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
            success, task_id = limiter.schedule_task(func_path, payload)

            # Assert
            assert success is True, "task should be scheduled after recovery"
            self.assert_task_existence(limiter, redis_client, func_path, payload, task_id)

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