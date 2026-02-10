"""Contract tests for task lifecycle management.

These tests define the expected behavior for TaskLifecycle context managers
that handle concurrency slot cleanup and task state management.
"""

import pytest


class TaskLifecycleContractTest:
    """Abstract test suite that any TaskLifecycle implementation must pass.

    Subclasses must provide:
        - redis_client: A fixture that returns a Redis client
        - mock_limiter: A fixture that returns a limiter (can be mocked)
        - task_id: A fixture that returns a task ID for testing
        - inflight_key: A fixture that returns the in-flight key for the task
    """

    # ==================== Contract Tests ====================

    @staticmethod
    def test_lifecycle_removes_task_from_concurrency_set(
        redis_client, mock_limiter, task_id, inflight_key, lifecycle_class
    ):
        """Contract: task must be removed from concurrency set after completion."""
        # Arrange
        # Simulate a running task.
        redis_client.zadd(
            mock_limiter.concurrency_key,
            {
                "other_task_1": 100,
                "other_task_2": 100,
                task_id: 100,
            },
        )
        redis_client.set(inflight_key, "1")
        initial_count = redis_client.zcard(mock_limiter.concurrency_key)

        # Act
        with lifecycle_class(mock_limiter, task_id):
            # Do nothing--it simulates a task that just finishes normally.
            pass

        # Assert
        assert redis_client.zcard(mock_limiter.concurrency_key) == initial_count - 1, (
            "concurrency set must have one less task after completion"
        )
        assert redis_client.zscore(mock_limiter.concurrency_key, task_id) is None, (
            f"task {task_id} must be removed from concurrency set"
        )
        assert (
            redis_client.zscore(mock_limiter.concurrency_key, "other_task_1")
            is not None
        ), "other tasks must remain in concurrency set"

    @staticmethod
    def test_lifecycle_removes_active_marker(
        redis_client, mock_limiter, task_id, inflight_key, lifecycle_class
    ):
        """Contract: task inflight marker must be removed after completion."""
        # Arrange
        redis_client.zadd(mock_limiter.concurrency_key, {task_id: 100})
        redis_client.set(inflight_key, "1")

        # Act
        with lifecycle_class(mock_limiter, task_id):
            # Do nothing--simulate a task that finishes successfully.
            pass

        # Assert
        assert redis_client.exists(inflight_key) == 0, (
            f"inflight marker at {inflight_key} must be removed after completion"
        )

    @staticmethod
    def test_lifecycle_cleans_up_on_exception(
        redis_client, mock_limiter, task_id, inflight_key, lifecycle_class
    ):
        """Contract: cleanup must happen even when task raises exception."""
        # Arrange
        redis_client.zadd(mock_limiter.concurrency_key, {task_id: 100})
        redis_client.set(inflight_key, "1")

        # Act
        with pytest.raises(ValueError, match="Task failed"):
            with lifecycle_class(mock_limiter, task_id):
                raise ValueError("Task failed")

        # Assert
        # Cleanup should still happen.
        assert redis_client.zcard(mock_limiter.concurrency_key) == 0, (
            "task must be removed from concurrency set even after exception"
        )
        assert redis_client.exists(inflight_key) == 0, (
            "inflight marker must be removed even after exception"
        )

    @staticmethod
    def test_lifecycle_triggers_consume(
        redis_client, mock_limiter, task_id, inflight_key, lifecycle_class
    ):
        """Contract: lifecycle must trigger consume to process next tasks."""
        # Arrange
        redis_client.zadd(mock_limiter.concurrency_key, {task_id: 100})
        redis_client.set(inflight_key, "1")

        # Act
        with lifecycle_class(mock_limiter, task_id):
            pass

        # Assert
        (
            mock_limiter.trigger_consume.assert_called_once(),
            "trigger_consume must be called to ensure processing doesn't stall",
        )

    @staticmethod
    def test_lifecycle_triggers_consume_even_on_exception(
        redis_client, mock_limiter, task_id, inflight_key, lifecycle_class
    ):
        """Contract: trigger_consume must be called even when task fails."""
        # Arrange
        redis_client.zadd(mock_limiter.concurrency_key, {task_id: 100})
        redis_client.set(inflight_key, "1")

        # Act
        with pytest.raises(RuntimeError):
            with lifecycle_class(mock_limiter, task_id):
                raise RuntimeError("Simulated crash")

        # Assert
        (
            mock_limiter.trigger_consume.assert_called_once(),
            "trigger_consume must be called even after exception",
        )
