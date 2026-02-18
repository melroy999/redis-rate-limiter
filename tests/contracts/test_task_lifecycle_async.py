"""Async contract tests for task lifecycle management.

These tests define the expected behaviour for ``AsyncTaskLifecycle`` context
managers that handle concurrency slot cleanup and task state management.

This module mirrors ``test_task_lifecycle.py`` with async method signatures.
"""

import pytest


class AsyncTaskLifecycleContractTest:
    """Abstract test suite that any ``AsyncTaskLifecycle`` implementation must pass.

    Subclasses are required to provide the following:
        - async_redis_client: An async fixture that returns an async Redis client.
        - mock_limiter: An async fixture that returns a limiter (which may be mocked).
        - task_id: A fixture that returns a task ID for testing.
        - inflight_key: A fixture that returns the in-flight key for the task.
        - lifecycle_class: A fixture that returns the async lifecycle class.
    """

    @staticmethod
    async def test_lifecycle_removes_task_from_concurrency_set(
        async_redis_client, mock_limiter, task_id, inflight_key, lifecycle_class
    ):
        """Contract: the task must be removed from the concurrency set after completion."""
        # Arrange
        # Simulate a running task within the concurrency set.
        await async_redis_client.zadd(
            mock_limiter.concurrency_key,
            {
                "other_task_1": 100,
                "other_task_2": 100,
                task_id: 100,
            },
        )
        await async_redis_client.set(inflight_key, "1")
        initial_count = await async_redis_client.zcard(mock_limiter.concurrency_key)

        # Act
        async with lifecycle_class(mock_limiter, task_id):
            # No action is performed; this simulates a task that completes normally.
            pass

        # Assert
        assert (
            await async_redis_client.zcard(mock_limiter.concurrency_key)
            == initial_count - 1
        ), "concurrency set must have one less task after completion"
        assert (
            await async_redis_client.zscore(mock_limiter.concurrency_key, task_id)
            is None
        ), f"task {task_id} must be removed from concurrency set"
        assert (
            await async_redis_client.zscore(
                mock_limiter.concurrency_key, "other_task_1"
            )
            is not None
        ), "other tasks must remain in concurrency set"

    @staticmethod
    async def test_lifecycle_removes_active_marker(
        async_redis_client, mock_limiter, task_id, inflight_key, lifecycle_class
    ):
        """Contract: the task in-flight marker must be removed after completion."""
        # Arrange
        await async_redis_client.zadd(mock_limiter.concurrency_key, {task_id: 100})
        await async_redis_client.set(inflight_key, "1")

        # Act
        async with lifecycle_class(mock_limiter, task_id):
            # No action is performed; this simulates a task that finishes successfully.
            pass

        # Assert
        assert await async_redis_client.exists(inflight_key) == 0, (
            f"inflight marker at {inflight_key} must be removed after completion"
        )

    @staticmethod
    async def test_lifecycle_cleans_up_on_exception(
        async_redis_client, mock_limiter, task_id, inflight_key, lifecycle_class
    ):
        """Contract: cleanup must be performed even when the task raises an exception."""
        # Arrange
        await async_redis_client.zadd(mock_limiter.concurrency_key, {task_id: 100})
        await async_redis_client.set(inflight_key, "1")

        # Act
        with pytest.raises(ValueError, match="Task failed"):
            async with lifecycle_class(mock_limiter, task_id):
                raise ValueError("Task failed")

        # Assert
        # Cleanup should still have been performed.
        assert await async_redis_client.zcard(mock_limiter.concurrency_key) == 0, (
            "task must be removed from concurrency set even after exception"
        )
        assert await async_redis_client.exists(inflight_key) == 0, (
            "inflight marker must be removed even after exception"
        )

    @staticmethod
    async def test_lifecycle_triggers_consume(
        async_redis_client, mock_limiter, task_id, inflight_key, lifecycle_class
    ):
        """Contract: the lifecycle must trigger consumption to process subsequent tasks."""
        # Arrange
        await async_redis_client.zadd(mock_limiter.concurrency_key, {task_id: 100})
        await async_redis_client.set(inflight_key, "1")

        # Act
        async with lifecycle_class(mock_limiter, task_id):
            pass

        # Assert
        mock_limiter.trigger_consume.assert_awaited_once()

    @staticmethod
    async def test_lifecycle_triggers_consume_even_on_exception(
        async_redis_client, mock_limiter, task_id, inflight_key, lifecycle_class
    ):
        """Contract: ``trigger_consume`` must be called even when the task fails."""
        # Arrange
        await async_redis_client.zadd(mock_limiter.concurrency_key, {task_id: 100})
        await async_redis_client.set(inflight_key, "1")

        # Act
        with pytest.raises(RuntimeError):
            async with lifecycle_class(mock_limiter, task_id):
                raise RuntimeError("Simulated crash")

        # Assert
        mock_limiter.trigger_consume.assert_awaited_once()
