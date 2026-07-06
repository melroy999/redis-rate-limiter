"""Contract tests for task lifecycle management.

These tests define the expected behaviour for any task lifecycle context
manager implementation. All lifecycle implementations are required to satisfy
these behavioural contracts.

Sync implementations can use the ``SyncToAsyncLifecycleAdapter`` from
``tests.helpers.adapters`` to satisfy the async test interface.

Fixture dependencies:
    - ``async_redis_client``: from ``tests/conftest.py``.
    - ``mock_limiter``, ``task_id``, ``inflight_key``, ``create_lifecycle``:
      provided by this module (or subclass conftest).
"""

import pytest


class TaskLifecycleContractTest:
    """Abstract test suite that any task lifecycle implementation must pass.

    Subclasses are required to provide the following:
        - async_redis_client: An async fixture that returns an async Redis client.
        - mock_limiter: A fixture that returns a limiter (which may be mocked).
            The ``mock_limiter.redis`` attribute must be set to the correct
            Redis client type for the variant (sync or async).
        - task_id: A fixture that returns a task ID for testing.
        - inflight_key: A fixture that returns the in-flight key for the task.
        - create_lifecycle: A fixture that returns a factory for lifecycle
            instances (returning either native async lifecycles or
            ``SyncToAsyncLifecycleAdapter``-wrapped sync lifecycles).

    The factory signature is ``create_lifecycle(limiter, task_id, **kwargs)``.
    """

    @staticmethod
    async def test_lifecycle_starts_healthy(
        async_redis_client, mock_limiter, task_id, inflight_key, create_lifecycle
    ):
        """Contract: the lifecycle must report ``is_healthy=True`` upon entry."""
        # Arrange
        await async_redis_client.zadd(mock_limiter.concurrency_key, {task_id: 100})
        await async_redis_client.set(inflight_key, "1")

        # Act & Assert
        async with create_lifecycle(mock_limiter, task_id) as lifecycle:
            assert lifecycle.is_healthy is True, (
                "lifecycle must start in a healthy state"
            )

    @staticmethod
    async def test_lifecycle_removes_task_from_concurrency_set(
        async_redis_client, mock_limiter, task_id, inflight_key, create_lifecycle
    ):
        """Contract: the task must be removed from the concurrency
        set after completion."""
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
        async with create_lifecycle(mock_limiter, task_id):
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
        assert await async_redis_client.zscore(
            mock_limiter.concurrency_key, "other_task_1"
        ) == pytest.approx(100.0), (
            "other tasks must remain in concurrency set with their original score"
        )

    @staticmethod
    async def test_lifecycle_removes_active_marker(
        async_redis_client, mock_limiter, task_id, inflight_key, create_lifecycle
    ):
        """Contract: the task in-flight marker must be removed after completion."""
        # Arrange
        await async_redis_client.zadd(mock_limiter.concurrency_key, {task_id: 100})
        await async_redis_client.set(inflight_key, "1")

        # Act
        async with create_lifecycle(mock_limiter, task_id):
            # No action is performed; this simulates a task that finishes successfully.
            pass

        # Assert
        assert await async_redis_client.exists(inflight_key) == 0, (
            f"inflight marker at {inflight_key} must be removed after completion"
        )

    @staticmethod
    async def test_lifecycle_cleans_up_on_exception(
        async_redis_client, mock_limiter, task_id, inflight_key, create_lifecycle
    ):
        """Contract: cleanup must be performed even when the task
        raises an exception."""
        # Arrange
        await async_redis_client.zadd(mock_limiter.concurrency_key, {task_id: 100})
        await async_redis_client.set(inflight_key, "1")

        # Act
        with pytest.raises(ValueError, match="Task failed"):
            async with create_lifecycle(mock_limiter, task_id):
                raise ValueError("Task failed")

        # Assert
        assert await async_redis_client.zcard(mock_limiter.concurrency_key) == 0, (
            "task must be removed from concurrency set even after exception"
        )
        assert await async_redis_client.exists(inflight_key) == 0, (
            "inflight marker must be removed even after exception"
        )

    @staticmethod
    async def test_lifecycle_wakes_local_drain(
        async_redis_client, mock_limiter, task_id, inflight_key, create_lifecycle
    ):
        """Contract: the lifecycle must wake the local drain loop after a task completes.

        Cross-process notification is published atomically inside
        ``release.lua``; the only Python-side wake left is
        ``_schedule_drain`` for this worker's own drain loop.
        """
        # Arrange
        await async_redis_client.zadd(mock_limiter.concurrency_key, {task_id: 100})
        await async_redis_client.set(inflight_key, "1")

        # Act
        async with create_lifecycle(mock_limiter, task_id):
            pass

        # Assert
        mock_limiter._schedule_drain.assert_called_once()

    @staticmethod
    async def test_lifecycle_wakes_local_drain_even_on_exception(
        async_redis_client, mock_limiter, task_id, inflight_key, create_lifecycle
    ):
        """Contract: the local drain wake must fire even when the task raises."""
        # Arrange
        await async_redis_client.zadd(mock_limiter.concurrency_key, {task_id: 100})
        await async_redis_client.set(inflight_key, "1")

        # Act
        with pytest.raises(RuntimeError, match="Simulated crash"):
            async with create_lifecycle(mock_limiter, task_id):
                raise RuntimeError("Simulated crash")

        # Assert
        mock_limiter._schedule_drain.assert_called_once()
