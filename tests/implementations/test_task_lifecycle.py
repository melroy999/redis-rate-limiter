"""Tests for the TaskLifecycle context manager.

This module tests the TaskLifecycle implementation that manages concurrency
slots and task cleanup. It inherits the contract tests and adds generic
implementation-specific tests that operate with any rate limiter implementation.
"""

import os
import signal
import time
from typing import Literal
from unittest.mock import MagicMock, patch

import pytest

from celery_rate_limiter import TaskLifecycle
from tests.contracts.test_task_lifecycle import TaskLifecycleContractTest
from tests.implementations.conftest import MinimalRateLimiter

HeartbeatFailureMode = Literal["warn", "kill"]
HEARTBEAT_OVERRIDE_CASES: list[tuple[HeartbeatFailureMode, HeartbeatFailureMode]] = [
    ("warn", "kill"),
    ("kill", "warn"),
]


@pytest.fixture
def mock_limiter(redis_client, task_id):
    """Create a mock limiter that uses the real Redis client but mocks internal helpers.

    This fixture provides a limiter with real Redis operations but mocked
    Celery-specific methods, thereby avoiding the need for a full Celery setup.
    """
    limiter = MagicMock()
    limiter.redis = redis_client
    limiter.concurrency_key = "test:concurrency"
    limiter.id = "test_limiter"
    limiter.get_inflight_key.side_effect = lambda _: f"test:inflight:{task_id}"

    # A short duration is used for fast test execution.
    limiter.lease_duration = 0.2

    # Configure the return value for background thread tests.
    limiter.extend_lease.return_value = None
    return limiter


@pytest.fixture
def inflight_key(mock_limiter, task_id):
    """Provide the in-flight key for the test task."""
    return mock_limiter.get_inflight_key(task_id)


@pytest.fixture
def create_lifecycle():
    """Provide a factory for sync ``TaskLifecycle`` instances wrapped in an async adapter.

    The unified contract tests use ``async with create_lifecycle(limiter, task_id):``,
    so the sync lifecycle is wrapped in a ``SyncToAsyncLifecycleAdapter``.
    """
    from tests.helpers.adapters import SyncToAsyncLifecycleAdapter

    def _factory(limiter, task_id, **kwargs):
        return SyncToAsyncLifecycleAdapter(TaskLifecycle(limiter, task_id, **kwargs))

    return _factory


class TestTaskLifecycle(TaskLifecycleContractTest):
    """Contract compliance for the sync TaskLifecycle context manager implementation."""

    pass


class TestTaskLifecycleImplementation:
    """Tests for implementation-specific behaviour of the TaskLifecycle context manager."""

    @staticmethod
    def test_lifecycle_with_multiple_concurrent_tasks(
        redis_client, mock_limiter, task_id, inflight_key
    ):
        """Verify that the lifecycle only removes the specific task from the concurrency set."""
        # Arrange
        # Simulate five concurrent tasks.
        concurrent_tasks = {
            "other_task_1": 100,
            "other_task_2": 100,
            "other_task_3": 100,
            "other_task_4": 100,
            task_id: 100,
        }
        redis_client.zadd(mock_limiter.concurrency_key, concurrent_tasks)
        redis_client.set(inflight_key, "1")

        # Act & Assert
        # Prevent the heartbeat thread from starting.
        with patch("threading.Thread"):
            with TaskLifecycle(mock_limiter, task_id):
                # During execution, all five tasks should be present.
                assert redis_client.zcard(mock_limiter.concurrency_key) == 5, (
                    "all five tasks should be present during execution"
                )

        # After completion, only the target task should have been removed.
        assert redis_client.zcard(mock_limiter.concurrency_key) == 4, (
            "concurrency set should have four tasks after target completion"
        )
        assert redis_client.zscore(mock_limiter.concurrency_key, task_id) is None, (
            "target task must be removed from concurrency set"
        )
        assert (
            redis_client.zscore(mock_limiter.concurrency_key, "other_task_1")
            is not None
        ), "other tasks must remain in concurrency set"
        assert redis_client.exists(inflight_key) == 0, (
            "inflight marker must be removed after completion"
        )

    @staticmethod
    def test_lifecycle_handles_redis_failure_during_cleanup(
        redis_client, mock_limiter, task_id, inflight_key
    ):
        """Verify that the lifecycle raises an exception but still triggers consume on Redis failure."""
        # Arrange
        with patch.object(
            mock_limiter.redis,
            "zrem",
            side_effect=Exception("Redis connection lost"),
        ) as mock_zrem:
            # Act & Assert
            with patch("threading.Thread"):
                with pytest.raises(Exception, match="Redis connection lost"):
                    with TaskLifecycle(mock_limiter, task_id):
                        pass

                # Verify that the exception originated from zrem.
                mock_zrem.assert_called_once()

        # Assert that trigger_consume is still invoked.
        # noinspection PyUnboundLocalVariable
        mock_limiter.trigger_consume.assert_called_once()

    @pytest.mark.parametrize(
        "original, override",
        HEARTBEAT_OVERRIDE_CASES,
        ids=["default_warn_override_kill", "default_kill_override_warn"],
    )
    @staticmethod
    def test_heartbeat_failure_override_precedence(
        redis_client,
        task_id,
        limiter_id,
        original: HeartbeatFailureMode,
        override: HeartbeatFailureMode,
    ):
        """Verify that the override parameter takes precedence over the limiter default."""
        # Arrange
        # A real limiter is used to test the override mechanism.
        limiter = MinimalRateLimiter(
            redis_client=redis_client,
            limiter_id=f"{limiter_id}_task_lifecycle_heartbeat_override_precedence",
            limit=1,
            window=1,
            max_concurrency=1,
            max_age=1,
            on_heartbeat_failure=original,
        )

        # Act
        # Create the lifecycle with the override.
        lifecycle_with_override = limiter.task_lifecycle(
            task_id,
            on_heartbeat_failure_override=override,
        )

        # Assert
        # The override should take precedence.
        assert lifecycle_with_override.on_failure_action == override, (
            f"override {override} should take precedence over default {original}"
        )


class TestHeartbeatLoop:
    """Tests for the heartbeat loop that periodically extends the task lease."""

    @staticmethod
    def test_heartbeat_loop_extends_lease_periodically(
        redis_client, mock_limiter, task_id
    ):
        """Verify that the heartbeat loop extends the lease at regular intervals."""
        # Act
        with TaskLifecycle(mock_limiter, task_id):
            # Wait for at least one heartbeat interval.
            # The interval is lease_duration / 2.
            # Sleep slightly longer to ensure the heartbeat executes.
            time.sleep(0.75 * mock_limiter.lease_duration)

        # Assert
        # The heartbeat should have been invoked at least once during the context.
        assert mock_limiter.extend_lease.call_count >= 1, (
            "extend_lease must be called periodically by heartbeat loop"
        )

        # Verify that the correct parameters were passed.
        mock_limiter.extend_lease.assert_called_with(
            task_id, mock_limiter.lease_duration
        )

    @staticmethod
    def test_heartbeat_interval_calculation(mock_limiter, task_id):
        """Verify that the heartbeat interval is correctly calculated as lease_duration / 2."""
        # Arrange & Act
        with patch("threading.Thread"):
            lifecycle = TaskLifecycle(mock_limiter, task_id)

        # Assert
        expected_interval = mock_limiter.lease_duration / 2
        assert lifecycle.interval == expected_interval, (
            f"interval must be lease_duration / 2 = {expected_interval} seconds"
        )

    @staticmethod
    def test_heartbeat_loop_restores_health_on_recovery(
        redis_client, mock_limiter, task_id
    ):
        """Verify that the heartbeat loop restores the health status after recovering from a failure."""
        # Act
        with TaskLifecycle(mock_limiter, task_id) as lifecycle:
            # Simulate an unhealthy state.
            lifecycle.is_healthy = False

            # Wait for the heartbeat to execute multiple times.
            time.sleep(0.75 * mock_limiter.lease_duration)

            # Assert
            # The lifecycle should have recovered and be marked as healthy.
            assert lifecycle.is_healthy, "lifecycle must restore health after recovery"

    @staticmethod
    def test_heartbeat_loop_flags_unhealthy_on_failure_warn_mode(
        redis_client, mock_limiter, task_id
    ):
        """Verify that the heartbeat loop flags the lifecycle as unhealthy on failure in warn mode."""
        # Arrange
        mock_limiter.extend_lease.side_effect = Exception("Simulated Redis failure")

        # Act
        with TaskLifecycle(
            mock_limiter, task_id, on_heartbeat_failure="warn"
        ) as lifecycle:
            # Wait for the heartbeat to fail.
            time.sleep(0.75 * mock_limiter.lease_duration)

            # Assert
            # The lifecycle should be marked as unhealthy.
            assert not lifecycle.is_healthy, (
                "lifecycle must be marked unhealthy after heartbeat failure"
            )

    @staticmethod
    def test_heartbeat_loop_terminates_worker_on_failure_kill_mode(
        redis_client, mock_limiter, task_id
    ):
        """Verify that the heartbeat loop terminates the worker on failure in kill mode."""
        # Arrange
        mock_limiter.extend_lease.side_effect = Exception("Simulated Redis failure")

        # Act & Assert
        with patch("os.kill") as mock_kill:
            with TaskLifecycle(mock_limiter, task_id, on_heartbeat_failure="kill"):
                # Wait for the heartbeat to fail and trigger termination.
                time.sleep(0.75 * mock_limiter.lease_duration)

                # Verify that termination was attempted.
                assert mock_kill.call_count > 0, (
                    "os.kill must be called in kill mode on heartbeat failure"
                )

                # Verify the correct signal and PID.
                mock_kill.assert_called_with(os.getpid(), signal.SIGTERM)

    @staticmethod
    def test_heartbeat_loop_stops_on_exit(redis_client, mock_limiter, task_id):
        """Verify that the heartbeat loop stops when exiting the lifecycle context."""
        # Act
        lifecycle = TaskLifecycle(mock_limiter, task_id)
        lifecycle.__enter__()

        # The thread should be running.
        assert lifecycle._thread is not None, "thread must be created on enter"
        assert lifecycle._thread.is_alive(), "thread must be running during lifecycle"

        # Exit the context.
        lifecycle.__exit__(None, None, None)

        # Wait for the thread to stop.
        time.sleep(0.75 * mock_limiter.lease_duration)

        # Assert
        # The thread should have stopped.
        assert lifecycle._stop_event.is_set(), "stop event must be set on exit"
        assert not lifecycle._thread.is_alive(), (
            "thread must be stopped after exiting lifecycle"
        )

    @staticmethod
    def test_heartbeat_loop_calls_extend_lease_with_correct_parameters(
        redis_client, mock_limiter, task_id
    ):
        """Verify that the heartbeat loop calls extend_lease with the correct task_id and duration."""
        # Act
        with TaskLifecycle(mock_limiter, task_id):
            time.sleep(0.75 * mock_limiter.lease_duration)

        # Assert
        # Verify that extend_lease was called with the correct parameters.
        assert mock_limiter.extend_lease.call_count >= 1, (
            "extend_lease must be called at least once with correct parameters"
        )
        for call in mock_limiter.extend_lease.call_args_list:
            assert call[0][0] == task_id, "extend_lease must be called with task_id"
            assert call[0][1] == mock_limiter.lease_duration, (
                "extend_lease must be called with lease_duration"
            )


class TestExtendLease:
    """Tests for ``extend_lease()`` error handling."""

    @staticmethod
    def test_extend_lease_raises_key_error_for_unknown_task(generic_limiter):
        """Verify that ``extend_lease()`` raises a KeyError for unknown task identifiers."""
        # Act & Assert
        with pytest.raises(
            KeyError, match="task id was not found in the concurrency set"
        ):
            generic_limiter.extend_lease("nonexistent", 30)
