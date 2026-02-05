"""Tests for the TaskLifecycle context manager.

This module tests the TaskLifecycle implementation that manages concurrency
slots and task cleanup. It inherits contract tests and adds generic
implementation-specific tests that work with any rate limiter implementation.
"""

import os
import signal
import time
from unittest.mock import MagicMock, patch

import pytest

from celery_rate_limiter.limiters import TaskLifecycle
from tests.contracts.test_task_lifecycle import TaskLifecycleContractTest
from tests.implementations.conftest import MinimalRateLimiter


@pytest.fixture
def mock_limiter(redis_client, task_id):
    """Create a mock limiter that uses the real Redis client but mocks internal helpers.

    This fixture provides a limiter with real Redis operations but mocked
    Celery-specific methods to avoid needing a full Celery setup.
    """
    limiter = MagicMock()
    limiter.redis = redis_client
    limiter.concurrency_key = "test:concurrency"
    limiter.lease_duration = 0.2  # Short duration for fast tests
    limiter.id = "test_limiter"
    limiter.get_active_key.side_effect = lambda _: f"test:active:{task_id}"
    limiter.extend_lease.return_value = 1  # For background thread tests
    return limiter


@pytest.fixture
def active_key(mock_limiter, task_id):
    """Provide the active key for the test task."""
    return mock_limiter.get_active_key(task_id)


@pytest.fixture
def lifecycle_class():
    """Provide the TaskLifecycle class for contract tests."""
    return TaskLifecycle


class TestTaskLifecycle(TaskLifecycleContractTest):
    """Test TaskLifecycle context manager implementation.

    Inherits all contract tests from TaskLifecycleContractTest and adds
    generic implementation tests for heartbeat handling and lifecycle behavior.
    """

    # ==================== Implementation-Specific Tests ====================

    def test_lifecycle_with_multiple_concurrent_tasks(
        self, redis_client, mock_limiter, task_id, active_key
    ):
        """Verify lifecycle only removes the specific task from concurrency set."""
        # Arrange
        # Simulate 5 concurrent tasks.
        concurrent_tasks = {
            "other_task_1": 100,
            "other_task_2": 100,
            "other_task_3": 100,
            "other_task_4": 100,
            task_id: 100,
        }
        redis_client.zadd(mock_limiter.concurrency_key, concurrent_tasks)
        redis_client.set(active_key, "1")

        # Act & Assert
        # Prevent heartbeat thread from starting.
        with patch("threading.Thread"):
            with TaskLifecycle(mock_limiter, task_id):
                # During execution, all 5 tasks should be present.
                assert redis_client.zcard(mock_limiter.concurrency_key) == 5

        # After completion, only our task should be removed.
        assert redis_client.zcard(mock_limiter.concurrency_key) == 4
        assert redis_client.zscore(mock_limiter.concurrency_key, task_id) is None
        assert redis_client.zscore(mock_limiter.concurrency_key, "other_task_1") is not None
        assert redis_client.exists(active_key) == 0

    def test_lifecycle_handles_redis_failure_during_cleanup(
        self, redis_client, mock_limiter, task_id, active_key
    ):
        """Verify lifecycle raises exception but still triggers consume on Redis failure."""
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

                # Verify the exception came from zrem.
                mock_zrem.assert_called_once()

        # Assert that trigger_consume is still called.
        # noinspection PyUnboundLocalVariable
        mock_limiter.trigger_consume.assert_called_once()

    @pytest.mark.parametrize(
        "original, override",
        [("warn", "kill"), ("kill", "warn")],
        ids=["default_warn_override_kill", "default_kill_override_warn"]
    )
    def test_heartbeat_failure_override_precedence(
        self, redis_client, task_id, original, override
    ):
        """Verify that override parameter takes precedence over limiter default."""
        # Arrange
        # Use real limiter to test override mechanism.
        limiter = MinimalRateLimiter(
            redis_client=redis_client,
            limiter_id="test_id",
            limit=1,
            window=1,
            max_concurrency=1,
            max_age=1,
            on_heartbeat_failure=original,
        )

        # Act
        # Create lifecycle with override.
        lifecycle_with_override = limiter.task_lifecycle(
            task_id,
            on_heartbeat_failure_override=override,
        )

        # Assert
        # Override should take precedence.
        assert lifecycle_with_override.on_failure_action == override, (
            f"override {override} should take precedence over default {original}"
        )

    # ==================== Heartbeat Loop Tests ====================

    def test_heartbeat_loop_extends_lease_periodically(
        self, redis_client, mock_limiter, task_id
    ):
        """Verify heartbeat loop extends lease at regular intervals."""
        # Arrange
        mock_limiter.extend_lease.return_value = 1

        # Act
        with TaskLifecycle(mock_limiter, task_id):
            # Wait for at least one heartbeat interval.
            # The interval is lease_duration / 2.
            # Sleep for slightly longer to ensure the heartbeat runs.
            time.sleep(0.75 * mock_limiter.lease_duration)

        # Assert
        # Heartbeat should have been called at least once during the context.
        assert mock_limiter.extend_lease.call_count >= 1, (
            "extend_lease must be called periodically by heartbeat loop"
        )

        # Verify the correct parameters were passed.
        mock_limiter.extend_lease.assert_called_with(task_id, mock_limiter.lease_duration)

    def test_heartbeat_interval_calculation(self, mock_limiter, task_id):
        """Verify heartbeat interval is correctly calculated as lease_duration / 2."""
        # Arrange & Act
        with patch("threading.Thread"):
            lifecycle = TaskLifecycle(mock_limiter, task_id)

        # Assert
        expected_interval = mock_limiter.lease_duration / 2
        assert lifecycle.interval == expected_interval, (
            f"interval must be lease_duration / 2 = {expected_interval} seconds"
        )

    def test_heartbeat_loop_restores_health_on_recovery(
        self, redis_client, mock_limiter, task_id
    ):
        """Verify heartbeat loop restores health status after recovering from failure."""
        # Arrange
        mock_limiter.extend_lease.return_value  = 1

        # Act
        with TaskLifecycle(mock_limiter, task_id) as lifecycle:
            # Simulate an unhealthy state.
            lifecycle.is_healthy = False

            # Wait for heartbeat to run multiple times.
            time.sleep(0.75 * mock_limiter.lease_duration)

            # Assert
            # Lifecycle should have recovered and be healthy.
            assert lifecycle.is_healthy, "lifecycle must restore health after recovery"

    def test_heartbeat_loop_flags_unhealthy_on_failure_warn_mode(
        self, redis_client, mock_limiter, task_id
    ):
        """Verify heartbeat loop flags as unhealthy on failure in warn mode."""
        # Arrange
        mock_limiter.extend_lease.side_effect = Exception("Simulated Redis failure")

        # Act
        with patch("builtins.print") as mock_print, TaskLifecycle(
            mock_limiter, task_id, on_heartbeat_failure="warn"
        ) as lifecycle:
            # Wait for heartbeat to fail.
            time.sleep(0.75 * mock_limiter.lease_duration)

            # Assert
            # Lifecycle should be marked as unhealthy.
            assert not lifecycle.is_healthy, (
                "lifecycle must be marked unhealthy after heartbeat failure"
            )

    def test_heartbeat_loop_terminates_worker_on_failure_kill_mode(
        self, redis_client, mock_limiter, task_id
    ):
        """Verify heartbeat loop terminates worker on failure in kill mode."""
        # Arrange
        mock_limiter.extend_lease.side_effect = Exception("Simulated Redis failure")

        # Act & Assert
        with patch("builtins.print") as mock_print, patch("os.kill") as mock_kill:
            with TaskLifecycle(
                mock_limiter, task_id, on_heartbeat_failure="kill"
            ):
                # Wait for heartbeat to fail and trigger termination.
                time.sleep(0.75 * mock_limiter.lease_duration)

                # Verify termination was attempted.
                assert mock_kill.call_count > 0, (
                    "os.kill must be called in kill mode on heartbeat failure"
                )

                # Verify correct signal and PID.
                mock_kill.assert_called_with(os.getpid(), signal.SIGTERM)

    def test_heartbeat_loop_stops_on_exit(self, redis_client, mock_limiter, task_id):
        """Verify heartbeat loop stops when exiting lifecycle context."""
        # Arrange
        mock_limiter.extend_lease.return_value = 1

        # Act
        lifecycle = TaskLifecycle(mock_limiter, task_id)
        lifecycle.__enter__()

        # Thread should be running.
        assert lifecycle._thread is not None, "thread must be created on enter"
        assert lifecycle._thread.is_alive(), "thread must be running during lifecycle"

        # Exit the context.
        lifecycle.__exit__(None, None, None)

        # Wait for thread to stop.
        time.sleep(0.75 * mock_limiter.lease_duration)

        # Assert
        # Thread should have stopped.
        assert lifecycle._stop_event.is_set(), "stop event must be set on exit"
        assert not lifecycle._thread.is_alive(), (
            "thread must be stopped after exiting lifecycle"
        )

    def test_heartbeat_loop_calls_extend_lease_with_correct_parameters(
        self, redis_client, mock_limiter, task_id
    ):
        """Verify heartbeat loop calls extend_lease with correct task_id and duration."""
        # Arrange
        mock_limiter.extend_lease.return_value = 1

        # Act
        with TaskLifecycle(mock_limiter, task_id):
            time.sleep(0.75 * mock_limiter.lease_duration)

        # Assert
        # Verify extend_lease was called with the correct parameters.
        assert mock_limiter.extend_lease.call_count >= 1
        for call in mock_limiter.extend_lease.call_args_list:
            assert call[0][0] == task_id, "extend_lease must be called with task_id"
            assert call[0][1] == mock_limiter.lease_duration, (
                "extend_lease must be called with lease_duration"
            )
