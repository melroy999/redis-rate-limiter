"""Tests for the ``TaskLifecycle`` context manager.

This module tests the sync ``TaskLifecycle`` implementation that manages
concurrency slots, in-flight key cleanup, and heartbeat-based lease renewal.
It inherits the unified contract tests from ``TaskLifecycleContractTest`` and
adds implementation-specific behavioral and observability tests.

Fixture dependencies:
    - ``redis_client``, ``limiter_id``: from ``tests/conftest.py``.
    - ``generic_limiter``, ``task_id``: from ``tests/implementations/conftest.py``.
"""

import inspect
import logging
import os
import signal
import time
from typing import Literal
from unittest.mock import MagicMock, patch

import pytest

from redis_rate_limiter import TaskLifecycle
from tests.contracts.test_task_lifecycle import TaskLifecycleContractTest
from tests.helpers.utils import assert_log_emitted
from tests.implementations.conftest import MinimalRateLimiter

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

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
    limiter.get_inflight_key.side_effect = lambda tid: f"test:inflight:{tid}"

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


# ---------------------------------------------------------------------------
# Concrete test cases
# ---------------------------------------------------------------------------


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

        # Act
        # Prevent the heartbeat thread from starting.
        with patch("threading.Thread"):
            with TaskLifecycle(mock_limiter, task_id):
                # During execution, all five tasks should be present.
                assert redis_client.zcard(mock_limiter.concurrency_key) == 5, (
                    "all five tasks should be present during execution"
                )

        # Assert
        # After completion, only the target task should have been removed.
        assert redis_client.zcard(mock_limiter.concurrency_key) == 4, (
            "concurrency set should have four tasks after target completion"
        )
        assert redis_client.zscore(mock_limiter.concurrency_key, task_id) is None, (
            "target task must be removed from concurrency set"
        )
        assert redis_client.zscore(
            mock_limiter.concurrency_key, "other_task_1"
        ) == pytest.approx(100.0), (
            "other tasks must remain in concurrency set with their original score"
        )
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
            side_effect=ConnectionError("Redis connection lost"),
        ) as mock_zrem:
            # Act & Assert
            with patch("threading.Thread"):
                with pytest.raises(ConnectionError, match="Redis connection lost"):
                    with TaskLifecycle(mock_limiter, task_id):
                        pass

                # Verify that the exception originated from zrem.
                mock_zrem.assert_called_once()

        # Assert that trigger_consume is still invoked.
        # noinspection PyUnboundLocalVariable
        mock_limiter.trigger_consume.assert_called_once()

    @staticmethod
    def test_empty_task_id_skips_inflight_cleanup(redis_client):
        """Verify that an empty ``task_id`` skips inflight key deletion."""
        # Arrange
        limiter = MagicMock()
        limiter.redis = redis_client
        limiter.concurrency_key = "test:concurrency"
        limiter.id = "test_limiter"
        limiter.lease_duration = 0.2
        limiter.extend_lease.return_value = None

        # Act
        with patch("threading.Thread"):
            with TaskLifecycle(limiter, task_id=""):
                pass

        # Assert
        # trigger_consume should still be called on exit.
        limiter.trigger_consume.assert_called_once()

    @staticmethod
    @pytest.mark.parametrize(
        "original, override",
        HEARTBEAT_OVERRIDE_CASES,
        ids=["default_warn_override_kill", "default_kill_override_warn"],
    )
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


# ---------------------------------------------------------------------------
# Observability tests
# ---------------------------------------------------------------------------


class TestTaskLifecycleObservability:
    """Observability tests for the ``TaskLifecycle`` context manager log emissions."""

    @staticmethod
    def test_lifecycle_cleanup_emits_debug_log(
        redis_client, mock_limiter, task_id, inflight_key, caplog
    ):
        """Verify that the lifecycle cleanup emits a DEBUG log with removal details."""
        # Arrange
        redis_client.zadd(mock_limiter.concurrency_key, {task_id: 100})
        redis_client.set(inflight_key, "1")

        # Act
        with caplog.at_level(logging.DEBUG, logger="redis_rate_limiter.core.limiters"):
            with patch("threading.Thread"):
                with TaskLifecycle(mock_limiter, task_id):
                    pass

        # Assert
        assert_log_emitted(
            caplog.records,
            level="DEBUG",
            required_fragments=[
                f"limiter={mock_limiter.id}",
                f"task_id={task_id}",
                "removed_concurrency=True",
                "removed_inflight=True",
            ],
            message="should emit a debug log for concurrency slot release with limiter id, task id, and removal counts",
        )

    @staticmethod
    def test_empty_task_id_emits_removed_inflight_false(redis_client, caplog):
        """Verify that an empty ``task_id`` emits ``removed_inflight=False`` in the cleanup log."""
        # Arrange
        limiter = MagicMock()
        limiter.redis = redis_client
        limiter.concurrency_key = "test:concurrency"
        limiter.id = "test_limiter"
        limiter.lease_duration = 0.2
        limiter.extend_lease.return_value = None

        # Act
        with caplog.at_level(logging.DEBUG, logger="redis_rate_limiter.core.limiters"):
            with patch("threading.Thread"):
                with TaskLifecycle(limiter, task_id=""):
                    pass

        # Assert
        assert_log_emitted(
            caplog.records,
            level="DEBUG",
            required_fragments=["removed_inflight=False"],
            message="empty task_id should log removed_inflight=False",
        )


# ---------------------------------------------------------------------------
# Heartbeat loop tests
# ---------------------------------------------------------------------------


class TestHeartbeatLoop:
    """Tests for the heartbeat loop that periodically extends the task lease."""

    @staticmethod
    def test_heartbeat_interval_calculation(mock_limiter, task_id):
        """Verify that the heartbeat interval is correctly calculated as ``lease_duration / 2``."""
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
    def test_heartbeat_loop_calls_extend_lease_with_correct_parameters(
        redis_client, mock_limiter, task_id
    ):
        """Verify that the heartbeat loop calls ``extend_lease`` with the correct ``task_id`` and duration."""
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


# ---------------------------------------------------------------------------
# Heartbeat observability tests
# ---------------------------------------------------------------------------


class TestHeartbeatLoopObservability:
    """Observability tests for log emissions from the heartbeat loop."""

    @staticmethod
    def test_lifecycle_entry_emits_debug_log(mock_limiter, task_id, caplog):
        """Verify that lifecycle entry emits a DEBUG log with limiter id, task id, and heartbeat interval."""
        # Act
        with caplog.at_level(logging.DEBUG, logger="redis_rate_limiter.core.limiters"):
            with TaskLifecycle(mock_limiter, task_id):
                time.sleep(0.75 * mock_limiter.lease_duration)

        # Assert
        assert_log_emitted(
            caplog.records,
            level="DEBUG",
            required_fragments=[
                f"limiter={mock_limiter.id}",
                f"task_id={task_id}",
                "heartbeat_interval_s=0.1",
            ],
            message="should emit a debug log for lifecycle entry with limiter id, task id, and heartbeat interval",
        )

    @staticmethod
    def test_heartbeat_recovery_emits_info_log(mock_limiter, task_id, caplog):
        """Verify that heartbeat recovery emits an INFO log with task id and limiter id."""
        # Act
        with caplog.at_level(logging.INFO, logger="redis_rate_limiter.core.limiters"):
            with TaskLifecycle(mock_limiter, task_id) as lifecycle:
                lifecycle.is_healthy = False
                time.sleep(0.75 * mock_limiter.lease_duration)

        # Assert
        assert_log_emitted(
            caplog.records,
            level="INFO",
            required_fragments=[
                f"task {task_id}",
                f"limiter {mock_limiter.id}",
                "restored",
            ],
            message="should emit an info log for heartbeat connection restoration with task id and limiter id",
        )

    @staticmethod
    def test_heartbeat_failure_warn_mode_emits_critical_log(
        mock_limiter, task_id, caplog
    ):
        """Verify that heartbeat failure in warn mode emits a CRITICAL log."""
        # Arrange
        mock_limiter.extend_lease.side_effect = Exception("Simulated Redis failure")

        # Act
        with caplog.at_level(
            logging.CRITICAL, logger="redis_rate_limiter.core.limiters"
        ):
            with TaskLifecycle(mock_limiter, task_id, on_heartbeat_failure="warn"):
                time.sleep(0.75 * mock_limiter.lease_duration)

        # Assert
        assert_log_emitted(
            caplog.records,
            level="CRITICAL",
            required_fragments=[
                f"task {task_id}",
                "flagged as unhealthy",
                "Simulated Redis failure",
            ],
            message="should emit a critical log for heartbeat failure with task id and error message",
        )

    @staticmethod
    def test_heartbeat_failure_kill_mode_emits_critical_log(
        mock_limiter, task_id, caplog
    ):
        """Verify that heartbeat failure in kill mode emits a CRITICAL log with termination action."""
        # Arrange
        mock_limiter.extend_lease.side_effect = Exception("Simulated Redis failure")

        # Act
        with caplog.at_level(
            logging.CRITICAL, logger="redis_rate_limiter.core.limiters"
        ):
            with patch("os.kill"):
                with TaskLifecycle(mock_limiter, task_id, on_heartbeat_failure="kill"):
                    time.sleep(0.75 * mock_limiter.lease_duration)

        # Assert
        assert_log_emitted(
            caplog.records,
            level="CRITICAL",
            required_fragments=[
                f"task {task_id}",
                "terminating worker",
                "Simulated Redis failure",
            ],
            message="should emit a critical log for heartbeat failure with task id, error, and termination action",
        )


# ---------------------------------------------------------------------------
# Extend lease tests
# ---------------------------------------------------------------------------


class TestExtendLease:
    """Tests for ``extend_lease()`` success and error handling."""

    @staticmethod
    def test_extend_lease_raises_key_error_for_unknown_task(generic_limiter):
        """Verify that ``extend_lease()`` raises a KeyError for unknown task identifiers."""
        # Act & Assert
        with pytest.raises(KeyError, match=r'in the concurrency set\."'):
            generic_limiter.extend_lease("nonexistent", 30)

    @staticmethod
    def test_extend_lease_succeeds_for_existing_task(
        generic_limiter, redis_client, task_id
    ):
        """Verify that ``extend_lease()`` updates the score for a task present in the concurrency set."""
        # Arrange
        # Seed the concurrency sorted set with a low score so the update is observable.
        initial_score = 1000.0
        redis_client.zadd(generic_limiter.concurrency_key, {task_id: initial_score})

        # Act
        generic_limiter.extend_lease(task_id, 30)

        # Assert
        new_score = redis_client.zscore(generic_limiter.concurrency_key, task_id)
        assert new_score is not None, (
            "task should still be present in the concurrency set after lease extension"
        )
        assert new_score > initial_score, (
            "lease extension should update the score to a value greater than the initial score"
        )



# ---------------------------------------------------------------------------
# Extend lease observability tests
# ---------------------------------------------------------------------------


class TestExtendLeaseObservability:
    """Observability tests for ``extend_lease()`` log emissions."""

    @staticmethod
    def test_extend_lease_unknown_task_emits_debug_log(generic_limiter, caplog):
        """Verify that ``extend_lease()`` emits a DEBUG log with ``renewed=False`` for unknown tasks."""
        # Act
        with caplog.at_level(logging.DEBUG, logger="redis_rate_limiter.core.limiters"):
            with pytest.raises(KeyError, match="not found in the concurrency set"):
                generic_limiter.extend_lease("nonexistent", 30)

        # Assert
        assert_log_emitted(
            caplog.records,
            level="DEBUG",
            required_fragments=[
                f"limiter={generic_limiter.id}",
                "task_id=nonexistent",
                "duration_s=30",
                "renewed=False",
            ],
            message="should emit a debug log containing the limiter id, task id, duration, and renewed=False",
        )

    @staticmethod
    def test_extend_lease_success_emits_debug_log(
        generic_limiter, redis_client, task_id, caplog
    ):
        """Verify that ``extend_lease()`` emits a DEBUG log with ``renewed=True`` for existing tasks."""
        # Arrange
        redis_client.zadd(generic_limiter.concurrency_key, {task_id: 1000.0})

        # Act
        with caplog.at_level(logging.DEBUG, logger="redis_rate_limiter.core.limiters"):
            generic_limiter.extend_lease(task_id, 30)

        # Assert
        assert_log_emitted(
            caplog.records,
            level="DEBUG",
            required_fragments=[
                f"limiter={generic_limiter.id}",
                f"task_id={task_id}",
                "duration_s=30",
                "renewed=True",
            ],
            message="should emit a debug log containing the limiter id, task id, duration, and renewed=True",
        )


# ---------------------------------------------------------------------------
# Signature tests
# ---------------------------------------------------------------------------


class TestTaskLifecycleSignatures:
    """Signature tests for ``TaskLifecycle`` default parameter values."""

    @staticmethod
    def test_default_on_heartbeat_failure_is_warn():
        """Verify that the default ``on_heartbeat_failure`` parameter is lowercase ``'warn'``.

        Mutation target: ``on_heartbeat_failure`` default value in ``TaskLifecycle.__init__``.
        """
        # Arrange & Act
        sig = inspect.signature(TaskLifecycle.__init__)

        # Assert
        assert sig.parameters["on_heartbeat_failure"].default == "warn", (
            "default on_heartbeat_failure must be lowercase 'warn'"
        )
