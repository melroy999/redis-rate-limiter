"""Threading-specific behavioural tests for the ``ThreadPoolRateLimiter`` implementation."""

import threading
import time
from unittest.mock import MagicMock, patch

import pytest


class TestThreadPoolRateLimiter:
    """Tests that are specific to the threading backend dispatch and lifecycle logic."""

    @staticmethod
    def test_dispatch_task_submits_to_executor(
        limiter, default_payload, func_path, task_id
    ):
        """Verify that ``_dispatch_task`` submits a callable to the thread pool executor."""
        # Act
        with (
            patch(
                "celery_rate_limiter.backends.threading.limiter.import_string",
                return_value=MagicMock(),
            ),
            patch.object(limiter.executor, "submit") as mock_submit,
        ):
            limiter._dispatch_task(func_path, default_payload, task_id)

        # Assert
        mock_submit.assert_called_once()
        submitted_fn = mock_submit.call_args[0][0]
        assert callable(submitted_fn), "submitted argument should be callable"

    @staticmethod
    def test_dispatch_task_resolves_function_path(
        limiter, default_payload, func_path, task_id
    ):
        """Verify that ``_dispatch_task`` uses ``import_string`` to resolve the function path."""
        # Act
        with (
            patch.object(limiter.executor, "submit"),
            patch(
                "celery_rate_limiter.backends.threading.limiter.import_string"
            ) as mock_import,
        ):
            mock_import.return_value = MagicMock()
            limiter._dispatch_task(func_path, default_payload, task_id)

        # Assert
        mock_import.assert_called_once_with(func_path)

    @staticmethod
    def test_dispatch_task_wraps_in_lifecycle(
        limiter, default_payload, func_path, task_id
    ):
        """Verify that ``_dispatch_task`` wraps execution within the ``task_lifecycle`` context manager."""
        # Arrange
        mock_lifecycle = MagicMock()

        # Act
        with (
            patch.object(
                limiter, "task_lifecycle", return_value=mock_lifecycle
            ) as mock_tl,
            patch(
                "celery_rate_limiter.backends.threading.limiter.import_string"
            ) as mock_import,
            patch.object(limiter.executor, "submit") as mock_submit,
        ):
            mock_import.return_value = MagicMock()
            limiter._dispatch_task(func_path, default_payload, task_id)

            # Execute the submitted wrapper to trigger the lifecycle context.
            submitted_fn = mock_submit.call_args[0][0]
            submitted_fn()

        # Assert
        mock_tl.assert_called_once_with(task_id)
        mock_lifecycle.__enter__.assert_called_once()
        mock_lifecycle.__exit__.assert_called_once()

    @staticmethod
    def test_dispatch_task_import_failure_propagates(
        limiter, default_payload, func_path, task_id
    ):
        """Verify that an ``import_string()`` failure propagates from ``_dispatch_task()``."""
        # Act & Assert
        with patch(
            "celery_rate_limiter.backends.threading.limiter.import_string",
            side_effect=ModuleNotFoundError("No module named 'nonexistent'"),
        ):
            with pytest.raises(
                ModuleNotFoundError, match="No module named 'nonexistent'"
            ):
                limiter._dispatch_task(func_path, default_payload, task_id)

    @staticmethod
    def test_schedule_drain_wakes_drain_loop(limiter):
        """Verify that ``_schedule_drain`` delegates to the base-class ``DrainLoop``."""
        # Arrange
        delay = 1.75

        # Act
        with patch.object(limiter._drain_loop, "wake") as mock_wake:
            limiter._schedule_drain(delay=delay)

        # Assert
        mock_wake.assert_called_once_with(delay)


class TestLocalCapacityGuard:
    """Tests for the local capacity guard in ``ThreadPoolRateLimiter``."""

    @staticmethod
    def test_has_local_capacity_returns_true_when_below_max_workers(limiter):
        """Verify that ``_has_local_capacity()`` returns ``True`` when the local dispatch count is below ``max_workers``."""
        assert limiter._has_local_capacity() is True, (
            "_has_local_capacity should return True when no tasks are dispatched"
        )
        assert limiter._local_dispatched == 0, "initial dispatch count should be zero"

    @staticmethod
    def test_has_local_capacity_returns_false_at_max_workers(limiter):
        """Verify that ``_has_local_capacity()`` returns ``False`` when the dispatch count equals ``max_workers``."""
        # Arrange
        # Simulate max_workers tasks dispatched.
        limiter._local_dispatched = limiter.executor._max_workers

        # Assert
        assert limiter._has_local_capacity() is False, (
            "_has_local_capacity should return False at max_workers"
        )

    @staticmethod
    def test_dispatch_task_increments_and_decrements_counter(
        limiter, func_path, task_id
    ):
        """Verify that ``_dispatch_task()`` increments the counter before submission and decrements after completion."""
        # Arrange
        started = threading.Event()
        proceed = threading.Event()

        def blocking_task(**kwargs):
            started.set()
            proceed.wait(timeout=5.0)

        # Act
        # Dispatch a task that blocks until we release it.
        with (
            patch(
                "celery_rate_limiter.backends.threading.limiter.import_string",
                return_value=blocking_task,
            ),
            patch.object(limiter, "task_lifecycle") as mock_lifecycle,
        ):
            mock_lifecycle.return_value.__enter__ = MagicMock(return_value=None)
            mock_lifecycle.return_value.__exit__ = MagicMock(return_value=False)
            limiter._dispatch_task(func_path, {}, task_id)

            # Wait for the task to start running in the thread pool.
            started.wait(timeout=5.0)

            # Assert
            # Counter should be 1 while task is running.
            assert limiter._local_dispatched == 1, (
                "dispatch count should be 1 while task is running"
            )

            # Release the task.
            proceed.set()

        # Allow thread pool task to complete.
        time.sleep(0.2)

        # Counter should return to 0 after completion.
        assert limiter._local_dispatched == 0, (
            "dispatch count should return to 0 after task completion"
        )
