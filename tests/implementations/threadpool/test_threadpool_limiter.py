"""Threading-specific behavioural tests for the
``ThreadPoolRateLimiter`` implementation.

Fixture dependencies:
    - ``redis_client``, ``func_path``, ``payload``: from ``tests/conftest.py``.
    - ``task_id``: from ``tests/implementations/conftest.py``.
    - ``limiter``: from ``tests/implementations/threadpool/conftest.py``.
"""

import logging
import threading
import time
from unittest.mock import MagicMock, patch

import pytest

from tests.helpers.utils import assert_log_emitted


@pytest.mark.behavior
class TestThreadPoolRateLimiter:
    """Tests that are specific to the threading backend dispatch and lifecycle logic."""

    @staticmethod
    def test_dispatch_task_resolves_function_path(limiter, payload, func_path, task_id):
        """Verify that ``_dispatch_task`` uses
        ``import_string`` to resolve the function path."""
        # Act
        with (
            patch.object(limiter.executor, "submit"),
            patch(
                "redis_rate_limiter.backends.threading.limiter.import_string"
            ) as mock_import,
        ):
            mock_import.return_value = MagicMock()
            limiter._dispatch_task(func_path, payload, task_id)

        # Assert
        mock_import.assert_called_once_with(func_path)

    @staticmethod
    def test_dispatch_task_wraps_in_lifecycle(limiter, payload, func_path, task_id):
        """Verify that ``_dispatch_task`` wraps execution
        within the ``task_lifecycle`` context manager."""
        # Arrange
        mock_lifecycle = MagicMock()

        # Act
        with (
            patch.object(
                limiter, "task_lifecycle", return_value=mock_lifecycle
            ) as mock_tl,
            patch(
                "redis_rate_limiter.backends.threading.limiter.import_string"
            ) as mock_import,
            patch.object(limiter.executor, "submit") as mock_submit,
        ):
            mock_import.return_value = MagicMock()
            limiter._dispatch_task(func_path, payload, task_id)

            # Execute the submitted wrapper to trigger the lifecycle context.
            submitted_fn = mock_submit.call_args[0][0]
            submitted_fn()

        # Assert
        mock_tl.assert_called_once_with(task_id)
        mock_lifecycle.__enter__.assert_called_once()
        mock_lifecycle.__exit__.assert_called_once()

    @staticmethod
    def test_dispatch_task_import_failure_propagates(
        limiter, payload, func_path, task_id
    ):
        """Verify that an ``import_string()`` failure
        propagates from ``_dispatch_task()``."""
        # Act & Assert
        with patch(
            "redis_rate_limiter.backends.threading.limiter.import_string",
            side_effect=ModuleNotFoundError("No module named 'nonexistent'"),
        ):
            with pytest.raises(
                ModuleNotFoundError, match="No module named 'nonexistent'"
            ):
                limiter._dispatch_task(func_path, payload, task_id)

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


@pytest.mark.behavior
class TestLocalCapacityGuard:
    """Tests for the local capacity guard in ``ThreadPoolRateLimiter``."""

    @staticmethod
    def test_has_local_capacity_returns_true_when_below_max_workers(limiter):
        """Verify that ``_has_local_capacity()`` returns
        ``True`` when the dispatch count is below
        ``max_workers``."""
        # Act & Assert
        assert limiter._has_local_capacity() is True, (
            "_has_local_capacity should return True when no tasks are dispatched"
        )
        assert limiter._local_dispatched == 0, "initial dispatch count should be zero"

    @staticmethod
    def test_has_local_capacity_returns_false_at_max_workers(limiter):
        """Verify that ``_has_local_capacity()`` returns
        ``False`` when the dispatch count equals
        ``max_workers``."""
        # Arrange
        limiter._local_dispatched = limiter._local_max_workers

        # Act
        result = limiter._has_local_capacity()

        # Assert
        assert result is False, "_has_local_capacity should return False at max_workers"

    @staticmethod
    def test_dispatch_task_increments_and_decrements_counter(
        limiter, func_path, task_id
    ):
        """Verify that ``_dispatch_task()`` increments the
        counter before submission and decrements after
        completion."""
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
                "redis_rate_limiter.backends.threading.limiter.import_string",
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
            assert limiter._local_dispatched == 1, (
                "dispatch count should be 1 while task is running"
            )

            proceed.set()

        # Allow thread pool task to complete.
        time.sleep(0.2)

        assert limiter._local_dispatched == 0, (
            "dispatch count should return to 0 after task completion"
        )

    @staticmethod
    def test_dispatch_task_increments_counter_additively(limiter, func_path, task_id):
        """Verify that ``_dispatch_task()`` uses additive
        increment, not assignment, for the dispatch
        counter."""
        # Arrange
        started_1 = threading.Event()
        started_2 = threading.Event()
        proceed = threading.Event()

        def blocking_task_1(**kwargs):
            started_1.set()
            proceed.wait(timeout=5.0)

        def blocking_task_2(**kwargs):
            started_2.set()
            proceed.wait(timeout=5.0)

        call_count = 0

        def import_side_effect(path):
            nonlocal call_count
            call_count += 1
            return blocking_task_1 if call_count == 1 else blocking_task_2

        # Act
        with (
            patch(
                "redis_rate_limiter.backends.threading.limiter.import_string",
                side_effect=import_side_effect,
            ),
            patch.object(limiter, "task_lifecycle") as mock_lifecycle,
        ):
            mock_lifecycle.return_value.__enter__ = MagicMock(return_value=None)
            mock_lifecycle.return_value.__exit__ = MagicMock(return_value=False)
            limiter._dispatch_task(func_path, {}, f"{task_id}_1")
            limiter._dispatch_task(func_path, {}, f"{task_id}_2")

            # Wait for both tasks to start running in the thread pool.
            started_1.wait(timeout=5.0)
            started_2.wait(timeout=5.0)

            # Assert
            assert limiter._local_dispatched == 2, (
                "dispatch count should be 2 with two concurrent tasks"
            )

            proceed.set()

        # Allow thread pool tasks to complete.
        time.sleep(0.2)

        assert limiter._local_dispatched == 0, (
            "dispatch count should return to 0 after both tasks complete"
        )


# ---------------------------------------------------------------------------
# Observability tests
# ---------------------------------------------------------------------------


@pytest.mark.observability
class TestThreadPoolDispatchObservability:
    """Observability tests for the ``_dispatch_task`` log emissions."""

    @staticmethod
    def test_dispatch_task_emits_debug_log(
        limiter, payload, func_path, task_id, caplog
    ):
        """Verify that ``_dispatch_task`` emits a DEBUG log
        with limiter id, task id, func path, and local
        dispatch count."""
        # Act
        with caplog.at_level(
            logging.DEBUG, logger="redis_rate_limiter.backends.threading.limiter"
        ):
            with (
                patch(
                    "redis_rate_limiter.backends.threading.limiter.import_string",
                    return_value=MagicMock(),
                ),
                patch.object(limiter.executor, "submit"),
            ):
                limiter._dispatch_task(func_path, payload, task_id)

        # Assert
        assert_log_emitted(
            caplog.records,
            level="DEBUG",
            label="[ThreadPoolRateLimiter]",
            required_fragments=[
                f"limiter={limiter.id}",
                f"task_id={task_id}",
                f"func_path={func_path}",
                "local_dispatched=1",
            ],
            message="should emit a debug log containing the "
            "limiter id, task id, func path, and "
            "local dispatch count",
        )
