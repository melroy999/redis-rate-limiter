"""ProcessPool-specific behavioural tests for the
``ProcessPoolRateLimiter`` implementation.

Fixture dependencies:
    - ``redis_client``, ``func_path``, ``payload``: from ``tests/conftest.py``.
    - ``task_id``: from ``tests/implementations/conftest.py``.
    - ``limiter``: from ``tests/implementations/processpool/conftest.py``.
"""

import logging
from concurrent.futures import Future
from unittest.mock import MagicMock, patch

import pytest

from tests.helpers.utils import assert_log_emitted


@pytest.mark.behavior
class TestProcessPoolRateLimiter:
    """Tests for the process pool backend dispatch and lifecycle logic."""

    @staticmethod
    def test_dispatch_task_submits_to_executor(limiter, payload, func_path, task_id):
        """Verify that ``_dispatch_task`` submits the target
        function to the process pool executor."""
        # Arrange
        mock_func = MagicMock()
        mock_future = MagicMock(spec=Future)

        # Act
        with (
            patch(
                "redis_rate_limiter.backends.processpool.limiter.import_string",
                return_value=mock_func,
            ),
            patch.object(limiter, "task_lifecycle") as mock_lifecycle,
            patch.object(
                limiter.executor, "submit", return_value=mock_future
            ) as mock_submit,
        ):
            mock_lifecycle.return_value.__enter__ = MagicMock(return_value=None)
            limiter._dispatch_task(func_path, payload, task_id)

        # Assert
        mock_submit.assert_called_once_with(mock_func, **payload)

    @staticmethod
    def test_dispatch_task_resolves_function_path(limiter, payload, func_path, task_id):
        """Verify that ``_dispatch_task`` uses
        ``import_string`` to resolve the function path."""
        # Arrange
        mock_future = MagicMock(spec=Future)

        # Act
        with (
            patch.object(limiter, "task_lifecycle") as mock_lifecycle,
            patch.object(limiter.executor, "submit", return_value=mock_future),
            patch(
                "redis_rate_limiter.backends.processpool.limiter.import_string"
            ) as mock_import,
        ):
            mock_lifecycle.return_value.__enter__ = MagicMock(return_value=None)
            mock_import.return_value = MagicMock()
            limiter._dispatch_task(func_path, payload, task_id)

        # Assert
        mock_import.assert_called_once_with(func_path)

    @staticmethod
    def test_dispatch_task_enters_lifecycle_before_submit(
        limiter, payload, func_path, task_id
    ):
        """Verify that ``_dispatch_task`` enters the task
        lifecycle before submitting to the executor."""
        # Arrange
        call_order = []
        mock_lifecycle = MagicMock()
        mock_lifecycle.__enter__ = MagicMock(
            side_effect=lambda: call_order.append("lifecycle_enter")
        )
        mock_future = MagicMock(spec=Future)

        def tracking_submit(*args, **kwargs):
            call_order.append("submit")
            return mock_future

        # Act
        with (
            patch.object(
                limiter, "task_lifecycle", return_value=mock_lifecycle
            ) as mock_tl,
            patch(
                "redis_rate_limiter.backends.processpool.limiter.import_string",
                return_value=MagicMock(),
            ),
            patch.object(limiter.executor, "submit", side_effect=tracking_submit),
        ):
            limiter._dispatch_task(func_path, payload, task_id)

        # Assert
        mock_tl.assert_called_once_with(task_id)
        assert call_order == ["lifecycle_enter", "submit"], (
            "lifecycle should be entered before the task is submitted"
        )

    @staticmethod
    def test_dispatch_task_exits_lifecycle_on_future_completion(
        limiter, payload, func_path, task_id
    ):
        """Verify that ``_dispatch_task`` exits the task
        lifecycle when the future completes."""
        # Arrange
        mock_lifecycle = MagicMock()
        mock_lifecycle.__enter__ = MagicMock(return_value=None)
        mock_future = MagicMock(spec=Future)
        callbacks = []
        mock_future.add_done_callback.side_effect = lambda cb: callbacks.append(cb)

        # Act
        with (
            patch.object(limiter, "task_lifecycle", return_value=mock_lifecycle),
            patch(
                "redis_rate_limiter.backends.processpool.limiter.import_string",
                return_value=MagicMock(),
            ),
            patch.object(limiter.executor, "submit", return_value=mock_future),
        ):
            limiter._dispatch_task(func_path, payload, task_id)

        # Assert
        assert len(callbacks) == 1, "exactly one done callback should be registered"

        # Simulate future completion.
        callbacks[0](mock_future)
        mock_lifecycle.__exit__.assert_called_once_with(None, None, None)

    @staticmethod
    def test_dispatch_task_import_failure_propagates(
        limiter, payload, func_path, task_id
    ):
        """Verify that an ``import_string()`` failure
        propagates from ``_dispatch_task()``."""
        # Act & Assert
        with patch(
            "redis_rate_limiter.backends.processpool.limiter.import_string",
            side_effect=ModuleNotFoundError("No module named 'nonexistent'"),
        ):
            with pytest.raises(
                ModuleNotFoundError, match="No module named 'nonexistent'"
            ):
                limiter._dispatch_task(func_path, payload, task_id)

    @staticmethod
    def test_dispatch_task_passes_payload_as_kwargs(limiter, func_path, task_id):
        """Verify that ``_dispatch_task`` passes the payload
        as keyword arguments to ``submit``."""
        # Arrange
        payload = {"user_id": 42, "action": "process"}
        mock_func = MagicMock()
        mock_future = MagicMock(spec=Future)

        # Act
        with (
            patch(
                "redis_rate_limiter.backends.processpool.limiter.import_string",
                return_value=mock_func,
            ),
            patch.object(limiter, "task_lifecycle") as mock_lifecycle,
            patch.object(
                limiter.executor, "submit", return_value=mock_future
            ) as mock_submit,
        ):
            mock_lifecycle.return_value.__enter__ = MagicMock(return_value=None)
            limiter._dispatch_task(func_path, payload, task_id)

        # Assert
        mock_submit.assert_called_once_with(mock_func, user_id=42, action="process")

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
    """Tests for the local capacity guard in ``ProcessPoolRateLimiter``."""

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
    def test_dispatch_task_increments_counter_and_callback_decrements(
        limiter, func_path, task_id
    ):
        """Verify that ``_dispatch_task()`` increments the
        counter before submission and the done callback
        decrements it."""
        # Arrange
        mock_future = MagicMock(spec=Future)
        callbacks = []
        mock_future.add_done_callback.side_effect = lambda cb: callbacks.append(cb)

        # Act
        with (
            patch(
                "redis_rate_limiter.backends.processpool.limiter.import_string",
                return_value=MagicMock(),
            ),
            patch.object(limiter, "task_lifecycle") as mock_lifecycle,
            patch.object(limiter.executor, "submit", return_value=mock_future),
        ):
            mock_lifecycle.return_value.__enter__ = MagicMock(return_value=None)
            limiter._dispatch_task(func_path, {}, task_id)

        # Assert
        assert limiter._local_dispatched == 1, (
            "dispatch count should be 1 while future is pending"
        )

        # Simulate future completion via the done callback.
        callbacks[0](mock_future)

        assert limiter._local_dispatched == 0, (
            "dispatch count should return to 0 after done callback"
        )

    @staticmethod
    def test_dispatch_task_increments_counter_additively(limiter, func_path, task_id):
        """Verify that ``_dispatch_task()`` uses additive
        increment, not assignment, for the dispatch
        counter."""
        # Arrange
        callbacks = []

        def capture_callback(cb):
            callbacks.append(cb)

        mock_future_1 = MagicMock(spec=Future)
        mock_future_1.add_done_callback.side_effect = capture_callback
        mock_future_2 = MagicMock(spec=Future)
        mock_future_2.add_done_callback.side_effect = capture_callback

        submit_returns = iter([mock_future_1, mock_future_2])

        # Act
        with (
            patch(
                "redis_rate_limiter.backends.processpool.limiter.import_string",
                return_value=MagicMock(),
            ),
            patch.object(limiter, "task_lifecycle") as mock_lifecycle,
            patch.object(
                limiter.executor,
                "submit",
                side_effect=lambda *a, **kw: next(submit_returns),
            ),
        ):
            mock_lifecycle.return_value.__enter__ = MagicMock(return_value=None)
            limiter._dispatch_task(func_path, {}, f"{task_id}_1")
            limiter._dispatch_task(func_path, {}, f"{task_id}_2")

        # Assert
        assert limiter._local_dispatched == 2, (
            "dispatch count should be 2 with two pending futures"
        )

        # Simulate both futures completing.
        callbacks[0](mock_future_1)
        callbacks[1](mock_future_2)

        assert limiter._local_dispatched == 0, (
            "dispatch count should return to 0 after both done callbacks"
        )


# ---------------------------------------------------------------------------
# Observability tests
# ---------------------------------------------------------------------------


@pytest.mark.observability
class TestProcessPoolDispatchObservability:
    """Observability tests for the ``_dispatch_task`` log emissions."""

    @staticmethod
    def test_dispatch_task_emits_debug_log(
        limiter, payload, func_path, task_id, caplog
    ):
        """Verify that ``_dispatch_task`` emits a DEBUG log
        with limiter id, task id, func path, and local
        dispatch count."""
        # Arrange
        mock_future = MagicMock(spec=Future)

        # Act
        with caplog.at_level(
            logging.DEBUG, logger="redis_rate_limiter.backends.processpool.limiter"
        ):
            with (
                patch(
                    "redis_rate_limiter.backends.processpool.limiter.import_string",
                    return_value=MagicMock(),
                ),
                patch.object(limiter, "task_lifecycle") as mock_lifecycle,
                patch.object(limiter.executor, "submit", return_value=mock_future),
            ):
                mock_lifecycle.return_value.__enter__ = MagicMock(return_value=None)
                limiter._dispatch_task(func_path, payload, task_id)

        # Assert
        assert_log_emitted(
            caplog.records,
            level="DEBUG",
            label="[ProcessPoolRateLimiter]",
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
