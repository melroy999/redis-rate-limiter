"""Threading-specific behavioural tests for the ``ThreadPoolRateLimiter`` implementation."""

from unittest.mock import MagicMock, patch


class TestThreadPoolRateLimiter:
    """Tests that are specific to the threading backend dispatch and lifecycle logic."""

    def test_dispatch_task_submits_to_executor(
        self, limiter, default_payload, func_path, task_id
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

    def test_dispatch_task_resolves_function_path(
        self, limiter, default_payload, func_path, task_id
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

    def test_dispatch_task_wraps_in_lifecycle(
        self, limiter, default_payload, func_path, task_id
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

    def test_schedule_drain_wakes_drain_loop(self, limiter):
        """Verify that ``_schedule_drain`` delegates to the base-class ``DrainLoop``."""
        # Arrange
        delay = 1.75

        # Act
        with patch.object(limiter._drain_loop, "wake") as mock_wake:
            limiter._schedule_drain(delay=delay)

        # Assert
        mock_wake.assert_called_once_with(delay)
