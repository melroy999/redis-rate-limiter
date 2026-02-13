"""Tests for the Celery task helper modules."""

from unittest.mock import MagicMock, patch

from celery_rate_limiter.backends.celery.tasks.worker import generic_rate_limited_worker


class TestGenericWorkerTask:
    """Test suite for the ``generic_rate_limited_worker`` task behaviour."""

    def test_generic_worker_resolves_and_executes_function(self):
        """Verify that the generic worker resolves the callable and executes it with the payload keyword arguments."""
        # Arrange
        limiter = MagicMock()
        lifecycle_context = MagicMock()
        lifecycle_context.__enter__.return_value = lifecycle_context
        lifecycle_context.__exit__.return_value = False
        limiter.task_lifecycle.return_value = lifecycle_context
        target_func = MagicMock(return_value={"ok": True})
        payload = {"alpha": 1, "beta": 2}

        # Act
        with (
            patch(
                "celery_rate_limiter.core.decorators._get_default_limiter",
                return_value=limiter,
            ) as mock_get,
            patch(
                "celery_rate_limiter.backends.celery.tasks.worker.import_string",
                return_value=target_func,
            ) as mock_import,
        ):
            result = generic_rate_limited_worker.run(
                limiter_id="worker_limiter",
                func_path="json.dumps",
                payload=payload,
                _rate_limit_task_id="task-42",
            )

        # Assert
        assert result == {"ok": True}, "generic worker should return target result"
        mock_get.assert_called_once_with("worker_limiter")
        limiter.task_lifecycle.assert_called_once_with("task-42")
        mock_import.assert_called_once_with("json.dumps")
        target_func.assert_called_once_with(**payload)
