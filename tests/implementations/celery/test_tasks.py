"""Tests for Celery task helper modules."""

import json
from unittest.mock import MagicMock, patch

import pytest

from celery_rate_limiter.tasks.dispatcher import attempt_consume
from celery_rate_limiter.tasks.worker import generic_rate_limited_worker, import_string


class TestImportString:
    """Test suite for import_string() behavior."""

    def test_import_string_resolves_valid_function(self):
        """Verify import_string resolves valid callable import path."""
        # Act
        resolved = import_string("json.dumps")

        # Assert
        assert resolved is json.dumps, (
            "import_string should resolve json.dumps callable"
        )

    def test_import_string_raises_type_error_for_non_callable(self):
        """Verify import_string raises TypeError for non-callable targets."""
        # Act & Assert
        with pytest.raises(TypeError, match="not callable"):
            import_string("json.__doc__")

    def test_import_string_raises_on_invalid_module(self):
        """Verify import_string raises ModuleNotFoundError for missing module."""
        # Act & Assert
        with pytest.raises(ModuleNotFoundError):
            import_string("missing_module_for_tests.function_name")

    def test_import_string_raises_on_missing_attribute(self):
        """Verify import_string raises AttributeError for missing attribute."""
        # Act & Assert
        with pytest.raises(AttributeError):
            import_string("json.this_attribute_does_not_exist")

    @pytest.mark.parametrize(
        ("invalid_path", "expected_exception"),
        [
            ("", ValueError),
            ("no_dot_path", ValueError),
            ("  json.dumps  ", ModuleNotFoundError),
        ],
        ids=["empty_string", "no_dot", "whitespace_padded"],
    )
    def test_import_string_raises_on_malformed_path(
        self,
        invalid_path: str,
        expected_exception: type[Exception],
    ):
        """Verify import_string raises on structurally invalid import paths."""
        # Act & Assert
        with pytest.raises(expected_exception):
            import_string(invalid_path)


class TestAttemptConsumeTask:
    """Test suite for attempt_consume task behavior."""

    def test_attempt_consume_calls_drain(self):
        """Verify attempt_consume resolves limiter and calls drain()."""
        # Arrange
        limiter = MagicMock()

        # Act
        with patch(
            "celery_rate_limiter.tasks.dispatcher.CeleryRateLimiter.get",
            return_value=limiter,
        ) as mock_get:
            attempt_consume.run("my_limiter")

        # Assert
        mock_get.assert_called_once_with("my_limiter")
        limiter.drain.assert_called_once_with()

    def test_attempt_consume_raises_on_unknown_limiter(self):
        """Verify ValueError from CeleryRateLimiter.get() propagates."""
        # Act & Assert
        with patch(
            "celery_rate_limiter.tasks.dispatcher.CeleryRateLimiter.get",
            side_effect=ValueError("not found"),
        ):
            with pytest.raises(ValueError, match="not found"):
                attempt_consume.run("missing_limiter")


class TestGenericWorkerTask:
    """Test suite for generic_rate_limited_worker task behavior."""

    def test_generic_worker_resolves_and_executes_function(self):
        """Verify generic worker resolves callable and executes it with payload kwargs."""
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
                "celery_rate_limiter.decorators.CeleryRateLimiter.get",
                return_value=limiter,
            ) as mock_get,
            patch(
                "celery_rate_limiter.tasks.worker.import_string",
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
