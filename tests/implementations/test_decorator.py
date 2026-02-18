"""Tests for the ``rate_limited`` decorator behavior."""

from unittest.mock import MagicMock, patch

import pytest

from celery_rate_limiter import rate_limited


@pytest.fixture
def limiter_mock():
    """Create a limiter mock with a mocked ``task_lifecycle`` context manager."""
    lifecycle_context = MagicMock()
    lifecycle_context.__enter__.return_value = lifecycle_context
    lifecycle_context.__exit__.return_value = False

    limiter = MagicMock()
    limiter.task_lifecycle.return_value = lifecycle_context
    return limiter, lifecycle_context


class TestRateLimitedDecorator:
    """Test suite for decorator-driven task lifecycle wrapping."""

    @staticmethod
    def test_decorator_wraps_function_in_task_lifecycle(limiter_mock):
        """Verify that the decorated function execution is wrapped in ``task_lifecycle()``."""
        # Arrange
        limiter, lifecycle_context = limiter_mock

        @rate_limited("decorator_limiter")
        def wrapped_function(value: int) -> int:
            return value * 2

        # Act
        with patch(
            "celery_rate_limiter.core.decorators._get_default_limiter",
            return_value=limiter,
        ):
            result = wrapped_function(4, _rate_limit_task_id="task-123")

        # Assert
        assert result == 8, "decorated function should return wrapped result"
        limiter.task_lifecycle.assert_called_once_with("task-123")
        lifecycle_context.__enter__.assert_called_once()
        lifecycle_context.__exit__.assert_called_once()

    @staticmethod
    def test_decorator_pops_rate_limit_task_id_from_kwargs(limiter_mock):
        """Verify that ``_rate_limit_task_id`` is consumed and not forwarded to the wrapped function."""
        # Arrange
        limiter, _ = limiter_mock
        captured_kwargs = {}

        @rate_limited("decorator_limiter")
        def wrapped_function(**kwargs):
            captured_kwargs.update(kwargs)
            return "ok"

        # Act
        with patch(
            "celery_rate_limiter.core.decorators._get_default_limiter",
            return_value=limiter,
        ):
            result = wrapped_function(alpha=1, _rate_limit_task_id="task-456")

        # Assert
        assert result == "ok", "wrapped function return value should be preserved"
        assert "_rate_limit_task_id" not in captured_kwargs, (
            "_rate_limit_task_id should not be forwarded to wrapped function"
        )
        assert captured_kwargs["alpha"] == 1, "non-reserved kwargs should be preserved"

    @staticmethod
    def test_decorator_resolves_limiter_id_from_argument(limiter_mock):
        """Verify that the decorator's limiter_id argument takes precedence for limiter lookup."""
        # Arrange
        limiter, _ = limiter_mock

        @rate_limited("explicit_limiter_id")
        def wrapped_function(**kwargs):
            return kwargs.get("payload", "ok")

        # Act
        with patch(
            "celery_rate_limiter.core.decorators._get_default_limiter",
            return_value=limiter,
        ) as mock_get:
            wrapped_function(
                payload="done",
                limiter_id="ignored_limiter_id",
                _rate_limit_task_id="task-789",
            )

        # Assert
        mock_get.assert_called_once_with("explicit_limiter_id")

    @staticmethod
    def test_decorator_resolves_limiter_id_from_kwargs(limiter_mock):
        """Verify that the decorator falls back to ``kwargs['limiter_id']`` when the argument is None."""
        # Arrange
        limiter, _ = limiter_mock

        @rate_limited()
        def wrapped_function(**kwargs):
            return kwargs.get("payload")

        # Act
        with patch(
            "celery_rate_limiter.core.decorators._get_default_limiter",
            return_value=limiter,
        ) as mock_get:
            wrapped_function(
                payload={"x": 1},
                limiter_id="kwargs_limiter_id",
                _rate_limit_task_id="task-999",
            )

        # Assert
        mock_get.assert_called_once_with("kwargs_limiter_id")

    @staticmethod
    def test_decorator_returns_function_result(limiter_mock):
        """Verify that the decorator returns the wrapped function's result unchanged."""
        # Arrange
        limiter, _ = limiter_mock

        @rate_limited("result_limiter")
        def wrapped_function(left: int, right: int) -> dict:
            return {"sum": left + right}

        # Act
        with patch(
            "celery_rate_limiter.core.decorators._get_default_limiter",
            return_value=limiter,
        ):
            result = wrapped_function(2, 5, _rate_limit_task_id="task-111")

        # Assert
        assert result == {"sum": 7}, "decorator should not alter wrapped return value"

    @staticmethod
    def test_decorator_propagates_wrapped_function_exception(limiter_mock):
        """Verify that exceptions from the wrapped function propagate and that lifecycle cleanup is performed."""
        # Arrange
        limiter, lifecycle_context = limiter_mock

        @rate_limited("error_limiter")
        def wrapped_function() -> None:
            raise RuntimeError("wrapped function failed")

        # Act & Assert
        with patch(
            "celery_rate_limiter.core.decorators._get_default_limiter",
            return_value=limiter,
        ):
            with pytest.raises(RuntimeError, match="wrapped function failed"):
                wrapped_function(_rate_limit_task_id="task-error")

        lifecycle_context.__enter__.assert_called_once()
        lifecycle_context.__exit__.assert_called_once()

    @staticmethod
    def test_decorator_raises_when_task_id_missing(limiter_mock):
        """Verify that a missing ``_rate_limit_task_id`` raises a KeyError."""
        # Arrange
        limiter, lifecycle_context = limiter_mock

        @rate_limited("missing_task_id_limiter")
        def wrapped_function(**kwargs):
            return kwargs

        # Act & Assert
        with patch(
            "celery_rate_limiter.core.decorators._get_default_limiter",
            return_value=limiter,
        ) as mock_get:
            with pytest.raises(KeyError, match="_rate_limit_task_id"):
                wrapped_function(alpha=1)

        mock_get.assert_called_once_with("missing_task_id_limiter")
        limiter.task_lifecycle.assert_not_called()
        lifecycle_context.__enter__.assert_not_called()

    @staticmethod
    def test_decorator_preserves_function_metadata():
        """Verify that ``functools.wraps`` preserves the function metadata."""

        # Arrange
        def original_function():
            """original function docstring."""
            return "ok"

        # Act
        decorated = rate_limited("metadata_limiter")(original_function)

        # Assert
        assert decorated.__name__ == original_function.__name__, (
            "decorator should preserve function __name__"
        )
        assert decorated.__doc__ == original_function.__doc__, (
            "decorator should preserve function __doc__"
        )

    @staticmethod
    def test_decorator_uses_injected_limiter_resolver(limiter_mock):
        """Verify that the decorator can resolve limiters via an injected backend resolver."""
        # Arrange
        limiter, _ = limiter_mock
        resolver = MagicMock(return_value=limiter)

        @rate_limited("resolver_limiter", get_limiter=resolver)
        def wrapped_function(value: int) -> int:
            return value

        # Act
        result = wrapped_function(9, _rate_limit_task_id="task-resolver")

        # Assert
        assert result == 9, (
            "decorator should preserve return value with custom resolver"
        )
        resolver.assert_called_once_with("resolver_limiter")

    @staticmethod
    def test_decorator_raises_value_error_when_limiter_id_missing(limiter_mock):
        """Verify that a ``ValueError`` is raised when neither the decorator argument nor kwargs provide a limiter_id."""
        # Arrange
        limiter, _ = limiter_mock

        @rate_limited()
        def wrapped_function(**kwargs):
            return kwargs

        # Act & Assert
        with patch(
            "celery_rate_limiter.core.decorators._get_default_limiter",
            return_value=limiter,
        ):
            with pytest.raises(ValueError, match="Missing limiter id"):
                wrapped_function(_rate_limit_task_id="task-no-limiter-id")

    @staticmethod
    def test_decorator_uses_default_limiter_resolver(limiter_mock):
        """Verify that ``_get_default_limiter`` is exercised when no custom resolver is provided."""
        # Arrange
        limiter, _ = limiter_mock

        @rate_limited("default_resolver_limiter")
        def wrapped_function(value: int) -> int:
            return value * 3

        # Act
        with patch(
            "celery_rate_limiter.backends.threading.ThreadPoolRateLimiter.get",
            return_value=limiter,
        ) as mock_get:
            result = wrapped_function(7, _rate_limit_task_id="task-default-resolver")

        # Assert
        assert result == 21, (
            "decorated function should return wrapped result via default resolver"
        )
        mock_get.assert_called_once_with("default_resolver_limiter")
