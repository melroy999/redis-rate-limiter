"""Tests for the ``rate_limited`` decorator behavior.

Fixture dependencies:
    - ``task_id``: from ``tests/implementations/conftest.py``.
    - ``limiter_id``: from ``tests/conftest.py``.
"""

import inspect
import logging
from unittest.mock import MagicMock, patch

import pytest

from redis_rate_limiter import rate_limited
from redis_rate_limiter.core.decorators import _get_default_limiter
from tests.helpers.utils import assert_log_emitted

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def limiter_mock():
    """Create a limiter mock with a mocked ``task_lifecycle`` context manager."""
    lifecycle_context = MagicMock()
    lifecycle_context.__enter__.return_value = lifecycle_context
    lifecycle_context.__exit__.return_value = False

    limiter = MagicMock()
    limiter.task_lifecycle.return_value = lifecycle_context
    return limiter, lifecycle_context


# ---------------------------------------------------------------------------
# Concrete test cases
# ---------------------------------------------------------------------------


@pytest.mark.behavior
class TestRateLimitedDecorator:
    """Test suite for decorator-driven task lifecycle wrapping."""

    @staticmethod
    def test_decorator_wraps_function_in_task_lifecycle(
        limiter_mock, limiter_id, task_id
    ):
        """Verify that the decorated function execution is
        wrapped in ``task_lifecycle()``.
        """
        # Arrange
        limiter, lifecycle_context = limiter_mock

        @rate_limited(limiter_id)
        def wrapped_function(value: int) -> int:
            return value * 2

        # Act
        with patch(
            "redis_rate_limiter.core.decorators._get_default_limiter",
            return_value=limiter,
        ):
            result = wrapped_function(4, _rate_limit_task_id=task_id)

        # Assert
        assert result == 8, "decorated function should return wrapped result"
        limiter.task_lifecycle.assert_called_once_with(task_id)
        lifecycle_context.__enter__.assert_called_once()
        lifecycle_context.__exit__.assert_called_once()

    @staticmethod
    def test_decorator_pops_rate_limit_task_id_from_kwargs(
        limiter_mock, limiter_id, task_id
    ):
        """Verify that ``_rate_limit_task_id`` is consumed
        and not forwarded to the wrapped function.
        """
        # Arrange
        limiter, _ = limiter_mock
        captured_kwargs = {}

        @rate_limited(limiter_id)
        def wrapped_function(**kwargs):
            captured_kwargs.update(kwargs)
            return "ok"

        # Act
        with patch(
            "redis_rate_limiter.core.decorators._get_default_limiter",
            return_value=limiter,
        ):
            result = wrapped_function(alpha=1, _rate_limit_task_id=task_id)

        # Assert
        assert result == "ok", "wrapped function return value should be preserved"
        assert "_rate_limit_task_id" not in captured_kwargs, (
            "_rate_limit_task_id should not be forwarded to wrapped function"
        )
        assert captured_kwargs["alpha"] == 1, "non-reserved kwargs should be preserved"

    @staticmethod
    def test_decorator_forwards_limiter_id_kwarg_to_wrapped_function(
        limiter_mock, limiter_id, task_id
    ):
        """Verify that ``limiter_id`` in kwargs is read
        (not popped) and forwarded to the wrapped function.
        """
        # Arrange
        limiter, _ = limiter_mock
        captured_kwargs = {}

        @rate_limited()
        def wrapped_function(**kwargs):
            captured_kwargs.update(kwargs)
            return "ok"

        # Act
        with patch(
            "redis_rate_limiter.core.decorators._get_default_limiter",
            return_value=limiter,
        ):
            wrapped_function(
                limiter_id=limiter_id,
                _rate_limit_task_id=task_id,
            )

        # Assert
        assert "limiter_id" in captured_kwargs, (
            "limiter_id should be forwarded to the wrapped function (get, not pop)"
        )
        assert captured_kwargs["limiter_id"] == limiter_id, (
            "forwarded limiter_id value should match the original"
        )

    @staticmethod
    def test_decorator_resolves_limiter_id_from_argument(
        limiter_mock, limiter_id, task_id
    ):
        """Verify that the decorator's limiter_id argument
        takes precedence for limiter lookup.
        """
        # Arrange
        limiter, _ = limiter_mock

        @rate_limited(limiter_id)
        def wrapped_function(**kwargs):
            return kwargs.get("payload", "ok")

        # Act
        with patch(
            "redis_rate_limiter.core.decorators._get_default_limiter",
            return_value=limiter,
        ) as mock_get:
            wrapped_function(
                payload="done",
                limiter_id="ignored_limiter_id",
                _rate_limit_task_id=task_id,
            )

        # Assert
        mock_get.assert_called_once_with(limiter_id)

    @staticmethod
    def test_decorator_resolves_limiter_id_from_kwargs(
        limiter_mock, limiter_id, task_id
    ):
        """Verify that the decorator falls back to
        ``kwargs['limiter_id']`` when the argument is None.
        """
        # Arrange
        limiter, _ = limiter_mock

        @rate_limited()
        def wrapped_function(**kwargs):
            return kwargs.get("payload")

        # Act
        with patch(
            "redis_rate_limiter.core.decorators._get_default_limiter",
            return_value=limiter,
        ) as mock_get:
            wrapped_function(
                payload={"x": 1},
                limiter_id=limiter_id,
                _rate_limit_task_id=task_id,
            )

        # Assert
        mock_get.assert_called_once_with(limiter_id)

    @staticmethod
    def test_decorator_propagates_wrapped_function_exception(
        limiter_mock, limiter_id, task_id
    ):
        """Verify that exceptions from the wrapped function
        propagate and that lifecycle cleanup is performed.
        """
        # Arrange
        limiter, lifecycle_context = limiter_mock

        @rate_limited(limiter_id)
        def wrapped_function() -> None:
            raise RuntimeError("wrapped function failed")

        # Act & Assert
        with patch(
            "redis_rate_limiter.core.decorators._get_default_limiter",
            return_value=limiter,
        ):
            with pytest.raises(RuntimeError, match="wrapped function failed"):
                wrapped_function(_rate_limit_task_id=task_id)

        lifecycle_context.__enter__.assert_called_once()
        lifecycle_context.__exit__.assert_called_once()

    @staticmethod
    def test_decorator_raises_when_task_id_missing(limiter_mock, limiter_id):
        """Verify that a missing ``_rate_limit_task_id`` raises a KeyError."""
        # Arrange
        limiter, lifecycle_context = limiter_mock

        @rate_limited(limiter_id)
        def wrapped_function(**kwargs):
            return kwargs

        # Act & Assert
        with patch(
            "redis_rate_limiter.core.decorators._get_default_limiter",
            return_value=limiter,
        ) as mock_get:
            with pytest.raises(KeyError, match="_rate_limit_task_id"):
                wrapped_function(alpha=1)

        mock_get.assert_called_once_with(limiter_id)
        limiter.task_lifecycle.assert_not_called()
        lifecycle_context.__enter__.assert_not_called()

    @staticmethod
    def test_decorator_preserves_function_metadata(limiter_id):
        """Verify that ``functools.wraps`` preserves the function metadata."""

        # Arrange
        def original_function():
            """original function docstring."""
            return "ok"

        # Act
        decorated = rate_limited(limiter_id)(original_function)

        # Assert
        assert decorated.__name__ == original_function.__name__, (
            "decorator should preserve function __name__"
        )
        assert decorated.__doc__ == original_function.__doc__, (
            "decorator should preserve function __doc__"
        )

    @staticmethod
    def test_decorator_uses_injected_limiter_resolver(
        limiter_mock, limiter_id, task_id
    ):
        """Verify that the decorator can resolve limiters
        via an injected backend resolver.
        """
        # Arrange
        limiter, _ = limiter_mock
        resolver = MagicMock(return_value=limiter)

        @rate_limited(limiter_id, get_limiter=resolver)
        def wrapped_function(value: int) -> int:
            return value

        # Act
        result = wrapped_function(9, _rate_limit_task_id=task_id)

        # Assert
        assert result == 9, (
            "decorator should preserve return value with custom resolver"
        )
        resolver.assert_called_once_with(limiter_id)

    @staticmethod
    def test_decorator_raises_value_error_when_limiter_id_missing(
        limiter_mock, task_id
    ):
        """Verify that a ``ValueError`` is raised when neither
        the decorator argument nor kwargs provide a limiter_id.
        """
        # Arrange
        limiter, _ = limiter_mock

        @rate_limited()
        def wrapped_function(**kwargs):
            return kwargs

        # Act & Assert
        with patch(
            "redis_rate_limiter.core.decorators._get_default_limiter",
            return_value=limiter,
        ):
            with pytest.raises(ValueError, match="Missing limiter id"):
                wrapped_function(_rate_limit_task_id=task_id)

    @staticmethod
    def test_decorator_uses_default_limiter_resolver(limiter_mock, limiter_id, task_id):
        """Verify that ``_get_default_limiter`` is exercised
        when no custom resolver is provided.
        """
        # Arrange
        limiter, _ = limiter_mock

        @rate_limited(limiter_id)
        def wrapped_function(value: int) -> int:
            return value * 3

        # Act
        with patch(
            "redis_rate_limiter.backends.threading.ThreadPoolRateLimiter.get",
            return_value=limiter,
        ) as mock_get:
            result = wrapped_function(7, _rate_limit_task_id=task_id)

        # Assert
        assert result == 21, (
            "decorated function should return wrapped result via default resolver"
        )
        mock_get.assert_called_once_with(limiter_id)

    @staticmethod
    def test_raises_attribute_error_when_resolver_returns_none(limiter_id, task_id):
        """Verify that an ``AttributeError`` propagates when
        the resolver returns ``None``.

        The decorator calls ``limiter.task_lifecycle()`` without a ``None``
        check, so a resolver that returns ``None`` raises ``AttributeError``.
        """
        # Arrange
        resolver = MagicMock(return_value=None)

        @rate_limited(limiter_id, get_limiter=resolver)
        def wrapped_function() -> str:
            return "ok"

        # Act & Assert
        with pytest.raises(AttributeError):
            wrapped_function(_rate_limit_task_id=task_id)

    @staticmethod
    def test_propagates_exception_from_lifecycle_enter(
        limiter_mock, limiter_id, task_id
    ):
        """Verify that an exception from
        ``task_lifecycle().__enter__`` propagates unchanged.

        The context manager entry is not wrapped in a try/except, so
        exceptions from ``__enter__`` propagate directly to the caller.
        """
        # Arrange
        limiter, lifecycle_context = limiter_mock
        lifecycle_context.__enter__.side_effect = RuntimeError("lifecycle entry failed")

        @rate_limited(limiter_id, get_limiter=MagicMock(return_value=limiter))
        def wrapped_function() -> str:
            return "ok"

        # Act & Assert
        with pytest.raises(RuntimeError, match="lifecycle entry failed"):
            wrapped_function(_rate_limit_task_id=task_id)


# ---------------------------------------------------------------------------
# Observability tests
# ---------------------------------------------------------------------------


@pytest.mark.observability
class TestRateLimitedDecoratorObservability:
    """Observability tests for the ``rate_limited`` decorator."""

    @staticmethod
    def test_decorator_emits_entry_and_completion_debug_logs(
        limiter_mock, limiter_id, task_id, caplog
    ):
        """Verify that the decorated function emits debug
        logs for entry and completion.
        """
        # Arrange
        limiter, _ = limiter_mock

        @rate_limited(limiter_id)
        def wrapped_function(value: int) -> int:
            return value * 2

        # Act
        with caplog.at_level(
            logging.DEBUG, logger="redis_rate_limiter.core.decorators"
        ):
            with patch(
                "redis_rate_limiter.core.decorators._get_default_limiter",
                return_value=limiter,
            ):
                wrapped_function(4, _rate_limit_task_id=task_id)

        # Assert
        assert_log_emitted(
            caplog.records,
            "DEBUG",
            [
                "decorator entered",
                f"limiter={limiter_id}",
                f"task_id={task_id}",
                "func=",
                "wrapped_function",
            ],
            "should emit a debug log for the decorator entry"
            " with limiter id, task id, and func qualname",
        )
        assert_log_emitted(
            caplog.records,
            "DEBUG",
            [
                "execution completed",
                f"limiter={limiter_id}",
                f"task_id={task_id}",
                "func=",
                "wrapped_function",
            ],
            "should emit a debug log for the task completion"
            " with limiter id, task id, and func qualname",
        )


@pytest.mark.behavior
class TestGetDefaultLimiter:
    """Tests for the ``_get_default_limiter`` module-level helper."""

    @staticmethod
    def test_forwards_limiter_id_to_backend_get(limiter_id):
        """Verify that ``_get_default_limiter`` passes the
        limiter_id argument to ``ThreadPoolRateLimiter.get``.
        """
        # Act
        with patch(
            "redis_rate_limiter.backends.threading.ThreadPoolRateLimiter.get",
            return_value=MagicMock(),
        ) as mock_get:
            _get_default_limiter(limiter_id)

        # Assert
        mock_get.assert_called_once_with(limiter_id)


# ---------------------------------------------------------------------------
# Signature tests
# ---------------------------------------------------------------------------


@pytest.mark.signature
class TestRateLimitedSignatures:
    """Signature tests for ``rate_limited()`` default parameter values."""

    @staticmethod
    def test_rate_limited_default_parameters():
        """Verify that ``limiter_id`` and ``get_limiter`` have the expected defaults.

        Mutation target: ``limiter_id`` and ``get_limiter``
        default values in ``rate_limited()``.
        """
        # Arrange & Act
        sig = inspect.signature(rate_limited)

        # Assert
        assert sig.parameters["limiter_id"].default is None, (
            "limiter_id should default to None"
        )
        assert sig.parameters["get_limiter"].default is None, (
            "get_limiter should default to None"
        )
