"""Dramatiq-specific class API behavioural tests.

Core class-API behaviour is covered by
``tests/implementations/test_rate_limiter_class_api.py``.
This module contains only those tests that are specific to
the Dramatiq backend context.

Fixture dependencies:
    - ``redis_client``: from ``tests/conftest.py``.
    - ``_reset_limiter_class_state``: from
      ``tests/implementations/dramatiq/conftest.py``.
"""

import pytest

from redis_rate_limiter import DramatiqRateLimiter


@pytest.mark.behavior
class TestDramatiqRateLimiterClassApi:
    """Dramatiq-specific tests for class API and backend context behaviour."""

    @staticmethod
    def test_configure_without_broker_raises_error(redis_client):
        """Verify that ``configure`` raises an error when the
        ``broker`` argument is not provided."""
        # Arrange
        DramatiqRateLimiter._reset()

        # Act & Assert
        with pytest.raises(
            RuntimeError,
            match=r"^DramatiqRateLimiter\.configure\(redis_client, broker=broker\) must be called",
        ):
            DramatiqRateLimiter.configure(redis_client)

    @staticmethod
    def test_reset_clears_backend_context_to_none():
        """Verify that ``_reset()`` sets the backend
        attribute to exactly ``None``."""
        # Act
        DramatiqRateLimiter._reset()

        # Assert
        # _has_backend_context uses ``is not None``, so a falsy
        # non-None value like "" would incorrectly signal that
        # the broker is configured.
        assert DramatiqRateLimiter._broker is None, (
            "_broker must be None after reset, not another falsy value"
        )
