"""Huey-specific class API behavioural tests.

Core class-API behaviour is covered by
``tests/implementations/test_rate_limiter_class_api.py``.
This module contains only those tests that are specific to
the Huey backend context.

Fixture dependencies:
    - ``redis_client``: from ``tests/conftest.py``.
    - ``_reset_limiter_class_state``: from
      ``tests/implementations/huey/conftest.py``.
"""

import pytest

from redis_rate_limiter import HueyRateLimiter


@pytest.mark.behavior
class TestHueyRateLimiterClassApi:
    """Huey-specific tests for class API and backend context behaviour."""

    @staticmethod
    def test_configure_without_huey_raises_error(redis_client):
        """Verify that ``configure`` raises an error when the
        ``huey`` argument is not provided."""
        # Arrange
        HueyRateLimiter._reset()

        # Act & Assert
        with pytest.raises(
            RuntimeError,
            match=r"^HueyRateLimiter\.configure\(redis_client, huey=huey_instance\) must be called",
        ):
            HueyRateLimiter.configure(redis_client)

    @staticmethod
    def test_reset_clears_backend_context_to_none():
        """Verify that ``_reset()`` sets the backend
        attribute to exactly ``None``."""
        # Act
        HueyRateLimiter._reset()

        # Assert
        # _has_backend_context uses ``is not None``, so a falsy
        # non-None value like "" would incorrectly signal that
        # the huey instance is configured.
        assert HueyRateLimiter._huey is None, (
            "_huey must be None after reset, not another falsy value"
        )
