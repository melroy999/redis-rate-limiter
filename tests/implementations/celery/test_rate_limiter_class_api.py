"""Celery-specific class API behavioural tests.

Core class-API behaviour is covered by ``tests/implementations/test_rate_limiter_class_api.py``.
This module contains only those tests that are specific to the Celery backend context.

Fixture dependencies:
    - ``redis_client``: from ``tests/conftest.py``.
    - ``_reset_limiter_class_state``: from ``tests/implementations/celery/conftest.py``.
"""

import pytest

from redis_rate_limiter import CeleryRateLimiter


class TestCeleryRateLimiterClassApi:
    """Celery-specific tests for class API and backend context behaviour."""

    @staticmethod
    def test_configure_without_celery_app_raises_error(redis_client):
        """Verify that ``configure`` raises an error when the ``celery_app`` argument is not provided."""
        # Arrange
        CeleryRateLimiter._reset()

        # Act & Assert
        with pytest.raises(RuntimeError, match="celery_app"):
            CeleryRateLimiter.configure(redis_client)

    @staticmethod
    def test_reset_clears_backend_context_to_none():
        """Verify that ``_reset()`` sets the backend attribute to exactly ``None``."""
        # Act
        CeleryRateLimiter._reset()

        # Assert
        # Identity check, not truthiness, catches None -> "" mutations.
        assert CeleryRateLimiter._celery_app is None, (
            "_celery_app must be None after reset, not another falsy value"
        )
