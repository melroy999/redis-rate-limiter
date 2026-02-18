"""Celery-specific class API behavioural tests.

Core class-API behaviour is covered by ``tests/implementations/test_rate_limiter_class_api.py``.
This module contains only those tests that are specific to the Celery backend context.
"""

import pytest

from celery_rate_limiter import CeleryRateLimiter


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
