"""Celery-specific class API behavior tests.

Core class-API behavior is covered by ``tests/implementations/test_rate_limiter_class_api.py``.
This module keeps only tests that are specific to Celery backend context.
"""

import pytest

from celery_rate_limiter import CeleryRateLimiter


class TestCeleryRateLimiterClassApi:
    """Celery-only tests for class API/backend context behavior."""

    def test_configure_without_celery_app_raises_error(self, redis_client):
        """Verify configure fails when celery_app is missing."""
        # Arrange
        CeleryRateLimiter._reset()

        # Act & Assert
        with pytest.raises(RuntimeError, match="celery_app"):
            CeleryRateLimiter.configure(redis_client)
