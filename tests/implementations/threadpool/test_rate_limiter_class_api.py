"""Threading-specific class API behavior tests.

Core class-API behavior is covered by ``tests/implementations/test_rate_limiter_class_api.py``.
This module keeps only tests that are specific to threading backend context.
"""

import pytest

from celery_rate_limiter import ThreadPoolRateLimiter


class TestThreadPoolRateLimiterClassApi:
    """Threading-only tests for class API/backend context behavior."""

    def test_configure_without_executor_raises_error(self, redis_client):
        """Verify configure fails when executor is missing."""
        # Arrange
        ThreadPoolRateLimiter._reset()

        # Act & Assert
        with pytest.raises(RuntimeError, match="executor"):
            ThreadPoolRateLimiter.configure(redis_client)
