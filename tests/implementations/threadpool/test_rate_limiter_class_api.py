"""Threading-specific class API behavioural tests.

Core class-API behaviour is covered by ``tests/implementations/test_rate_limiter_class_api.py``.
This module contains only those tests that are specific to the threading backend context.

Fixture dependencies:
    - ``redis_client``: from ``tests/conftest.py``.
    - ``_reset_limiter_class_state``: from ``tests/implementations/threadpool/conftest.py``.
"""

import pytest

from redis_rate_limiter import ThreadPoolRateLimiter


class TestThreadPoolRateLimiterClassApi:
    """Threading-specific tests for class API and backend context behaviour."""

    @staticmethod
    def test_configure_without_executor_raises_error(redis_client):
        """Verify that ``configure`` raises an error when the ``executor`` argument is not provided."""
        # Arrange
        ThreadPoolRateLimiter._reset()

        # Act & Assert
        with pytest.raises(RuntimeError, match="executor"):
            ThreadPoolRateLimiter.configure(redis_client)

    @staticmethod
    def test_reset_clears_backend_context_to_none():
        """Verify that ``_reset()`` sets the backend attribute to exactly ``None``."""
        # Act
        ThreadPoolRateLimiter._reset()

        # Assert
        # Identity check, not truthiness, catches None -> "" mutations.
        assert ThreadPoolRateLimiter._executor is None, (
            "_executor must be None after reset, not another falsy value"
        )
