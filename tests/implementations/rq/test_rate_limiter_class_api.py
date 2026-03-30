"""RQ-specific class API behavioural tests.

Core class-API behaviour is covered by
``tests/implementations/test_rate_limiter_class_api.py``.
This module contains only those tests that are specific to
the RQ backend context.

Fixture dependencies:
    - ``redis_client``: from ``tests/conftest.py``.
    - ``_reset_limiter_class_state``: from
      ``tests/implementations/rq/conftest.py``.
"""

import pytest

from redis_rate_limiter import RQRateLimiter


@pytest.mark.behavior
class TestRQRateLimiterClassApi:
    """RQ-specific tests for class API and backend context behaviour."""

    @staticmethod
    def test_configure_without_queue_raises_error(redis_client):
        """Verify that ``configure`` raises an error when the
        ``queue`` argument is not provided."""
        # Arrange
        RQRateLimiter._reset()

        # Act & Assert
        with pytest.raises(
            RuntimeError,
            match=r"^RQRateLimiter\.configure\(redis_client, queue=queue\) must be called",
        ):
            RQRateLimiter.configure(redis_client)

    @staticmethod
    def test_reset_clears_backend_context_to_none():
        """Verify that ``_reset()`` sets the backend
        attribute to exactly ``None``."""
        # Act
        RQRateLimiter._reset()

        # Assert
        # _has_backend_context uses ``is not None``, so a falsy
        # non-None value like "" would incorrectly signal that
        # the queue is configured.
        assert RQRateLimiter._queue is None, (
            "_queue must be None after reset, not another falsy value"
        )
