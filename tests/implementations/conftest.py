"""Fixtures for generic implementation tests.

This module provides fixtures for testing implementations that don't require
Celery-specific functionality.
"""

import pytest

from src.celery_rate_limiter.limiters import AbstractDistributedRateLimiter


class MinimalRateLimiter(AbstractDistributedRateLimiter):
    """Minimal concrete implementation for testing abstract behavior.

    This implementation provides the minimum required methods to test
    the abstract base class behavior without Celery dependencies.
    """

    def _dispatch_task(self, func_path: str, payload: dict, task_id: str) -> None:
        """No-op dispatch for testing."""
        pass

    def _schedule_drain(self, delay: float = 0.0) -> None:
        """No-op schedule for testing."""
        pass


@pytest.fixture
def task_id():
    """Provide a consistent task ID for testing."""
    return "task123"


@pytest.fixture
def generic_limiter(redis_client):
    """Create a minimal rate limiter for testing abstract behavior.

    This limiter provides a concrete implementation without Celery dependencies,
    suitable for testing generic rate limiter functionality.

    Args:
        redis_client: The Redis client fixture from parent conftest.

    Yields:
        A configured MinimalRateLimiter instance for testing.
    """
    # SETUP
    limiter_id = "test_limiter"
    test_limiter = MinimalRateLimiter(
        redis_client=redis_client,
        limiter_id=limiter_id,
        limit=5,
        window=60,
        max_concurrency=2,
        max_age=3600,
        lease_duration=30,
    )

    yield test_limiter

    # TEARDOWN: Clear keys associated with this limiter
    keys = redis_client.keys(f"{limiter_id}:*")
    if keys:
        redis_client.delete(*keys)
