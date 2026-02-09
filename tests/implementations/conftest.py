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


class TrackingRateLimiter(MinimalRateLimiter):
    """Concrete limiter that records dispatch and drain scheduling calls."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dispatched_tasks: list[dict] = []
        self.scheduled_drains: list[float] = []

    def _dispatch_task(self, func_path: str, payload: dict, task_id: str) -> None:
        """Record dispatch calls for assertions in drain tests."""
        self.dispatched_tasks.append(
            {"func_path": func_path, "payload": payload, "task_id": task_id}
        )

    def _schedule_drain(self, delay: float = 0.0) -> None:
        """Record scheduled drain delay for assertions."""
        self.scheduled_drains.append(delay)


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
    # Setup
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

    # Teardown
    # Clear keys associated with this limiter.
    keys = redis_client.keys(f"{limiter_id}:*")
    if keys:
        redis_client.delete(*keys)


@pytest.fixture
def tracking_limiter(redis_client):
    """Create a tracking limiter that records dispatch and schedule calls."""
    # Setup
    limiter_id = "tracking_limiter"
    test_limiter = TrackingRateLimiter(
        redis_client=redis_client,
        limiter_id=limiter_id,
        limit=5,
        window=60,
        max_concurrency=2,
        max_age=3600,
        lease_duration=30,
    )

    yield test_limiter

    # Teardown
    keys = redis_client.keys(f"{limiter_id}:*")
    if keys:
        redis_client.delete(*keys)
