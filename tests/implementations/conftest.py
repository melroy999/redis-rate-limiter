"""Fixtures for generic implementation tests.

This module provides fixtures for testing implementations that don't require
Celery-specific functionality.
"""

from uuid import uuid4

import pytest

from celery_rate_limiter import AbstractDistributedRateLimiter


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
    """Provide a unique task ID for testing to reduce accidental coupling."""
    return f"task_{uuid4().hex[:8]}"


@pytest.fixture
def generic_limiter(redis_client, default_limiter_id):
    """Create a minimal rate limiter for testing abstract behavior.

    This limiter provides a concrete implementation without Celery dependencies,
    suitable for testing generic rate limiter functionality.

    Args:
        redis_client: The Redis client fixture from parent conftest.
        default_limiter_id: A unique base limiter ID used to derive the fixture limiter ID.

    Yields:
        A configured MinimalRateLimiter instance for testing.
    """
    # Setup
    limiter_id = f"{default_limiter_id}_generic"
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
def tracking_limiter(redis_client, default_limiter_id):
    """Create a tracking limiter that records dispatch and schedule calls."""
    # Setup
    limiter_id = f"{default_limiter_id}_tracking"
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


@pytest.fixture
def make_limiter_pool(redis_client, default_limiter_id):
    """Factory fixture to create N limiter instances sharing the same Redis state.

    Simulates N independent workers all using the same rate limiter, which
    is the intended distributed deployment topology.

    Args:
        redis_client: The Redis client fixture from parent conftest.
        default_limiter_id: Unique base ID for this test.

    Yields:
        A factory function that accepts (n, *, limiter_cls, **kwargs).
    """
    limiter_id = None

    def _factory(n, *, limiter_cls=MinimalRateLimiter, **kwargs):
        nonlocal limiter_id
        limiter_id = f"{default_limiter_id}_concurrent"
        defaults = dict(
            limit=5, window=60, max_concurrency=2, max_age=3600, lease_duration=30
        )
        defaults.update(kwargs)
        return [
            limiter_cls(redis_client=redis_client, limiter_id=limiter_id, **defaults)
            for _ in range(n)
        ]

    yield _factory

    # Teardown
    if limiter_id:
        keys = redis_client.keys(f"{limiter_id}:*")
        if keys:
            redis_client.delete(*keys)
