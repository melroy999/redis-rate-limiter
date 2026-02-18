"""Fixtures for generic implementation tests.

This module provides fixtures for testing implementations that do not require
Celery-specific functionality.
"""

from uuid import uuid4

import pytest

from celery_rate_limiter import AbstractDistributedRateLimiter


class MinimalRateLimiter(AbstractDistributedRateLimiter):
    """Minimal concrete implementation of the abstract rate limiter for testing purposes.

    This class provides the minimum set of required methods to test the
    abstract base class behavior without introducing Celery dependencies.
    """

    def _dispatch_task(self, func_path: str, payload: dict, task_id: str) -> None:
        """Dispatch implementation that performs no action, used for testing."""
        pass

    def _schedule_drain(self, delay: float = 0.0) -> None:
        """Drain scheduling implementation that performs no action, used for testing."""
        pass


class TrackingRateLimiter(MinimalRateLimiter):
    """Concrete rate limiter that records all dispatch and drain scheduling invocations."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dispatched_tasks: list[dict] = []
        self.scheduled_drains: list[float] = []

    def _dispatch_task(self, func_path: str, payload: dict, task_id: str) -> None:
        """Record the dispatch invocation for subsequent assertion in drain tests."""
        self.dispatched_tasks.append(
            {"func_path": func_path, "payload": payload, "task_id": task_id}
        )

    def _schedule_drain(self, delay: float = 0.0) -> None:
        """Record the scheduled drain delay for subsequent assertion."""
        self.scheduled_drains.append(delay)


@pytest.fixture
def task_id():
    """Provide a unique task identifier for testing to prevent accidental coupling."""
    return f"task_{uuid4().hex[:8]}"


@pytest.fixture
def generic_limiter(redis_client, limiter_id):
    """Create a minimal rate limiter instance for testing abstract behavior.

    This fixture provides a concrete implementation without Celery dependencies,
    and is therefore suitable for testing generic rate limiter functionality.

    Args:
        redis_client: The Redis client fixture provided by the parent conftest.
        limiter_id: A unique base limiter identifier from which the fixture limiter identifier is derived.

    Yields:
        A configured MinimalRateLimiter instance ready for testing.
    """
    # Setup.
    limiter_id = f"{limiter_id}_generic"
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

    # Teardown: stop the subscriber thread, then clear all Redis keys.
    test_limiter.shutdown()
    keys = redis_client.keys(f"{limiter_id}:*")
    if keys:
        redis_client.delete(*keys)


@pytest.fixture
def tracking_limiter(redis_client, limiter_id):
    """Create a tracking rate limiter that records all dispatch and schedule invocations."""
    # Setup.
    limiter_id = f"{limiter_id}_tracking"
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

    # Teardown: stop the subscriber thread, then clear all Redis keys.
    test_limiter.shutdown()
    keys = redis_client.keys(f"{limiter_id}:*")
    if keys:
        redis_client.delete(*keys)


@pytest.fixture
def make_limiter_pool(redis_client, limiter_id):
    """Factory fixture that creates N rate limiter instances sharing the same Redis state.

    This simulates N independent workers that all operate against the same
    rate limiter, which corresponds to the intended distributed deployment topology.

    Args:
        redis_client: The Redis client fixture provided by the parent conftest.
        limiter_id: A unique base identifier for the current test.

    Yields:
        A factory function that accepts (n, *, limiter_cls, **kwargs).
    """
    pool_limiter_id = None
    created_limiters: list = []

    def _factory(n, *, limiter_cls=MinimalRateLimiter, **kwargs):
        nonlocal pool_limiter_id
        pool_limiter_id = f"{limiter_id}_concurrent"
        defaults = dict(
            limit=5, window=60, max_concurrency=2, max_age=3600, lease_duration=30
        )
        defaults.update(kwargs)
        limiters = [
            limiter_cls(redis_client=redis_client, limiter_id=pool_limiter_id, **defaults)
            for _ in range(n)
        ]
        created_limiters.extend(limiters)
        return limiters

    yield _factory

    # Teardown: stop subscriber threads, then clear all Redis keys.
    for lim in created_limiters:
        lim.shutdown()
    if pool_limiter_id:
        keys = redis_client.keys(f"{pool_limiter_id}:*")
        if keys:
            redis_client.delete(*keys)
