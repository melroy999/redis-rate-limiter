"""Shared fixtures for threading backend test suites."""

from concurrent.futures import ThreadPoolExecutor

import pytest

from celery_rate_limiter import ThreadPoolRateLimiter


@pytest.fixture(scope="session")
def executor():
    """Provide a ThreadPoolExecutor for testing."""
    pool = ThreadPoolExecutor(max_workers=4)
    yield pool
    pool.shutdown(wait=True)


# noinspection PyProtectedMember
@pytest.fixture(autouse=True)
def _reset_limiter_class_state(redis_client, executor):
    """Reset ThreadPoolRateLimiter class-level state before and after each test.

    This prevents singleton cache pollution between tests and ensures
    configure() is called with the test fixtures.
    """
    ThreadPoolRateLimiter._reset()
    ThreadPoolRateLimiter.configure(redis_client, executor=executor)
    yield
    ThreadPoolRateLimiter._reset()


@pytest.fixture
def limiter(redis_client, executor, default_limiter_id):
    """Setup and teardown for the ThreadPoolRateLimiter.

    Yields:
        A configured ThreadPoolRateLimiter instance for testing.
    """
    # Setup
    limiter_id = default_limiter_id
    test_limiter = ThreadPoolRateLimiter.create(
        limiter_id=limiter_id,
        limit=5,
        window=60,
        max_concurrency=2,
        max_age=3600,
        lease_duration=30,
        override=True,
    )

    yield test_limiter

    # Teardown
    # Clear keys associated with this limiter.
    keys = redis_client.keys(f"{limiter_id}:*")
    if keys:
        redis_client.delete(*keys)
