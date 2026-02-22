"""Shared fixtures for the thread pool backend test suites."""

from concurrent.futures import ThreadPoolExecutor

import pytest

from celery_rate_limiter import ThreadPoolRateLimiter


@pytest.fixture(scope="session")
def executor():
    """Provide a session-scoped ThreadPoolExecutor instance for testing."""
    pool = ThreadPoolExecutor(max_workers=4)
    yield pool
    pool.shutdown(wait=True)


# noinspection PyProtectedMember
@pytest.fixture(autouse=True)
def _reset_limiter_class_state(redis_client, executor):
    """Reset the ThreadPoolRateLimiter class-level state before and after each test.

    This is performed to prevent singleton cache pollution between tests and
    to ensure that configure() is invoked with the appropriate test fixtures.
    """
    ThreadPoolRateLimiter._reset()
    ThreadPoolRateLimiter.configure(redis_client, executor=executor)
    yield
    ThreadPoolRateLimiter._reset()


@pytest.fixture
def limiter(redis_client, limiter_id, _reset_limiter_class_state):
    """Perform setup and teardown for a ThreadPoolRateLimiter instance.

    Yields:
        A configured ThreadPoolRateLimiter instance ready for testing.
    """
    # Setup.
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

    # Teardown: stop background threads, then clear all Redis keys.
    test_limiter.shutdown()
    keys = redis_client.keys(f"{limiter_id}:*")
    if keys:
        redis_client.delete(*keys)
