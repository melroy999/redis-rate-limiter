"""Shared fixtures for the thread pool backend test suites."""

from concurrent.futures import ThreadPoolExecutor

import pytest

from redis_rate_limiter import ThreadPoolRateLimiter
from tests.helpers.utils import cleanup_managed_limiter
from tests.implementations.conftest import DEFAULT_LIMITER_CONFIG


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
    # Setup
    test_limiter = ThreadPoolRateLimiter.create(
        limiter_id=limiter_id,
        **DEFAULT_LIMITER_CONFIG,
        override=True,
    )

    yield test_limiter

    # Teardown
    test_limiter.shutdown()
    cleanup_managed_limiter(redis_client, limiter_id)
