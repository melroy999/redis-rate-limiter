"""Shared fixtures for the process pool backend test suites."""

from concurrent.futures import ProcessPoolExecutor

import pytest

from redis_rate_limiter import ProcessPoolRateLimiter
from tests.implementations.conftest import DEFAULT_LIMITER_CONFIG


@pytest.fixture(scope="session")
def executor():
    """Provide a session-scoped ProcessPoolExecutor instance for testing."""
    pool = ProcessPoolExecutor(max_workers=4)
    yield pool
    pool.shutdown(wait=True)


# noinspection PyProtectedMember
@pytest.fixture(autouse=True)
def _reset_limiter_class_state(redis_client, executor):
    """Reset the ProcessPoolRateLimiter class-level state before and after each test.

    This is performed to prevent singleton cache pollution between tests and
    to ensure that configure() is invoked with the appropriate test fixtures.
    """
    ProcessPoolRateLimiter._reset()
    ProcessPoolRateLimiter.configure(redis_client, executor=executor)
    yield
    ProcessPoolRateLimiter._reset()


@pytest.fixture
def limiter(redis_client, limiter_id, _reset_limiter_class_state):
    """Perform setup and teardown for a ProcessPoolRateLimiter instance.

    Yields:
        A configured ProcessPoolRateLimiter instance ready for testing.
    """
    # Setup
    test_limiter = ProcessPoolRateLimiter.create(
        limiter_id=limiter_id,
        **DEFAULT_LIMITER_CONFIG,
        override=True,
    )

    yield test_limiter

    # Teardown
    test_limiter.shutdown()
    keys = redis_client.keys(f"{limiter_id}:*")
    if keys:
        redis_client.delete(*keys)
