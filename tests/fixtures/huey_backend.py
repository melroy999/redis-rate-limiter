"""Shared fixtures for the Huey backend test suites."""

import os

import pytest
from huey import RedisHuey

from redis_rate_limiter import HueyRateLimiter
from tests.helpers.utils import cleanup_managed_limiter
from tests.implementations.conftest import DEFAULT_LIMITER_CONFIG

# Redis configuration is derived from environment variables.
# The default values target localhost:6379, but may be overridden for Docker Compose.
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))


@pytest.fixture(scope="session")
def huey_instance():
    """Provide a session-scoped Huey instance for testing.

    The instance does not need to process tasks because contract tests only
    test ``consume()`` return values (not dispatch), and behavioral tests
    mock the generic worker task wrapper.
    """
    return RedisHuey(
        "test_huey",
        host=REDIS_HOST,
        port=REDIS_PORT,
    )


# noinspection PyProtectedMember
@pytest.fixture(autouse=True)
def _reset_limiter_class_state(redis_client, huey_instance):
    """Reset the HueyRateLimiter class-level state before and after each test.

    This is performed to prevent singleton cache pollution between tests and
    to ensure that configure() is invoked with the appropriate test fixtures.
    """
    HueyRateLimiter._reset()
    HueyRateLimiter.configure(redis_client, huey=huey_instance)
    yield
    HueyRateLimiter._reset()


@pytest.fixture
def limiter(redis_client, limiter_id, _reset_limiter_class_state):
    """Perform setup and teardown for a HueyRateLimiter instance.

    Yields:
        A configured HueyRateLimiter instance ready for testing.
    """
    # Setup
    test_limiter = HueyRateLimiter.create(
        limiter_id=limiter_id,
        **DEFAULT_LIMITER_CONFIG,
        override=True,
    )

    yield test_limiter

    # Teardown
    test_limiter.shutdown()
    cleanup_managed_limiter(redis_client, limiter_id)
