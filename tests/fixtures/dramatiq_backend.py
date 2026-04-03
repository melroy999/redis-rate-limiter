"""Shared fixtures for the Dramatiq backend test suites."""

import os

import dramatiq
import pytest
from dramatiq.brokers.redis import RedisBroker

from redis_rate_limiter import DramatiqRateLimiter
from tests.helpers.utils import cleanup_managed_limiter
from tests.implementations.conftest import DEFAULT_LIMITER_CONFIG

# Redis configuration is derived from environment variables.
# The default values target localhost:6379, but may be overridden for Docker Compose.
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))


@pytest.fixture(scope="session")
def dramatiq_broker():
    """Provide a session-scoped Dramatiq RedisBroker for testing.

    The broker does not need to process messages because contract tests only
    test ``consume()`` return values (not dispatch), and behavioral tests
    mock ``generic_rate_limited_worker.send()``.
    """
    broker = RedisBroker(host=REDIS_HOST, port=REDIS_PORT)
    dramatiq.set_broker(broker)
    return broker


# noinspection PyProtectedMember
@pytest.fixture(autouse=True)
def _reset_limiter_class_state(redis_client, dramatiq_broker):
    """Reset the DramatiqRateLimiter class-level state before and after each test.

    This is performed to prevent singleton cache pollution between tests and
    to ensure that configure() is invoked with the appropriate test fixtures.
    """
    DramatiqRateLimiter._reset()
    DramatiqRateLimiter.configure(redis_client, broker=dramatiq_broker)
    yield
    DramatiqRateLimiter._reset()


@pytest.fixture
def limiter(redis_client, limiter_id, _reset_limiter_class_state):
    """Perform setup and teardown for a DramatiqRateLimiter instance.

    Yields:
        A configured DramatiqRateLimiter instance ready for testing.
    """
    # Setup
    test_limiter = DramatiqRateLimiter.create(
        limiter_id=limiter_id,
        **DEFAULT_LIMITER_CONFIG,
        override=True,
    )

    yield test_limiter

    # Teardown
    test_limiter.shutdown()
    cleanup_managed_limiter(redis_client, limiter_id)
