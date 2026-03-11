"""Shared fixtures for the RQ backend test suites."""

import os

import pytest
from rq import Queue

from redis_rate_limiter import RQRateLimiter

# Redis configuration is derived from environment variables.
# The default values target localhost:6379, but may be overridden for Docker Compose.
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))


@pytest.fixture(scope="session")
def rq_queue():
    """Provide a session-scoped RQ Queue instance for testing.

    The Queue does not need to process jobs because contract tests only
    test ``consume()`` return values (not dispatch), and behavioral tests
    mock ``queue.enqueue()``.
    """
    from redis import Redis

    conn = Redis(host=REDIS_HOST, port=REDIS_PORT)
    return Queue(connection=conn)


# noinspection PyProtectedMember
@pytest.fixture(autouse=True)
def _reset_limiter_class_state(redis_client, rq_queue):
    """Reset the RQRateLimiter class-level state before and after each test.

    This is performed to prevent singleton cache pollution between tests and
    to ensure that configure() is invoked with the appropriate test fixtures.
    """
    RQRateLimiter._reset()
    RQRateLimiter.configure(redis_client, queue=rq_queue)
    yield
    RQRateLimiter._reset()


@pytest.fixture
def limiter(redis_client, limiter_id, _reset_limiter_class_state):
    """Perform setup and teardown for an RQRateLimiter instance.

    Yields:
        A configured RQRateLimiter instance ready for testing.
    """
    # Setup
    test_limiter = RQRateLimiter.create(
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
    test_limiter.shutdown()
    keys = redis_client.keys(f"{limiter_id}:*")
    if keys:
        redis_client.delete(*keys)
