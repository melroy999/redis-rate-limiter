import os

import pytest
import redis

from celery_rate_limiter.limiters import CeleryRateLimiter

pytest_plugins = ("celery.contrib.pytest",)


# Redis configuration from environment variables.
# Defaults to localhost:6379, but can be overridden for Docker Compose.
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))


@pytest.fixture(scope="session")
def _redis_connection():
    """Connect to a real Redis instance for testing.

    We only do this once and just flush the database between tests.

    Connection details can be configured via environment variables:
    - REDIS_HOST: Redis hostname (default: localhost)
    - REDIS_PORT: Redis port (default: 6379)

    Yields:
        A Redis client connected to the configured Redis instance.
    """
    client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    # Verify connection works before starting suite.
    try:
        client.ping()
    except redis.exceptions.ConnectionError:
        pytest.fail(
            f"Could not connect to Redis at {REDIS_HOST}:{REDIS_PORT}. Is it running?\n"
            f"Tip: Use docker-compose up redis or set REDIS_HOST/REDIS_PORT environment variables."
        )

    yield client
    client.close()


@pytest.fixture(scope="function")
def redis_client(_redis_connection):
    """Connect to a real Redis instance for testing.

    Flushes the database before and after each test.

    Yields:
        A Redis client for use in tests.
    """
    # Flush before and after the test.
    _redis_connection.flushall()
    yield _redis_connection
    _redis_connection.flushall()


@pytest.fixture(scope="session")
def celery_config():
    """Configure the celery_app fixture.

    Uses the same Redis configuration as tests (from environment variables).

    Returns:
        A dictionary with Celery configuration for testing.
    """
    redis_url = f"redis://{REDIS_HOST}:{REDIS_PORT}/0"
    return {
        "broker_url": redis_url,
        "result_backend": redis_url,
        "task_always_eager": True,
    }


@pytest.fixture(scope="session")
def func_path():
    """Fictional function path for test task scheduling.

    Returns:
        A placeholder function path string representing a non-existent
        Celery task, used when scheduling test tasks.
    """
    return "rate_limiter.test.task.function"


@pytest.fixture(scope="session")
def default_payload():
    """Default payload for test task scheduling.

    Returns:
        A generic payload dictionary for use in tests where the
        specific payload content is not relevant to the test.
    """
    return {"user_id": 123}


# noinspection PyProtectedMember
@pytest.fixture(autouse=True)
def _reset_limiter_class_state(redis_client, celery_app):
    """Reset CeleryRateLimiter class-level state before and after each test.

    This prevents singleton cache pollution between tests and ensures
    configure() is called with the test fixtures.
    """
    CeleryRateLimiter._reset()
    CeleryRateLimiter.configure(redis_client, celery_app=celery_app)
    yield
    CeleryRateLimiter._reset()


@pytest.fixture
def limiter(redis_client, celery_app):
    """Setup and teardown for the CeleryRateLimiter.

    Yields:
        A configured CeleryRateLimiter instance for testing.
    """
    # Setup
    limiter_id = "test_limiter"
    test_limiter = CeleryRateLimiter.create(
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
