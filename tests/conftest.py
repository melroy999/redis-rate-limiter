import pytest
import redis

from src import CeleryRateLimiter

pytest_plugins = ("celery.contrib.pytest",)


@pytest.fixture(scope="session")
def _redis_connection():
    """Connect to a real Redis instance for testing.

    We only do this once and just flush the database between tests.

    Yields:
        A Redis client connected to localhost.
    """
    client = redis.Redis(host="localhost", port=6379, decode_responses=True)

    # Verify connection works before starting suite.
    try:
        client.ping()
    except redis.exceptions.ConnectionError:
        pytest.fail("Could not connect to Redis. Is it running?")

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

    Returns:
        A dictionary with Celery configuration for testing.
    """
    return {
        "broker_url": "redis://localhost:6379/0",
        "result_backend": "redis://localhost:6379/0",
        "task_always_eager": True,
    }


@pytest.fixture
def limiter(redis_client, celery_app):
    """Setup and teardown for the CeleryRateLimiter.

    Yields:
        A configured CeleryRateLimiter instance for testing.
    """
    # SETUP
    limiter_id = "test_limiter"
    test_limiter = CeleryRateLimiter(
        redis_client=redis_client,
        celery_app=celery_app,
        limiter_id=limiter_id,
        limit=5,
        window=60,
        max_concurrency=2,
        max_age=3600,
        lease_duration=30,
    )

    yield test_limiter

    # TEARDOWN: Clear keys associated with this limiter
    keys = redis_client.keys(f"{limiter_id}:*")
    if keys:
        redis_client.delete(*keys)
