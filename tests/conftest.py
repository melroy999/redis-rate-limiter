import pytest
import redis

from src import CeleryRateLimiter


pytest_plugins = ("celery.contrib.pytest", )


@pytest.fixture(scope="session")
def redis_client():
    """Connects to a real Redis instance for testing."""
    client = redis.Redis(host='localhost', port=6379, decode_responses=True)
    yield client
    client.flushall() # Clean up after all tests

@pytest.fixture(scope="session")
def celery_config():
    """Configures the celery_app fixture."""
    return {
        'broker_url': 'redis://localhost:6379/0',
        'result_backend': 'redis://localhost:6379/0',
        'task_always_eager': True,
    }


@pytest.fixture
def limiter(redis_client, celery_app):
    """
    Setup and Teardown for the CeleryRateLimiter.
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
        max_age=3600
    )

    yield test_limiter

    # TEARDOWN: Clear keys associated with this limiter
    keys = redis_client.keys(f"{limiter_id}:*")
    if keys:
        redis_client.delete(*keys)

