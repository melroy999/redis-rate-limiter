import os
from uuid import uuid4

import pytest
import redis

# Redis configuration from environment variables.
# Defaults to localhost:6379, but can be overridden for Docker Compose.
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))


def _safe_id_component(value: str) -> str:
    """Sanitize string values for Redis key/id readability."""
    return "".join(char if char.isalnum() else "_" for char in value)


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


@pytest.fixture
def default_limiter_id(request) -> str:
    """Provide a unique limiter id per test to reduce accidental coupling."""
    test_name = _safe_id_component(request.node.name)
    return f"limiter_{test_name}_{uuid4().hex[:8]}"


@pytest.fixture(scope="module")
def default_module_limiter_id(request) -> str:
    """Provide a unique limiter id per module for module-scoped fixtures."""
    module_name = _safe_id_component(request.module.__name__)
    return f"module_limiter_{module_name}_{uuid4().hex[:8]}"


@pytest.fixture
def default_lock_key(request) -> str:
    """Provide a unique lock key per test."""
    test_name = _safe_id_component(request.node.name)
    return f"lock_{test_name}_{uuid4().hex[:8]}"
