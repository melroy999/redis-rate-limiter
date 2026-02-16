import os
from uuid import uuid4

import pytest
import redis

# Redis configuration is derived from environment variables.
# The default values target localhost:6379, but may be overridden for Docker Compose.
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))


def _safe_id_component(value: str) -> str:
    """Sanitize the given string value to ensure readability within Redis keys and identifiers."""
    return "".join(char if char.isalnum() else "_" for char in value)


@pytest.fixture(scope="session")
def _redis_connection():
    """Establish a connection to a live Redis instance for the test session.

    The connection is created once per session; the database is flushed
    between individual tests rather than reconnecting. Connection details
    may be configured via the following environment variables:

    - REDIS_HOST: The Redis hostname (default: localhost).
    - REDIS_PORT: The Redis port number (default: 6379).

    Yields:
        A Redis client connected to the configured Redis instance.
    """
    client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    # Verify that the connection is operational before starting the suite.
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
    """Provide a per-test Redis client with an isolated database state.

    The database is flushed both before and after each test to prevent
    residual state from affecting subsequent test cases.

    Yields:
        A Redis client suitable for use within an individual test.
    """
    # Flush the database before and after the test.
    _redis_connection.flushall()
    yield _redis_connection
    _redis_connection.flushall()


@pytest.fixture(scope="session")
def func_path():
    """Provide a fictitious function path for use in test task scheduling.

    Returns:
        A placeholder function path string representing a non-existent
        Celery task, intended for use when scheduling test tasks.
    """
    return "rate_limiter.test.task.function"


@pytest.fixture(scope="session")
def default_payload():
    """Provide a default payload dictionary for test task scheduling.

    Returns:
        A generic payload dictionary intended for use in tests where
        the specific payload content is not relevant to the assertion.
    """
    return {"user_id": 123}


@pytest.fixture
def default_limiter_id(request) -> str:
    """Provide a unique limiter identifier for each test to prevent accidental coupling."""
    test_name = _safe_id_component(request.node.name)
    return f"limiter_{test_name}_{uuid4().hex[:8]}"


@pytest.fixture(scope="module")
def default_module_limiter_id(request) -> str:
    """Provide a unique limiter identifier per module for use in module-scoped fixtures."""
    module_name = _safe_id_component(request.module.__name__)
    return f"module_limiter_{module_name}_{uuid4().hex[:8]}"


@pytest.fixture
def default_lock_key(request) -> str:
    """Provide a unique lock key for each test."""
    test_name = _safe_id_component(request.node.name)
    return f"lock_{test_name}_{uuid4().hex[:8]}"
