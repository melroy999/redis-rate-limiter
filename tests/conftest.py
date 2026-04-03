"""Root conftest providing foundational fixtures for the entire test suite.

Fixtures provided:

- ``redis_client`` (function): sync Redis client (no ``flushdb``; namespace-isolated).
- ``async_redis_client`` (function): async Redis client (no ``flushdb``; namespace-isolated).
  Each test receives a fresh connection to avoid event loop conflicts with ``asyncio_mode = "auto"``.
- ``limiter_id`` (function): unique ``limiter_{test}_{pid}_{uuid}`` identifier per test.
- ``module_limiter_id`` (module): shared limiter identifier within a single test module.
- ``lock_key`` (function): unique ``lock_{test}_{pid}_{uuid}`` key per test.
- ``func_path`` (session): static function path string for task scheduling.
- ``payload`` (session): static payload dictionary for task scheduling.

Redis connection details are derived from the ``REDIS_HOST`` and ``REDIS_PORT``
environment variables (defaulting to ``localhost:6379``).

Isolation strategy: every test receives a unique ``limiter_id`` and ``lock_key``
that include both the PID and a UUID fragment, guaranteeing no key collisions
across concurrent processes (e.g., mutmut parallel forks). The ``flushdb()``
calls that previously provided isolation have been removed because they cause
cross-process data destruction when multiple forked pytest sessions share the
same Redis instance.
"""

import os
from uuid import uuid4

import pytest
import redis
import redis.asyncio

pytest_plugins = ["tests.plugins.mutmut_defaults_patch"]


def pytest_collection_modifyitems(items: list) -> None:
    """Move tests marked ``@pytest.mark.timeout_safety_net`` to the front.

    Without reordering, an unguarded test may run first under mutmut's
    ``-x`` (fail-fast) mode, block on a long join or shutdown timeout,
    and exhaust the CPU budget (SIGXCPU) before a guarded test gets a
    chance to detect and kill the mutant.
    """
    early = []
    rest = []
    for item in items:
        if item.get_closest_marker("timeout_safety_net"):
            early.append(item)
        else:
            rest.append(item)
    items[:] = early + rest


# Redis configuration is derived from environment variables.
# The default values target localhost:6379, but may be overridden for Docker Compose.
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))


def _safe_id_component(value: str) -> str:
    """Sanitize the given string value to ensure readability within Redis keys and identifiers."""
    return "".join(char if char.isalnum() else "_" for char in value)


def _unique_suffix() -> str:
    """Return a process-scoped unique suffix for Redis key namespacing.

    Includes the PID to guarantee uniqueness across forked processes
    (e.g., mutmut parallel children), and a UUID fragment for uniqueness
    within a single process.
    """
    return f"{os.getpid()}_{uuid4().hex[:8]}"


@pytest.fixture(scope="session")
def _redis_connection():
    """Establish a connection to a live Redis instance for the test session.

    The connection is created once per session. Connection details may be
    configured via the following environment variables:

    - REDIS_HOST: The Redis hostname (default: localhost).
    - REDIS_PORT: The Redis port number (default: 6379).

    A write+read+delete warmup cycle verifies the server is ready before
    tests begin. No ``flushdb()`` is performed because multiple forked
    processes may share the same Redis instance.

    Yields:
        A Redis client connected to the configured Redis instance.
    """
    client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    # Wait for Redis to become fully operational before starting the suite.
    # A single PING can succeed before Redis has finished its startup sequence
    # (e.g., loading an RDB/AOF snapshot, allocating internal data structures).
    # Issuing a write+read+delete cycle ensures the server is ready to serve
    # real commands, which eliminates transient failures in the first few tests.
    import time

    for attempt in range(10):
        try:
            client.ping()
            client.set("__warmup__", "1")
            client.get("__warmup__")
            client.delete("__warmup__")
            # No flushdb() here: multiple forked processes (e.g., mutmut
            # parallel children) may share this Redis instance. All keys are
            # namespace-isolated via unique limiter_id and lock_key fixtures.
            break
        except redis.exceptions.ConnectionError:
            if attempt == 9:
                pytest.fail(
                    f"Could not connect to Redis at {REDIS_HOST}:{REDIS_PORT}. Is it running?\n"
                    f"Tip: Use docker-compose up redis or set REDIS_HOST/REDIS_PORT environment variables."
                )
            time.sleep(0.5)

    yield client
    client.close()


@pytest.fixture(scope="function")
def redis_client(_redis_connection):
    """Provide a per-test Redis client.

    Test isolation is achieved through unique key namespaces (``limiter_id``,
    ``lock_key``) rather than ``flushdb()``. This allows multiple forked
    processes (e.g., mutmut parallel children) to share the same Redis
    instance without cross-contamination.

    Yields:
        A Redis client suitable for use within an individual test.
    """
    yield _redis_connection


@pytest.fixture(scope="session")
def func_path():
    """Provide a fictitious function path for use in test task scheduling.

    Returns:
        A placeholder function path string representing a non-existent
        Celery task, intended for use when scheduling test tasks.
    """
    return "rate_limiter.test.task.function"


@pytest.fixture(scope="session")
def payload():
    """Provide a default payload dictionary for test task scheduling.

    Returns:
        A generic payload dictionary intended for use in tests where
        the specific payload content is not relevant to the assertion.
    """
    return {"user_id": 123}


@pytest.fixture
def limiter_id(request) -> str:
    """Provide a unique limiter identifier for each test to prevent accidental coupling."""
    test_name = _safe_id_component(request.node.name)
    return f"limiter_{test_name}_{_unique_suffix()}"


@pytest.fixture(scope="module")
def module_limiter_id(request) -> str:
    """Provide a unique limiter identifier per module for use in module-scoped fixtures."""
    module_name = _safe_id_component(request.module.__name__)
    return f"module_limiter_{module_name}_{_unique_suffix()}"


@pytest.fixture
def lock_key(request) -> str:
    """Provide a unique lock key for each test."""
    test_name = _safe_id_component(request.node.name)
    return f"lock_{test_name}_{_unique_suffix()}"


@pytest.fixture(scope="function")
async def async_redis_client():
    """Provide a per-test async Redis client.

    Each test receives a fresh connection to avoid event loop conflicts
    between session-scoped async fixtures and function-scoped tests.
    Test isolation is achieved through unique key namespaces rather than
    ``flushdb()``, allowing safe concurrent use across forked processes.
    """
    client = redis.asyncio.Redis(
        host=REDIS_HOST, port=REDIS_PORT, decode_responses=True
    )
    try:
        await client.ping()
    except redis.exceptions.ConnectionError:
        pytest.fail(f"Could not connect to Redis (async) at {REDIS_HOST}:{REDIS_PORT}.")
    yield client
    await client.aclose()
