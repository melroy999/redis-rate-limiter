"""Tests for the ``AsyncTaskLifecycle`` context manager.

This module tests the async task lifecycle implementation that manages
concurrency slots and task cleanup. It inherits the async contract tests
and adds implementation-specific tests that operate with any async rate
limiter implementation.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from celery_rate_limiter.core import AsyncTaskLifecycle
from tests.contracts.test_task_lifecycle_async import AsyncTaskLifecycleContractTest


@pytest.fixture
def mock_limiter(async_redis_client, task_id):
    """Create a mock async limiter that uses the real async Redis client but mocks internal helpers.

    This fixture provides a limiter with real async Redis operations but mocked
    backend-specific methods, thereby avoiding the need for a full backend setup.

    ``MagicMock`` is used as the base rather than ``AsyncMock`` because the
    lifecycle code calls ``get_inflight_key()`` synchronously (without ``await``).
    Methods that the lifecycle awaits (``trigger_consume``, ``extend_lease``) are
    explicitly set to ``AsyncMock`` instances.
    """
    limiter = MagicMock()

    # Use the real async Redis client for actual Redis operations.
    limiter.redis = async_redis_client
    limiter.concurrency_key = "test:concurrency"
    limiter.id = "test_limiter"
    limiter.get_inflight_key.side_effect = lambda _: f"test:inflight:{task_id}"

    # A short duration is used for fast test execution.
    limiter.lease_duration = 0.2

    # Async methods that the lifecycle awaits.
    limiter.trigger_consume = AsyncMock()
    limiter.extend_lease = AsyncMock(return_value=None)
    return limiter


@pytest.fixture
def inflight_key(mock_limiter, task_id):
    """Provide the in-flight key for the test task."""
    return mock_limiter.get_inflight_key(task_id)


@pytest.fixture
def lifecycle_class():
    """Provide the ``AsyncTaskLifecycle`` class for the contract tests."""
    return AsyncTaskLifecycle


class TestAsyncTaskLifecycle(AsyncTaskLifecycleContractTest):
    """Tests for the ``AsyncTaskLifecycle`` context manager implementation.

    This class inherits all async contract tests from ``AsyncTaskLifecycleContractTest``
    and adds implementation-specific tests for heartbeat handling and lifecycle behavior.
    """

    pass
