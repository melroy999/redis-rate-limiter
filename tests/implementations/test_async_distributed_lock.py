"""Tests for the ``AsyncDistributedLock`` implementation.

This module tests the async Redis-based distributed lock implementation.
It inherits the async contract tests and adds implementation-specific tests.
"""

import pytest

from celery_rate_limiter.core import AsyncDistributedLock
from tests.contracts.test_distributed_lock_async import AsyncDistributedLockContractTest


@pytest.fixture
def lock_key(default_lock_key):
    """Provide a unique lock key for the test."""
    return default_lock_key


@pytest.fixture
def create_lock():
    """Factory fixture for creating ``AsyncDistributedLock`` instances."""
    return AsyncDistributedLock


class TestAsyncDistributedLock(AsyncDistributedLockContractTest):
    """Tests for the async Redis-based ``AsyncDistributedLock`` implementation.

    This class inherits all async contract tests from ``AsyncDistributedLockContractTest``
    and adds implementation-specific tests for the async Redis-based lock.
    """

    pass
