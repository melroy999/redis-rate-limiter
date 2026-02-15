"""Tests for the DistributedLock implementation.

This module tests the Redis-based distributed lock implementation used by
the Celery rate limiter. It inherits the contract tests and adds
implementation-specific tests.
"""

import pytest

from celery_rate_limiter import DistributedLock
from tests.contracts.test_distributed_lock import DistributedLockContractTest


@pytest.fixture
def lock_key(default_lock_key):
    """Provide a unique lock key for the test."""
    return default_lock_key


@pytest.fixture
def create_lock():
    """Factory fixture for creating DistributedLock instances."""
    return DistributedLock


class TestDistributedLock(DistributedLockContractTest):
    """Tests for the Redis-based DistributedLock implementation.

    This class inherits all contract tests from DistributedLockContractTest
    and adds implementation-specific tests for the Redis-based lock.
    """

    # ==================== Implementation-Specific Tests ====================

    def test_lock_stores_uuid_token(self, redis_client, lock_key, create_lock):
        """Implementation detail: the lock should use a UUID as the token format."""
        # Arrange
        lock = create_lock(redis_client, lock_key, timeout_ms=1000)

        # Act
        with lock as acquired:
            assert acquired is True
            token = redis_client.get(lock_key)

            # Assert
            # The token should conform to UUID format (i.e., it contains dashes and has the expected length).
            assert "-" in token, "token should be UUID format (contains dashes)"
            assert len(token) == 36, "UUID should be 36 characters long"

    def test_lock_uses_redis_set_nx(self, redis_client, lock_key, create_lock):
        """Implementation detail: the lock should use the Redis SET command with the NX option."""
        # Arrange
        lock = create_lock(redis_client, lock_key, timeout_ms=1000)

        # Act
        with lock as acquired:
            # Assert
            # The key should exist and have a TTL assigned.
            assert acquired is True
            assert redis_client.exists(lock_key) == 1
            ttl = redis_client.pttl(lock_key)
            assert 0 < ttl <= 1000, f"TTL should be set and <= 1000ms, got {ttl}ms"

