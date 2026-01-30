"""Contract tests for distributed lock implementations.

These tests define the expected behavior for any distributed lock implementation.
All lock implementations should satisfy these behavioral contracts.
"""

import time

import pytest

SHORT_TIMEOUT_MS = 10


class DistributedLockContractTest:
    """Abstract test suite that any distributed lock implementation must pass.

    Subclasses must provide:
        - redis_client: A fixture that returns a Redis client
        - lock_key: A fixture that returns a unique lock key for testing

    The subclass should also define how to create lock instances.
    """

    # ==================== Contract Tests ====================

    @staticmethod
    def test_lock_acquires_and_releases_automatically(redis_client, lock_key, create_lock):
        """Contract: lock must acquire on enter and release on exit."""
        # Arrange
        lock = create_lock(redis_client, lock_key, timeout_ms=1000)

        # Act & Assert
        # Lock should be acquired in context.
        with lock as acquired:
            assert acquired is True, "lock should be acquired successfully"
            assert redis_client.exists(lock_key) == 1, "lock key must exist in Redis"
            assert redis_client.get(lock_key) == lock.token, "lock must store correct token"

        # Assert
        # Lock should be released after context.
        assert redis_client.exists(lock_key) == 0, "lock must be released after context exit"

    @staticmethod
    def test_lock_prevents_concurrent_acquisition(redis_client, lock_key, create_lock):
        """Contract: second lock attempt on same key must fail while first holds it."""
        # Arrange
        lock_1 = create_lock(redis_client, lock_key, timeout_ms=1000)
        lock_2 = create_lock(redis_client, lock_key, timeout_ms=1000)

        # Act & Assert
        with lock_1 as acquired_1:
            assert acquired_1 is True, "first lock should acquire successfully"

            with lock_2 as acquired_2:
                assert acquired_2 is False, "second lock must fail due to mutual exclusion"
                assert redis_client.get(lock_key) == lock_1.token, (
                    "original lock must still hold the lock"
                )

    @staticmethod
    def test_lock_expires_after_timeout(redis_client, lock_key, create_lock):
        """Contract: lock must automatically expire after timeout period."""
        # Arrange
        lock_1 = create_lock(redis_client, lock_key, timeout_ms=SHORT_TIMEOUT_MS)
        lock_2 = create_lock(redis_client, lock_key, timeout_ms=5000)

        # Act
        with lock_1 as acquired_1:
            assert acquired_1 is True, "first lock should acquire"

            # Wait for expiration (double the timeout time should be safe).
            time.sleep(2 * SHORT_TIMEOUT_MS / 1000)
            assert redis_client.exists(lock_key) == 0, "lock must expire after timeout"

            # Second lock should now succeed.
            with lock_2 as acquired_2:
                assert acquired_2 is True, "second lock should acquire after first expires"
                assert redis_client.get(lock_key) == lock_2.token

    @staticmethod
    def test_lock_releases_on_exception(redis_client, lock_key, create_lock):
        """Contract: lock must release even when exception occurs in context."""
        # Arrange
        lock = create_lock(redis_client, lock_key, timeout_ms=5000)

        # Act
        try:
            with lock:
                assert redis_client.exists(lock_key) == 1, "lock should be acquired"
                raise ValueError("Simulated failure")
        except ValueError:
            # Do nothing--expected exception.
            pass

        # Assert
        assert redis_client.exists(lock_key) == 0, (
            "lock must be released even after exception"
        )

    @staticmethod
    def test_lock_only_releases_own_token(redis_client, lock_key, create_lock):
        """Contract: lock must not delete another lock's token after expiration."""
        # Arrange
        lock_1 = create_lock(redis_client, lock_key, timeout_ms=SHORT_TIMEOUT_MS)
        lock_2 = create_lock(redis_client, lock_key, timeout_ms=5000)

        # Act
        with lock_1 as acquired_1:
            assert acquired_1 is True

            # Wait for lock_1 to expire.
            time.sleep(2 * SHORT_TIMEOUT_MS / 1000)

            # Lock_2 acquires the expired lock.
            with lock_2 as acquired_2:
                assert acquired_2 is True
                assert redis_client.get(lock_key) == lock_2.token

                # Manually exit lock_1, effectively simulating lock_1 finishing after its lock expired.
                lock_1.__exit__(None, None, None)

                # Assert lock_2 still holds the lock.
                assert redis_client.exists(lock_key) == 1, (
                    "lock_2 must still exist after lock_1 cleanup"
                )
                assert redis_client.get(lock_key) == lock_2.token, (
                    "lock_2 token must remain unchanged"
                )

    @staticmethod
    def test_lock_has_unique_token(redis_client, lock_key, create_lock):
        """Contract: each lock instance must have a unique token."""
        # Arrange & Act
        lock_1 = create_lock(redis_client, lock_key, timeout_ms=1000)
        lock_2 = create_lock(redis_client, lock_key, timeout_ms=1000)

        # Assert
        assert hasattr(lock_1, "token"), "lock must have a token attribute"
        assert hasattr(lock_2, "token"), "lock must have a token attribute"
        assert lock_1.token != lock_2.token, "each lock must have a unique token"
        assert len(lock_1.token) > 0, "token must not be empty"
        assert len(lock_2.token) > 0, "token must not be empty"
