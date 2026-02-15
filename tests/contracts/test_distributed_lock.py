"""Contract tests for distributed lock implementations.

These tests define the expected behaviour for any distributed lock
implementation. All lock implementations are required to satisfy these
behavioural contracts.
"""

import time

SHORT_TIMEOUT_MS = 10


class DistributedLockContractTest:
    """Abstract test suite that any distributed lock implementation must pass.

    Subclasses are required to provide the following:
        - redis_client: A fixture that returns a Redis client.
        - lock_key: A fixture that returns a unique lock key for testing.

    The subclass must also define how lock instances are to be created.
    """

    # ==================== Contract Tests ====================

    @staticmethod
    def test_lock_acquires_and_releases_automatically(
        redis_client, lock_key, create_lock
    ):
        """Contract: the lock must be acquired upon context entry and released upon context exit."""
        # Arrange
        lock = create_lock(redis_client, lock_key, timeout_ms=1000)

        # Act & Assert
        # The lock should be acquired within the context.
        with lock as acquired:
            assert acquired is True, "lock should be acquired successfully"
            assert redis_client.exists(lock_key) == 1, "lock key must exist in Redis"
            assert redis_client.get(lock_key) == lock.token, (
                "lock must store correct token"
            )

        # Assert
        # The lock should be released after the context is exited.
        assert redis_client.exists(lock_key) == 0, (
            "lock must be released after context exit"
        )

    @staticmethod
    def test_lock_prevents_concurrent_acquisition(redis_client, lock_key, create_lock):
        """Contract: a second lock attempt on the same key must fail while the first lock is held."""
        # Arrange
        lock_1 = create_lock(redis_client, lock_key, timeout_ms=1000)
        lock_2 = create_lock(redis_client, lock_key, timeout_ms=1000)

        # Act & Assert
        with lock_1 as acquired_1:
            assert acquired_1 is True, "first lock should acquire successfully"

            with lock_2 as acquired_2:
                assert acquired_2 is False, (
                    "second lock must fail due to mutual exclusion"
                )
                assert redis_client.get(lock_key) == lock_1.token, (
                    "original lock must still hold the lock"
                )

    @staticmethod
    def test_lock_expires_after_timeout(redis_client, lock_key, create_lock):
        """Contract: the lock must automatically expire after the timeout period elapses."""
        # Arrange
        lock_1 = create_lock(redis_client, lock_key, timeout_ms=SHORT_TIMEOUT_MS)
        lock_2 = create_lock(redis_client, lock_key, timeout_ms=5000)

        # Act
        with lock_1 as acquired_1:
            assert acquired_1 is True, "first lock should acquire"

            # Wait for expiration (doubling the timeout duration should be sufficient).
            time.sleep(2 * SHORT_TIMEOUT_MS / 1000)
            assert redis_client.exists(lock_key) == 0, "lock must expire after timeout"

            # The second lock should now succeed.
            with lock_2 as acquired_2:
                assert acquired_2 is True, (
                    "second lock should acquire after first expires"
                )
                assert redis_client.get(lock_key) == lock_2.token

    @staticmethod
    def test_lock_releases_on_exception(redis_client, lock_key, create_lock):
        """Contract: the lock must be released even when an exception occurs within the context."""
        # Arrange
        lock = create_lock(redis_client, lock_key, timeout_ms=5000)

        # Act
        try:
            with lock:
                assert redis_client.exists(lock_key) == 1, "lock should be acquired"
                raise ValueError("Simulated failure")
        except ValueError:
            # No action required--the exception is expected.
            pass

        # Assert
        assert redis_client.exists(lock_key) == 0, (
            "lock must be released even after exception"
        )

    @staticmethod
    def test_lock_only_releases_own_token(redis_client, lock_key, create_lock):
        """Contract: the lock must not delete another lock's token after its own expiration."""
        # Arrange
        lock_1 = create_lock(redis_client, lock_key, timeout_ms=SHORT_TIMEOUT_MS)
        lock_2 = create_lock(redis_client, lock_key, timeout_ms=5000)

        # Act
        with lock_1 as acquired_1:
            assert acquired_1 is True

            # Wait for the first lock to expire.
            time.sleep(2 * SHORT_TIMEOUT_MS / 1000)

            # The second lock acquires the now-expired lock.
            with lock_2 as acquired_2:
                assert acquired_2 is True
                assert lock_2.token != lock_1.token, (
                    "the two locks must have different tokens"
                )
                assert redis_client.get(lock_key) == lock_2.token

                # Manually exit the first lock, effectively simulating the
                # scenario in which lock_1 completes after its lock has expired.
                lock_1.__exit__(None, None, None)

                # Assert that lock_2 still holds the lock.
                assert redis_client.exists(lock_key) == 1, (
                    "lock_2 must still exist after lock_1 cleanup"
                )
                assert redis_client.get(lock_key) == lock_2.token, (
                    "lock_2 token must remain unchanged"
                )

    @staticmethod
    def test_lock_has_unique_token(redis_client, lock_key, create_lock):
        """Contract: each lock instance must possess a unique token."""
        # Arrange & Act
        lock_1 = create_lock(redis_client, lock_key, timeout_ms=1000)
        lock_2 = create_lock(redis_client, lock_key, timeout_ms=1000)

        # Assert
        assert hasattr(lock_1, "token"), "lock must have a token attribute"
        assert hasattr(lock_2, "token"), "lock must have a token attribute"
        assert lock_1.token != lock_2.token, "each lock must have a unique token"
        assert len(lock_1.token) > 0, "token must not be empty"
        assert len(lock_2.token) > 0, "token must not be empty"
