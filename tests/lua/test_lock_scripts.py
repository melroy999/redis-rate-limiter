"""Tests for the inline distributed lock Lua scripts.

Calls ``redis.eval()`` directly with controlled Redis state to verify
the lock acquisition, contention-aware release, and simple release
scripts defined in ``redis_rate_limiter.core.limiters``.

Fixture dependencies:
    - ``redis_client``, ``lock_key``: from ``tests/conftest.py``.
"""

from tests.lua.conftest import (
    COOLDOWN_MS,
    LOCK_ACQUIRE_SCRIPT,
    LOCK_RELEASE_SCRIPT,
    LOCK_SIMPLE_RELEASE_SCRIPT,
    TIMEOUT_MS,
)


class TestLockAcquireScript:
    """Tests for ``LOCK_ACQUIRE_SCRIPT``: contention-aware lock acquisition."""

    @staticmethod
    def test_acquires_when_lock_is_free(redis_client, lock_key):
        """Verify that acquisition succeeds when the lock is free."""
        # Arrange
        cooldown_key = f"{lock_key}:cd:worker-1"
        contention_key = f"{lock_key}:contention"
        token = "my-token"

        # Act
        result = redis_client.eval(
            LOCK_ACQUIRE_SCRIPT,
            3,
            lock_key,
            cooldown_key,
            contention_key,
            token,
            TIMEOUT_MS,
        )

        # Assert
        assert result == 1, "acquire should return 1 when lock is free"
        assert redis_client.get(lock_key) == token, (
            "lock key should hold the acquiring token"
        )

    @staticmethod
    def test_denied_when_lock_held_by_other(redis_client, lock_key):
        """Verify that acquisition is denied when the lock is held by another worker."""
        # Arrange
        cooldown_key = f"{lock_key}:cd:worker-1"
        contention_key = f"{lock_key}:contention"
        redis_client.set(lock_key, "other-token", px=TIMEOUT_MS)

        # Act
        result = redis_client.eval(
            LOCK_ACQUIRE_SCRIPT,
            3,
            lock_key,
            cooldown_key,
            contention_key,
            "my-token",
            TIMEOUT_MS,
        )

        # Assert
        assert result == 0, "acquire should return 0 when lock is held by another"

    @staticmethod
    def test_denied_when_cooldown_active(redis_client, lock_key):
        """Verify that acquisition is denied when the per-worker cooldown key exists."""
        # Arrange
        cooldown_key = f"{lock_key}:cd:worker-1"
        contention_key = f"{lock_key}:contention"
        redis_client.set(cooldown_key, "1", px=COOLDOWN_MS)

        # Act
        result = redis_client.eval(
            LOCK_ACQUIRE_SCRIPT,
            3,
            lock_key,
            cooldown_key,
            contention_key,
            "my-token",
            TIMEOUT_MS,
        )

        # Assert
        assert result == 0, "acquire should return 0 when cooldown is active"
        assert not redis_client.exists(lock_key), (
            "lock key should not be set when cooldown blocks acquisition"
        )

    @staticmethod
    def test_increments_contention_counter_on_failure(redis_client, lock_key):
        """Verify that a failed acquisition increments the shared contention counter."""
        # Arrange
        cooldown_key = f"{lock_key}:cd:worker-1"
        contention_key = f"{lock_key}:contention"
        redis_client.set(lock_key, "other-token", px=TIMEOUT_MS)

        # Act
        redis_client.eval(
            LOCK_ACQUIRE_SCRIPT,
            3,
            lock_key,
            cooldown_key,
            contention_key,
            "my-token",
            TIMEOUT_MS,
        )

        # Assert
        assert redis_client.get(contention_key) == "1", (
            "contention counter should be incremented to 1 after failed acquisition"
        )

    @staticmethod
    def test_sets_ttl_on_contention_counter(redis_client, lock_key):
        """Verify that the contention counter has a TTL after a failed acquisition."""
        # Arrange
        cooldown_key = f"{lock_key}:cd:worker-1"
        contention_key = f"{lock_key}:contention"
        redis_client.set(lock_key, "other-token", px=TIMEOUT_MS)

        # Act
        redis_client.eval(
            LOCK_ACQUIRE_SCRIPT,
            3,
            lock_key,
            cooldown_key,
            contention_key,
            "my-token",
            TIMEOUT_MS,
        )

        # Assert
        pttl = redis_client.pttl(contention_key)
        assert pttl > 0, "contention counter should have a positive PTTL"


class TestLockReleaseScript:
    """Tests for ``LOCK_RELEASE_SCRIPT``: contention-aware lock release."""

    @staticmethod
    def test_releases_when_token_matches(redis_client, lock_key):
        """Verify that release succeeds when the token matches."""
        # Arrange
        cooldown_key = f"{lock_key}:cd:worker-1"
        contention_key = f"{lock_key}:contention"
        token = "my-token"
        redis_client.set(lock_key, token)

        # Act
        result = redis_client.eval(
            LOCK_RELEASE_SCRIPT,
            3,
            lock_key,
            cooldown_key,
            contention_key,
            token,
            COOLDOWN_MS,
        )

        # Assert
        assert result == 1, "release should return 1 when token matches"
        assert not redis_client.exists(lock_key), (
            "lock key should be deleted after release"
        )

    @staticmethod
    def test_rejected_when_token_mismatches(redis_client, lock_key):
        """Verify that release is rejected when the token does not match."""
        # Arrange
        cooldown_key = f"{lock_key}:cd:worker-1"
        contention_key = f"{lock_key}:contention"
        redis_client.set(lock_key, "other-token")

        # Act
        result = redis_client.eval(
            LOCK_RELEASE_SCRIPT,
            3,
            lock_key,
            cooldown_key,
            contention_key,
            "my-token",
            COOLDOWN_MS,
        )

        # Assert
        assert result == 0, "release should return 0 when token mismatches"
        assert redis_client.exists(lock_key), (
            "lock key should still exist after rejected release"
        )

    @staticmethod
    def test_sets_cooldown_when_contention_detected(redis_client, lock_key):
        """Verify that a cooldown key is set when contention
        was detected during the lock hold."""
        # Arrange
        cooldown_key = f"{lock_key}:cd:worker-1"
        contention_key = f"{lock_key}:contention"
        token = "my-token"
        redis_client.set(lock_key, token)
        redis_client.set(contention_key, "2")

        # Act
        redis_client.eval(
            LOCK_RELEASE_SCRIPT,
            3,
            lock_key,
            cooldown_key,
            contention_key,
            token,
            COOLDOWN_MS,
        )

        # Assert
        assert redis_client.exists(cooldown_key), (
            "cooldown key should be set when contention was detected"
        )
        pttl = redis_client.pttl(cooldown_key)
        assert pttl > 0, "cooldown key should have a positive PTTL"

    @staticmethod
    def test_resets_contention_counter_after_cooldown(redis_client, lock_key):
        """Verify that the contention counter is deleted after setting the cooldown."""
        # Arrange
        cooldown_key = f"{lock_key}:cd:worker-1"
        contention_key = f"{lock_key}:contention"
        token = "my-token"
        redis_client.set(lock_key, token)
        redis_client.set(contention_key, "1")

        # Act
        redis_client.eval(
            LOCK_RELEASE_SCRIPT,
            3,
            lock_key,
            cooldown_key,
            contention_key,
            token,
            COOLDOWN_MS,
        )

        # Assert
        assert not redis_client.exists(contention_key), (
            "contention counter should be deleted after setting cooldown"
        )

    @staticmethod
    def test_no_cooldown_when_no_contention(redis_client, lock_key):
        """Verify that no cooldown key is set when no contention was recorded."""
        # Arrange
        cooldown_key = f"{lock_key}:cd:worker-1"
        contention_key = f"{lock_key}:contention"
        token = "my-token"
        redis_client.set(lock_key, token)

        # Act
        redis_client.eval(
            LOCK_RELEASE_SCRIPT,
            3,
            lock_key,
            cooldown_key,
            contention_key,
            token,
            COOLDOWN_MS,
        )

        # Assert
        assert not redis_client.exists(cooldown_key), (
            "cooldown key should not be set when no contention was recorded"
        )


class TestLockSimpleReleaseScript:
    """Tests for ``LOCK_SIMPLE_RELEASE_SCRIPT``: non-contention lock release."""

    @staticmethod
    def test_releases_when_token_matches(redis_client, lock_key):
        """Verify that simple release succeeds when the token matches."""
        # Arrange
        token = "my-token"
        redis_client.set(lock_key, token)

        # Act
        result = redis_client.eval(LOCK_SIMPLE_RELEASE_SCRIPT, 1, lock_key, token)

        # Assert
        assert result == 1, "simple release should return 1 when token matches"
        assert not redis_client.exists(lock_key), (
            "lock key should be deleted after simple release"
        )

    @staticmethod
    def test_rejected_when_token_mismatches(redis_client, lock_key):
        """Verify that simple release is rejected when the token does not match."""
        # Arrange
        redis_client.set(lock_key, "other-token")

        # Act
        result = redis_client.eval(LOCK_SIMPLE_RELEASE_SCRIPT, 1, lock_key, "my-token")

        # Assert
        assert result == 0, "simple release should return 0 when token mismatches"
        assert redis_client.exists(lock_key), (
            "lock key should still exist after rejected simple release"
        )

    @staticmethod
    def test_returns_zero_when_lock_does_not_exist(redis_client, lock_key):
        """Verify that simple release returns 0 when the lock key does not exist."""
        # Act
        result = redis_client.eval(LOCK_SIMPLE_RELEASE_SCRIPT, 1, lock_key, "any-token")

        # Assert
        assert result == 0, "simple release should return 0 when lock does not exist"
