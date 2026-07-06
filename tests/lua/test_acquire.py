"""Tests for the ``acquire.lua`` Lua script.

Calls ``redis.eval()`` directly with controlled Redis state to verify
the ASGI sliding window counter's return value structure, boundary
decisions, and TTL behavior.

Fixture dependencies:
    - ``redis_client``: from ``tests/conftest.py``.
    - ``base_key``: from ``tests/lua/conftest.py``.
"""

import pytest

from tests.lua.conftest import ACQUIRE_SOURCE, LIMIT, WINDOW_SIZE, get_window_keys


def _eval_acquire(redis_client, base_key, window_size=WINDOW_SIZE, limit=LIMIT):
    """Invoke ``acquire.lua`` via ``eval()`` with the given parameters."""
    return redis_client.eval(ACQUIRE_SOURCE, 1, base_key, window_size, limit)


@pytest.mark.behavior
class TestAcquireReturnValues:
    """Tests for the ``acquire.lua`` return value structure and field correctness."""

    @staticmethod
    def test_allowed_returns_five_element_array(redis_client, base_key):
        """Verify that an allowed request returns a 5-element array."""
        # Act
        result = _eval_acquire(redis_client, base_key)

        # Assert
        assert len(result) == 5, "acquire should return a 5-element array"

    @staticmethod
    def test_allowed_status_code_is_one(redis_client, base_key):
        """Verify that an allowed request returns status code 1."""
        # Act
        result = _eval_acquire(redis_client, base_key)

        # Assert
        assert result[0] == 1, "status code should be 1 when under the limit"

    @staticmethod
    def test_allowed_remaining_is_decremented(redis_client, base_key):
        """Verify that ``remaining`` is decremented after an allowed request."""
        # Act
        result = _eval_acquire(redis_client, base_key)

        # Assert
        assert result[1] == LIMIT - 1, (
            "remaining should equal limit - 1 after first acquire from empty window"
        )

    @staticmethod
    def test_allowed_current_count_is_incremented(redis_client, base_key):
        """Verify that the current window count is incremented
        after an allowed request."""
        # Act
        result = _eval_acquire(redis_client, base_key)

        # Assert
        assert result[4] == 1, (
            "current_count should be 1 after first acquire from empty window"
        )

    @staticmethod
    def test_reset_in_ms_is_positive(redis_client, base_key):
        """Verify that ``reset_in_ms`` is positive when called within a window."""
        # Act
        result = _eval_acquire(redis_client, base_key)

        # Assert
        assert result[2] > 0, "reset_in_ms should be positive within a window"
        assert result[2] <= WINDOW_SIZE * 1000, (
            "reset_in_ms should not exceed the window size"
        )

    @staticmethod
    def test_previous_count_reflects_previous_window_key(redis_client, base_key):
        """Verify that ``result[3]`` reflects the previous window counter value."""
        # Arrange
        _, previous_key = get_window_keys(redis_client, base_key)
        redis_client.set(previous_key, "7")

        # Act
        result = _eval_acquire(redis_client, base_key)

        # Assert
        assert result[3] == 7, "previous_count should reflect the previous window key"

    @staticmethod
    def test_denied_status_code_is_zero(redis_client, base_key):
        """Verify that a denied request returns status code 0."""
        # Arrange
        current_key, _ = get_window_keys(redis_client, base_key)
        redis_client.set(current_key, str(LIMIT))

        # Act
        result = _eval_acquire(redis_client, base_key)

        # Assert
        assert result[0] == 0, "status code should be 0 when rate limit is reached"

    @staticmethod
    def test_denied_does_not_increment_counter(redis_client, base_key):
        """Verify that a denied request does not increment the
        current window counter."""
        # Arrange
        current_key, _ = get_window_keys(redis_client, base_key)
        redis_client.set(current_key, str(LIMIT))

        # Act
        result = _eval_acquire(redis_client, base_key)

        # Assert
        assert result[4] == LIMIT, (
            "current_count should be unchanged after denied request"
        )


@pytest.mark.behavior
class TestAcquireBoundaryDecisions:
    """Tests for ``acquire.lua`` boundary conditions."""

    @staticmethod
    def test_denies_when_estimate_equals_limit(redis_client, base_key):
        """Verify that acquisition is denied when the estimated count equals the limit.

        The check is strictly less than (``estimated_count < rate_limit``).
        """
        # Arrange
        current_key, _ = get_window_keys(redis_client, base_key)
        redis_client.set(current_key, str(LIMIT))

        # Act
        result = _eval_acquire(redis_client, base_key)

        # Assert
        assert result[0] == 0, (
            "acquire should deny when estimated count equals the limit"
        )

    @staticmethod
    def test_allows_when_estimate_is_one_below_limit(redis_client, base_key):
        """Verify that acquisition is allowed when the estimated
        count is one below the limit."""
        # Arrange
        current_key, _ = get_window_keys(redis_client, base_key)
        redis_client.set(current_key, str(LIMIT - 1))

        # Act
        result = _eval_acquire(redis_client, base_key)

        # Assert
        assert result[0] == 1, (
            "acquire should allow when estimated count is one below the limit"
        )

    @staticmethod
    def test_pexpire_set_on_first_increment_only(redis_client, base_key):
        """Verify that the window key's TTL is established once.

        The expiry is set when the window counter is first created.
        Later increments within the same window must not reset the
        TTL.
        """
        # Act
        # First acquire: sets PEXPIRE.
        _eval_acquire(redis_client, base_key)
        current_key, _ = get_window_keys(redis_client, base_key)
        pttl_after_first = redis_client.pttl(current_key)

        # Second acquire: should NOT reset PEXPIRE.
        _eval_acquire(redis_client, base_key)
        pttl_after_second = redis_client.pttl(current_key)

        # Assert
        expected_max_pttl = 2 * WINDOW_SIZE * 1000 + 10000
        assert 0 < pttl_after_first <= expected_max_pttl, (
            "PTTL should be set after first acquire"
        )
        assert pttl_after_second <= pttl_after_first, (
            "PTTL should not increase after second acquire (PEXPIRE not reset)"
        )
