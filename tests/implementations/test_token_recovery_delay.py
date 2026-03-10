"""Tests for the sliding-window token recovery delay calculation.

This module covers ``_calculate_token_recovery_delay``, which computes the
wait time until the next token becomes available based on previous-window
decay. Tests are written once in async form via the mixin pattern; the sync
implementation participates via ``SyncToAsyncLimiterAdapter``.

Fixture dependencies:
    - ``stub_limiter``, ``async_stub_limiter``:
      from ``tests/implementations/conftest.py``.
"""

import pytest

from tests.helpers.adapters import SyncToAsyncLimiterAdapter

# ---------------------------------------------------------------------------
# Unified implementation tests
# ---------------------------------------------------------------------------


@pytest.mark.behavior
class TokenRecoveryDelayTests:
    """Unified tests for the sliding-window token recovery delay calculation.

    Subclasses must provide a ``limiter`` fixture that returns either a
    ``SyncToAsyncLimiterAdapter``-wrapped sync limiter or a native async limiter.
    """

    @staticmethod
    async def test_token_recovery_returns_fractional_wait_when_decay_applies(limiter):
        """Verify that a positive fractional delay is returned
        when previous-window decay can free a token.
        """
        # Arrange
        # A 1-second window is used so the arithmetic is straightforward to verify.
        limiter.window = 1.0
        limiter.limit = 5

        # Act
        delay = limiter._calculate_token_recovery_delay(
            val_previous=5, val_current=3, reset_in_ms=500
        )

        # Assert
        assert delay > 0, (
            "delay should be positive when decay has not yet freed a token"
        )
        assert delay < 1.0, "delay should be less than the full window"

    @staticmethod
    async def test_token_recovery_returns_zero_when_decay_already_freed_token(
        limiter,
    ):
        """Verify that zero delay is returned when
        previous-window decay has already freed a token.
        """
        # Arrange
        limiter.window = 1.0
        limiter.limit = 5

        # Act
        delay = limiter._calculate_token_recovery_delay(
            val_previous=10, val_current=1, reset_in_ms=100
        )

        # Assert
        assert delay == pytest.approx(0.0), (
            "delay should be 0.0 when decay has already freed a token"
        )

    @staticmethod
    async def test_token_recovery_fallback_when_val_previous_is_zero(limiter):
        """Verify that the delay falls back to reset_in_ms when
        the previous window has no requests.
        """
        # Arrange
        limiter.window = 1.0
        limiter.limit = 5

        # Act
        delay = limiter._calculate_token_recovery_delay(
            val_previous=0, val_current=3, reset_in_ms=500
        )

        # Assert
        # Fallback formula: reset_in_ms / 1000.0 = 0.5
        assert delay == pytest.approx(0.5), (
            "delay should equal reset_in_ms / 1000 when val_previous is zero"
        )

    @staticmethod
    async def test_token_recovery_fallback_when_val_current_equals_limit(limiter):
        """Verify that the delay falls back to reset_in_ms
        when the current window is at the limit.
        """
        # Arrange
        limiter.window = 1.0
        limiter.limit = 5

        # Act
        delay = limiter._calculate_token_recovery_delay(
            val_previous=5, val_current=5, reset_in_ms=500
        )

        # Assert
        # Fallback formula: reset_in_ms / 1000.0 = 0.5
        assert delay == pytest.approx(0.5), (
            "delay should equal reset_in_ms / 1000 when val_current equals limit"
        )

    @staticmethod
    async def test_token_recovery_primary_path_exact_value(limiter):
        """Verify the exact delay value computed via the primary decay formula."""
        # Arrange
        limiter.window = 1.0
        limiter.limit = 5

        # Act
        delay = limiter._calculate_token_recovery_delay(
            val_previous=5, val_current=3, reset_in_ms=500
        )

        # Assert
        # t_needed_ms = 1000 * (1.0 - (5 - 3) / 5) = 600
        # time_passed_ms = 1000 - 500 = 500
        # wait_ms = 600 - 500 = 100
        # delay = 100 / 1000.0 = 0.1
        assert delay == pytest.approx(0.1), (
            "delay should be 0.1 seconds for the given inputs"
        )

    @staticmethod
    async def test_token_recovery_primary_path_zero_when_wait_ms_zero(limiter):
        """Verify that zero delay is returned when wait_ms is exactly zero."""
        # Arrange
        limiter.window = 1.0
        limiter.limit = 5

        # Act
        # t_needed_ms = 1000 * (1.0 - (5 - 3) / 5) = 600
        # time_passed_ms = 1000 - 400 = 600
        # wait_ms = 600 - 600 = 0 (<= 0)
        delay = limiter._calculate_token_recovery_delay(
            val_previous=5, val_current=3, reset_in_ms=400
        )

        # Assert
        assert delay == pytest.approx(0.0), (
            "delay should be 0.0 when wait_ms is exactly zero"
        )

    @staticmethod
    async def test_token_recovery_primary_path_when_val_previous_is_one(limiter):
        """Verify that ``val_previous=1`` takes the primary
        decay path, not the fallback.
        """
        # Arrange
        limiter.window = 1.0
        limiter.limit = 5

        # Act
        # t_needed_ms = 1000 * (1.0 - (5 - 3) / 1) = -1000
        # time_passed_ms = 1000 - 500 = 500
        # wait_ms = -1000 - 500 = -1500 (<= 0)
        # Primary path returns 0.0; the fallback would return 0.5.
        delay = limiter._calculate_token_recovery_delay(
            val_previous=1, val_current=3, reset_in_ms=500
        )

        # Assert
        assert delay == pytest.approx(0.0), (
            "val_previous=1 should take the primary path and return 0.0"
        )

    @staticmethod
    async def test_token_recovery_primary_path_fractional_wait_ms(limiter):
        """Verify that a fractional wait_ms between 0 and 1
        returns the exact value, not the floor.
        """
        # Arrange
        limiter.window = 1.0
        limiter.limit = 5

        # Act
        # t_needed_ms = 1000 * (1.0 - (5 - 4) / 3) = 666.667
        # time_passed_ms = 1000 - 334 = 666
        # wait_ms = 666.667 - 666 = 0.667 (positive but < 1)
        delay = limiter._calculate_token_recovery_delay(
            val_previous=3, val_current=4, reset_in_ms=334
        )

        # Assert
        # delay = 0.667 / 1000 = 0.000667, NOT the 0.001 floor.
        expected = (1000 * (1.0 - (5 - 4) / 3) - (1000 - 334)) / 1000.0
        assert delay == pytest.approx(expected), (
            "fractional wait_ms should return the exact delay, not the 0.001 floor"
        )


# ---------------------------------------------------------------------------
# Concrete test cases
# ---------------------------------------------------------------------------


@pytest.mark.behavior
class TestSyncTokenRecoveryDelay(TokenRecoveryDelayTests):
    """Sync rate limiter token recovery exercised through the async adapter."""

    @pytest.fixture
    def limiter(self, stub_limiter):
        """Wrap the sync generic limiter in an async adapter."""
        return SyncToAsyncLimiterAdapter(stub_limiter)


@pytest.mark.behavior
class TestAsyncTokenRecoveryDelay(TokenRecoveryDelayTests):
    """Async rate limiter token recovery exercised natively."""

    @pytest.fixture
    def limiter(self, async_stub_limiter):
        """Provide the async generic limiter directly."""
        return async_stub_limiter
