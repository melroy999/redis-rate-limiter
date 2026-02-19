"""Property-based tests for the token recovery delay calculation invariants.

These tests complement the deterministic tests located in
``tests/implementations/test_internal_helpers.py::TestTokenRecoveryDelay``.

The ``_calculate_token_recovery_delay()`` method computes the earliest time
at which the sliding window estimate drops below the configured limit via
previous-window decay. The property tests verify that the result is always
positive, bounded by the window size, and (on the primary path) that the
estimate is indeed below the limit after waiting the computed delay.
"""

import pytest
from hypothesis import HealthCheck, assume, given, settings
from hypothesis import strategies as st

from tests.algorithms.sliding_window_counter import sliding_window_estimate
from tests.implementations.conftest import MinimalRateLimiter

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def property_limiter(property_redis_client, module_limiter_id):
    """Provide a module-scoped rate limiter for token recovery property tests."""
    return MinimalRateLimiter(
        redis_client=property_redis_client,
        limiter_id=f"{module_limiter_id}_property_token_recovery",
        limit=10,
        window=1.0,
        max_concurrency=5,
        max_age=3600,
        lease_duration=30,
    )


# ---------------------------------------------------------------------------
# Concrete test cases
# ---------------------------------------------------------------------------


class TestTokenRecoveryDelayProperties:
    """Property-based tests for the ``_calculate_token_recovery_delay`` calculation.

    The method has two code paths:

    - **Fallback path** (``val_previous <= 0`` or ``val_current >= limit``):
      returns ``reset_in_ms / 1000 + 0.001``.
    - **Primary path**: solves the sliding window decay equation for the
      earliest time at which ``estimated < limit``, returning a fractional
      delay in seconds.

    These tests verify structural invariants that hold across both paths.
    """

    @given(
        limit=st.integers(min_value=1, max_value=1000),
        window=st.floats(
            min_value=0.01,
            max_value=60.0,
            allow_nan=False,
            allow_infinity=False,
        ),
        val_previous=st.integers(min_value=0, max_value=2000),
        val_current=st.integers(min_value=0, max_value=2000),
        reset_fraction=st.floats(
            min_value=0.0,
            max_value=1.0,
            allow_nan=False,
            allow_infinity=False,
        ),
    )
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_delay_is_always_positive(
        self,
        property_limiter,
        limit,
        window,
        val_previous,
        val_current,
        reset_fraction,
    ):
        """Property: the delay is always at least 0.001 seconds (the minimum floor)."""
        # Arrange
        property_limiter.limit = limit
        property_limiter.window = window
        reset_in_ms = int(reset_fraction * window * 1000)

        # Act
        delay = property_limiter._calculate_token_recovery_delay(
            val_previous=val_previous,
            val_current=val_current,
            reset_in_ms=reset_in_ms,
        )

        # Assert
        assert delay >= 0.001, (
            f"delay should be >= 0.001, got {delay} "
            f"(limit={limit}, window={window}, prev={val_previous}, "
            f"curr={val_current}, reset_in_ms={reset_in_ms})"
        )

    @given(
        limit=st.integers(min_value=1, max_value=1000),
        window=st.floats(
            min_value=0.01,
            max_value=60.0,
            allow_nan=False,
            allow_infinity=False,
        ),
        val_previous=st.integers(min_value=0, max_value=2000),
        val_current=st.integers(min_value=0, max_value=2000),
        reset_fraction=st.floats(
            min_value=0.0,
            max_value=1.0,
            allow_nan=False,
            allow_infinity=False,
        ),
    )
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_delay_is_bounded_by_window_plus_floor(
        self,
        property_limiter,
        limit,
        window,
        val_previous,
        val_current,
        reset_fraction,
    ):
        """Property: the delay never exceeds ``window + 0.001`` seconds."""
        # Arrange
        property_limiter.limit = limit
        property_limiter.window = window
        reset_in_ms = int(reset_fraction * window * 1000)

        # Act
        delay = property_limiter._calculate_token_recovery_delay(
            val_previous=val_previous,
            val_current=val_current,
            reset_in_ms=reset_in_ms,
        )

        # Assert
        upper_bound = window + 0.001
        assert delay <= upper_bound + 1e-9, (
            f"delay should be <= window + 0.001 ({upper_bound}), got {delay} "
            f"(limit={limit}, window={window}, prev={val_previous}, "
            f"curr={val_current}, reset_in_ms={reset_in_ms})"
        )

    @given(
        limit=st.integers(min_value=1, max_value=1000),
        window=st.floats(
            min_value=0.01,
            max_value=60.0,
            allow_nan=False,
            allow_infinity=False,
        ),
        val_previous=st.integers(min_value=1, max_value=2000),
        val_current=st.integers(min_value=0, max_value=2000),
        reset_fraction=st.floats(
            min_value=0.001,
            max_value=1.0,
            allow_nan=False,
            allow_infinity=False,
        ),
    )
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_estimate_below_limit_after_waiting_delay(
        self,
        property_limiter,
        limit,
        window,
        val_previous,
        val_current,
        reset_fraction,
    ):
        """Property: on the primary path, the sliding window estimate is below the limit after waiting the computed delay.

        This property validates the formula correctness by computing the
        estimate at the projected elapsed time and verifying that it is
        strictly less than the configured limit.
        """
        # Arrange
        # Restrict to the primary path where decay can free a token.
        assume(val_previous > 0)
        assume(val_current < limit)
        property_limiter.limit = limit
        property_limiter.window = window
        window_ms = window * 1000
        reset_in_ms = max(1, int(reset_fraction * window_ms))
        time_passed_ms = window_ms - reset_in_ms

        # Act
        delay = property_limiter._calculate_token_recovery_delay(
            val_previous=val_previous,
            val_current=val_current,
            reset_in_ms=reset_in_ms,
        )

        # Compute the elapsed time after waiting the delay.
        elapsed_after_delay_ms = time_passed_ms + delay * 1000

        # Skip cases where the delay would take us past the window boundary,
        # as the estimate function assumes elapsed_ms <= window_ms.
        assume(elapsed_after_delay_ms <= window_ms + 1)

        # Assert
        # Verify that the estimate is below the limit after waiting the delay.
        estimate = sliding_window_estimate(
            val_previous, val_current, window_ms, elapsed_after_delay_ms
        )
        assert estimate < limit + 1e-6, (
            f"estimate should be < limit after waiting delay; "
            f"got estimate={estimate}, limit={limit}, delay={delay}s, "
            f"elapsed_after={elapsed_after_delay_ms}ms "
            f"(prev={val_previous}, curr={val_current}, window_ms={window_ms}, "
            f"reset_in_ms={reset_in_ms})"
        )
