"""Property-based tests for sliding window counter invariants.

These tests use Hypothesis to verify mathematical properties that hold
for ANY valid input, without re-implementing the formula itself.
"""

import pytest
from hypothesis import given, strategies as st

from tests.algorithms.sliding_window_counter import is_allowed, sliding_window_estimate


class TestSlidingWindowProperties:
    """Property-based tests for sliding window counter invariants.

    These tests use Hypothesis to verify mathematical properties that hold
    for ANY valid input, without re-implementing the formula itself.
    """

    @staticmethod
    @given(
        previous_count=st.integers(min_value=0, max_value=1000),
        current_count=st.integers(min_value=0, max_value=1000),
        window_ms=st.integers(min_value=1, max_value=100000),
        elapsed_ms=st.integers(min_value=0, max_value=100000),
    )
    def test_estimate_is_bounded(previous_count, current_count, window_ms, elapsed_ms):
        """Property: estimate is always between current_count and (previous + current)."""
        # Arrange
        elapsed_ms = elapsed_ms % (window_ms + 1)

        # Act
        estimate = sliding_window_estimate(previous_count, current_count, window_ms, elapsed_ms)

        # Assert
        lower = current_count
        upper = previous_count + current_count
        assert lower <= estimate <= upper, (
            f"estimate {estimate} should be in range [{lower}, {upper}]\n"
            f"(prev={previous_count}, curr={current_count}, window={window_ms}ms, elapsed={elapsed_ms}ms)"
        )

    @staticmethod
    @given(
        previous_count=st.integers(min_value=1, max_value=1000),
        current_count=st.integers(min_value=0, max_value=1000),
        window_ms=st.integers(min_value=10, max_value=10000),
    )
    def test_estimate_decreases_as_time_passes(previous_count, current_count, window_ms):
        """Property: estimate decreases monotonically as elapsed time increases."""
        # Act
        estimates = [
            sliding_window_estimate(previous_count, current_count, window_ms, elapsed)
            for elapsed in range(0, window_ms + 1, max(1, window_ms // 10))
        ]

        # Assert
        for i in range(len(estimates) - 1):
            assert estimates[i] >= estimates[i + 1], (
                f"estimate should decrease over time, but estimates[{i}]={estimates[i]} "
                f"< estimates[{i + 1}]={estimates[i + 1]}"
                f"(prev={previous_count}, curr={current_count}, window={window_ms}ms)"
            )

    @staticmethod
    @given(
        current_count=st.integers(min_value=0, max_value=1000),
        window_ms=st.integers(min_value=1, max_value=100000),
        elapsed_ms=st.integers(min_value=0, max_value=100000),
    )
    def test_current_count_always_contributes_fully(current_count, window_ms, elapsed_ms):
        """Property: current_count contributes fully regardless of elapsed time."""
        # Arrange
        elapsed_ms = elapsed_ms % (window_ms + 1)

        # Act
        estimate = sliding_window_estimate(0, current_count, window_ms, elapsed_ms)

        # Assert
        assert estimate == pytest.approx(current_count), (
            f"with previous=0, estimate should equal current_count ({current_count}), "
            f"got {estimate} at elapsed={elapsed_ms}ms"
        )

    @staticmethod
    @given(
        previous_count=st.integers(min_value=0, max_value=1000),
        current_count=st.integers(min_value=0, max_value=1000),
        window_ms=st.integers(min_value=1, max_value=100000),
        elapsed_ms=st.integers(min_value=0, max_value=100000),
        limit=st.integers(min_value=1, max_value=1000),
    )
    def test_is_allowed_matches_estimate_comparison(
        previous_count, current_count, window_ms, elapsed_ms, limit
    ):
        """Property: is_allowed returns True iff estimate < limit."""
        # Arrange
        elapsed_ms = elapsed_ms % (window_ms + 1)

        # Act
        estimate = sliding_window_estimate(previous_count, current_count, window_ms, elapsed_ms)
        allowed = is_allowed(previous_count, current_count, window_ms, elapsed_ms, limit)

        # Assert
        expected = estimate < limit
        assert allowed == expected, (
            f"is_allowed returned {allowed}, but estimate ({estimate}) < limit ({limit}) is {expected}\n"
            f"(prev={previous_count}, curr={current_count}, window={window_ms}ms, elapsed={elapsed_ms}ms)"
        )

    @staticmethod
    @given(
        previous_count=st.integers(min_value=0, max_value=1000),
        current_count=st.integers(min_value=0, max_value=1000),
        window_ms=st.integers(min_value=1, max_value=100000),
    )
    def test_estimate_equals_previous_plus_current_at_start(
        previous_count, current_count, window_ms
    ):
        """Property: at elapsed=0, estimate equals previous + current."""
        # Act
        estimate = sliding_window_estimate(previous_count, current_count, window_ms, 0)

        # Assert
        expected = previous_count + current_count
        assert estimate == pytest.approx(expected), (
            f"at elapsed=0, estimate should be {expected} (prev + curr), got {estimate}"
        )

    @staticmethod
    @given(
        previous_count=st.integers(min_value=0, max_value=1000),
        current_count=st.integers(min_value=0, max_value=1000),
        window_ms=st.integers(min_value=1, max_value=100000),
    )
    def test_estimate_equals_current_at_window_end(previous_count, current_count, window_ms):
        """Property: at elapsed=window_ms, estimate equals current_count only."""
        # Act
        estimate = sliding_window_estimate(previous_count, current_count, window_ms, window_ms)

        # Assert
        assert estimate == pytest.approx(current_count), (
            f"at elapsed=window_ms, estimate should be {current_count} (curr only), got {estimate}"
        )
