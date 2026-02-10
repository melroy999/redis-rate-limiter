"""Property and unit tests for the sliding window counter algorithm.

These tests verify the mathematical correctness of the sliding window counter
algorithm independently of the Redis/Lua implementation. This serves as a
"peace of mind" validation that the core algorithm logic is correct.

Why these tests exist:

The actual rate limiting logic runs in Lua on Redis, using Redis server time
(via the TIME command). This makes it impractical to unit test the Lua code
directly with mocked time--we cannot control Redis's clock.

Instead, we extract the pure algorithm into Python and test it exhaustively
here. This gives us confidence that:

1. The mathematical formula is correct for all configurations
2. Edge cases (window boundaries, empty windows) behave as expected
3. The 2x burst bound is maintained across all scenarios

The integration tests then verify that the Lua implementation matches this
expected behavior, using real time with short windows.

Algorithm reference (from consume.lua):

    time_passed_in_current = now_ms - current_window_start
    weight = (window_size_ms - time_passed_in_current) / window_size_ms
    estimated_count = current_count + (previous_count * weight)

The sliding window counter allows requests when estimated_count < limit.

Sliding window counter vs fixed window:

The sliding window counter approximates a true sliding window using two
adjacent fixed windows. A simple fixed window algorithm would allow 2x limit
instantly at a window boundary. The counter instead weights the previous
window's contribution, providing smoother rate limiting.
"""

import pytest

from tests.algorithms.sliding_window_counter import is_allowed, sliding_window_estimate


class TestWeightCalculation:
    """Tests for the weight calculation that distinguishes the counter algorithm.

    The counter's core innovation is the weight applied to the previous window.
    This weight decreases linearly as time progresses, creating a smooth
    approximation of a true sliding window.
    """

    @staticmethod
    def test_weight_is_one_at_window_start():
        """At the start of a window, previous window has full weight (1.0)."""
        # Arrange
        window_ms = 1000
        elapsed_ms = 0

        # Act
        estimated = sliding_window_estimate(
            previous_count=10,
            current_count=0,
            window_ms=window_ms,
            elapsed_ms=elapsed_ms,
        )

        # Assert
        # weight = (1000 - 0) / 1000 = 1.0, so estimated = 0 + (10 * 1.0) = 10.0
        assert estimated == pytest.approx(10.0), (
            f"estimated request count at elapsed=0ms should be 10.0, got {estimated}"
        )

    @staticmethod
    def test_weight_is_half_at_window_midpoint():
        """At the midpoint of a window, previous window has half weight (0.5)."""
        # Arrange
        window_ms = 1000
        elapsed_ms = 500

        # Act
        estimated = sliding_window_estimate(
            previous_count=10,
            current_count=0,
            window_ms=window_ms,
            elapsed_ms=elapsed_ms,
        )

        # Assert
        # weight = (1000 - 500) / 1000 = 0.5, so estimated = 0 + (10 * 0.5) = 5.0
        assert estimated == pytest.approx(5.0), (
            f"estimated request count at elapsed=500ms should be 5.0, got {estimated}"
        )

    @staticmethod
    def test_weight_is_zero_at_window_end():
        """At the end of a window, previous window has zero weight (0.0)."""
        # Arrange
        window_ms = 1000
        elapsed_ms = 1000

        # Act
        estimated = sliding_window_estimate(
            previous_count=10,
            current_count=0,
            window_ms=window_ms,
            elapsed_ms=elapsed_ms,
        )

        # Assert
        # weight = (1000 - 1000) / 1000 = 0.0, so estimated = 0 + (10 * 0.0) = 0.0
        assert estimated == pytest.approx(0.0), (
            f"estimated request count at elapsed=1000ms should be 0.0, got {estimated}"
        )

    @staticmethod
    def test_current_window_always_has_full_weight():
        """Property: current window count is always added with weight 1.0."""
        # Arrange
        current_count = 5
        time_positions = [0, 250, 500, 750, 999]

        # Act & Assert
        for elapsed in time_positions:
            estimated = sliding_window_estimate(
                previous_count=0,
                current_count=current_count,
                window_ms=1000,
                elapsed_ms=elapsed,
            )
            assert estimated == pytest.approx(5.0), (
                f"estimated request count should be 5.0 at elapsed={elapsed}ms, got {estimated}"
            )

    @staticmethod
    @pytest.mark.parametrize(
        "elapsed_ms,expected_weight",
        [
            (0, 1.0),
            (100, 0.9),
            (200, 0.8),
            (300, 0.7),
            (400, 0.6),
            (500, 0.5),
            (600, 0.4),
            (700, 0.3),
            (800, 0.2),
            (900, 0.1),
            (1000, 0.0),
        ],
    )
    def test_weight_decreases_linearly(elapsed_ms, expected_weight):
        """Property: weight decreases linearly from 1.0 to 0.0 across the window."""
        # Arrange
        previous_count = 100

        # Act
        estimated = sliding_window_estimate(
            previous_count=previous_count,
            current_count=0,
            window_ms=1000,
            elapsed_ms=elapsed_ms,
        )

        # Assert
        expected_estimate = previous_count * expected_weight
        assert estimated == pytest.approx(expected_estimate, rel=1e-9), (
            f"estimated request count at {elapsed_ms}ms should be {expected_estimate}, got {estimated}"
        )


class TestEstimateFormula:
    """Tests for the combined estimate calculation."""

    @staticmethod
    def test_combined_counts_at_midpoint():
        """Both previous and current counts contribute to the estimate."""
        # Arrange
        previous_count = 10
        current_count = 5

        # Act
        estimated = sliding_window_estimate(
            previous_count=previous_count,
            current_count=current_count,
            window_ms=1000,
            elapsed_ms=500,
        )

        # Assert
        # estimated = 5 + (10 * 0.5) = 10.0
        assert estimated == pytest.approx(10.0), (
            f"estimated request count should be 10.0 (curr=5 + prev=10*0.5), got {estimated}"
        )

    @staticmethod
    def test_empty_windows_yield_zero_estimate():
        """Property: empty windows result in zero estimate at any time position."""
        # Act & Assert
        for elapsed in [0, 500, 1000]:
            estimated = sliding_window_estimate(
                previous_count=0,
                current_count=0,
                window_ms=1000,
                elapsed_ms=elapsed,
            )
            assert estimated == pytest.approx(0.0), (
                f"estimated request count should be 0.0 with empty windows, got {estimated} at elapsed={elapsed}ms"
            )

    @staticmethod
    @pytest.mark.parametrize(
        "prev,curr,window,elapsed,expected",
        [
            # different window sizes
            (10, 5, 60000, 30000, 10.0),
            (10, 5, 500, 250, 10.0),
            (10, 5, 2000, 1000, 10.0),
            # asymmetric counts
            (100, 0, 1000, 100, 90.0),
            (0, 100, 1000, 100, 100.0),
            (50, 50, 1000, 500, 75.0),
        ],
    )
    def test_formula_across_configurations(prev, curr, window, elapsed, expected):
        """Formula produces correct results for various configurations."""
        # Act
        estimated = sliding_window_estimate(prev, curr, window, elapsed)

        # Assert
        assert estimated == pytest.approx(expected, rel=1e-9), (
            f"estimated request count should be {expected}, got {estimated} "
            f"(prev={prev}, curr={curr}, window={window}ms, elapsed={elapsed}ms)"
        )


class TestRateLimitDecision:
    """Tests for the is_allowed decision function."""

    @staticmethod
    def test_allowed_when_under_limit():
        """Requests are allowed when estimate is below limit."""
        # Arrange
        limit = 10

        # Act
        allowed = is_allowed(
            previous_count=0,
            current_count=5,
            window_ms=1000,
            elapsed_ms=0,
            limit=limit,
        )

        # Assert
        assert allowed is True, (
            f"is_allowed should return True when under limit, got {allowed}"
        )

    @staticmethod
    def test_denied_when_at_limit():
        """Requests are denied when estimate equals limit."""
        # Arrange
        limit = 10

        # Act
        allowed = is_allowed(
            previous_count=0,
            current_count=10,
            window_ms=1000,
            elapsed_ms=0,
            limit=limit,
        )

        # Assert
        assert allowed is False, (
            f"is_allowed should return False when at limit, got {allowed}"
        )

    @staticmethod
    def test_denied_when_over_limit():
        """Requests are denied when estimate exceeds limit."""
        # Arrange
        limit = 10

        # Act
        allowed = is_allowed(
            previous_count=10,
            current_count=10,
            window_ms=1000,
            elapsed_ms=0,
            limit=limit,
        )

        # Assert
        # estimated = 10 + (10 * 1.0) = 20.0, which exceeds limit of 10
        assert allowed is False, (
            f"is_allowed should return False when over limit, got {allowed}"
        )

    @staticmethod
    def test_previous_window_ages_out():
        """Requests become allowed as previous window ages out."""
        # Arrange
        limit = 10

        # At window start: estimated = 0 + (10 * 1.0) = 10.0, denied
        result = is_allowed(10, 0, 1000, 0, limit)
        assert result is False, (
            f"is_allowed should return False at elapsed=0ms (estimate=10, limit=10), got {result}"
        )

        # After 100ms: estimated = 0 + (10 * 0.9) = 9.0, allowed
        result = is_allowed(10, 0, 1000, 100, limit)
        assert result is True, (
            f"is_allowed should return True at elapsed=100ms (estimate=9, limit=10), got {result}"
        )

        # At midpoint: estimated = 0 + (10 * 0.5) = 5.0, allowed
        result = is_allowed(10, 0, 1000, 500, limit)
        assert result is True, (
            f"is_allowed should return True at elapsed=500ms (estimate=5, limit=10), got {result}"
        )


class TestBurstBoundProperty:
    """Tests verifying the 2x burst bound property.

    The algorithm can allow up to 2x limit in a window-sized period when:

    1. Previous window is empty (no requests)
    2. Requests arrive at the very end of the current (empty) window
    3. Requests continue into the next window

    In this scenario:
    - End of window N: up to `limit` requests can be consumed (previous empty)
    - Start of window N+1: `limit` more as previous weight decays

    This is a known algorithmic property, not a bug. These tests verify
    the bound is respected and document the expected behavior.

    See also: tests/integration/test_rate_limiting.py for behavioral tests
    that verify this property with the real Redis/Lua implementation.
    """

    @staticmethod
    def test_max_burst_at_boundary_with_empty_history():
        """Demonstrate 2x burst scenario: empty previous allows limit at window end."""
        # Arrange
        limit = 10
        window_ms = 1000

        # Act: consume at the very end of window N (empty previous)
        # At elapsed=999ms, weight = 0.001, so previous has negligible contribution
        consumed_in_window_n = 0
        for _ in range(limit):
            if is_allowed(0, consumed_in_window_n, window_ms, 999, limit):
                consumed_in_window_n += 1

        # Assert
        assert consumed_in_window_n == limit, (
            f"consumed count in window N should be {limit}, got {consumed_in_window_n}"
        )

    @staticmethod
    def test_second_window_allows_more_after_first_window_burst():
        """After bursting at window end, more requests allowed as weight decays."""
        # Arrange
        limit = 10
        window_ms = 1000
        # Previous window (N) had limit requests consumed at its end
        previous_count = limit

        # At window N+1 start: estimated = 0 + (10 * 1.0) = 10.0, exactly at limit
        result = is_allowed(previous_count, 0, window_ms, 0, limit)
        assert result is False, (
            f"is_allowed should return False at window boundary (estimate=10, limit=10), got {result}"
        )

        # At elapsed=1ms: estimated = 0 + (10 * 0.999) = 9.99 < 10, allowed
        result = is_allowed(previous_count, 0, window_ms, 1, limit)
        assert result is True, (
            f"is_allowed should return True at elapsed=1ms (estimate=9.99, limit=10), got {result}"
        )

        # Consume more requests in window N+1, spread throughout the window
        # Each request requires weight to decay enough: weight < (limit - current) / limit
        consumed_in_window_n1 = 0
        for elapsed in range(1, window_ms + 1):
            if is_allowed(
                previous_count, consumed_in_window_n1, window_ms, elapsed, limit
            ):
                consumed_in_window_n1 += 1
                if consumed_in_window_n1 >= limit:
                    break

        # Assert
        assert consumed_in_window_n1 == limit, (
            f"consumed count in window N+1 should be {limit}, got {consumed_in_window_n1}"
        )

    @staticmethod
    def test_burst_cannot_exceed_2x_limit():
        """Property: total burst in worst case scenario is exactly 2x limit."""
        # Arrange
        limit = 10
        window_ms = 1000

        # Window N: empty previous, consume at end
        consumed_n = 0
        for _ in range(limit * 2):
            if is_allowed(0, consumed_n, window_ms, window_ms - 1, limit):
                consumed_n += 1

        # Window N+1: previous has consumed_n, consume as weight decays over full window
        consumed_n1 = 0
        for elapsed in range(1, window_ms + 1):
            if is_allowed(consumed_n, consumed_n1, window_ms, elapsed, limit):
                consumed_n1 += 1
                if consumed_n1 >= limit:
                    break

        # Assert
        total_burst = consumed_n + consumed_n1
        assert consumed_n == limit, (
            f"consumed in window N should be {limit}, got {consumed_n}"
        )
        assert consumed_n1 == limit, (
            f"consumed in window N+1 should be {limit}, got {consumed_n1}"
        )
        assert total_burst == 2 * limit, (
            f"total burst should be {2 * limit}, got {total_burst}"
        )

        # Verify no more can be consumed: both windows at limit
        result = is_allowed(limit, limit, window_ms, 100, limit)
        assert result is False, (
            f"is_allowed should return False when both windows at limit, got {result}"
        )

    @staticmethod
    @pytest.mark.parametrize(
        "limit,window_ms",
        [
            (1, 100),
            (1, 1000),
            (5, 500),
            (10, 1000),
            (10, 2000),
            (100, 60000),
            (1000, 1000),
        ],
    )
    def test_2x_bound_holds_across_configurations(limit, window_ms):
        """Property: 2x bound holds for various limit and window configurations."""
        # Window N: consume at end with empty previous
        consumed_n = 0
        for _ in range(limit * 2):
            if is_allowed(0, consumed_n, window_ms, window_ms - 1, limit):
                consumed_n += 1

        # Window N+1: consume as previous weight decays over the full window
        consumed_n1 = 0
        for elapsed in range(1, window_ms + 1):
            if is_allowed(consumed_n, consumed_n1, window_ms, elapsed, limit):
                consumed_n1 += 1
                if consumed_n1 >= limit:
                    break

        # Assert
        assert consumed_n == limit, (
            f"consumed in window N should be {limit}, got {consumed_n}"
        )
        assert consumed_n1 == limit, (
            f"consumed in window N+1 should be {limit}, got {consumed_n1}"
        )
        assert consumed_n + consumed_n1 == 2 * limit, (
            f"total burst should be {2 * limit}, got {consumed_n + consumed_n1}"
        )


class TestSmoothingBehavior:
    """Tests demonstrating how the counter smooths rate limiting.

    A fixed window algorithm allows 2x limit instantly at boundaries. The
    sliding window counter instead spreads the impact of previous requests
    over time, resulting in smoother rate limiting.
    """

    @staticmethod
    def test_previous_requests_gradually_lose_influence():
        """Property: previous window's influence fades gradually, not abruptly."""
        # Arrange
        limit = 10
        previous_count = 10

        # Act
        # Track how many requests are allowed as time passes
        allowed_at_times = {}
        for elapsed in range(0, 1001, 100):
            current = 0
            while is_allowed(previous_count, current, 1000, elapsed, limit):
                current += 1
            allowed_at_times[elapsed] = current

        # Assert
        # At t=0: can allow 0 (estimated=10, at limit)
        assert allowed_at_times[0] == 0, (
            f"allowed requests at t=0 should be 0, got {allowed_at_times[0]}"
        )
        # At t=100: can allow 1 (estimated=9 after 1 request)
        assert allowed_at_times[100] == 1, (
            f"allowed requests at t=100 should be 1, got {allowed_at_times[100]}"
        )
        # At t=500: can allow 5 (estimated=5 after 5 requests)
        assert allowed_at_times[500] == 5, (
            f"allowed requests at t=500 should be 5, got {allowed_at_times[500]}"
        )
        # At t=1000: can allow 10 (previous fully aged out)
        assert allowed_at_times[1000] == 10, (
            f"allowed requests at t=1000 should be 10, got {allowed_at_times[1000]}"
        )

    @staticmethod
    def test_steady_state_maintains_limit():
        """In steady state, the algorithm maintains approximately limit per window."""
        # Arrange
        limit = 10
        window_ms = 1000

        # Simulate steady consumption: both windows at limit
        previous_count = limit
        current_count = 0

        # At midpoint: estimated = current + (previous * 0.5) = 0 + 5 = 5
        # Can consume 5 more before hitting limit
        allowed_count = 0
        while is_allowed(
            previous_count, current_count + allowed_count, window_ms, 500, limit
        ):
            allowed_count += 1

        # Assert
        assert allowed_count == 5, (
            f"allowed count at midpoint with full previous should be 5, got {allowed_count}"
        )
