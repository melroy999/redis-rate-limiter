"""Property and unit tests for the sliding window counter algorithm.

These tests verify the mathematical correctness of the sliding window counter
algorithm independently of the Redis/Lua implementation. As such, they serve
as a validation that the core algorithmic logic is correct.

Motivation:

The production rate limiting logic is executed in Lua on Redis, relying on
Redis server time (obtained via the TIME command). This makes it impractical
to unit test the Lua code directly with mocked time, as the Redis clock
cannot be controlled externally.

Instead, the pure algorithm is extracted into Python and tested exhaustively
in this module. This approach provides confidence that:

1. The mathematical formula is correct for all configurations.
2. Edge cases (i.e., window boundaries, empty windows) behave as expected.
3. The 2x burst bound is maintained across all scenarios.

The integration tests subsequently verify that the Lua implementation matches
the expected behavior, using real time with short windows.

Algorithm reference (from consume.lua):

    time_passed_in_current = now_ms - current_window_start
    weight = (window_size_ms - time_passed_in_current) / window_size_ms
    estimated_count = current_count + (previous_count * weight)

The sliding window counter permits requests when estimated_count < limit.

Sliding window counter versus fixed window:

The sliding window counter approximates a true sliding window using two
adjacent fixed windows. A simple fixed window algorithm would permit 2x the
limit instantaneously at a window boundary. The counter instead weights the
previous window's contribution, thereby providing smoother rate limiting.
"""

import pytest

from tests.algorithms.sliding_window_counter import is_allowed, sliding_window_estimate


class TestWeightCalculation:
    """Tests for the weight calculation that distinguishes the counter algorithm.

    The central mechanism of the counter is the weight applied to the previous
    window. This weight decreases linearly as time progresses, producing a
    smooth approximation of a true sliding window.
    """

    @staticmethod
    def test_weight_is_one_at_window_start():
        """Verify that the previous window is assigned full weight
        (1.0) at the window start."""
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
        """Verify that the previous window is assigned half weight
        (0.5) at the window midpoint."""
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
        """Verify that the previous window is assigned zero weight
        (0.0) at the window end."""
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
        """Property: the current window count is always added with a weight of 1.0."""
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
                f"estimated request count should be 5.0 at "
                f"elapsed={elapsed}ms, got {estimated}"
            )

    @staticmethod
    @pytest.mark.parametrize(
        ("elapsed_ms", "expected_weight"),
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
        ids=[
            "t=0ms_w=1.0",
            "t=100ms_w=0.9",
            "t=200ms_w=0.8",
            "t=300ms_w=0.7",
            "t=400ms_w=0.6",
            "t=500ms_w=0.5",
            "t=600ms_w=0.4",
            "t=700ms_w=0.3",
            "t=800ms_w=0.2",
            "t=900ms_w=0.1",
            "t=1000ms_w=0.0",
        ],
    )
    def test_weight_decreases_linearly(elapsed_ms, expected_weight):
        """Property: the weight decreases linearly from 1.0 to 0.0 across the window."""
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
            f"estimated request count at {elapsed_ms}ms should be "
            f"{expected_estimate}, got {estimated}"
        )


class TestEstimateFormula:
    """Tests for the combined estimate calculation formula."""

    @staticmethod
    def test_combined_counts_at_midpoint():
        """Verify that both the previous and current counts
        contribute to the estimate."""
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
            f"estimated request count should be 10.0 "
            f"(curr=5 + prev=10*0.5), got {estimated}"
        )

    @staticmethod
    def test_empty_windows_yield_zero_estimate():
        """Property: empty windows yield a zero estimate at any time position."""
        # Act & Assert
        for elapsed in [0, 500, 1000]:
            estimated = sliding_window_estimate(
                previous_count=0,
                current_count=0,
                window_ms=1000,
                elapsed_ms=elapsed,
            )
            assert estimated == pytest.approx(0.0), (
                f"estimated request count should be 0.0 with "
                f"empty windows, got {estimated} at "
                f"elapsed={elapsed}ms"
            )

    @staticmethod
    @pytest.mark.parametrize(
        ("prev", "curr", "window", "elapsed", "expected"),
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
        ids=[
            "60s_window_midpoint",
            "500ms_window_midpoint",
            "2s_window_midpoint",
            "prev_dominant_early",
            "curr_only_early",
            "balanced_midpoint",
        ],
    )
    def test_formula_across_configurations(prev, curr, window, elapsed, expected):
        """Verify that the formula produces correct results across
        various configurations."""
        # Act
        estimated = sliding_window_estimate(prev, curr, window, elapsed)

        # Assert
        assert estimated == pytest.approx(expected, rel=1e-9), (
            f"estimated request count should be {expected}, got {estimated} "
            f"(prev={prev}, curr={curr}, window={window}ms, elapsed={elapsed}ms)"
        )


class TestRateLimitDecision:
    """Tests for the is_allowed decision function that governs rate limit admission."""

    @staticmethod
    def test_allowed_when_under_limit():
        """Verify that requests are permitted when the estimate is below the limit."""
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
        """Verify that requests are denied when the estimate equals the limit."""
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
        """Verify that requests are denied when the estimate exceeds the limit."""
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
        """Verify that requests become permitted as the previous window ages out."""
        # Arrange
        limit = 10

        # At the window start: estimated = 0 + (10 * 1.0) = 10.0, denied
        result = is_allowed(10, 0, 1000, 0, limit)
        assert result is False, (
            f"is_allowed should return False at elapsed=0ms "
            f"(estimate=10, limit=10), got {result}"
        )

        # After 100ms: estimated = 0 + (10 * 0.9) = 9.0, permitted
        result = is_allowed(10, 0, 1000, 100, limit)
        assert result is True, (
            f"is_allowed should return True at elapsed=100ms "
            f"(estimate=9, limit=10), got {result}"
        )

        # At the midpoint: estimated = 0 + (10 * 0.5) = 5.0, permitted
        result = is_allowed(10, 0, 1000, 500, limit)
        assert result is True, (
            f"is_allowed should return True at elapsed=500ms "
            f"(estimate=5, limit=10), got {result}"
        )


class TestBurstBoundProperty:
    """Tests verifying the 2x burst bound property of the algorithm.

    The algorithm can permit up to 2x the limit when an empty previous
    window is followed by a boundary crossing. This is a known algorithmic
    property, not a defect.
    """

    @staticmethod
    def test_max_burst_at_boundary_with_empty_history():
        """Verify that an empty previous window permits the full
        limit at the window end (2x burst scenario)."""
        # Arrange
        limit = 10
        window_ms = 1000

        # Act
        # Consume at the very end of window N (empty previous).
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
        """Verify that additional requests are permitted as the
        weight decays after a burst at the window end."""
        # Arrange
        limit = 10
        window_ms = 1000
        # The previous window (N) had the full limit of requests consumed at its end
        previous_count = limit

        # At window N+1 start: estimated = 0 + (10 * 1.0) = 10.0, exactly at limit
        result = is_allowed(previous_count, 0, window_ms, 0, limit)
        assert result is False, (
            f"is_allowed should return False at window boundary "
            f"(estimate=10, limit=10), got {result}"
        )

        # At elapsed=1ms: estimated = 0 + (10 * 0.999) = 9.99 < 10, allowed
        result = is_allowed(previous_count, 0, window_ms, 1, limit)
        assert result is True, (
            f"is_allowed should return True at elapsed=1ms "
            f"(estimate=9.99, limit=10), got {result}"
        )

        # Consume additional requests in window N+1, spread throughout the window.
        # Each request requires the weight to decay sufficiently:
        # weight < (limit - current) / limit
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
            f"consumed count in window N+1 should be {limit}, "
            f"got {consumed_in_window_n1}"
        )

    @staticmethod
    def test_burst_cannot_exceed_2x_limit():
        """Property: the total burst in the worst-case scenario is
        exactly 2x the limit."""
        # Arrange
        limit = 10
        window_ms = 1000

        # Window N: the previous window is empty; consume at the end
        consumed_n = 0
        for _ in range(limit * 2):
            if is_allowed(0, consumed_n, window_ms, window_ms - 1, limit):
                consumed_n += 1

        # Window N+1: the previous window has consumed_n; consume
        # as the weight decays over the full window
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

        # Verify that no additional requests can be consumed
        # when both windows are at the limit
        result = is_allowed(limit, limit, window_ms, 100, limit)
        assert result is False, (
            f"is_allowed should return False when both windows at limit, got {result}"
        )

    @staticmethod
    @pytest.mark.parametrize(
        ("limit", "window_ms"),
        [
            (1, 100),
            (1, 1000),
            (5, 500),
            (10, 1000),
            (10, 2000),
            (100, 60000),
            (1000, 1000),
        ],
        ids=[
            "limit=1_window=100ms",
            "limit=1_window=1s",
            "limit=5_window=500ms",
            "limit=10_window=1s",
            "limit=10_window=2s",
            "limit=100_window=60s",
            "limit=1000_window=1s",
        ],
    )
    def test_2x_bound_holds_across_configurations(limit, window_ms):
        """Property: the 2x bound holds for various limit and window configurations."""
        # Window N: consume at the end with an empty previous window
        consumed_n = 0
        for _ in range(limit * 2):
            if is_allowed(0, consumed_n, window_ms, window_ms - 1, limit):
                consumed_n += 1

        # Window N+1: consume as the previous weight decays over the full window
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
    """Tests demonstrating how the counter produces smoother rate limiting.

    A fixed window algorithm permits 2x the limit instantaneously at boundaries.
    The sliding window counter instead distributes the impact of previous requests
    over time, resulting in a smoother rate limiting profile.
    """

    @staticmethod
    def test_previous_requests_gradually_lose_influence():
        """Property: the previous window's influence fades gradually, not abruptly."""
        # Arrange
        limit = 10
        previous_count = 10

        # Act
        # Track how many requests are permitted as time passes
        allowed_at_times = {}
        for elapsed in range(0, 1001, 100):
            current = 0
            while is_allowed(previous_count, current, 1000, elapsed, limit):
                current += 1
            allowed_at_times[elapsed] = current

        # Assert
        # At t=0: 0 requests can be permitted (estimated=10, at the limit)
        assert allowed_at_times[0] == 0, (
            f"allowed requests at t=0 should be 0, got {allowed_at_times[0]}"
        )
        # At t=100: 1 request can be permitted (estimated=9 after 1 request)
        assert allowed_at_times[100] == 1, (
            f"allowed requests at t=100 should be 1, got {allowed_at_times[100]}"
        )
        # At t=500: 5 requests can be permitted (estimated=5 after 5 requests)
        assert allowed_at_times[500] == 5, (
            f"allowed requests at t=500 should be 5, got {allowed_at_times[500]}"
        )
        # At t=1000: 10 requests can be permitted
        # (the previous window has fully aged out)
        assert allowed_at_times[1000] == 10, (
            f"allowed requests at t=1000 should be 10, got {allowed_at_times[1000]}"
        )

    @staticmethod
    def test_steady_state_maintains_limit():
        """Verify that the algorithm maintains approximately the
        limit per window in the steady state."""
        # Arrange
        limit = 10
        window_ms = 1000

        # Simulate steady consumption: both windows are at the limit
        previous_count = limit
        current_count = 0

        # At the midpoint: estimated = current + (previous * 0.5) = 0 + 5 = 5
        # Five additional requests can be consumed before reaching the limit
        allowed_count = 0
        while is_allowed(
            previous_count, current_count + allowed_count, window_ms, 500, limit
        ):
            allowed_count += 1

        # Assert
        assert allowed_count == 5, (
            f"allowed count at midpoint with full previous "
            f"should be 5, got {allowed_count}"
        )
