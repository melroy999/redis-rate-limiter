"""Tests for adaptive jitter calculation in AbstractDistributedRateLimiter.

Smart jitter prevents thundering herd at window resets by spreading retry
attempts based on system load.
"""

import itertools

import pytest

from celery_rate_limiter.limiters import CeleryRateLimiter


class TestSmartJitter:
    """Test suite for adaptive jitter implementation."""

    @pytest.fixture
    def limiter(self, redis_client, celery_app):
        """Create a limiter with default jitter settings for testing."""
        return CeleryRateLimiter(
            redis_client=redis_client,
            celery_app=celery_app,
            limiter_id="test_jitter",
            limit=10,
            window=1,
            max_concurrency=5,
            jitter_enabled=True,
            jitter_min_pct=0.02,
            jitter_max_pct=0.08,
        )

    def test_jitter_disabled_returns_zero(self, limiter):
        """Verify jitter returns 0 when disabled."""
        # Arrange
        limiter.jitter_enabled = False

        # Act
        jitter = limiter._calculate_smart_jitter(
            remaining_tasks=100,
            remaining_tokens=0,
            active_concurrency=3,
        )

        # Assert
        assert jitter == 0.0, "disabled jitter should return 0"

    def test_jitter_scales_with_window_size(self, redis_client, celery_app):
        """Verify average jitter is proportional to window size."""
        # Arrange
        short_limiter = CeleryRateLimiter(
            redis_client=redis_client,
            celery_app=celery_app,
            limiter_id="test_short",
            limit=10,
            window=1,
            max_concurrency=5,
        )
        long_limiter = CeleryRateLimiter(
            redis_client=redis_client,
            celery_app=celery_app,
            limiter_id="test_long",
            limit=10,
            window=60,
            max_concurrency=5,
        )

        # Act
        # Take multiple samples to test statistical properties.
        samples = 100
        short_jitters = [
            short_limiter._calculate_smart_jitter(
                remaining_tasks=50, remaining_tokens=0, active_concurrency=3
            )
            for _ in range(samples)
        ]
        long_jitters = [
            long_limiter._calculate_smart_jitter(
                remaining_tasks=50, remaining_tokens=0, active_concurrency=3
            )
            for _ in range(samples)
        ]

        # Assert
        avg_short = sum(short_jitters) / len(short_jitters)
        avg_long = sum(long_jitters) / len(long_jitters)

        assert avg_long > avg_short, "longer window should produce larger jitter"
        assert avg_long / avg_short == pytest.approx(60.0, rel=0.15), (
            f"jitter ratio ({avg_long / avg_short:.1f}) should match "
            f"window ratio (60.0)"
        )

    def test_jitter_increases_with_load(self, limiter):
        """Verify average jitter increases under higher contention."""
        # Arrange
        samples = 100

        # Act
        # Take multiple samples at each load level.
        low_load_jitters = [
            limiter._calculate_smart_jitter(
                remaining_tasks=5,
                remaining_tokens=0,
                active_concurrency=1,
            )
            for _ in range(samples)
        ]
        medium_load_jitters = [
            limiter._calculate_smart_jitter(
                remaining_tasks=50,
                remaining_tokens=0,
                active_concurrency=3,
            )
            for _ in range(samples)
        ]
        high_load_jitters = [
            limiter._calculate_smart_jitter(
                remaining_tasks=200,
                remaining_tokens=0,
                active_concurrency=5,
            )
            for _ in range(samples)
        ]

        # Assert
        avg_low = sum(low_load_jitters) / len(low_load_jitters)
        avg_medium = sum(medium_load_jitters) / len(medium_load_jitters)
        avg_high = sum(high_load_jitters) / len(high_load_jitters)

        assert avg_low < avg_medium < avg_high, (
            f"average jitter should increase with load: "
            f"low={avg_low:.4f}, medium={avg_medium:.4f}, high={avg_high:.4f}"
        )

    @pytest.mark.parametrize(
        "remaining_tasks, active_concurrency",
        itertools.product([0, 5, 50, 200], [0, 2, 5]),
    )
    def test_jitter_within_configured_bounds(
        self, limiter, remaining_tasks, active_concurrency
    ):
        """Verify jitter stays within configured min/max percentages."""
        # Arrange
        min_expected = limiter.window * limiter.jitter_min_pct
        max_expected = limiter.window * limiter.jitter_max_pct

        # Act
        jitter = limiter._calculate_smart_jitter(
            remaining_tasks=remaining_tasks,
            remaining_tokens=0,
            active_concurrency=active_concurrency,
        )

        # Assert
        assert min_expected <= jitter <= max_expected, (
            f"jitter {jitter}s out of bounds [{min_expected}, {max_expected}] "
            f"for tasks={remaining_tasks}, concurrency={active_concurrency}"
        )

    def test_jitter_is_randomized(self, limiter):
        """Verify repeated calls produce varied output."""
        # Act
        # Call jitter calculation multiple times with identical inputs.
        jitters = [
            limiter._calculate_smart_jitter(
                remaining_tasks=50,
                remaining_tokens=0,
                active_concurrency=3,
            )
            for _ in range(100)
        ]

        # Assert
        unique_jitters = set(jitters)
        assert len(unique_jitters) > 10, (
            f"jitter should be randomized, got only {len(unique_jitters)} unique values"
        )

        # Mean should be near the middle of the configured range.
        mean_jitter = sum(jitters) / len(jitters)
        expected_mean = (
            limiter.window * limiter.jitter_min_pct
            + limiter.window * limiter.jitter_max_pct
        ) / 2
        assert abs(mean_jitter - expected_mean) < 0.01, (
            f"mean jitter ({mean_jitter:.4f}) should be near "
            f"expected mean ({expected_mean:.4f})"
        )

    def test_custom_jitter_percentages(self, redis_client, celery_app):
        """Verify custom jitter percentages are respected."""
        # Arrange
        custom_limiter = CeleryRateLimiter(
            redis_client=redis_client,
            celery_app=celery_app,
            limiter_id="test_custom",
            limit=10,
            window=10,
            max_concurrency=5,
            jitter_min_pct=0.01,
            jitter_max_pct=0.05,
        )

        # Act
        jitter = custom_limiter._calculate_smart_jitter(
            remaining_tasks=100,
            remaining_tokens=0,
            active_concurrency=3,
        )

        # Assert
        # Custom bounds for 10s window: 100ms to 500ms.
        assert 0.1 <= jitter <= 0.5, (
            f"jitter {jitter}s should be within custom bounds [0.1, 0.5]"
        )

    def test_concurrency_pressure_increases_jitter(self, limiter):
        """Verify higher concurrency pressure produces larger average jitter."""
        # Arrange
        samples = 100

        # Act
        # Take multiple samples at each concurrency level.
        low_concurrency_jitters = [
            limiter._calculate_smart_jitter(
                remaining_tasks=50,
                remaining_tokens=0,
                active_concurrency=1,
            )
            for _ in range(samples)
        ]
        high_concurrency_jitters = [
            limiter._calculate_smart_jitter(
                remaining_tasks=50,
                remaining_tokens=0,
                active_concurrency=5,
            )
            for _ in range(samples)
        ]

        # Assert
        avg_low = sum(low_concurrency_jitters) / len(low_concurrency_jitters)
        avg_high = sum(high_concurrency_jitters) / len(high_concurrency_jitters)

        assert avg_high > avg_low, (
            f"higher concurrency pressure should increase jitter: "
            f"low={avg_low:.4f}, high={avg_high:.4f}"
        )

    def test_jitter_precision(self, limiter):
        """Verify jitter is rounded to 3 decimal places."""
        # Act
        jitter = limiter._calculate_smart_jitter(
            remaining_tasks=50,
            remaining_tokens=0,
            active_concurrency=3,
        )

        # Assert
        # Jitter should be rounded to millisecond precision.
        assert jitter == round(jitter, 3), (
            f"jitter {jitter} should be rounded to 3 decimal places"
        )
