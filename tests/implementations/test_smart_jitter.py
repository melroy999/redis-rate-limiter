"""Tests for the adaptive jitter calculation in AbstractDistributedRateLimiter.

Smart jitter prevents the thundering herd problem at window resets by spreading
retry attempts based on system load.
"""

import logging
import random
from unittest.mock import patch

import pytest

from tests.implementations.conftest import MinimalRateLimiter


class TestSmartJitter:
    """Test suite for the adaptive jitter implementation."""

    @staticmethod
    def _seeded_random_values(samples: int, seed: int) -> list[float]:
        """Generate deterministic pseudo-random values for use in jitter tests."""
        seeded_rng = random.Random(seed)
        return [seeded_rng.random() for _ in range(samples)]

    @pytest.fixture
    def make_limiter(self, redis_client, limiter_id):
        """Factory fixture for creating generic limiters with specific jitter settings."""

        def _make_limiter(
            *,
            limiter_suffix: str,
            limit: int = 10,
            window: int = 1,
            max_concurrency: int = 5,
            jitter_enabled: bool = True,
            jitter_min_pct: float = 0.02,
            jitter_max_pct: float = 0.08,
        ) -> MinimalRateLimiter:
            return MinimalRateLimiter(
                redis_client=redis_client,
                limiter_id=f"{limiter_id}_{limiter_suffix}",
                limit=limit,
                window=window,
                max_concurrency=max_concurrency,
                jitter_enabled=jitter_enabled,
                jitter_min_pct=jitter_min_pct,
                jitter_max_pct=jitter_max_pct,
            )

        return _make_limiter

    @pytest.fixture
    def limiter(self, make_limiter):
        """Create a limiter with the default jitter settings for testing."""
        return make_limiter(
            limiter_suffix="jitter_default",
        )

    @staticmethod
    def test_jitter_disabled_returns_zero(limiter):
        """Verify that the jitter returns zero when it is disabled."""
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

    def test_jitter_scales_with_window_size(self, make_limiter):
        """Verify that the average jitter is proportional to the window size."""
        # Arrange
        short_limiter = make_limiter(
            limiter_suffix="jitter_short_window",
            window=1,
        )
        long_limiter = make_limiter(
            limiter_suffix="jitter_long_window",
            window=60,
        )

        # Act
        # Multiple samples are taken to test statistical properties.
        samples = 100
        random_values = self._seeded_random_values(samples, seed=20260207)
        with patch(
            "celery_rate_limiter.core.limiters.random.random",
            side_effect=iter(random_values),
        ):
            short_jitters = [
                short_limiter._calculate_smart_jitter(
                    remaining_tasks=50, remaining_tokens=0, active_concurrency=3
                )
                for _ in range(samples)
            ]
        with patch(
            "celery_rate_limiter.core.limiters.random.random",
            side_effect=iter(random_values),
        ):
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
        """Verify that the average jitter increases under higher contention."""
        # Arrange
        samples = 100
        random_values = self._seeded_random_values(samples, seed=20260208)

        # Act
        # Multiple samples are taken at each load level.
        with patch(
            "celery_rate_limiter.core.limiters.random.random",
            side_effect=iter(random_values),
        ):
            low_load_jitters = [
                limiter._calculate_smart_jitter(
                    remaining_tasks=5,
                    remaining_tokens=0,
                    active_concurrency=1,
                )
                for _ in range(samples)
            ]
        with patch(
            "celery_rate_limiter.core.limiters.random.random",
            side_effect=iter(random_values),
        ):
            medium_load_jitters = [
                limiter._calculate_smart_jitter(
                    remaining_tasks=50,
                    remaining_tokens=0,
                    active_concurrency=3,
                )
                for _ in range(samples)
            ]
        with patch(
            "celery_rate_limiter.core.limiters.random.random",
            side_effect=iter(random_values),
        ):
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

        monotonicity_violations = [
            (idx, low, medium, high)
            for idx, (low, medium, high) in enumerate(
                zip(
                    low_load_jitters,
                    medium_load_jitters,
                    high_load_jitters,
                    strict=True,
                )
            )
            if not (low <= medium <= high)
        ]
        first_violation = (
            monotonicity_violations[0] if monotonicity_violations else None
        )
        assert not monotonicity_violations, (
            f"paired jitter monotonicity violated for load pressure; "
            f"violations={len(monotonicity_violations)}, first={first_violation}, "
            f"avg_low={avg_low:.4f}, avg_medium={avg_medium:.4f}, avg_high={avg_high:.4f}"
        )

    def test_jitter_is_randomized(self, limiter):
        """Verify that repeated calls produce varied output."""
        # Act
        # The jitter calculation is invoked multiple times with identical inputs.
        samples = 100
        random_values = self._seeded_random_values(samples, seed=20260209)
        with patch(
            "celery_rate_limiter.core.limiters.random.random",
            side_effect=iter(random_values),
        ):
            jitters = [
                limiter._calculate_smart_jitter(
                    remaining_tasks=50,
                    remaining_tokens=0,
                    active_concurrency=3,
                )
                for _ in range(samples)
            ]

        # Assert
        unique_jitters = set(jitters)
        assert len(unique_jitters) > 10, (
            f"jitter should be randomized, got only {len(unique_jitters)} unique values"
        )

        # The mean should be near the middle of the configured range.
        mean_jitter = sum(jitters) / len(jitters)
        expected_mean = (
            limiter.window * limiter.jitter_min_pct
            + limiter.window * limiter.jitter_max_pct
        ) / 2
        assert abs(mean_jitter - expected_mean) < 0.01, (
            f"mean jitter ({mean_jitter:.4f}) should be near "
            f"expected mean ({expected_mean:.4f})"
        )

    @staticmethod
    def test_custom_jitter_percentages(make_limiter, caplog):
        """Verify that custom jitter percentages produce the correct jitter value."""
        # Arrange
        custom_limiter = make_limiter(
            limiter_suffix="jitter_custom_percentages",
            window=10,
            jitter_min_pct=0.01,
            jitter_max_pct=0.05,
        )

        # Act
        # Pin random to 0.5 so the output is deterministic.
        # load_pressure=1.0 (100 >= 100), conc_pressure=3/5=0.6
        # combined=0.7*1.0 + 0.3*0.6=0.88, scale=0.3+0.88*0.7=0.916
        # range=0.4*0.916=0.3664, jitter=0.1+0.3664*0.5=0.2832, round=0.283
        with (
            caplog.at_level(logging.DEBUG, logger="celery_rate_limiter.core.limiters"),
            patch(
                "celery_rate_limiter.core.limiters.random.random",
                return_value=0.5,
            ),
        ):
            jitter = custom_limiter._calculate_smart_jitter(
                remaining_tasks=100,
                remaining_tokens=0,
                active_concurrency=3,
            )

        # Assert
        assert jitter == pytest.approx(0.283), (
            f"jitter {jitter}s should equal 0.283s for custom percentages"
        )
        assert any(
            record.levelname == "DEBUG"
            and custom_limiter.id in record.message
            and "remaining_tasks=100" in record.message
            and "remaining_tokens=0" in record.message
            and "active_concurrency=3" in record.message
            and "0.283" in record.message
            for record in caplog.records
        ), "should emit a debug log containing the limiter id, input parameters, and computed jitter"

    def test_concurrency_pressure_increases_jitter(self, limiter):
        """Verify that higher concurrency pressure produces a larger average jitter.

        A seeded random stream is used so that this unit test is deterministic.
        """
        # Arrange
        samples = 100
        random_values = self._seeded_random_values(samples, seed=20260210)

        # Act
        # Each concurrency level is sampled multiple times.
        # The exact same random values are reused for both levels so that any
        # difference arises from concurrency pressure, not random chance.
        with patch(
            "celery_rate_limiter.core.limiters.random.random",
            side_effect=iter(random_values),
        ):
            low_concurrency_jitters = [
                limiter._calculate_smart_jitter(
                    remaining_tasks=50,
                    remaining_tokens=0,
                    active_concurrency=1,
                )
                for _ in range(samples)
            ]
        with patch(
            "celery_rate_limiter.core.limiters.random.random",
            side_effect=iter(random_values),
        ):
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
        monotonicity_violations = [
            (idx, low, high)
            for idx, (low, high) in enumerate(
                zip(low_concurrency_jitters, high_concurrency_jitters, strict=True)
            )
            if high < low
        ]
        first_violation = (
            monotonicity_violations[0] if monotonicity_violations else None
        )
        assert not monotonicity_violations, (
            f"paired jitter monotonicity violated for concurrency pressure; "
            f"violations={len(monotonicity_violations)}, first={first_violation}, "
            f"avg_low={avg_low:.4f}, avg_high={avg_high:.4f}"
        )

    @staticmethod
    def test_jitter_precision(limiter):
        """Verify that the jitter is rounded to three decimal places."""
        # Act
        jitter = limiter._calculate_smart_jitter(
            remaining_tasks=50,
            remaining_tokens=0,
            active_concurrency=3,
        )

        # Assert
        # The jitter should be rounded to millisecond precision.
        assert jitter == round(jitter, 3), (
            f"jitter {jitter} should be rounded to 3 decimal places"
        )
