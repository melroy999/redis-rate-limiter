"""Tests for the adaptive jitter calculation in AbstractDistributedRateLimiter.

Smart jitter prevents the thundering herd problem at window resets by spreading
retry attempts based on system load. Tests are written once in async form; the
sync implementation participates via the ``SyncToAsyncLimiterAdapter``, while
the async implementation runs natively.

Fixture dependencies:
    - ``redis_client``, ``async_redis_client``,
      ``limiter_id``: from ``tests/conftest.py``.
"""

import logging
import random
from unittest.mock import patch

import pytest

from tests.helpers.adapters import SyncToAsyncLimiterAdapter
from tests.helpers.utils import assert_log_emitted
from tests.implementations.conftest import AsyncStubRateLimiter, StubRateLimiter

# ---------------------------------------------------------------------------
# Unified implementation tests
# ---------------------------------------------------------------------------


@pytest.mark.behavior
class SmartJitterTests:
    """Unified test suite for the adaptive jitter implementation.

    Subclasses must provide a ``limiter`` fixture that creates a limiter with
    default jitter settings (window=1, limit=10, max_concurrency=5).
    """

    @staticmethod
    def _seeded_random_values(samples: int, seed: int) -> list[float]:
        """Generate deterministic pseudo-random values for use in jitter tests."""
        seeded_rng = random.Random(seed)
        return [seeded_rng.random() for _ in range(samples)]

    @staticmethod
    async def test_jitter_disabled_returns_zero(limiter):
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

    async def test_jitter_scales_with_window_size(self, limiter):
        """Verify that the average jitter is proportional to the window size."""
        # Arrange
        samples = 100
        random_values = self._seeded_random_values(samples, seed=20260207)

        # Act
        limiter.window = 1
        with patch(
            "redis_rate_limiter.core.limiters.random.random",
            side_effect=iter(random_values),
        ):
            short_jitters = [
                limiter._calculate_smart_jitter(
                    remaining_tasks=50, remaining_tokens=0, active_concurrency=3
                )
                for _ in range(samples)
            ]
        limiter.window = 60
        with patch(
            "redis_rate_limiter.core.limiters.random.random",
            side_effect=iter(random_values),
        ):
            long_jitters = [
                limiter._calculate_smart_jitter(
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

    async def test_jitter_increases_with_load(self, limiter):
        """Verify that the average jitter increases under higher contention."""
        # Arrange
        samples = 100
        random_values = self._seeded_random_values(samples, seed=20260208)

        # Act
        with patch(
            "redis_rate_limiter.core.limiters.random.random",
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
            "redis_rate_limiter.core.limiters.random.random",
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
            "redis_rate_limiter.core.limiters.random.random",
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
            f"avg_low={avg_low:.4f}, avg_medium={avg_medium:.4f}, "
            f"avg_high={avg_high:.4f}"
        )

    async def test_jitter_is_randomized(self, limiter):
        """Verify that repeated calls produce varied output."""
        # Act
        samples = 100
        random_values = self._seeded_random_values(samples, seed=20260209)
        with patch(
            "redis_rate_limiter.core.limiters.random.random",
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
    async def test_custom_jitter_percentages(limiter):
        """Verify that custom jitter percentages produce the correct jitter value."""
        # Arrange
        limiter.window = 10
        limiter.jitter_min_pct = 0.01
        limiter.jitter_max_pct = 0.05

        # Act
        # Pin random to 0.5 so the output is deterministic.
        # load_pressure=1.0 (100 >= 100), conc_pressure=3/5=0.6
        # combined=0.7*1.0 + 0.3*0.6=0.88, scale=0.3+0.88*0.7=0.916
        # range=0.4*0.916=0.3664, jitter=0.1+0.3664*0.5=0.2832, round=0.283
        with patch(
            "redis_rate_limiter.core.limiters.random.random",
            return_value=0.5,
        ):
            jitter = limiter._calculate_smart_jitter(
                remaining_tasks=100,
                remaining_tokens=0,
                active_concurrency=3,
            )

        # Assert
        assert jitter == pytest.approx(0.283), (
            f"jitter {jitter}s should equal 0.283s for custom percentages"
        )

    async def test_concurrency_pressure_increases_jitter(self, limiter):
        """Verify that higher concurrency pressure produces a larger average jitter.

        A seeded random stream is used so that this unit test is deterministic.
        """
        # Arrange
        samples = 100
        random_values = self._seeded_random_values(samples, seed=20260210)

        # Act
        # The exact same random values are reused for both levels so that any
        # difference arises from concurrency pressure, not random chance.
        with patch(
            "redis_rate_limiter.core.limiters.random.random",
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
            "redis_rate_limiter.core.limiters.random.random",
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
    async def test_jitter_at_zero_remaining_tasks(limiter):
        """Verify that ``remaining_tasks=0`` produces the minimum jitter range."""
        # Arrange
        limiter.window = 10
        limiter.jitter_min_pct = 0.01
        limiter.jitter_max_pct = 0.05

        # Act
        # Pin random to 0.5 for determinism.
        # load_pressure=0.0, concurrency_pressure=0/5=0.0
        # combined=0.0, scale=0.3, range=0.4*0.3=0.12, jitter=0.1+0.12*0.5=0.16
        with patch(
            "redis_rate_limiter.core.limiters.random.random",
            return_value=0.5,
        ):
            jitter = limiter._calculate_smart_jitter(
                remaining_tasks=0,
                remaining_tokens=5,
                active_concurrency=0,
            )

        # Assert
        assert jitter == pytest.approx(0.16), (
            f"jitter {jitter}s should equal 0.16s for zero"
            f" remaining tasks and zero concurrency"
        )

    @staticmethod
    async def test_jitter_precision(limiter):
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


# ---------------------------------------------------------------------------
# Unified observability tests
# ---------------------------------------------------------------------------


@pytest.mark.observability
class SmartJitterObservabilityTests:
    """Observability tests for the adaptive jitter implementation.

    Subclasses must provide the same ``limiter`` fixture as ``SmartJitterTests``.
    """

    @staticmethod
    async def test_custom_jitter_emits_debug_log(limiter, caplog):
        """Verify that ``_calculate_smart_jitter()`` emits a
        debug log with the input parameters and computed jitter.
        """
        # Arrange
        limiter.window = 10
        limiter.jitter_min_pct = 0.01
        limiter.jitter_max_pct = 0.05

        # Act
        with (
            caplog.at_level(logging.DEBUG, logger="redis_rate_limiter.core.limiters"),
            patch(
                "redis_rate_limiter.core.limiters.random.random",
                return_value=0.5,
            ),
        ):
            limiter._calculate_smart_jitter(
                remaining_tasks=100,
                remaining_tokens=0,
                active_concurrency=3,
            )

        # Assert
        assert_log_emitted(
            caplog.records,
            "DEBUG",
            [
                f"limiter={limiter.id}",
                "remaining_tasks=100",
                "remaining_tokens=0",
                "active_concurrency=3",
                "load_pressure=1.000",
                "concurrency_pressure=0.600",
                "jitter_s=0.283",
            ],
            "should emit a debug log containing the limiter id,"
            " input parameters, computed pressures, and jitter",
        )


# ---------------------------------------------------------------------------
# Concrete test cases
# ---------------------------------------------------------------------------


@pytest.mark.behavior
class TestSyncSmartJitter(SmartJitterTests, SmartJitterObservabilityTests):
    """Sync rate limiter smart jitter exercised through the async adapter."""

    @pytest.fixture
    def limiter(self, redis_client, limiter_id):
        """Create a sync limiter wrapped in the async adapter."""
        _limiter = StubRateLimiter(
            redis_client=redis_client,
            limiter_id=f"{limiter_id}_jitter",
            limit=10,
            window=1,
            max_concurrency=5,
        )
        yield SyncToAsyncLimiterAdapter(_limiter)
        _limiter.shutdown()


@pytest.mark.behavior
class TestAsyncSmartJitter(SmartJitterTests, SmartJitterObservabilityTests):
    """Async rate limiter smart jitter exercised natively."""

    @pytest.fixture
    async def limiter(self, async_redis_client, limiter_id):
        """Create a native async limiter."""
        lim = AsyncStubRateLimiter(
            redis_client=async_redis_client,
            limiter_id=f"{limiter_id}_jitter",
            limit=10,
            window=1,
            max_concurrency=5,
        )
        await lim.start()
        yield lim
        await lim.shutdown()
