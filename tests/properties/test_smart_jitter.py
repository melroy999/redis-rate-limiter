"""Property-based tests for smart jitter invariants.

These tests complement deterministic tests in
`tests/implementations/test_smart_jitter.py`.

Rationale for both layers:
1. Deterministic seeded tests give stable, reproducible regression checks.
2. Hypothesis tests broaden coverage across many generated random streams.

Using both avoids flaky CI while still validating that behavior is not an
artifact of one hand-picked random sequence.
"""

from unittest.mock import patch

import pytest
from hypothesis import HealthCheck, assume, given, settings, strategies as st

from celery_rate_limiter.limiters import CeleryRateLimiter


# Random values used by jitter calculations.
# `random.random()` yields values in [0.0, 1.0), so we exclude 1.0.
random_stream_strategy = st.lists(
    st.floats(
        min_value=0.0,
        max_value=1.0,
        allow_nan=False,
        allow_infinity=False,
        exclude_max=True,
    ),
    min_size=20,
    max_size=200,
)


@pytest.fixture(scope="module")
def property_limiter(
    property_redis_client,
    property_celery_app,
    default_module_limiter_id,
):
    """Module-scoped limiter for jitter property tests."""
    return CeleryRateLimiter(
        redis_client=property_redis_client,
        celery_app=property_celery_app,
        limiter_id=f"{default_module_limiter_id}_property_jitter",
        limit=10,
        window=1,
        max_concurrency=5,
        jitter_enabled=True,
        jitter_min_pct=0.02,
        jitter_max_pct=0.08,
    )


class TestSmartJitterProperties:
    """Property-based tests for smart jitter.

    Why this class exists even with seeded implementation tests:
    - Seeded tests prove known scenarios and remain fully reproducible.
    - These properties generate many random streams and verify invariants
      hold beyond those curated seeds.
    """

    @given(random_stream=random_stream_strategy)
    @settings(
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    def test_load_pressure_is_monotonic(self, property_limiter, random_stream):
        """Property: paired random streams preserve load-based jitter ordering."""
        # Arrange
        samples = len(random_stream)

        # Act
        # Use the exact same random values in each scenario to isolate
        # only the pressure change as the source of output differences.
        with patch(
            "celery_rate_limiter.limiters.random.random",
            side_effect=iter(random_stream),
        ):
            low_load_jitters = [
                property_limiter._calculate_smart_jitter(
                    remaining_tasks=5,
                    remaining_tokens=0,
                    active_concurrency=1,
                )
                for _ in range(samples)
            ]

        with patch(
            "celery_rate_limiter.limiters.random.random",
            side_effect=iter(random_stream),
        ):
            medium_load_jitters = [
                property_limiter._calculate_smart_jitter(
                    remaining_tasks=50,
                    remaining_tokens=0,
                    active_concurrency=3,
                )
                for _ in range(samples)
            ]

        with patch(
            "celery_rate_limiter.limiters.random.random",
            side_effect=iter(random_stream),
        ):
            high_load_jitters = [
                property_limiter._calculate_smart_jitter(
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
                zip(low_load_jitters, medium_load_jitters, high_load_jitters, strict=True)
            )
            if not (low <= medium <= high)
        ]
        first_violation = monotonicity_violations[0] if monotonicity_violations else None
        assert not monotonicity_violations, (
            f"paired jitter monotonicity violated for load pressure; "
            f"violations={len(monotonicity_violations)}, first={first_violation}, "
            f"avg_low={avg_low:.4f}, avg_medium={avg_medium:.4f}, avg_high={avg_high:.4f}"
        )

    @given(
        random_stream=random_stream_strategy,
        low_active=st.integers(min_value=0, max_value=4),
        high_active=st.integers(min_value=1, max_value=5),
    )
    @settings(
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    def test_concurrency_pressure_is_monotonic(
        self, property_limiter, random_stream, low_active, high_active
    ):
        """Property: paired random streams preserve concurrency-based jitter ordering."""
        # Arrange
        assume(low_active < high_active)
        samples = len(random_stream)

        # Act
        with patch(
            "celery_rate_limiter.limiters.random.random",
            side_effect=iter(random_stream),
        ):
            low_concurrency_jitters = [
                property_limiter._calculate_smart_jitter(
                    remaining_tasks=50,
                    remaining_tokens=0,
                    active_concurrency=low_active,
                )
                for _ in range(samples)
            ]

        with patch(
            "celery_rate_limiter.limiters.random.random",
            side_effect=iter(random_stream),
        ):
            high_concurrency_jitters = [
                property_limiter._calculate_smart_jitter(
                    remaining_tasks=50,
                    remaining_tokens=0,
                    active_concurrency=high_active,
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
        first_violation = monotonicity_violations[0] if monotonicity_violations else None
        assert not monotonicity_violations, (
            f"paired jitter monotonicity violated for concurrency pressure; "
            f"violations={len(monotonicity_violations)}, first={first_violation}, "
            f"avg_low={avg_low:.4f}, avg_high={avg_high:.4f}"
        )
