"""Property-based tests for the in-flight TTL calculation invariants.

These tests complement the deterministic tests located in
``tests/implementations/test_task_data_helpers.py::TestInflightTtl``.

The ``_get_inflight_ttl()`` method computes a conservative TTL for in-flight
deduplication keys using the formula::

    ttl = ceil(max(1, max_age) + max(1, lease_duration) + max(1, window))

The property tests verify that the formula satisfies its structural invariants
(lower bound, monotonicity, integer type) across the full input space.

Fixture dependencies:
    - ``property_redis_client``, ``module_limiter_id``: from ``tests/conftest.py``
      (via ``tests/properties/conftest.py``).
"""

import pytest
from hypothesis import HealthCheck, assume, given, settings
from hypothesis import strategies as st

from tests.implementations.conftest import MinimalRateLimiter

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def property_limiter(property_redis_client, module_limiter_id):
    """Provide a module-scoped rate limiter for in-flight TTL property tests."""
    return MinimalRateLimiter(
        redis_client=property_redis_client,
        limiter_id=f"{module_limiter_id}_property_inflight_ttl",
        limit=10,
        window=1.0,
        max_concurrency=5,
        max_age=3600,
        lease_duration=30,
    )


# ---------------------------------------------------------------------------
# Concrete test cases
# ---------------------------------------------------------------------------


class TestInflightTtlProperties:
    """Property-based tests for the ``_get_inflight_ttl`` calculation.

    The formula applies ``max(1.0, x)`` to each of three parameters before
    summing and ceiling. These tests verify the structural invariants that
    follow from this design.
    """

    @staticmethod
    @given(
        window=st.floats(
            min_value=0.0,
            max_value=10000.0,
            allow_nan=False,
            allow_infinity=False,
        ),
        lease_duration=st.integers(min_value=0, max_value=10000),
        max_age=st.integers(min_value=0, max_value=10000),
        max_age_override=st.one_of(
            st.none(), st.integers(min_value=0, max_value=10000)
        ),
    )
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_ttl_lower_bound_is_three(
        property_limiter, window, lease_duration, max_age, max_age_override
    ):
        """Property: the TTL is always at least 3, given the max(1.0, ...) floor on each component."""
        # Arrange
        property_limiter.window = window
        property_limiter.lease_duration = lease_duration
        property_limiter.max_age = max_age

        # Act
        ttl = property_limiter._get_inflight_ttl(max_age_override=max_age_override)

        # Assert
        assert ttl >= 3, (
            f"TTL should be >= 3 (three components each floored to 1.0), "
            f"got {ttl} with window={window}, lease_duration={lease_duration}, "
            f"max_age={max_age}, override={max_age_override}"
        )

    @staticmethod
    @given(
        window=st.floats(
            min_value=0.0,
            max_value=10000.0,
            allow_nan=False,
            allow_infinity=False,
        ),
        lease_duration=st.integers(min_value=0, max_value=10000),
        max_age_a=st.integers(min_value=0, max_value=10000),
        max_age_b=st.integers(min_value=0, max_value=10000),
    )
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_ttl_is_monotonic_in_max_age(
        property_limiter, window, lease_duration, max_age_a, max_age_b
    ):
        """Property: increasing max_age yields a TTL that is greater than or equal to the original."""
        # Arrange
        assume(max_age_a <= max_age_b)
        property_limiter.window = window
        property_limiter.lease_duration = lease_duration

        # Act
        property_limiter.max_age = max_age_a
        ttl_a = property_limiter._get_inflight_ttl()
        property_limiter.max_age = max_age_b
        ttl_b = property_limiter._get_inflight_ttl()

        # Assert
        assert ttl_a <= ttl_b, (
            f"TTL should be monotonic in max_age: ttl({max_age_a})={ttl_a} > ttl({max_age_b})={ttl_b}"
        )

    @staticmethod
    @given(
        window=st.floats(
            min_value=0.0,
            max_value=10000.0,
            allow_nan=False,
            allow_infinity=False,
        ),
        max_age=st.integers(min_value=0, max_value=10000),
        lease_a=st.integers(min_value=0, max_value=10000),
        lease_b=st.integers(min_value=0, max_value=10000),
    )
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_ttl_is_monotonic_in_lease_duration(
        property_limiter, window, max_age, lease_a, lease_b
    ):
        """Property: increasing lease_duration yields a TTL that is greater than or equal to the original."""
        # Arrange
        assume(lease_a <= lease_b)
        property_limiter.window = window
        property_limiter.max_age = max_age

        # Act
        property_limiter.lease_duration = lease_a
        ttl_a = property_limiter._get_inflight_ttl()
        property_limiter.lease_duration = lease_b
        ttl_b = property_limiter._get_inflight_ttl()

        # Assert
        assert ttl_a <= ttl_b, (
            f"TTL should be monotonic in lease_duration: ttl({lease_a})={ttl_a} > ttl({lease_b})={ttl_b}"
        )

    @staticmethod
    @given(
        max_age=st.integers(min_value=0, max_value=10000),
        lease_duration=st.integers(min_value=0, max_value=10000),
        window_a=st.floats(
            min_value=0.0,
            max_value=10000.0,
            allow_nan=False,
            allow_infinity=False,
        ),
        window_b=st.floats(
            min_value=0.0,
            max_value=10000.0,
            allow_nan=False,
            allow_infinity=False,
        ),
    )
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_ttl_is_monotonic_in_window(
        property_limiter, max_age, lease_duration, window_a, window_b
    ):
        """Property: increasing window yields a TTL that is greater than or equal to the original."""
        # Arrange
        assume(window_a <= window_b)
        property_limiter.max_age = max_age
        property_limiter.lease_duration = lease_duration

        # Act
        property_limiter.window = window_a
        ttl_a = property_limiter._get_inflight_ttl()
        property_limiter.window = window_b
        ttl_b = property_limiter._get_inflight_ttl()

        # Assert
        assert ttl_a <= ttl_b, (
            f"TTL should be monotonic in window: ttl({window_a})={ttl_a} > ttl({window_b})={ttl_b}"
        )

    @staticmethod
    @given(
        window=st.floats(
            min_value=0.0,
            max_value=10000.0,
            allow_nan=False,
            allow_infinity=False,
        ),
        lease_duration=st.integers(min_value=0, max_value=10000),
        max_age=st.integers(min_value=0, max_value=10000),
    )
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_ttl_is_always_an_integer(
        property_limiter, window, lease_duration, max_age
    ):
        """Property: the TTL is always of type ``int``, as required by the Redis EX option."""
        # Arrange
        property_limiter.window = window
        property_limiter.lease_duration = lease_duration
        property_limiter.max_age = max_age

        # Act
        ttl = property_limiter._get_inflight_ttl()

        # Assert
        assert isinstance(ttl, int), (
            f"TTL should be an integer, got {type(ttl).__name__} with value {ttl}"
        )
