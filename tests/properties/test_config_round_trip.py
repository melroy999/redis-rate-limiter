"""Property-based tests for the configuration persist/hydrate round-trip invariants.

These tests verify that the ``_build_persist_config`` to JSON to
``_apply_config_overrides`` cycle preserves all configuration fields.
The persist/apply chain is the mechanism
by which the ``ManagedRateLimiter`` class API synchronizes configuration between
multiple worker processes via Redis.

The round-trip involves two layers of cooperative inheritance:

- ``AbstractRateLimiter._build_persist_config()`` returns ``{limit, window}``.
- ``DistributedRateLimiterMixin._build_persist_config()`` extends with
  ``{max_concurrency, max_age, lease_duration}``.
- ``AbstractRateLimiter._apply_config_overrides()`` applies ``limit`` and ``window``.
- ``DistributedRateLimiterMixin._apply_config_overrides()`` applies the three
  distributed-specific fields.

Fixture dependencies:
    - ``property_redis_client``, ``module_limiter_id``: from ``tests/conftest.py``
      (via ``tests/properties/conftest.py``).
"""

import json

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from tests.implementations.conftest import MinimalRateLimiter

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def property_limiter(property_redis_client, module_limiter_id):
    """Provide a module-scoped rate limiter for config round-trip property tests."""
    return MinimalRateLimiter(
        redis_client=property_redis_client,
        limiter_id=f"{module_limiter_id}_property_config_round_trip",
        limit=10,
        window=1.0,
        max_concurrency=5,
        max_age=3600,
        lease_duration=30,
    )


# ---------------------------------------------------------------------------
# Concrete test cases
# ---------------------------------------------------------------------------


class TestConfigRoundTripProperties:
    """Property-based tests for the configuration persist/apply round-trip.

    The ``_build_persist_config`` method produces a dictionary of all
    configuration fields. The ``_apply_config_overrides`` method applies
    such a dictionary back to the limiter instance. Together, they form the
    serialization/deserialization boundary used by the managed class API.
    """

    @staticmethod
    @given(
        limit=st.integers(min_value=1, max_value=10000),
        window=st.floats(
            min_value=0.01,
            max_value=3600.0,
            allow_nan=False,
            allow_infinity=False,
        ),
        max_concurrency=st.integers(min_value=1, max_value=1000),
        max_age=st.integers(min_value=1, max_value=86400),
        lease_duration=st.integers(min_value=1, max_value=3600),
    )
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_persist_then_apply_preserves_all_fields(
        property_limiter,
        limit,
        window,
        max_concurrency,
        max_age,
        lease_duration,
    ):
        """Property: all persisted fields survive a JSON round-trip via build/apply."""
        # Arrange
        # Set the limiter fields to the generated values.
        property_limiter.limit = limit
        property_limiter.window = window
        property_limiter.max_concurrency = max_concurrency
        property_limiter.max_age = max_age
        property_limiter.lease_duration = lease_duration

        # Act
        # Build the config, serialize to JSON (as Redis stores it), and parse back.
        config = property_limiter._build_persist_config()
        serialized = json.dumps(config)
        deserialized = json.loads(serialized)

        # Mutate the limiter to different values so the apply is not a no-op.
        property_limiter.limit = 1
        property_limiter.window = window
        property_limiter.max_concurrency = 1
        property_limiter.max_age = 1
        property_limiter.lease_duration = 1

        # Apply the deserialized config back.
        property_limiter._apply_config_overrides(deserialized)

        # Assert
        assert property_limiter.limit == limit, (
            f"limit should survive round-trip: "
            f"expected {limit}, got {property_limiter.limit}"
        )
        assert property_limiter.window == pytest.approx(window), (
            f"window should survive round-trip: "
            f"expected {window}, got {property_limiter.window}"
        )
        assert property_limiter.max_concurrency == max_concurrency, (
            f"max_concurrency should survive round-trip: expected {max_concurrency}, "
            f"got {property_limiter.max_concurrency}"
        )
        assert property_limiter.max_age == max_age, (
            f"max_age should survive round-trip: "
            f"expected {max_age}, "
            f"got {property_limiter.max_age}"
        )
        assert property_limiter.lease_duration == lease_duration, (
            f"lease_duration should survive round-trip: expected {lease_duration}, "
            f"got {property_limiter.lease_duration}"
        )

    @staticmethod
    @given(
        limit=st.integers(min_value=1, max_value=10000),
        window=st.floats(
            min_value=0.01,
            max_value=3600.0,
            allow_nan=False,
            allow_infinity=False,
        ),
        max_concurrency=st.integers(min_value=1, max_value=1000),
        max_age=st.integers(min_value=1, max_value=86400),
        lease_duration=st.integers(min_value=1, max_value=3600),
        extra_key=st.text(min_size=1, max_size=20).filter(
            lambda k: (
                k
                not in {
                    "limit",
                    "window",
                    "max_concurrency",
                    "max_age",
                    "lease_duration",
                }
            )
        ),
        extra_value=st.integers(min_value=0, max_value=1000),
    )
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_extra_keys_in_overrides_are_silently_ignored(
        property_limiter,
        limit,
        window,
        max_concurrency,
        max_age,
        lease_duration,
        extra_key,
        extra_value,
    ):
        """Property: unknown keys in the overrides dictionary
        do not raise exceptions or corrupt state."""
        # Arrange
        # Set the window first so the apply does not trigger a pause.
        property_limiter.window = window
        overrides = {
            "limit": limit,
            "window": window,
            "max_concurrency": max_concurrency,
            "max_age": max_age,
            "lease_duration": lease_duration,
            extra_key: extra_value,
        }

        # Act
        property_limiter._apply_config_overrides(overrides)

        # Assert
        assert property_limiter.limit == limit, (
            f"limit should be applied despite extra key: expected {limit}, "
            f"got {property_limiter.limit}"
        )
        assert property_limiter.window == pytest.approx(window), (
            f"window should be applied despite extra key: expected {window}, "
            f"got {property_limiter.window}"
        )
        assert property_limiter.max_concurrency == max_concurrency, (
            f"max_concurrency should be applied despite extra key: "
            f"expected {max_concurrency}, got {property_limiter.max_concurrency}"
        )
