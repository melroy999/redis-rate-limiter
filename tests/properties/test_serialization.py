"""Property-based tests for payload serialization.

These tests employ Hypothesis to verify that any JSON-serializable payload
survives the round-trip through Redis storage, regardless of its structure.

Fixture dependencies:
    - ``property_redis_client``, ``module_limiter_id``: from ``tests/conftest.py``
      (via ``tests/properties/conftest.py``).
    - ``func_path``: from ``tests/conftest.py``.
"""

import json

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from tests.helpers.strategies import nested_dict
from tests.helpers.utils import dict_equals_approx
from tests.implementations.conftest import MinimalRateLimiter

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _clear_limiter_keys(redis_client, limiter):
    """Delete all Redis keys belonging to the given limiter instance."""
    keys = redis_client.keys(f"{limiter.id}:*")
    if keys:
        redis_client.delete(*keys)


@pytest.fixture(scope="module")
def property_limiter(
    property_redis_client,
    module_limiter_id,
):
    """Provide the default module-scoped rate limiter for property-based tests."""
    return MinimalRateLimiter(
        redis_client=property_redis_client,
        limiter_id=f"{module_limiter_id}_property_default",
        limit=100,
        window=60,
        max_concurrency=50,
        max_age=3600,
        lease_duration=30,
    )


# ---------------------------------------------------------------------------
# Concrete test cases
# ---------------------------------------------------------------------------


class TestSerializationProperties:
    """Property-based tests verifying payload serialization invariants through Redis."""

    @staticmethod
    @given(payload=nested_dict)
    @settings(
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    def test_json_payload_survives_redis_round_trip(
        property_limiter, property_redis_client, payload, func_path
    ):
        """Property: any JSON-serializable dict payload survives a Redis round-trip unchanged.

        This property verifies that, regardless of the dictionary structure (i.e.,
        nested dicts, lists as values, primitives, Unicode), the data is preserved
        through the following stages:
        1. JSON serialization.
        2. Storage in Redis.
        3. Retrieval from Redis.
        4. JSON deserialization.
        """
        # Arrange
        # Ensure a clean state for each example.
        _clear_limiter_keys(property_redis_client, property_limiter)

        # Act
        try:
            success, task_id = property_limiter.schedule_task(func_path, payload)

            # Assert
            # Scheduling should always succeed for valid JSON payloads
            assert success is True, (
                f"scheduling failed for valid JSON payload: {payload}\n"
                f"this indicates a bug in payload handling"
            )

            # Retrieve the task data from Redis
            all_members = property_redis_client.zrange(
                property_limiter.buffer_key, 0, -1
            )
            results = [m for m in all_members if f'"{task_id}"' in m]

            # Assert that the task was stored
            assert len(results) > 0, "task should be found in buffer"
            task_data_str = results[0]
            task_data = json.loads(task_data_str)

            # Extract the payload from the stored task data.
            # The limiter wraps payloads with metadata: {"data": ..., "meta": {...}}
            stored_enhanced_payload = task_data.get("payload")
            if (
                isinstance(stored_enhanced_payload, dict)
                and "data" in stored_enhanced_payload
            ):
                # Extract only the data portion
                stored_payload = stored_enhanced_payload["data"]
            else:
                # Fallback for non-enhanced payloads
                stored_payload = stored_enhanced_payload

            # The payload should survive the round-trip intact.
            # Approximate equality is used for floats to account for JSON precision limits.
            assert dict_equals_approx(stored_payload, payload), (
                f"payload mismatch after round-trip\n"
                f"original: {payload}\n"
                f"retrieved: {stored_payload}"
            )

        finally:
            # Cleanup.
            _clear_limiter_keys(property_redis_client, property_limiter)

    @staticmethod
    @given(
        payload=st.dictionaries(
            st.text(min_size=1, max_size=50),
            st.one_of(st.integers(), st.text(max_size=100), st.booleans()),
            min_size=1,
            max_size=20,
        )
    )
    @settings(
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    def test_nonempty_dict_payloads_are_schedulable(
        property_limiter, property_redis_client, payload, func_path
    ):
        """Property: any non-empty dictionary payload can be scheduled successfully.

        This verifies that the rate limiter does not reject valid dictionary payloads.
        """
        # Arrange
        _clear_limiter_keys(property_redis_client, property_limiter)

        # Act
        success, task_id = property_limiter.schedule_task(func_path, payload)

        # Assert
        assert success is True, f"scheduling should succeed for payload: {payload}"
        assert len(task_id) > 0, "task ID should not be empty"
        assert property_redis_client.exists(
            property_limiter.get_inflight_key(task_id)
        ), "task should be marked as in-flight"

        # Cleanup
        _clear_limiter_keys(property_redis_client, property_limiter)

    @staticmethod
    @given(payload=nested_dict)
    @settings(
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    def test_task_signature_is_deterministic(payload):
        """Property: repeated signature generation for the same payload is deterministic."""
        # Act
        signature_1 = MinimalRateLimiter._get_task_signature_str(
            "myapp.tasks.process", payload
        )
        signature_2 = MinimalRateLimiter._get_task_signature_str(
            "myapp.tasks.process", payload
        )

        # Assert
        assert signature_1 == signature_2, (
            "task signature must be stable across repeated calls"
        )
