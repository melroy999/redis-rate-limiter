"""Property-based tests for payload serialization.

These tests use Hypothesis to verify that ANY JSON-serializable payload
survives the round-trip through Redis storage, regardless of structure.
"""

import json

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from celery_rate_limiter.limiters import CeleryRateLimiter
from tests.helpers.strategies import nested_dict
from tests.helpers.utils import dict_equals_approx


class TestSerializationProperties:
    """Property-based tests for payload serialization invariants."""

    @given(payload=nested_dict)
    @settings(
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    def test_json_payload_survives_redis_round_trip(
        self, property_limiter, property_redis_client, payload, func_path
    ):
        """Property: any JSON-serializable dict payload survives Redis round-trip unchanged.

        This property verifies that regardless of dict structure (nested dicts,
        lists as values, primitives, Unicode, etc.), the data is preserved through:
        1. JSON serialization.
        2. Storage in Redis.
        3. Retrieval from Redis.
        4. JSON deserialization.
        """
        # Arrange
        # Clean state for each example.
        property_redis_client.flushdb()

        # Act
        try:
            success, task_id = property_limiter.schedule_task(func_path, payload)

            # Assert
            # Scheduling should always succeed for valid JSON payloads.
            assert success is True, (
                f"scheduling failed for valid JSON payload: {payload}\n"
                f"this indicates a bug in payload handling"
            )

            # Retrieve the task data from Redis.
            _, results = property_redis_client.zscan(
                property_limiter.buffer_key, match=f'*"{task_id}"*'
            )

            # Assert task was stored.
            assert len(results) > 0, "task should be found in buffer"
            task_data_str, score = results[0]
            task_data = json.loads(task_data_str)

            # Extract the payload from the stored task data.
            # The limiter wraps payloads with metadata: {'data': ..., 'meta': {...}}
            stored_enhanced_payload = task_data.get("payload")
            if (
                isinstance(stored_enhanced_payload, dict)
                and "data" in stored_enhanced_payload
            ):
                # Extract just the data portion.
                stored_payload = stored_enhanced_payload["data"]
            else:
                # Fallback for non-enhanced payloads.
                stored_payload = stored_enhanced_payload

            # Property: the payload should survive the round-trip.
            # Use approximate equality for floats to handle JSON precision limits.
            assert dict_equals_approx(stored_payload, payload), (
                f"payload mismatch after round-trip\n"
                f"original: {payload}\n"
                f"retrieved: {stored_payload}"
            )

        finally:
            # Cleanup.
            property_redis_client.flushdb()

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
        self, property_limiter, property_redis_client, payload, func_path
    ):
        """Property: any non-empty dictionary payload can be successfully scheduled.

        This verifies that the rate limiter doesn't reject valid dictionary payloads.
        """
        # Arrange
        property_redis_client.flushdb()

        # Act
        success, task_id = property_limiter.schedule_task(func_path, payload)

        # Assert
        assert success is True, f"scheduling should succeed for payload: {payload}"
        assert len(task_id) > 0, "task ID should not be empty"
        assert property_redis_client.exists(
            property_limiter.get_inflight_key(task_id)
        ), "task should be marked as in-flight"

        # Cleanup
        property_redis_client.flushdb()

    @given(payload=nested_dict)
    @settings(
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    def test_task_signature_is_deterministic(self, payload):
        """Property: repeated signature generation for same payload is deterministic."""
        # Act
        signature_1 = CeleryRateLimiter._get_task_signature_str(
            "myapp.tasks.process", payload
        )
        signature_2 = CeleryRateLimiter._get_task_signature_str(
            "myapp.tasks.process", payload
        )

        # Assert
        assert signature_1 == signature_2, (
            "task signature must be stable across repeated calls"
        )
