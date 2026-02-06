"""Property-based tests for payload serialization.

These tests use Hypothesis to verify that ANY JSON-serializable payload
survives the round-trip through Redis storage, regardless of structure.
"""

import json

import pytest
from hypothesis import HealthCheck, given, settings, strategies as st

from celery_rate_limiter.limiters import CeleryRateLimiter
from helpers.strategies import nested_dict
from helpers.utils import dict_equals_approx


@pytest.fixture(scope="module")
def property_redis_client(_redis_connection):
    """Module-scoped Redis client for property-based tests (performance)."""
    yield _redis_connection
    _redis_connection.flushdb()


@pytest.fixture(scope="module")
def property_celery_app(celery_config):
    """Module-scoped Celery app for property-based tests."""
    from celery import Celery

    app = Celery("test_app")
    app.config_from_object(celery_config)
    return app


@pytest.fixture(scope="module")
def property_limiter(property_redis_client, property_celery_app):
    """Module-scoped limiter for property-based tests."""
    return CeleryRateLimiter(
        redis_client=property_redis_client,
        celery_app=property_celery_app,
        limiter_id="property_test_limiter",
        limit=100,
        window=60,
        max_concurrency=50,
        max_age=3600,
        lease_duration=30,
    )


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
            if isinstance(stored_enhanced_payload, dict) and "data" in stored_enhanced_payload:
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
            property_limiter.get_active_key(task_id)
        ), "task should be marked as active"

        # Cleanup
        property_redis_client.flushdb()
