"""Property-based tests for concurrency invariants."""

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from tests.implementations.conftest import MinimalRateLimiter


@pytest.fixture(scope="module")
def property_redis_client(_redis_connection):
    """Module-scoped Redis client for property-based tests."""
    yield _redis_connection
    _redis_connection.flushdb()


@pytest.fixture(scope="module")
def property_limiter(
    property_redis_client,
    default_module_limiter_id,
):
    """Module-scoped limiter for concurrency-invariant property tests."""
    return MinimalRateLimiter(
        redis_client=property_redis_client,
        limiter_id=f"{default_module_limiter_id}_property_concurrency",
        limit=10_000,
        window=60,
        max_concurrency=3,
        max_age=3600,
        lease_duration=30,
    )


class TestConcurrencyInvariantProperties:
    """Property-based tests for concurrency bound behavior."""

    @staticmethod
    @given(
        operations=st.lists(
            st.sampled_from(["schedule", "consume", "complete"]),
            min_size=1,
            max_size=80,
        )
    )
    @settings(
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    def test_active_concurrency_never_exceeds_max(
        property_limiter, property_redis_client, operations
    ):
        """Property: active concurrency never exceeds configured max_concurrency."""
        # Arrange
        property_redis_client.flushdb()
        next_payload_id = 0
        active_task_ids = set()

        # Act
        for operation in operations:
            if operation == "schedule":
                property_limiter.schedule_task(
                    "myapp.tasks.work",
                    {"payload_id": next_payload_id},
                )
                next_payload_id += 1
            elif operation == "consume":
                result = property_limiter.consume()
                if result["success"] and result["task"] is not None:
                    active_task_ids.add(result["task"]["id"])
                assert (
                    result["active_concurrency"] <= property_limiter.max_concurrency
                ), (
                    "consume should never report active_concurrency above max_concurrency"
                )
            else:
                if active_task_ids:
                    task_id = active_task_ids.pop()
                    property_redis_client.zrem(
                        property_limiter.concurrency_key, task_id
                    )
                    property_redis_client.delete(
                        property_limiter.get_inflight_key(task_id)
                    )

            assert (
                property_redis_client.zcard(property_limiter.concurrency_key)
                <= property_limiter.max_concurrency
            ), "concurrency set cardinality must never exceed max_concurrency"

        # Cleanup
        property_redis_client.flushdb()
