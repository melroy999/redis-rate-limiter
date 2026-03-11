"""Property-based tests for concurrency invariants.

These tests employ Hypothesis to verify that the concurrency bound is never
exceeded under arbitrary sequences of schedule, consume, and complete operations.

Fixture dependencies:
    - ``property_redis_client``, ``module_limiter_id``: from ``tests/conftest.py``
      (via ``tests/properties/conftest.py``).
"""

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from tests.implementations.conftest import StubRateLimiter


def _clear_limiter_keys(redis_client, limiter):
    """Delete all Redis keys belonging to the given limiter instance."""
    keys = redis_client.keys(f"{limiter.id}:*")
    if keys:
        redis_client.delete(*keys)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def property_limiter(
    property_redis_client,
    module_limiter_id,
):
    """Provide a module-scoped rate limiter for concurrency-invariant property tests."""
    return StubRateLimiter(
        redis_client=property_redis_client,
        limiter_id=f"{module_limiter_id}_property_concurrency",
        limit=10_000,
        window=60,
        max_concurrency=3,
        max_age=3600,
        lease_duration=30,
    )


# ---------------------------------------------------------------------------
# Concrete test cases
# ---------------------------------------------------------------------------


@pytest.mark.behavior
class TestConcurrencyInvariantProperties:
    """Property-based tests verifying that the concurrency bound is never violated."""

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
        """Property: the active concurrency never exceeds
        the configured max_concurrency."""
        # Arrange
        _clear_limiter_keys(property_redis_client, property_limiter)
        next_payload_id = 0
        active_task_ids = set()

        # Act & Assert
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
                    "consume should never report"
                    " active_concurrency above max_concurrency"
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
        _clear_limiter_keys(property_redis_client, property_limiter)
