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

from tests.helpers.utils import property_test_cleanup

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def property_limiter(make_property_limiter):
    """Provide a module-scoped rate limiter for concurrency-invariant property tests."""
    return make_property_limiter(
        "concurrency", limit=10_000, window=60, max_concurrency=3
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
        with property_test_cleanup(
            property_redis_client, property_limiter
        ) as scheduled_ids:
            next_payload_id = 0
            active_task_ids: set[str] = set()

            # Act & Assert
            for operation in operations:
                if operation == "schedule":
                    _success, task_id = property_limiter.schedule_task(
                        "myapp.tasks.work",
                        {"payload_id": next_payload_id},
                    )
                    scheduled_ids.append(task_id)
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
