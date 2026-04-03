"""Tests for the ``renew.lua`` Lua script.

Calls ``redis.eval()`` directly with controlled Redis state to verify
lease renewal behavior, return values, and side effects.

Fixture dependencies:
    - ``redis_client``: from ``tests/conftest.py``.
    - ``concurrency_key``: from ``tests/lua/conftest.py``.
"""

import pytest

from tests.lua.conftest import LEASE_DURATION, RENEW_SOURCE, get_redis_timestamp


def _eval_renew(redis_client, concurrency_key, task_id, lease_duration=LEASE_DURATION):
    """Invoke ``renew.lua`` via ``eval()`` with the given parameters."""
    return redis_client.eval(RENEW_SOURCE, 1, concurrency_key, task_id, lease_duration)


@pytest.mark.behavior
class TestRenew:
    """Tests for the ``renew.lua`` lease renewal script."""

    @staticmethod
    def test_returns_one_when_task_exists(redis_client, concurrency_key):
        """Verify that renewing an existing task returns 1."""
        # Arrange
        now = get_redis_timestamp(redis_client)
        redis_client.zadd(concurrency_key, {"task-1": now + 10})

        # Act
        result = _eval_renew(redis_client, concurrency_key, "task-1")

        # Assert
        assert result == 1, "renew should return 1 when task exists"

    @staticmethod
    def test_returns_zero_when_task_not_found(redis_client, concurrency_key):
        """Verify that renewing a non-existent task returns 0."""
        # Act
        result = _eval_renew(redis_client, concurrency_key, "nonexistent-task")

        # Assert
        assert result == 0, "renew should return 0 when task is not found"

    @staticmethod
    def test_updates_score_to_new_expiry(redis_client, concurrency_key):
        """Verify that renewal updates the ZSCORE to a new, later expiry."""
        # Arrange
        now = get_redis_timestamp(redis_client)
        original_score = now + 10
        redis_client.zadd(concurrency_key, {"task-1": original_score})

        # Act
        _eval_renew(redis_client, concurrency_key, "task-1")

        # Assert
        new_score = redis_client.zscore(concurrency_key, "task-1")
        assert new_score > original_score, (
            "renewed score should be greater than the original score"
        )

    @staticmethod
    def test_does_not_add_new_member_on_missing_task(redis_client, concurrency_key):
        """Verify that renewing a missing task does not add it
        to the concurrency set."""
        # Act
        _eval_renew(redis_client, concurrency_key, "nonexistent-task")

        # Assert
        assert redis_client.zcard(concurrency_key) == 0, (
            "concurrency set should remain empty after renew on missing task"
        )

    @staticmethod
    def test_preserves_other_members(redis_client, concurrency_key):
        """Verify that renewing one member does not affect other members."""
        # Arrange
        now = get_redis_timestamp(redis_client)
        redis_client.zadd(concurrency_key, {"task-1": now + 10, "task-2": now + 20})
        original_task2_score = redis_client.zscore(concurrency_key, "task-2")

        # Act
        _eval_renew(redis_client, concurrency_key, "task-1")

        # Assert
        assert redis_client.zscore(concurrency_key, "task-2") == original_task2_score, (
            "other members' scores should be unchanged after"
            " renewing a different member"
        )
