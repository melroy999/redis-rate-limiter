"""Tests for the ``health.lua`` Lua script.

Calls ``redis.eval()`` directly with controlled Redis state to verify
return value structure, field accuracy, and the read-only guarantee.

Fixture dependencies:
    - ``redis_client``: from ``tests/conftest.py``.
    - ``base_key``, ``buffer_key``, ``concurrency_key``: from ``tests/lua/conftest.py``.
"""

import pytest

from tests.lua.conftest import (
    HEALTH_SOURCE,
    WINDOW_SIZE,
    get_redis_timestamp,
    get_window_keys,
)


def _eval_health(
    redis_client, base_key, buffer_key, concurrency_key, window_size=WINDOW_SIZE
):
    """Invoke ``health.lua`` via ``eval()`` with the given parameters."""
    return redis_client.eval(
        HEALTH_SOURCE, 3, base_key, buffer_key, concurrency_key, window_size
    )


@pytest.mark.behavior
class TestHealthReturnValues:
    """Tests for the ``health.lua`` return value structure and field accuracy."""

    @staticmethod
    def test_returns_six_element_array(
        redis_client, base_key, buffer_key, concurrency_key
    ):
        """Verify that ``health.lua`` returns a 6-element array."""
        # Act
        result = _eval_health(redis_client, base_key, buffer_key, concurrency_key)

        # Assert
        assert len(result) == 6, "health should return a 6-element array"

    @staticmethod
    def test_clean_state_returns_all_zeros(
        redis_client, base_key, buffer_key, concurrency_key
    ):
        """Verify that all counters are zero when no keys exist."""
        # Act
        result = _eval_health(redis_client, base_key, buffer_key, concurrency_key)

        # Assert
        assert result[0] == 0, "previous_count should be 0 on clean state"
        assert result[1] == 0, "current_count should be 0 on clean state"

        # estimated_count is returned as a Lua number; Redis converts it to a string.
        assert float(result[2]) == 0.0, "estimated_count should be 0.0 on clean state"
        assert result[3] == 0, "active_now should be 0 on clean state"

        # reset_in_ms may be 0 or positive depending on window position.
        assert result[4] >= 0, "reset_in_ms should be non-negative"
        assert result[5] == 0, "buffer_count should be 0 on clean state"

    @staticmethod
    def test_previous_count_reflects_previous_window_key(
        redis_client, base_key, buffer_key, concurrency_key
    ):
        """Verify that ``result[0]`` reflects the previous window counter value."""
        # Arrange
        _, previous_key = get_window_keys(redis_client, base_key)
        redis_client.set(previous_key, "7")

        # Act
        result = _eval_health(redis_client, base_key, buffer_key, concurrency_key)

        # Assert
        assert result[0] == 7, "previous_count should reflect the previous window key"

    @staticmethod
    def test_current_count_reflects_current_window_key(
        redis_client, base_key, buffer_key, concurrency_key
    ):
        """Verify that ``result[1]`` reflects the current window counter value."""
        # Arrange
        current_key, _ = get_window_keys(redis_client, base_key)
        redis_client.set(current_key, "3")

        # Act
        result = _eval_health(redis_client, base_key, buffer_key, concurrency_key)

        # Assert
        assert result[1] == 3, "current_count should reflect the current window key"

    @staticmethod
    def test_estimated_count_is_bounded_by_window_counts(
        redis_client, base_key, buffer_key, concurrency_key
    ):
        """Verify that ``estimated_count`` is bounded between ``current_count``
        and ``current_count + previous_count`` when both windows have
        non-zero values."""
        # Arrange
        current_key, previous_key = get_window_keys(redis_client, base_key)
        redis_client.set(current_key, "3")
        redis_client.set(previous_key, "7")

        # Act
        result = _eval_health(redis_client, base_key, buffer_key, concurrency_key)

        # Assert
        assert float(result[2]) >= 3, (
            "estimated_count should be at least the current window count"
        )
        assert float(result[2]) <= 10, (
            "estimated_count should not exceed the sum of current and previous window counts"
        )
        assert float(result[2]) > 3, (
            "estimated_count should include a non-zero contribution from the previous window"
        )

    @staticmethod
    def test_active_count_reflects_concurrency_set(
        redis_client, base_key, buffer_key, concurrency_key
    ):
        """Verify that ``result[3]`` reflects the concurrency sorted set cardinality."""
        # Arrange
        now = get_redis_timestamp(redis_client)
        for i in range(3):
            redis_client.zadd(concurrency_key, {f"task-{i}": now + 3600})

        # Act
        result = _eval_health(redis_client, base_key, buffer_key, concurrency_key)

        # Assert
        assert result[3] == 3, "active_now should reflect concurrency set cardinality"

    @staticmethod
    def test_buffer_count_reflects_buffer_sorted_set(
        redis_client, base_key, buffer_key, concurrency_key, build_task_json
    ):
        """Verify that ``result[5]`` reflects the buffer sorted set cardinality."""
        # Arrange
        for i in range(2):
            task_json = build_task_json(f"task-{i}")
            redis_client.zadd(buffer_key, {task_json: i})

        # Act
        result = _eval_health(redis_client, base_key, buffer_key, concurrency_key)

        # Assert
        assert result[5] == 2, "buffer_count should reflect buffer set cardinality"


@pytest.mark.behavior
class TestHealthReadOnly:
    """Tests verifying that ``health.lua`` does not modify Redis state."""

    @staticmethod
    def test_does_not_modify_redis_state(
        redis_client, base_key, buffer_key, concurrency_key, build_task_json
    ):
        """Verify that ``health.lua`` is purely read-only.

        Takes a snapshot of all relevant Redis state before and after the
        script execution and asserts they are identical.
        """
        # Arrange
        now = get_redis_timestamp(redis_client)
        current_key, previous_key = get_window_keys(redis_client, base_key)
        redis_client.set(current_key, "5")
        redis_client.set(previous_key, "3")
        redis_client.zadd(concurrency_key, {"task-a": now + 3600})
        task_json = build_task_json("task-b")
        redis_client.zadd(buffer_key, {task_json: 0})

        # Snapshot before.
        snap_current = redis_client.get(current_key)
        snap_previous = redis_client.get(previous_key)
        snap_concurrency = redis_client.zrange(concurrency_key, 0, -1, withscores=True)
        snap_buffer = redis_client.zrange(buffer_key, 0, -1, withscores=True)

        # Act
        _eval_health(redis_client, base_key, buffer_key, concurrency_key)

        # Assert
        assert redis_client.get(current_key) == snap_current, (
            "current window counter should be unchanged"
        )
        assert redis_client.get(previous_key) == snap_previous, (
            "previous window counter should be unchanged"
        )
        assert (
            redis_client.zrange(concurrency_key, 0, -1, withscores=True)
            == snap_concurrency
        ), "concurrency set should be unchanged"
        assert redis_client.zrange(buffer_key, 0, -1, withscores=True) == snap_buffer, (
            "buffer set should be unchanged"
        )
