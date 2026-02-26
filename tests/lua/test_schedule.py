"""Tests for the ``schedule.lua`` Lua script.

Calls ``redis.eval()`` directly with controlled Redis state to verify
task scheduling, priority ordering, and metadata injection behavior.

Fixture dependencies:
    - ``redis_client``: from ``tests/conftest.py``.
    - ``buffer_key``: from ``tests/lua/conftest.py``.
"""

import json

from tests.lua.conftest import SCHEDULE_SOURCE, build_task_json


def _eval_schedule(
    redis_client, buffer_key, task_json, priority=0, override_max_age=""
):
    """Invoke ``schedule.lua`` via ``eval()`` with the given parameters."""
    return redis_client.eval(
        SCHEDULE_SOURCE, 1, buffer_key, task_json, priority, override_max_age
    )


def _get_single_member_json(redis_client, buffer_key):
    """Retrieve and deserialize the single member from the buffer sorted set."""
    members = redis_client.zrange(buffer_key, 0, -1)
    assert len(members) == 1, "buffer should contain exactly one member"
    return json.loads(members[0])


class TestSchedule:
    """Tests for the ``schedule.lua`` task scheduling and metadata injection."""

    @staticmethod
    def test_task_added_to_buffer_sorted_set(redis_client, buffer_key):
        """Verify that scheduling a task adds it to the buffer sorted set."""
        # Arrange
        task_json = build_task_json("task-1")

        # Act
        _eval_schedule(redis_client, buffer_key, task_json)

        # Assert
        assert redis_client.zcard(buffer_key) == 1, (
            "buffer should contain one task after scheduling"
        )

    @staticmethod
    def test_priority_used_as_zadd_score(redis_client, buffer_key):
        """Verify that the provided priority is used as the ZADD score."""
        # Arrange
        task_json = build_task_json("task-1")

        # Act
        _eval_schedule(redis_client, buffer_key, task_json, priority=42)

        # Assert
        members_with_scores = redis_client.zrange(buffer_key, 0, -1, withscores=True)
        assert len(members_with_scores) == 1, "buffer should contain one member"
        _, score = members_with_scores[0]
        assert score == 42.0, "ZADD score should equal the provided priority"

    @staticmethod
    def test_meta_arrived_at_injected(redis_client, buffer_key):
        """Verify that ``__meta_arrived_at`` is injected into the stored task JSON."""
        # Arrange
        task_json = build_task_json("task-1")

        # Act
        _eval_schedule(redis_client, buffer_key, task_json)

        # Assert
        stored = _get_single_member_json(redis_client, buffer_key)
        assert "__meta_arrived_at" in stored, (
            "stored task should contain __meta_arrived_at"
        )
        assert isinstance(stored["__meta_arrived_at"], int), (
            "__meta_arrived_at should be an integer (milliseconds)"
        )
        assert stored["__meta_arrived_at"] > 0, (
            "__meta_arrived_at should be a positive timestamp"
        )

    @staticmethod
    def test_meta_max_age_injected_when_provided(redis_client, buffer_key):
        """Verify that ``__meta_max_age`` is injected when ARGV[3] is provided."""
        # Arrange
        task_json = build_task_json("task-1")

        # Act
        _eval_schedule(redis_client, buffer_key, task_json, override_max_age=300)

        # Assert
        stored = _get_single_member_json(redis_client, buffer_key)
        assert "__meta_max_age" in stored, (
            "stored task should contain __meta_max_age when provided"
        )
        assert stored["__meta_max_age"] == 300, (
            "__meta_max_age should equal the provided override value"
        )

    @staticmethod
    def test_meta_max_age_absent_when_argv3_is_empty_string(
        redis_client, buffer_key
    ):
        """Verify that ``__meta_max_age`` is absent when ARGV[3] is an empty string.

        When ``max_age`` is not provided, the Python layer passes ``""`` to
        the Lua script. ``tonumber("")`` returns ``nil``, so the conditional
        ``if override_max_age then`` is false and no field is injected.
        """
        # Arrange
        task_json = build_task_json("task-1")

        # Act
        _eval_schedule(redis_client, buffer_key, task_json, override_max_age="")

        # Assert
        stored = _get_single_member_json(redis_client, buffer_key)
        assert "__meta_max_age" not in stored, (
            "__meta_max_age should be absent when ARGV[3] is an empty string"
        )

    @staticmethod
    def test_gsub_handles_nested_closing_brace(redis_client, buffer_key):
        """Verify that ``string.gsub`` injects metadata at the correct position.

        The ``gsub`` pattern ``'}$'`` should match only the final closing
        brace of the JSON string, not a brace embedded within a string value.
        """
        # Arrange
        # Payload contains a value with a closing brace character.
        task_json = build_task_json(
            "task-1", payload={"nested": "value}with}braces"}
        )

        # Act
        _eval_schedule(redis_client, buffer_key, task_json)

        # Assert
        stored = _get_single_member_json(redis_client, buffer_key)
        assert "__meta_arrived_at" in stored, (
            "metadata should be injected even with braces in payload values"
        )
        assert stored["payload"]["nested"] == "value}with}braces", (
            "original payload values should be preserved"
        )
