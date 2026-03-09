"""Tests for the ``consume.lua`` Lua script.

Calls ``redis.eval()`` directly with controlled Redis state to verify
return value structure, boundary decisions, self-healing, DLQ routing,
and telemetry accuracy at the Lua level.

Fixture dependencies:
    - ``redis_client``: from ``tests/conftest.py``.
    - ``base_key``, ``buffer_key``, ``concurrency_key``, ``dlq_key``: from ``tests/lua/conftest.py``.
"""

from tests.lua.conftest import (
    CONSUME_SOURCE,
    LEASE_DURATION,
    LIMIT,
    MAX_AGE,
    MAX_CONCURRENCY,
    WINDOW_SIZE,
    get_redis_timestamp,
    get_window_keys,
)


def _eval_consume(
    redis_client,
    base_key,
    buffer_key,
    concurrency_key,
    dlq_key,
    window_size=WINDOW_SIZE,
    limit=LIMIT,
    max_concurrency=MAX_CONCURRENCY,
    max_age=MAX_AGE,
    lease_duration=LEASE_DURATION,
):
    """Invoke ``consume.lua`` via ``eval()`` with the given parameters."""
    return redis_client.eval(
        CONSUME_SOURCE,
        4,
        base_key,
        buffer_key,
        concurrency_key,
        dlq_key,
        window_size,
        limit,
        max_concurrency,
        max_age,
        lease_duration,
    )


def _add_task_to_buffer(redis_client, buffer_key, task_json, priority=0):
    """Add a pre-built task JSON string to the buffer sorted set."""
    redis_client.zadd(buffer_key, {task_json: priority})


class TestConsumeReturnValues:
    """Tests for the ``consume.lua`` return value structure and field correctness."""

    @staticmethod
    def test_success_returns_eight_element_array(
        redis_client, base_key, buffer_key, concurrency_key, dlq_key, build_task_json
    ):
        """Verify that a successful consumption returns an 8-element array."""
        # Arrange
        now = get_redis_timestamp(redis_client)
        task_json = build_task_json("task-1", arrived_at_ms=now * 1000)
        _add_task_to_buffer(redis_client, buffer_key, task_json)

        # Act
        result = _eval_consume(
            redis_client, base_key, buffer_key, concurrency_key, dlq_key
        )

        # Assert
        assert len(result) == 8, "consume should return an 8-element array"

    @staticmethod
    def test_success_status_code_is_one(
        redis_client, base_key, buffer_key, concurrency_key, dlq_key, build_task_json
    ):
        """Verify that a successful consumption returns status code 1."""
        # Arrange
        now = get_redis_timestamp(redis_client)
        task_json = build_task_json("task-1", arrived_at_ms=now * 1000)
        _add_task_to_buffer(redis_client, buffer_key, task_json)

        # Act
        result = _eval_consume(
            redis_client, base_key, buffer_key, concurrency_key, dlq_key
        )

        # Assert
        assert result[0] == 1, "status code should be 1 on successful consumption"

    @staticmethod
    def test_success_returns_task_json(
        redis_client, base_key, buffer_key, concurrency_key, dlq_key, build_task_json
    ):
        """Verify that a successful consumption returns the original task JSON string."""
        # Arrange
        now = get_redis_timestamp(redis_client)
        task_json = build_task_json("task-1", arrived_at_ms=now * 1000)
        _add_task_to_buffer(redis_client, buffer_key, task_json)

        # Act
        result = _eval_consume(
            redis_client, base_key, buffer_key, concurrency_key, dlq_key
        )

        # Assert
        assert result[1] == task_json, "result[1] should be the original task JSON"

    @staticmethod
    def test_success_remaining_reflects_post_consume(
        redis_client, base_key, buffer_key, concurrency_key, dlq_key, build_task_json
    ):
        """Verify that ``remaining`` reflects the post-consume state."""
        # Arrange
        now = get_redis_timestamp(redis_client)
        task_json = build_task_json("task-1", arrived_at_ms=now * 1000)
        _add_task_to_buffer(redis_client, buffer_key, task_json)

        # Act
        result = _eval_consume(
            redis_client, base_key, buffer_key, concurrency_key, dlq_key
        )

        # Assert
        # From an empty window: remaining = limit - 0 - 1 = limit - 1.
        assert result[2] == LIMIT - 1, (
            "remaining should equal limit - 1 after first consume from empty window"
        )

    @staticmethod
    def test_success_active_concurrency_is_incremented(
        redis_client, base_key, buffer_key, concurrency_key, dlq_key, build_task_json
    ):
        """Verify that ``active_now`` is incremented after successful consumption."""
        # Arrange
        now = get_redis_timestamp(redis_client)
        task_json = build_task_json("task-1", arrived_at_ms=now * 1000)
        _add_task_to_buffer(redis_client, buffer_key, task_json)

        # Act
        result = _eval_consume(
            redis_client, base_key, buffer_key, concurrency_key, dlq_key
        )

        # Assert
        assert result[3] == 1, (
            "active_now should be 1 after first consume from empty concurrency set"
        )

    @staticmethod
    def test_success_buffer_count_is_decremented(
        redis_client, base_key, buffer_key, concurrency_key, dlq_key, build_task_json
    ):
        """Verify that ``buffer_count`` reflects the post-consume buffer size."""
        # Arrange
        now = get_redis_timestamp(redis_client)
        task1 = build_task_json("task-1", arrived_at_ms=now * 1000)
        task2 = build_task_json("task-2", arrived_at_ms=now * 1000)
        _add_task_to_buffer(redis_client, buffer_key, task1, priority=0)
        _add_task_to_buffer(redis_client, buffer_key, task2, priority=1)

        # Act
        result = _eval_consume(
            redis_client, base_key, buffer_key, concurrency_key, dlq_key
        )

        # Assert
        assert result[5] == 1, (
            "buffer_count should be 1 after consuming one of two buffered tasks"
        )

    @staticmethod
    def test_success_current_count_is_incremented(
        redis_client, base_key, buffer_key, concurrency_key, dlq_key, build_task_json
    ):
        """Verify that the current window count is incremented after consumption."""
        # Arrange
        now = get_redis_timestamp(redis_client)
        task_json = build_task_json("task-1", arrived_at_ms=now * 1000)
        _add_task_to_buffer(redis_client, buffer_key, task_json)

        # Act
        result = _eval_consume(
            redis_client, base_key, buffer_key, concurrency_key, dlq_key
        )

        # Assert
        assert result[7] == 1, (
            "current_count should be 1 after first consume from empty window"
        )

    @staticmethod
    def test_denied_status_code_is_zero(
        redis_client, base_key, buffer_key, concurrency_key, dlq_key, build_task_json
    ):
        """Verify that a denied consumption returns status code 0."""
        # Arrange
        now = get_redis_timestamp(redis_client)
        task_json = build_task_json("task-1", arrived_at_ms=now * 1000)
        _add_task_to_buffer(redis_client, buffer_key, task_json)
        current_key, _ = get_window_keys(redis_client, base_key)
        redis_client.set(current_key, str(LIMIT))

        # Act
        result = _eval_consume(
            redis_client, base_key, buffer_key, concurrency_key, dlq_key
        )

        # Assert
        assert result[0] == 0, "status code should be 0 when rate limit is reached"

    @staticmethod
    def test_denied_returns_false_for_task(
        redis_client, base_key, buffer_key, concurrency_key, dlq_key, build_task_json
    ):
        """Verify that a denied consumption returns a falsy value for the task field."""
        # Arrange
        now = get_redis_timestamp(redis_client)
        task_json = build_task_json("task-1", arrived_at_ms=now * 1000)
        _add_task_to_buffer(redis_client, buffer_key, task_json)
        current_key, _ = get_window_keys(redis_client, base_key)
        redis_client.set(current_key, str(LIMIT))

        # Act
        result = _eval_consume(
            redis_client, base_key, buffer_key, concurrency_key, dlq_key
        )

        # Assert
        assert not result[1], "task field should be falsy when consumption is denied"

    @staticmethod
    def test_empty_buffer_returns_denied(
        redis_client, base_key, buffer_key, concurrency_key, dlq_key
    ):
        """Verify that an empty buffer returns status 0 even with available capacity."""
        # Act
        # No tasks added to buffer.
        result = _eval_consume(
            redis_client, base_key, buffer_key, concurrency_key, dlq_key
        )

        # Assert
        assert result[0] == 0, (
            "status should be 0 when buffer is empty despite available capacity"
        )


class TestConsumeBoundaryDecisions:
    """Tests for ``consume.lua`` boundary conditions and branching logic."""

    @staticmethod
    def test_denies_when_estimate_equals_limit(
        redis_client, base_key, buffer_key, concurrency_key, dlq_key, build_task_json
    ):
        """Verify that consumption is denied when the estimated count equals the limit.

        The check is strictly less than (``estimated_count < rate_limit``),
        so equality should result in denial.
        """
        # Arrange
        now = get_redis_timestamp(redis_client)
        task_json = build_task_json("task-1", arrived_at_ms=now * 1000)
        _add_task_to_buffer(redis_client, buffer_key, task_json)
        current_key, _ = get_window_keys(redis_client, base_key)
        redis_client.set(current_key, str(LIMIT))

        # Act
        result = _eval_consume(
            redis_client, base_key, buffer_key, concurrency_key, dlq_key
        )

        # Assert
        assert result[0] == 0, (
            "consume should deny when estimated count equals the limit"
        )

    @staticmethod
    def test_allows_when_estimate_is_one_below_limit(
        redis_client, base_key, buffer_key, concurrency_key, dlq_key, build_task_json
    ):
        """Verify that consumption is allowed when the estimated count is one below the limit."""
        # Arrange
        now = get_redis_timestamp(redis_client)
        task_json = build_task_json("task-1", arrived_at_ms=now * 1000)
        _add_task_to_buffer(redis_client, buffer_key, task_json)
        current_key, _ = get_window_keys(redis_client, base_key)
        redis_client.set(current_key, str(LIMIT - 1))

        # Act
        result = _eval_consume(
            redis_client, base_key, buffer_key, concurrency_key, dlq_key
        )

        # Assert
        assert result[0] == 1, (
            "consume should allow when estimated count is one below the limit"
        )

    @staticmethod
    def test_limit_zero_denies_all_requests(
        redis_client, base_key, buffer_key, concurrency_key, dlq_key, build_task_json
    ):
        """Verify that ``limit=0`` denies all consumption requests."""
        # Arrange
        now = get_redis_timestamp(redis_client)
        task_json = build_task_json("task-1", arrived_at_ms=now * 1000)
        _add_task_to_buffer(redis_client, buffer_key, task_json)

        # Act
        result = _eval_consume(
            redis_client, base_key, buffer_key, concurrency_key, dlq_key, limit=0
        )

        # Assert
        assert result[0] == 0, "limit=0 should deny all consumption requests"

    @staticmethod
    def test_concurrency_at_max_denies_request(
        redis_client, base_key, buffer_key, concurrency_key, dlq_key, build_task_json
    ):
        """Verify that consumption is denied when the concurrency set is at ``max_concurrency``."""
        # Arrange
        now = get_redis_timestamp(redis_client)
        task_json = build_task_json("task-1", arrived_at_ms=now * 1000)
        _add_task_to_buffer(redis_client, buffer_key, task_json)
        # Fill the concurrency set with valid (future) leases.
        for i in range(MAX_CONCURRENCY):
            redis_client.zadd(concurrency_key, {f"active-task-{i}": now + 3600})

        # Act
        result = _eval_consume(
            redis_client, base_key, buffer_key, concurrency_key, dlq_key
        )

        # Assert
        assert result[0] == 0, (
            "consume should deny when concurrency set is at max_concurrency"
        )

    @staticmethod
    def test_concurrency_below_max_allows_request(
        redis_client, base_key, buffer_key, concurrency_key, dlq_key, build_task_json
    ):
        """Verify that consumption is allowed when the concurrency set is one below ``max_concurrency``."""
        # Arrange
        now = get_redis_timestamp(redis_client)
        task_json = build_task_json("task-1", arrived_at_ms=now * 1000)
        _add_task_to_buffer(redis_client, buffer_key, task_json)
        for i in range(MAX_CONCURRENCY - 1):
            redis_client.zadd(concurrency_key, {f"active-task-{i}": now + 3600})

        # Act
        result = _eval_consume(
            redis_client, base_key, buffer_key, concurrency_key, dlq_key
        )

        # Assert
        assert result[0] == 1, (
            "consume should allow when concurrency is one below max_concurrency"
        )

    @staticmethod
    def test_expired_task_routed_to_dlq(
        redis_client, base_key, buffer_key, concurrency_key, dlq_key, build_task_json
    ):
        """Verify that an expired task is routed to the DLQ with correct side effects.

        Expected: status -1, task present in DLQ, removed from buffer,
        inflight key deleted.
        """
        # Arrange
        now = get_redis_timestamp(redis_client)
        expired_arrived_at = (now - MAX_AGE - 10) * 1000
        inflight_key = f"{base_key}:inflight:expired-task"
        task_json = build_task_json(
            "expired-task",
            arrived_at_ms=expired_arrived_at,
            inflight_key=inflight_key,
        )
        _add_task_to_buffer(redis_client, buffer_key, task_json)
        # Pre-create the inflight key so we can verify it gets deleted.
        redis_client.set(inflight_key, "1")

        # Act
        result = _eval_consume(
            redis_client, base_key, buffer_key, concurrency_key, dlq_key
        )

        # Assert
        assert result[0] == -1, "status should be -1 for expired tasks"
        dlq_contents = redis_client.lrange(dlq_key, 0, -1)
        assert len(dlq_contents) == 1, "DLQ should contain the expired task"
        assert redis_client.zcard(buffer_key) == 0, (
            "buffer should be empty after expired task removal"
        )
        assert redis_client.exists(inflight_key) == 0, (
            "inflight key should be deleted for expired tasks"
        )

    @staticmethod
    def test_expired_task_does_not_consume_token(
        redis_client, base_key, buffer_key, concurrency_key, dlq_key, build_task_json
    ):
        """Verify that an expired task does not increment the current window counter."""
        # Arrange
        now = get_redis_timestamp(redis_client)
        expired_arrived_at = (now - MAX_AGE - 10) * 1000
        task_json = build_task_json("expired-task", arrived_at_ms=expired_arrived_at)
        _add_task_to_buffer(redis_client, buffer_key, task_json)
        current_key, _ = get_window_keys(redis_client, base_key)

        # Act
        _eval_consume(redis_client, base_key, buffer_key, concurrency_key, dlq_key)

        # Assert
        counter = redis_client.get(current_key)
        assert counter is None, (
            "current window counter should not be incremented for expired tasks"
        )

    @staticmethod
    def test_pexpire_set_on_first_increment_only(
        redis_client, base_key, buffer_key, concurrency_key, dlq_key, build_task_json
    ):
        """Verify that ``PEXPIRE`` is set on the first increment only.

        A mutation changing ``current_count == 0`` to ``current_count == 1``
        would cause the TTL to be reset on every second consume instead of
        only on the first.
        """
        # Arrange
        now = get_redis_timestamp(redis_client)
        task1 = build_task_json("task-1", arrived_at_ms=now * 1000)
        task2 = build_task_json("task-2", arrived_at_ms=now * 1000)
        _add_task_to_buffer(redis_client, buffer_key, task1, priority=0)
        _add_task_to_buffer(redis_client, buffer_key, task2, priority=1)

        # Act
        # First consume: sets PEXPIRE.
        _eval_consume(redis_client, base_key, buffer_key, concurrency_key, dlq_key)
        current_key, _ = get_window_keys(redis_client, base_key)
        pttl_after_first = redis_client.pttl(current_key)

        # Second consume: should NOT reset PEXPIRE.
        _eval_consume(redis_client, base_key, buffer_key, concurrency_key, dlq_key)
        pttl_after_second = redis_client.pttl(current_key)

        # Assert
        expected_max_pttl = 2 * WINDOW_SIZE * 1000 + 10000
        assert 0 < pttl_after_first <= expected_max_pttl, (
            "PTTL should be set after first consume"
        )
        assert pttl_after_second <= pttl_after_first, (
            "PTTL should not increase after second consume (PEXPIRE not reset)"
        )

    @staticmethod
    def test_self_healing_removes_expired_leases(
        redis_client, base_key, buffer_key, concurrency_key, dlq_key, build_task_json
    ):
        """Verify that expired leases are removed before the rate limit evaluation."""
        # Arrange
        now = get_redis_timestamp(redis_client)
        task_json = build_task_json("task-1", arrived_at_ms=now * 1000)
        _add_task_to_buffer(redis_client, buffer_key, task_json)
        # Add an expired lease (score in the past).
        redis_client.zadd(concurrency_key, {"expired-lease": now - 10})

        # Act
        result = _eval_consume(
            redis_client, base_key, buffer_key, concurrency_key, dlq_key
        )

        # Assert
        # The expired lease should have been removed by ZREMRANGEBYSCORE.
        # The only member should be the newly registered task.
        assert result[0] == 1, "consume should succeed after removing expired lease"
        members = redis_client.zrange(concurrency_key, 0, -1)
        assert "expired-lease" not in members, (
            "expired lease should be removed by self-healing"
        )

    @staticmethod
    def test_self_healing_preserves_valid_leases(
        redis_client, base_key, buffer_key, concurrency_key, dlq_key, build_task_json
    ):
        """Verify that valid leases are not removed by the self-healing mechanism."""
        # Arrange
        now = get_redis_timestamp(redis_client)
        task_json = build_task_json("task-1", arrived_at_ms=now * 1000)
        _add_task_to_buffer(redis_client, buffer_key, task_json)
        # Add a valid lease (score in the future).
        redis_client.zadd(concurrency_key, {"valid-lease": now + 3600})

        # Act
        _eval_consume(redis_client, base_key, buffer_key, concurrency_key, dlq_key)

        # Assert
        members = redis_client.zrange(concurrency_key, 0, -1)
        assert "valid-lease" in members, (
            "valid lease should be preserved by self-healing"
        )


class TestConsumeTelemetry:
    """Tests for ``consume.lua`` telemetry accuracy in the returned array."""

    @staticmethod
    def test_remaining_is_zero_when_at_limit(
        redis_client, base_key, buffer_key, concurrency_key, dlq_key
    ):
        """Verify that ``remaining`` is 0 when the estimated count equals the limit."""
        # Arrange
        current_key, _ = get_window_keys(redis_client, base_key)
        redis_client.set(current_key, str(LIMIT))

        # Act
        result = _eval_consume(
            redis_client, base_key, buffer_key, concurrency_key, dlq_key
        )

        # Assert
        assert result[2] == 0, (
            "remaining should be 0 when estimated count equals the limit"
        )

    @staticmethod
    def test_reset_in_ms_is_positive(
        redis_client, base_key, buffer_key, concurrency_key, dlq_key
    ):
        """Verify that ``reset_in_ms`` is positive when called within a window."""
        # Act
        result = _eval_consume(
            redis_client, base_key, buffer_key, concurrency_key, dlq_key
        )

        # Assert
        assert result[4] > 0, "reset_in_ms should be positive within a window"
