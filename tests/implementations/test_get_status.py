"""Tests for ``get_status()`` on the generic rate limiter implementation.

Tests are written once in async form. The sync implementation participates
via the ``SyncToAsyncLimiterAdapter``; the async implementation runs natively.
"""

import inspect
from unittest.mock import patch

import pytest

from tests.helpers.adapters import SyncToAsyncLimiterAdapter

# ---------------------------------------------------------------------------
# Unified implementation tests
# ---------------------------------------------------------------------------


class GetStatusTests:
    """Unified test suite for ``get_status()`` behavior on both sync and async limiters.

    Subclasses must provide a ``limiter`` fixture that returns either a
    ``SyncToAsyncLimiterAdapter``-wrapped sync limiter or a native async limiter.
    """

    @staticmethod
    async def test_get_status_returns_expected_structure(limiter):
        """Verify that ``get_status()`` returns the required sections and subsection keys."""
        # Act
        status = await limiter.get_status()

        # Assert
        assert set(status.keys()) == {
            "limiter_id",
            "concurrency",
            "buffer",
            "rate_limit",
            "dispatcher",
        }, "status should include all top-level sections"

        assert set(status["concurrency"].keys()) == {"current", "max", "available"}, (
            "concurrency section should include current/max/available"
        )
        assert set(status["buffer"].keys()) == {"count"}, (
            "buffer section should include count"
        )
        assert set(status["rate_limit"].keys()) == {
            "val_previous",
            "val_current",
            "tokens_used",
            "limit",
            "window",
            "reset_in_ms",
        }, "rate_limit section should include the expected telemetry fields"
        assert set(status["dispatcher"].keys()) == {"is_locked"}, (
            "dispatcher section should include is_locked"
        )

    @staticmethod
    async def test_get_status_reflects_scheduled_tasks(limiter, func_path):
        """Verify that the ``get_status()`` buffer count reflects the scheduled task count."""
        # Arrange
        for idx in range(3):
            await limiter.schedule_task(func_path, {"idx": idx})

        # Act
        status = await limiter.get_status()

        # Assert
        assert int(status["buffer"]["count"]) == 3, (
            "status buffer count should match scheduled tasks"
        )

    @staticmethod
    async def test_get_status_reflects_rate_limit_state(limiter, func_path):
        """Verify that ``get_status()`` reflects the rate-limit telemetry after ``consume()`` is called."""
        # Arrange
        await limiter.schedule_task(func_path, {"idx": 1})
        consume_result = await limiter.consume()

        # Act
        status = await limiter.get_status()

        # Assert
        assert consume_result["success"] is True, (
            "consume should succeed for scheduled task"
        )
        assert status["rate_limit"]["limit"] == limiter.limit, (
            "status should report configured rate limit"
        )
        assert float(status["rate_limit"]["tokens_used"]) == pytest.approx(1.0), (
            "tokens_used should be 1.0 after a single consume in the first window"
        )

        # Verify individual result fields are not swapped.
        # In the first window, val_previous is 0 and val_current is >= 1.
        assert int(status["rate_limit"]["val_previous"]) == 0, (
            "val_previous should be 0 in the first window"
        )
        assert int(status["rate_limit"]["val_current"]) == 1, (
            "val_current should be 1 after a single consume in the first window"
        )
        # reset_in_ms is a positive number (time until window expires);
        # buffer count is 0 after the consume drained the buffer.
        assert int(status["rate_limit"]["reset_in_ms"]) > 0, (
            "reset_in_ms should be a positive number within the current window"
        )
        assert int(status["buffer"]["count"]) == 0, (
            "buffer count should be 0 after consume drained the buffer"
        )

    @staticmethod
    async def test_get_status_clean_state_values(limiter):
        """Verify that ``get_status()`` returns correct initial values for a fresh limiter."""
        # Act
        status = await limiter.get_status()

        # Assert
        assert status["limiter_id"] == limiter.id, (
            "limiter_id should match the configured identifier"
        )

        assert int(status["concurrency"]["current"]) == 0, (
            "concurrency current should be zero for a fresh limiter"
        )
        assert status["concurrency"]["max"] == limiter.max_concurrency, (
            "concurrency max should match the configured max_concurrency"
        )
        assert status["concurrency"]["available"] == limiter.max_concurrency, (
            "concurrency available should equal max when no tasks are active"
        )

        assert int(status["buffer"]["count"]) == 0, (
            "buffer count should be zero for a fresh limiter"
        )

        assert float(status["rate_limit"]["tokens_used"]) == 0.0, (
            "tokens_used should be zero for a fresh limiter"
        )
        assert status["rate_limit"]["limit"] == limiter.limit, (
            "rate_limit limit should match the configured limit"
        )
        assert status["rate_limit"]["window"] == limiter.window, (
            "rate_limit window should match the configured window"
        )

        assert status["dispatcher"]["is_locked"] == 0, (
            "dispatcher should not be locked for a fresh limiter"
        )

    @staticmethod
    async def test_get_status_available_is_zero_when_all_slots_used(limiter):
        """Verify that ``available`` is exactly 0 when all concurrency slots are occupied."""
        # Arrange
        # Seed the concurrency sorted set with max_concurrency tasks.
        # The ``zadd`` call may return a coroutine (async Redis) or an int (sync Redis).
        for i in range(limiter.max_concurrency):
            result = limiter.redis.zadd(
                limiter.concurrency_key, {f"saturating_task_{i}": 9999999999.0}
            )
            if inspect.isawaitable(result):
                await result

        # Act
        status = await limiter.get_status()

        # Assert
        assert int(status["concurrency"]["current"]) == limiter.max_concurrency, (
            "concurrency current should equal max_concurrency when all slots are used"
        )
        assert status["concurrency"]["available"] == 0, (
            "concurrency available should be exactly 0 when all slots are occupied"
        )
        assert float(status["rate_limit"]["tokens_used"]) == 0.0, (
            "tokens_used should remain 0 when only concurrency slots are seeded without consuming"
        )


# ---------------------------------------------------------------------------
# Concrete test cases
# ---------------------------------------------------------------------------


class TestSyncGetStatus(GetStatusTests):
    """Sync rate limiter ``get_status()`` exercised through the async adapter."""

    @pytest.fixture
    def limiter(self, generic_limiter):
        """Wrap the sync generic limiter in an async adapter."""
        return SyncToAsyncLimiterAdapter(generic_limiter)

    @staticmethod
    async def test_get_status_val_current_uses_correct_result_index(
        generic_limiter,
    ):
        """Verify that val_current maps to result[1], not result[2] (estimated_count).

        This mirrors the async-specific test to ensure the sync ``get_status()``
        implementation in ``limiters.py`` uses the correct result indices.
        """
        # Arrange
        # health.lua returns: [prev_count, curr_count, estimated_count, active_now, reset_in_ms, buffer_count]
        controlled_response = ["10", "5", "7.5", "2", "500", "3"]

        # Act
        with patch.object(
            generic_limiter, "_eval_script", return_value=controlled_response
        ):
            status = generic_limiter.get_status()

        # Assert
        assert status["rate_limit"]["val_current"] == "5", (
            "val_current should map to result[1] (current_count), not result[2] (estimated_count)"
        )
        assert float(status["rate_limit"]["tokens_used"]) == pytest.approx(7.5), (
            "tokens_used should map to result[2] (estimated_count), not result[1] (current_count)"
        )
        assert status["rate_limit"]["val_previous"] == "10", (
            "val_previous should map to result[0] (previous_count)"
        )


class TestAsyncGetStatus(GetStatusTests):
    """Async rate limiter ``get_status()`` exercised natively."""

    @pytest.fixture
    def limiter(self, async_generic_limiter):
        """Provide the async generic limiter directly."""
        return async_generic_limiter

    @staticmethod
    async def test_get_status_val_current_uses_correct_result_index(
        async_generic_limiter,
    ):
        """Verify that val_current maps to result[1], not result[2] (estimated_count).

        In the first window, val_current == tokens_used because estimated_count equals
        current_count when previous_count is 0. This test uses a controlled health.lua
        response where result[1] != result[2] to verify the index mapping.
        """
        # Arrange
        # health.lua returns: [prev_count, curr_count, estimated_count, active_now, reset_in_ms, buffer_count]
        controlled_response = ["10", "5", "7.5", "2", "500", "3"]

        # Act
        with patch.object(
            async_generic_limiter, "_eval_script", return_value=controlled_response
        ):
            status = await async_generic_limiter.get_status()

        # Assert
        assert status["rate_limit"]["val_current"] == "5", (
            "val_current should map to result[1] (current_count), not result[2] (estimated_count)"
        )
        assert float(status["rate_limit"]["tokens_used"]) == pytest.approx(7.5), (
            "tokens_used should map to result[2] (estimated_count), not result[1] (current_count)"
        )
        assert status["rate_limit"]["val_previous"] == "10", (
            "val_previous should map to result[0] (previous_count)"
        )
