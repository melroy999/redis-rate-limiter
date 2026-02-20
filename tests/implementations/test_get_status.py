"""Tests for ``get_status()`` on the generic rate limiter implementation.

Tests are written once in async form. The sync implementation participates
via the ``SyncToAsyncLimiterAdapter``; the async implementation runs natively.
"""

import inspect

import pytest

from tests.helpers.adapters import SyncToAsyncLimiterAdapter


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
        assert float(status["rate_limit"]["tokens_used"]) >= 1.0, (
            "tokens_used should increase after a successful consume"
        )

        # Verify individual result fields are not swapped.
        # In the first window, val_previous is 0 and val_current is >= 1.
        assert int(status["rate_limit"]["val_previous"]) == 0, (
            "val_previous should be 0 in the first window"
        )
        assert int(status["rate_limit"]["val_current"]) >= 1, (
            "val_current should reflect the consumed task count"
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


class TestAsyncGetStatus(GetStatusTests):
    """Async rate limiter ``get_status()`` exercised natively."""

    @pytest.fixture
    def limiter(self, async_generic_limiter):
        """Provide the async generic limiter directly."""
        return async_generic_limiter
