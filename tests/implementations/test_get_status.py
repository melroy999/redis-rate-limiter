"""Tests for ``get_status()`` on the generic rate limiter implementation.

Tests are written once in async form. The sync implementation participates
via the ``SyncToAsyncLimiterAdapter``; the async implementation runs natively.

Fixture dependencies:
    - ``stub_limiter``, ``async_stub_limiter``:
      from ``tests/implementations/conftest.py``.
    - ``func_path``: from ``tests/conftest.py``.
"""

import inspect
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tests.helpers.adapters import SyncToAsyncLimiterAdapter

# ---------------------------------------------------------------------------
# Unified implementation tests
# ---------------------------------------------------------------------------


@pytest.mark.behavior
class GetStatusTests:
    """Unified test suite for ``get_status()`` behavior on both
    sync and async limiters.

    Subclasses must provide a ``limiter`` fixture that returns either a
    ``SyncToAsyncLimiterAdapter``-wrapped sync limiter or a native async limiter.
    """

    @staticmethod
    async def test_get_status_returns_expected_structure(limiter):
        """Verify that ``get_status()`` returns the required
        sections and subsection keys.
        """
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
        """Verify that the ``get_status()`` buffer count reflects
        the scheduled task count.
        """
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
        """Verify that ``get_status()`` reflects rate-limit
        telemetry after ``consume()`` is called.
        """
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

        assert int(status["rate_limit"]["val_previous"]) == 0, (
            "val_previous should be 0 in the first window"
        )
        assert int(status["rate_limit"]["val_current"]) == 1, (
            "val_current should be 1 after a single consume in the first window"
        )
        assert int(status["rate_limit"]["reset_in_ms"]) > 0, (
            "reset_in_ms should be a positive number within the current window"
        )
        assert int(status["buffer"]["count"]) == 0, (
            "buffer count should be 0 after consume drained the buffer"
        )

    @staticmethod
    async def test_get_status_clean_state_values(limiter):
        """Verify that ``get_status()`` returns correct initial
        values for a fresh limiter.
        """
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
        """Verify that ``available`` is exactly 0 when all
        concurrency slots are occupied.
        """
        # Arrange
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
            "tokens_used should remain 0 when only concurrency"
            " slots are seeded without consuming"
        )

    @staticmethod
    async def test_get_status_available_clamps_to_zero_when_over_capacity(limiter):
        """Verify that ``available`` is clamped to 0 when
        concurrency exceeds ``max_concurrency``.
        """
        # Arrange
        for i in range(limiter.max_concurrency + 1):
            result = limiter.redis.zadd(
                limiter.concurrency_key, {f"overflow_task_{i}": 9999999999.0}
            )
            if inspect.isawaitable(result):
                await result

        # Act
        status = await limiter.get_status()

        # Assert
        assert int(status["concurrency"]["current"]) == limiter.max_concurrency + 1, (
            "concurrency current should exceed max_concurrency when over-seeded"
        )
        assert status["concurrency"]["available"] == 0, (
            "concurrency available should be clamped to 0, not negative"
        )

    @staticmethod
    async def test_get_status_reports_locked_dispatcher(limiter):
        """Verify that ``is_locked`` is 1 when the dispatch lock key exists in Redis."""
        # Arrange
        lock_key = f"{limiter.id}:dispatch_lock"
        result = limiter.redis.set(lock_key, "1", ex=10)
        if inspect.isawaitable(result):
            await result

        # Act
        status = await limiter.get_status()

        # Assert
        assert status["dispatcher"]["is_locked"] == 1, (
            "dispatcher is_locked should be 1 when the dispatch lock key exists"
        )

    @staticmethod
    async def test_get_status_val_current_uses_correct_result_index(limiter):
        """Verify that ``get_status()`` maps each health.lua
        return index to the correct result field.
        """
        # Arrange
        # health.lua returns: [prev_count, curr_count, estimated_count,
        # active_now, reset_in_ms, buffer_count]
        controlled_response = ["10", "5", "7.5", "2", "500", "3"]
        actual_limiter = getattr(limiter, "_inner", limiter)
        mock_cls = (
            AsyncMock
            if inspect.iscoroutinefunction(actual_limiter._eval_script)
            else MagicMock
        )

        # Act
        with patch.object(
            actual_limiter,
            "_eval_script",
            mock_cls(return_value=controlled_response),
        ):
            status = await limiter.get_status()

        # Assert
        assert status["rate_limit"]["val_previous"] == "10", (
            "val_previous should map to result[0] (previous_count)"
        )
        assert status["rate_limit"]["val_current"] == "5", (
            "val_current should map to result[1]"
            " (current_count), not result[2] (estimated_count)"
        )
        assert float(status["rate_limit"]["tokens_used"]) == pytest.approx(7.5), (
            "tokens_used should map to result[2]"
            " (estimated_count), not result[1] (current_count)"
        )
        assert int(status["concurrency"]["current"]) == 2, (
            "concurrency current should map to result[3] (active_now)"
        )
        assert int(status["rate_limit"]["reset_in_ms"]) == 500, (
            "reset_in_ms should map to result[4]"
        )
        assert int(status["buffer"]["count"]) == 3, (
            "buffer count should map to result[5]"
        )


# ---------------------------------------------------------------------------
# Concrete test cases
# ---------------------------------------------------------------------------


@pytest.mark.behavior
class TestSyncGetStatus(GetStatusTests):
    """Sync rate limiter ``get_status()`` exercised through the async adapter."""

    @pytest.fixture
    def limiter(self, stub_limiter):
        """Wrap the sync generic limiter in an async adapter."""
        return SyncToAsyncLimiterAdapter(stub_limiter)


@pytest.mark.behavior
class TestAsyncGetStatus(GetStatusTests):
    """Async rate limiter ``get_status()`` exercised natively."""

    @pytest.fixture
    def limiter(self, async_stub_limiter):
        """Provide the async generic limiter directly."""
        return async_stub_limiter
