"""Tests for ``get_status()`` on the generic rate limiter implementation.

Tests are written once in async form. The sync implementation participates
via the ``SyncToAsyncLimiterAdapter``; the async implementation runs natively.
"""

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
