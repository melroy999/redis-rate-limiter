"""Tests for ``get_status()`` on the generic rate limiter implementation."""


class TestGetStatus:
    """Test suite for ``get_status()`` behavior on the AbstractDistributedRateLimiter."""

    @staticmethod
    def test_get_status_returns_expected_structure(generic_limiter):
        """Verify that ``get_status()`` returns the required sections and subsection keys."""
        # Act
        status = generic_limiter.get_status()

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
    def test_get_status_reflects_scheduled_tasks(generic_limiter, func_path):
        """Verify that the ``get_status()`` buffer count reflects the scheduled task count."""
        # Arrange
        for idx in range(3):
            generic_limiter.schedule_task(func_path, {"idx": idx})

        # Act
        status = generic_limiter.get_status()

        # Assert
        assert int(status["buffer"]["count"]) == 3, (
            "status buffer count should match scheduled tasks"
        )

    @staticmethod
    def test_get_status_reflects_rate_limit_state(generic_limiter, func_path):
        """Verify that ``get_status()`` reflects the rate-limit telemetry after ``consume()`` is called."""
        # Arrange
        generic_limiter.schedule_task(func_path, {"idx": 1})
        consume_result = generic_limiter.consume()

        # Act
        status = generic_limiter.get_status()

        # Assert
        assert consume_result["success"] is True, (
            "consume should succeed for scheduled task"
        )
        assert status["rate_limit"]["limit"] == generic_limiter.limit, (
            "status should report configured rate limit"
        )
        assert float(status["rate_limit"]["tokens_used"]) >= 1.0, (
            "tokens_used should increase after a successful consume"
        )

