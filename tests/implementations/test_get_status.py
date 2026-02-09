"""Tests for get_status() on the generic rate limiter implementation."""

from unittest.mock import patch

import pytest
import redis


class TestGetStatus:
    """Test suite for get_status() behavior on AbstractDistributedRateLimiter."""

    def test_get_status_returns_expected_structure(self, generic_limiter):
        """Verify get_status() returns required sections and subsection keys."""
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
        }, "rate_limit section should include expected telemetry fields"
        assert set(status["dispatcher"].keys()) == {"is_locked"}, (
            "dispatcher section should include is_locked"
        )

    def test_get_status_reflects_scheduled_tasks(self, generic_limiter, func_path):
        """Verify get_status() buffer count reflects scheduled task count."""
        # Arrange
        for idx in range(3):
            generic_limiter.schedule_task(func_path, {"idx": idx})

        # Act
        status = generic_limiter.get_status()

        # Assert
        assert int(status["buffer"]["count"]) == 3, (
            "status buffer count should match scheduled tasks"
        )

    def test_get_status_reflects_rate_limit_state(self, generic_limiter, func_path):
        """Verify get_status() reflects rate-limit telemetry after consume()."""
        # Arrange
        generic_limiter.schedule_task(func_path, {"idx": 1})
        consume_result = generic_limiter.consume()

        # Act
        status = generic_limiter.get_status()

        # Assert
        assert consume_result["success"] is True, "consume should succeed for scheduled task"
        assert status["rate_limit"]["limit"] == generic_limiter.limit, (
            "status should report configured rate limit"
        )
        assert float(status["rate_limit"]["tokens_used"]) >= 1.0, (
            "tokens_used should increase after a successful consume"
        )

    def test_get_status_recovery_on_noscript_error(self, generic_limiter, redis_client):
        """Verify get_status() reloads Lua script and retries on NoScriptError."""
        # Arrange
        real_evalsha = redis_client.evalsha
        real_script_load = redis_client.script_load

        def mocked_evalsha_func(*args, **kwargs):
            if mocked_evalsha_func.call_count == 0:
                mocked_evalsha_func.call_count += 1
                raise redis.exceptions.NoScriptError("NOSCRIPT")
            return real_evalsha(*args, **kwargs)

        mocked_evalsha_func.call_count = 0

        # Act
        with (
            patch.object(
                generic_limiter.redis, "evalsha", side_effect=mocked_evalsha_func
            ) as mock_eval,
            patch.object(
                generic_limiter.redis, "script_load", side_effect=real_script_load
            ) as mock_load,
        ):
            status = generic_limiter.get_status()

            # Assert
            assert status["limiter_id"] == generic_limiter.id, (
                "status should still be returned after script recovery"
            )
            assert mock_eval.call_count == 2, "evalsha should be called twice (fail then retry)"
            assert mock_load.call_count == 1, "script_load should be called once to recover"

    def test_get_status_permanent_failure_raises_error(self, generic_limiter):
        """Verify permanent NoScriptError during get_status() raises RuntimeError."""
        # Arrange
        with patch.object(
            generic_limiter.redis,
            "evalsha",
            side_effect=redis.exceptions.NoScriptError("Permanent Failure"),
        ) as mock_eval:
            # Act & Assert
            with pytest.raises(RuntimeError, match="Redis failed to retain the Lua script"):
                generic_limiter.get_status()

            assert mock_eval.call_count == 2, "get_status should retry exactly once before failing"
