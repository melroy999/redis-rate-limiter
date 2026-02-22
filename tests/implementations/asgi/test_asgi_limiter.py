"""Tests for the ASGI rate limiter backend."""

import logging
from unittest.mock import patch

import pytest


class TestASGIRateLimiter:
    """Tests for the ``ASGIRateLimiter`` ``acquire`` method."""

    @staticmethod
    async def test_acquire_allowed_under_limit(limiter):
        """Verify that ``acquire`` returns ``allowed=True`` when under the rate limit."""
        # Act
        result = await limiter.acquire("user_1")

        # Assert
        assert result["allowed"] is True, "first request should be allowed"
        assert result["remaining"] >= 0, "remaining should be non-negative"

    @staticmethod
    async def test_acquire_returns_remaining_count(limiter):
        """Verify that ``remaining`` decreases with each ``acquire``."""
        # Act
        first = await limiter.acquire("user_2")
        second = await limiter.acquire("user_2")

        # Assert
        assert second["remaining"] == first["remaining"] - 1, (
            "remaining should decrement by exactly one per acquire"
        )

    @staticmethod
    async def test_acquire_denied_at_limit(limiter):
        """Verify that ``acquire`` returns ``allowed=False`` when the limit is exceeded."""
        # Arrange
        for _ in range(10):
            await limiter.acquire("user_3")

        # Act
        result = await limiter.acquire("user_3")

        # Assert
        assert result["allowed"] is False, "request should be denied at the limit"
        assert result["remaining"] == 0, "remaining should be 0 when denied"

    @staticmethod
    async def test_per_identity_isolation(limiter):
        """Verify that different identities have independent rate limits."""
        # Arrange
        # Exhaust one identity.
        for _ in range(10):
            await limiter.acquire("user_a")

        # Act
        result_b = await limiter.acquire("user_b")
        result_a = await limiter.acquire("user_a")

        # Assert
        assert result_b["allowed"] is True, "unrelated identity should still be allowed"
        assert result_a["allowed"] is False, "exhausted identity should still be denied"

    @staticmethod
    async def test_acquire_returns_reset_in_ms(limiter):
        """Verify that ``reset_in_ms`` is a positive integer."""
        # Act
        result = await limiter.acquire("user_4")

        # Assert
        assert isinstance(result["reset_in_ms"], int), (
            "reset_in_ms should be an integer"
        )
        assert result["reset_in_ms"] > 0, "reset_in_ms should be positive"

    @staticmethod
    async def test_acquire_returns_window_counters(limiter):
        """Verify that ``val_previous`` and ``val_current`` are returned."""
        # Act
        result = await limiter.acquire("user_5")

        # Assert
        assert "val_previous" in result, "result should include val_previous"
        assert "val_current" in result, "result should include val_current"
        assert result["val_previous"] == 0, (
            "val_previous should be zero on first acquire"
        )
        assert result["val_current"] >= 1, (
            "val_current should be at least 1 after acquire"
        )

    @staticmethod
    async def test_acquire_logs_exception_on_script_failure(limiter, caplog):
        """Verify that ``acquire`` logs an exception when the Lua script fails."""
        # Act & Assert
        with caplog.at_level(
            logging.ERROR, logger="celery_rate_limiter.backends.asgi.limiter"
        ):
            with patch.object(
                limiter, "_eval_script", side_effect=RuntimeError("script failed")
            ):
                with pytest.raises(RuntimeError, match="script failed"):
                    await limiter.acquire("user_error")

        # Assert
        assert any(
            record.levelname == "ERROR"
            and limiter.id in record.message
            and "user_error" in record.message
            for record in caplog.records
        ), "should emit an error log containing the limiter id and the key"
