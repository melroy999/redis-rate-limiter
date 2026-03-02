"""Tests for the ASGI rate limiter backend.

Fixture dependencies:
    - ``limiter``, ``_reset_asgi_limiter_class_state``: from ``tests/implementations/asgi/conftest.py``.
"""

import logging
from unittest.mock import AsyncMock, patch

import pytest

from celery_rate_limiter.backends.asgi import ASGIRateLimiter
from tests.helpers.utils import assert_log_emitted


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
    async def test_acquire_skips_refresh_within_interval(limiter):
        """Verify that ``acquire`` does not trigger ``refresh_config`` when the interval has not elapsed."""
        # Arrange
        fixed_now = 100.0
        limiter._last_refresh = fixed_now - 1.0

        # Act
        with patch(
            "celery_rate_limiter.backends.asgi.limiter.time.monotonic",
            return_value=fixed_now,
        ):
            with patch.object(
                limiter, "refresh_config", new=AsyncMock(return_value=False)
            ) as mock_refresh:
                await limiter.acquire("no_refresh_user")

        # Assert
        assert mock_refresh.call_count == 0, (
            "refresh_config should not be called when less than the refresh interval has elapsed"
        )

    @staticmethod
    async def test_acquire_refreshes_config_at_interval_boundary(limiter):
        """Verify that ``acquire`` triggers ``refresh_config`` when elapsed time equals the refresh interval."""
        # Arrange
        fixed_now = 100.0
        limiter._last_refresh = fixed_now - limiter._refresh_interval

        # Act
        with patch(
            "celery_rate_limiter.backends.asgi.limiter.time.monotonic",
            return_value=fixed_now,
        ):
            with patch.object(
                limiter, "refresh_config", new=AsyncMock(return_value=False)
            ) as mock_refresh:
                await limiter.acquire("boundary_user")

        # Assert
        assert mock_refresh.call_count == 1, (
            "refresh_config should be called when elapsed time equals the refresh interval"
        )

    @staticmethod
    async def test_acquire_raises_on_script_failure(limiter):
        """Verify that ``acquire`` raises when the Lua script fails."""
        # Act & Assert
        with patch.object(
            limiter, "_eval_script", side_effect=RuntimeError("script failed")
        ):
            with pytest.raises(RuntimeError, match="script failed"):
                await limiter.acquire("user_error")


# ---------------------------------------------------------------------------
# Observability tests
# ---------------------------------------------------------------------------


class TestASGIStartObservability:
    """Observability tests for the ``start()`` log emission."""

    @staticmethod
    async def test_start_emits_info_log(limiter_id, caplog):
        """Verify that ``start()`` emits an INFO log with the limiter id, limit, and window."""
        # Arrange & Act
        with caplog.at_level(
            logging.INFO, logger="celery_rate_limiter.backends.asgi.limiter"
        ):
            instance = await ASGIRateLimiter.create(
                limiter_id=f"{limiter_id}_start_log",
                limit=10,
                window=60,
                override=True,
            )

        # Assert
        assert_log_emitted(
            caplog.records,
            level="INFO",
            required_fragments=[
                f"id={instance.id}",
                "limit=10",
                "window_s=60",
            ],
            message="should emit an info log with the limiter id, limit, and window on initialization",
        )


class TestASGIAcquireObservability:
    """Observability tests for the ``acquire`` log emissions."""

    @staticmethod
    async def test_acquire_failure_emits_error_log(limiter, caplog):
        """Verify that ``acquire`` emits an ERROR log with limiter id and key on script failure."""
        # Act
        with caplog.at_level(
            logging.ERROR, logger="celery_rate_limiter.backends.asgi.limiter"
        ):
            with patch.object(
                limiter, "_eval_script", side_effect=RuntimeError("script failed")
            ):
                with pytest.raises(RuntimeError, match="script failed"):
                    await limiter.acquire("user_error")

        # Assert
        assert_log_emitted(
            caplog.records,
            level="ERROR",
            required_fragments=[
                f"limiter={limiter.id}",
                "key=user_error",
            ],
            message="should emit an error log containing the limiter id and the key",
        )


# ---------------------------------------------------------------------------
# Class variable tests
# ---------------------------------------------------------------------------


class TestASGILimiterClassVariables:
    """Verify class-level configuration defaults on ``ASGIRateLimiter``."""

    @staticmethod
    def test_refresh_interval_defaults_to_5():
        """Verify that the ``_refresh_interval`` class variable defaults to ``5.0``.

        Mutation target: ``_refresh_interval`` class variable in ``ASGIRateLimiter``.
        """
        # Assert
        assert ASGIRateLimiter._refresh_interval == 5.0, (
            "_refresh_interval class variable must default to 5.0"
        )

    @staticmethod
    async def test_last_refresh_initializes_to_zero(limiter):
        """Verify that ``_last_refresh`` initializes to ``0.0`` on a new instance."""
        # Assert
        assert limiter._last_refresh == 0.0, (
            "_last_refresh must initialize to 0.0 so the first acquire triggers a script refresh"
        )
