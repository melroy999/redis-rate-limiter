"""Tests for the internal helper methods of the rate limiter.

This module tests internal implementation details such as Lua script loading
via ``_register_script``, ``_eval_script`` NOSCRIPT recovery, task data
formatting, and other utility methods.
"""

import logging
from unittest.mock import patch

import pytest
import redis

from celery_rate_limiter.core.base import (
    AbstractAsyncRateLimiter,
    AbstractSyncRateLimiter,
)
from celery_rate_limiter.core.scripts import DEFAULT_RESOURCE_PACKAGES
from tests.helpers.adapters import SyncToAsyncLimiterAdapter


@pytest.mark.parametrize(
    "lua_script",
    [
        "schedule.lua",
        # A fictional non-existent file for testing the failure path.
        "missing.lua",
    ],
    ids=["existing_script", "missing_script"],
)
class TestInternalHelpers:
    """Tests for internal helper methods and Lua script loading."""

    @staticmethod
    def test_register_script_uses_cached_source(generic_limiter, lua_script):
        """Verify that script sources are loaded from disk only once and subsequently served from the cache."""
        # Arrange
        # Simulate a script that has already been loaded into the source cache.
        existing_content = "return 1"
        generic_limiter._script_sources[lua_script] = existing_content

        # Act
        # Mock the resource loader to track the number of invocations.
        with patch("celery_rate_limiter.core.scripts.resources.files") as mock_files:
            generic_limiter._register_script(lua_script)

            # Assert
            mock_files.assert_not_called()

        assert generic_limiter._script_sources[lua_script] == existing_content, (
            "cached script content should not be modified"
        )

    @staticmethod
    def test_register_script_raises_import_error_on_missing_source(
        generic_limiter, lua_script
    ):
        """Verify that an ImportError is raised when the Lua script cannot be loaded from any resource package."""
        # Arrange
        # Ensure the script source is not cached.
        generic_limiter._script_sources.pop(lua_script, None)

        # Act & Assert
        # Mock the resource loader to simulate a file system error.
        with patch(
            "celery_rate_limiter.core.scripts.resources.files",
            side_effect=FileNotFoundError("File system error"),
        ) as mock_files:
            with pytest.raises(ImportError, match=f"Could not load {lua_script}"):
                generic_limiter._register_script(lua_script)

            # Verify that all configured package candidates were attempted.
            assert mock_files.call_count == len(DEFAULT_RESOURCE_PACKAGES), (
                "resource loader should try each configured package candidate"
            )


class TestLuaScriptFallback:
    """Tests for the Lua script loader fallback mechanism."""

    @staticmethod
    def test_load_lua_script_falls_back_to_second_package(generic_limiter):
        """Verify that the script loader succeeds via the second package when the first raises ModuleNotFoundError."""
        # Arrange
        # Ensure the script source is not cached so the loader is invoked.
        generic_limiter._script_sources.pop("schedule.lua", None)

        from importlib import resources as real_resources

        original_files = real_resources.files
        call_count = {"n": 0}

        def selective_files(package_name):
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise ModuleNotFoundError(f"No module named '{package_name}'")
            return original_files(package_name)

        # Act
        with patch(
            "celery_rate_limiter.core.scripts.resources.files",
            side_effect=selective_files,
        ):
            generic_limiter._register_script("schedule.lua")

        # Assert
        assert "schedule.lua" in generic_limiter._script_sources, (
            "script source should be loaded after fallback to second package"
        )
        assert call_count["n"] == 2, (
            "resource loader should be called twice (first fails, second succeeds)"
        )


class TestTaskSignature:
    """Tests for the task-signature helper behavior."""

    @staticmethod
    def test_task_signature_is_deterministic_across_key_orders(generic_limiter):
        """Verify that ``_get_task_signature_str`` is deterministic regardless of dictionary key order."""
        # Arrange
        payload_a = {"user_id": 123, "flags": {"vip": True, "beta": False}}
        payload_b = {"flags": {"beta": False, "vip": True}, "user_id": 123}

        # Act
        signature_a = generic_limiter._get_task_signature_str(
            "myapp.tasks.process", payload_a
        )
        signature_b = generic_limiter._get_task_signature_str(
            "myapp.tasks.process", payload_b
        )

        # Assert
        assert signature_a == signature_b, (
            "task signature should be identical regardless of key insertion order"
        )


class TestInflightTtl:
    """Tests for the in-flight TTL calculation."""

    @staticmethod
    def test_inflight_ttl_defaults_to_limiter_max_age(generic_limiter):
        """Verify that the in-flight TTL includes max_age plus the lease and window slack."""
        # Arrange
        expected = (
            generic_limiter.max_age
            + generic_limiter.lease_duration
            + generic_limiter.window
        )

        # Act
        ttl = generic_limiter._get_inflight_ttl()

        # Assert
        assert ttl == expected, (
            "default inflight TTL should be max_age + lease_duration + window"
        )

    @staticmethod
    def test_inflight_ttl_uses_per_task_override(generic_limiter):
        """Verify that a per-task max_age override drives the in-flight TTL calculation."""
        # Arrange
        override = 7
        expected = override + generic_limiter.lease_duration + generic_limiter.window

        # Act
        ttl = generic_limiter._get_inflight_ttl(max_age_override=override)

        # Assert
        assert ttl == expected, (
            "override inflight TTL should use task max_age + lease_duration + window"
        )


class TestCleanupInflightKey:
    """Tests for the best-effort in-flight key cleanup on scheduling failures."""

    @staticmethod
    def test_cleanup_inflight_key_suppresses_redis_failure(generic_limiter, caplog):
        """Verify that ``_cleanup_inflight_key`` does not propagate Redis exceptions."""
        # Arrange
        inflight_key = f"{generic_limiter.id}:inflight:cleanup-test"

        # Act & Assert
        with caplog.at_level(logging.WARNING, logger="celery_rate_limiter.core.limiters"):
            with patch.object(
                generic_limiter.redis, "delete", side_effect=ConnectionError("redis down")
            ):
                # This invocation must not raise.
                generic_limiter._cleanup_inflight_key(inflight_key, "cleanup-test")

        # Assert
        assert any(
            record.levelname == "WARNING"
            and f"limiter={generic_limiter.id}" in record.message
            and "task_id=cleanup-test" in record.message
            and f"inflight_key={inflight_key}" in record.message
            and "redis down" in record.message
            for record in caplog.records
        ), "should emit a warning log containing the limiter id, task id, inflight key, and error"

    @staticmethod
    def test_cleanup_inflight_key_deletes_redis_key(
        generic_limiter, redis_client, caplog
    ):
        """Verify that ``_cleanup_inflight_key`` removes the in-flight key from Redis."""
        # Arrange
        inflight_key = f"{generic_limiter.id}:inflight:cleanup-del"
        redis_client.set(inflight_key, "1")
        assert redis_client.exists(inflight_key) == 1, (
            "precondition: inflight key must exist before cleanup"
        )

        # Act
        with caplog.at_level(logging.DEBUG, logger="celery_rate_limiter.core.limiters"):
            generic_limiter._cleanup_inflight_key(inflight_key, "cleanup-del")

        # Assert
        assert redis_client.exists(inflight_key) == 0, (
            "inflight key should be removed after cleanup"
        )
        assert any(
            record.levelname == "DEBUG"
            and f"limiter={generic_limiter.id}" in record.message
            and "task_id=cleanup-del" in record.message
            and f"inflight_key={inflight_key}" in record.message
            and "removed=1" in record.message
            for record in caplog.records
        ), "should emit a debug log containing the limiter id, task id, inflight key, and removal result"

    @staticmethod
    def test_cleanup_inflight_key_handles_missing_key_gracefully(generic_limiter):
        """Verify that ``_cleanup_inflight_key`` does not raise when the key does not exist."""
        # Arrange
        inflight_key = f"{generic_limiter.id}:inflight:nonexistent"

        # Act & Assert
        # This invocation must not raise.
        generic_limiter._cleanup_inflight_key(inflight_key, "nonexistent")


class TestAsyncCleanupInflightKey:
    """Tests for the async best-effort in-flight key cleanup on scheduling failures."""

    @staticmethod
    async def test_cleanup_inflight_key_suppresses_redis_failure(
        async_generic_limiter, caplog
    ):
        """Verify that the async ``_cleanup_inflight_key`` does not propagate Redis exceptions."""
        # Arrange
        inflight_key = f"{async_generic_limiter.id}:inflight:cleanup-test"

        # Act & Assert
        with caplog.at_level(logging.WARNING, logger="celery_rate_limiter.core.async_limiters"):
            with patch.object(
                async_generic_limiter.redis, "delete", side_effect=ConnectionError("redis down")
            ):
                # This invocation must not raise.
                await async_generic_limiter._cleanup_inflight_key(inflight_key, "cleanup-test")

        # Assert
        assert any(
            record.levelname == "WARNING"
            and f"limiter={async_generic_limiter.id}" in record.message
            and "task_id=cleanup-test" in record.message
            and "redis down" in record.message
            for record in caplog.records
        ), "should emit a warning log containing the limiter id, task id, and error"

    @staticmethod
    async def test_cleanup_inflight_key_deletes_redis_key(
        async_generic_limiter, async_redis_client, caplog
    ):
        """Verify that the async ``_cleanup_inflight_key`` removes the in-flight key from Redis."""
        # Arrange
        inflight_key = f"{async_generic_limiter.id}:inflight:cleanup-del"
        await async_redis_client.set(inflight_key, "1")
        assert await async_redis_client.exists(inflight_key) == 1, (
            "precondition: inflight key must exist before cleanup"
        )

        # Act
        with caplog.at_level(logging.DEBUG, logger="celery_rate_limiter.core.async_limiters"):
            await async_generic_limiter._cleanup_inflight_key(inflight_key, "cleanup-del")

        # Assert
        assert await async_redis_client.exists(inflight_key) == 0, (
            "inflight key should be removed after cleanup"
        )
        assert any(
            record.levelname == "DEBUG"
            and f"limiter={async_generic_limiter.id}" in record.message
            and "task_id=cleanup-del" in record.message
            and f"inflight_key={inflight_key}" in record.message
            and "removed=1" in record.message
            for record in caplog.records
        ), "should emit a debug log containing the limiter id, task id, inflight key, and removal result"

    @staticmethod
    async def test_cleanup_inflight_key_handles_missing_key_gracefully(
        async_generic_limiter,
    ):
        """Verify that the async ``_cleanup_inflight_key`` does not raise when the key does not exist."""
        # Arrange
        inflight_key = f"{async_generic_limiter.id}:inflight:nonexistent"

        # Act & Assert
        # This invocation must not raise.
        await async_generic_limiter._cleanup_inflight_key(inflight_key, "nonexistent")


class TokenRecoveryDelayTests:
    """Unified tests for the sliding-window token recovery delay calculation.

    Subclasses must provide a ``limiter`` fixture that returns either a
    ``SyncToAsyncLimiterAdapter``-wrapped sync limiter or a native async limiter.
    """

    @staticmethod
    async def test_token_recovery_returns_fractional_wait_when_decay_applies(limiter):
        """Verify that a positive fractional delay is returned when previous-window decay can free a token."""
        # Arrange
        # A 1-second window is used so the arithmetic is straightforward to verify.
        limiter.window = 1.0
        limiter.limit = 5

        # Act
        delay = limiter._calculate_token_recovery_delay(
            val_previous=5, val_current=3, reset_in_ms=500
        )

        # Assert
        assert delay > 0, (
            "delay should be positive when decay has not yet freed a token"
        )
        assert delay < 1.0, "delay should be less than the full window"

    @staticmethod
    async def test_token_recovery_returns_immediate_when_decay_already_freed_token(
        limiter,
    ):
        """Verify that an immediate retry is returned when previous-window decay has already freed a token."""
        # Arrange
        limiter.window = 1.0
        limiter.limit = 5

        # Act
        delay = limiter._calculate_token_recovery_delay(
            val_previous=10, val_current=1, reset_in_ms=100
        )

        # Assert
        assert delay == pytest.approx(0.001), (
            "delay should be 0.001 when decay has already freed a token"
        )

    @staticmethod
    async def test_token_recovery_fallback_when_val_previous_is_zero(limiter):
        """Verify that the delay falls back to reset_in_ms when the previous window has no requests."""
        # Arrange
        limiter.window = 1.0
        limiter.limit = 5

        # Act
        delay = limiter._calculate_token_recovery_delay(
            val_previous=0, val_current=3, reset_in_ms=500
        )

        # Assert
        # Fallback formula: reset_in_ms / 1000.0 + 0.001 = 0.501
        assert delay == pytest.approx(0.501), (
            "delay should equal reset_in_ms / 1000 + 0.001 when val_previous is zero"
        )

    @staticmethod
    async def test_token_recovery_fallback_when_val_current_equals_limit(limiter):
        """Verify that the delay falls back to reset_in_ms when the current window is at the limit."""
        # Arrange
        limiter.window = 1.0
        limiter.limit = 5

        # Act
        delay = limiter._calculate_token_recovery_delay(
            val_previous=5, val_current=5, reset_in_ms=500
        )

        # Assert
        # Fallback formula: reset_in_ms / 1000.0 + 0.001 = 0.501
        assert delay == pytest.approx(0.501), (
            "delay should equal reset_in_ms / 1000 + 0.001 when val_current equals limit"
        )

    @staticmethod
    async def test_token_recovery_primary_path_exact_value(limiter):
        """Verify the exact delay value computed via the primary decay formula."""
        # Arrange
        limiter.window = 1.0
        limiter.limit = 5

        # Act
        delay = limiter._calculate_token_recovery_delay(
            val_previous=5, val_current=3, reset_in_ms=500
        )

        # Assert
        # t_needed_ms = 1000 * (1.0 - (5 - 3) / 5) = 600
        # time_passed_ms = 1000 - 500 = 500
        # wait_ms = 600 - 500 = 100
        # delay = 100 / 1000.0 = 0.1
        assert delay == pytest.approx(0.1), (
            "delay should be 0.1 seconds for the given inputs"
        )

    @staticmethod
    async def test_token_recovery_primary_path_floor_when_wait_ms_zero(limiter):
        """Verify that the 0.001 floor is returned when wait_ms is exactly zero."""
        # Arrange
        limiter.window = 1.0
        limiter.limit = 5

        # Act
        # t_needed_ms = 1000 * (1.0 - (5 - 3) / 5) = 600
        # time_passed_ms = 1000 - 400 = 600
        # wait_ms = 600 - 600 = 0 (<= 0)
        delay = limiter._calculate_token_recovery_delay(
            val_previous=5, val_current=3, reset_in_ms=400
        )

        # Assert
        assert delay == pytest.approx(0.001), (
            "delay should be 0.001 when wait_ms is exactly zero"
        )

    @staticmethod
    async def test_token_recovery_primary_path_when_val_previous_is_one(limiter):
        """Verify that ``val_previous=1`` takes the primary decay path, not the fallback."""
        # Arrange
        limiter.window = 1.0
        limiter.limit = 5

        # Act
        # t_needed_ms = 1000 * (1.0 - (5 - 3) / 1) = -1000
        # time_passed_ms = 1000 - 500 = 500
        # wait_ms = -1000 - 500 = -1500 (<= 0)
        # Primary path returns 0.001 (floor); the fallback would return 0.501.
        delay = limiter._calculate_token_recovery_delay(
            val_previous=1, val_current=3, reset_in_ms=500
        )

        # Assert
        assert delay == pytest.approx(0.001), (
            "val_previous=1 should take the primary path and return 0.001"
        )

    @staticmethod
    async def test_token_recovery_primary_path_fractional_wait_ms(limiter):
        """Verify that a fractional wait_ms between 0 and 1 returns the exact value, not the floor."""
        # Arrange
        limiter.window = 1.0
        limiter.limit = 5

        # Act
        # t_needed_ms = 1000 * (1.0 - (5 - 4) / 3) = 666.667
        # time_passed_ms = 1000 - 334 = 666
        # wait_ms = 666.667 - 666 = 0.667 (positive but < 1)
        delay = limiter._calculate_token_recovery_delay(
            val_previous=3, val_current=4, reset_in_ms=334
        )

        # Assert
        # delay = 0.667 / 1000 = 0.000667, NOT the 0.001 floor.
        expected = (1000 * (1.0 - (5 - 4) / 3) - (1000 - 334)) / 1000.0
        assert delay == pytest.approx(expected), (
            "fractional wait_ms should return the exact delay, not the 0.001 floor"
        )


class TestSyncTokenRecoveryDelay(TokenRecoveryDelayTests):
    """Sync rate limiter token recovery exercised through the async adapter."""

    @pytest.fixture
    def limiter(self, generic_limiter):
        """Wrap the sync generic limiter in an async adapter."""
        return SyncToAsyncLimiterAdapter(generic_limiter)


class TestAsyncTokenRecoveryDelay(TokenRecoveryDelayTests):
    """Async rate limiter token recovery exercised natively."""

    @pytest.fixture
    def limiter(self, async_generic_limiter):
        """Provide the async generic limiter directly."""
        return async_generic_limiter


class _BareSyncLimiter(AbstractSyncRateLimiter):
    """Subclass of the sync base without mixin logic, used to test ``_eval_script`` in isolation."""

    pass


class _BareAsyncLimiter(AbstractAsyncRateLimiter):
    """Subclass of the async base without mixin logic, used to test ``_eval_script`` in isolation."""

    pass


class TestSyncEvalScript:
    """Tests for ``AbstractSyncRateLimiter._eval_script`` NOSCRIPT recovery."""

    @pytest.fixture
    def limiter(self, redis_client):
        """Create a minimal sync limiter with a pre-registered script."""
        _limiter = _BareSyncLimiter(
            redis_client=redis_client,
            limiter_id="eval_script_sync",
            limit=5,
            window=60,
        )
        _limiter._register_script("health.lua")
        return _limiter

    @staticmethod
    def test_eval_script_recovers_from_transient_noscript(limiter, caplog):
        """Verify that ``_eval_script`` re-registers and retries on a single NoScriptError."""
        # Arrange
        real_evalsha = limiter.redis.evalsha
        real_script_load = limiter.redis.script_load

        def fail_once(*args, **kwargs):
            if fail_once.calls == 0:
                fail_once.calls += 1
                raise redis.exceptions.NoScriptError("NOSCRIPT")
            return real_evalsha(*args, **kwargs)

        fail_once.calls = 0

        # Act
        with caplog.at_level(logging.WARNING, logger="celery_rate_limiter.core.base"):
            with (
                patch.object(limiter.redis, "evalsha", side_effect=fail_once) as mock_eval,
                patch.object(
                    limiter.redis, "script_load", side_effect=real_script_load
                ) as mock_load,
            ):
                result = limiter._eval_script(
                    "health.lua",
                    3,
                    "eval_script_sync",
                    "eval_script_sync:buffer",
                    "eval_script_sync:concurrency",
                    60,
                    5,
                    2,
                )

        # Assert
        assert result is not None, (
            "eval_script should return the script result after recovery"
        )
        assert mock_eval.call_count == 2, (
            "evalsha should be called twice (fail then retry)"
        )
        assert mock_load.call_count == 1, (
            "script_load should be called once to re-register"
        )
        assert any(
            record.levelname == "WARNING"
            and f"limiter={limiter.id}" in record.message
            and "script=health.lua" in record.message
            for record in caplog.records
        ), "should emit a warning log containing the limiter id and script name on NOSCRIPT recovery"

    @staticmethod
    def test_eval_script_raises_runtime_error_on_permanent_noscript(limiter):
        """Verify that ``_eval_script`` raises RuntimeError when the script cannot be retained."""
        # Arrange
        with patch.object(
            limiter.redis,
            "evalsha",
            side_effect=redis.exceptions.NoScriptError("Permanent"),
        ) as mock_eval:
            # Act & Assert
            with pytest.raises(
                RuntimeError, match="Redis failed to retain the Lua script"
            ):
                limiter._eval_script("health.lua", 0)

            assert mock_eval.call_count == 2, (
                "evalsha should be attempted twice before raising"
            )

    @staticmethod
    def test_eval_script_propagates_non_noscript_errors(limiter):
        """Verify that non-NoScriptError exceptions pass through without retry."""
        # Arrange
        with patch.object(
            limiter.redis,
            "evalsha",
            side_effect=redis.exceptions.ConnectionError("redis down"),
        ) as mock_eval:
            # Act & Assert
            with pytest.raises(redis.exceptions.ConnectionError, match="redis down"):
                limiter._eval_script("health.lua", 0)

            assert mock_eval.call_count == 1, (
                "evalsha should not retry on non-NoScriptError exceptions"
            )


class TestAsyncEvalScript:
    """Tests for ``AbstractAsyncRateLimiter._eval_script`` NOSCRIPT recovery."""

    @pytest.fixture
    async def limiter(self, async_redis_client):
        """Create a minimal async limiter with a pre-registered script."""
        _limiter = _BareAsyncLimiter(
            redis_client=async_redis_client,
            limiter_id="eval_script_async",
            limit=5,
            window=60,
        )
        await _limiter._register_script("health.lua")
        return _limiter

    @staticmethod
    async def test_eval_script_recovers_from_transient_noscript(limiter, caplog):
        """Verify that the async ``_eval_script`` re-registers and retries on a single NoScriptError."""
        # Arrange
        real_evalsha = limiter.redis.evalsha
        real_script_load = limiter.redis.script_load

        async def fail_once(*args, **kwargs):
            if fail_once.calls == 0:
                fail_once.calls += 1
                raise redis.exceptions.NoScriptError("NOSCRIPT")
            return await real_evalsha(*args, **kwargs)

        fail_once.calls = 0

        # Act
        with caplog.at_level(logging.WARNING, logger="celery_rate_limiter.core.base"):
            with (
                patch.object(limiter.redis, "evalsha", side_effect=fail_once) as mock_eval,
                patch.object(
                    limiter.redis, "script_load", side_effect=real_script_load
                ) as mock_load,
            ):
                result = await limiter._eval_script(
                    "health.lua",
                    3,
                    "eval_script_async",
                    "eval_script_async:buffer",
                    "eval_script_async:concurrency",
                    60,
                    5,
                    2,
                )

        # Assert
        assert result is not None, (
            "eval_script should return the script result after recovery"
        )
        assert mock_eval.call_count == 2, (
            "evalsha should be called twice (fail then retry)"
        )
        assert mock_load.call_count == 1, (
            "script_load should be called once to re-register"
        )
        assert any(
            record.levelname == "WARNING"
            and f"limiter={limiter.id}" in record.message
            and "script=health.lua" in record.message
            for record in caplog.records
        ), "should emit a warning log containing the limiter id and script name on async NOSCRIPT recovery"

    @staticmethod
    async def test_eval_script_raises_runtime_error_on_permanent_noscript(
        limiter,
    ):
        """Verify that the async ``_eval_script`` raises RuntimeError when the script cannot be retained."""

        # Arrange
        async def always_fail(*args, **kwargs):
            raise redis.exceptions.NoScriptError("Permanent")

        with patch.object(
            limiter.redis,
            "evalsha",
            side_effect=always_fail,
        ) as mock_eval:
            # Act & Assert
            with pytest.raises(
                RuntimeError, match="Redis failed to retain the Lua script"
            ):
                await limiter._eval_script("health.lua", 0)

            assert mock_eval.call_count == 2, (
                "evalsha should be attempted twice before raising"
            )

    @staticmethod
    async def test_eval_script_propagates_non_noscript_errors(limiter):
        """Verify that non-NoScriptError exceptions pass through without retry."""

        # Arrange
        async def connection_error(*args, **kwargs):
            raise redis.exceptions.ConnectionError("redis down")

        with patch.object(
            limiter.redis,
            "evalsha",
            side_effect=connection_error,
        ) as mock_eval:
            # Act & Assert
            with pytest.raises(redis.exceptions.ConnectionError, match="redis down"):
                await limiter._eval_script("health.lua", 0)

            assert mock_eval.call_count == 1, (
                "evalsha should not retry on non-NoScriptError exceptions"
            )


class WindowChangeLoggingTests:
    """Unified tests for the window-change detection log in ``_apply_config_overrides``.

    Subclasses must provide a ``limiter`` fixture that returns either a
    ``SyncToAsyncLimiterAdapter``-wrapped sync limiter or a native async limiter.
    """

    @staticmethod
    async def test_window_change_emits_info_log(limiter, caplog):
        """Verify that changing the window via ``_apply_config_overrides`` emits an INFO log."""
        # Arrange
        old_window = limiter.window
        new_window = old_window * 2

        # Act
        with caplog.at_level(logging.INFO, logger="celery_rate_limiter.core.base"):
            limiter._apply_config_overrides({"window": new_window})

        # Assert
        assert limiter.window == new_window, (
            "window should be updated to the new value"
        )
        assert any(
            record.levelname == "INFO"
            and f"limiter={limiter.id}" in record.message
            and f"new_window={new_window:g}" in record.message
            and "paused_for_s=" in record.message
            for record in caplog.records
        ), "should emit an info log containing the limiter id, new window, and pause duration"


class TestSyncWindowChangeLogging(WindowChangeLoggingTests):
    """Sync rate limiter window-change logging exercised through the async adapter."""

    @pytest.fixture
    def limiter(self, generic_limiter):
        """Wrap the sync generic limiter in an async adapter."""
        return SyncToAsyncLimiterAdapter(generic_limiter)


class TestAsyncWindowChangeLogging(WindowChangeLoggingTests):
    """Async rate limiter window-change logging exercised natively."""

    @pytest.fixture
    def limiter(self, async_generic_limiter):
        """Provide the async generic limiter directly."""
        return async_generic_limiter


class EmitMetricLoggingTests:
    """Unified tests for the ``_emit_metric`` warning log when the callback raises.

    Subclasses must provide a ``limiter_with_failing_callback`` fixture that
    returns a limiter configured with a callback that raises ``RuntimeError``.
    """

    @staticmethod
    async def test_emit_metric_logs_warning_on_callback_exception(
        limiter_with_failing_callback, caplog
    ):
        """Verify that ``_emit_metric`` emits a WARNING log when the callback raises."""
        # Arrange
        limiter = limiter_with_failing_callback

        # Act
        with caplog.at_level(logging.WARNING, logger="celery_rate_limiter.core.limiters"):
            limiter._emit_metric("consume", {"success": True})

        # Assert
        assert any(
            record.levelname == "WARNING"
            and f"limiter={limiter.id}" in record.message
            and "event=consume" in record.message
            and "callback boom" in record.message
            for record in caplog.records
        ), "should emit a warning log containing the limiter id, event name, and error message"


class TestSyncEmitMetricLogging(EmitMetricLoggingTests):
    """Sync rate limiter ``_emit_metric`` logging exercised through the async adapter."""

    @pytest.fixture
    def limiter_with_failing_callback(self, redis_client, limiter_id):
        """Create a sync limiter with a failing callback, wrapped in the adapter."""
        from tests.implementations.conftest import MinimalRateLimiter

        def failing_callback(event, data):
            raise RuntimeError("callback boom")

        limiter = MinimalRateLimiter(
            redis_client=redis_client,
            limiter_id=f"{limiter_id}_emit_metric_sync",
            limit=5,
            window=60,
            max_concurrency=2,
            metrics_callback=failing_callback,
        )
        yield SyncToAsyncLimiterAdapter(limiter)
        limiter.shutdown()


class TestAsyncEmitMetricLogging(EmitMetricLoggingTests):
    """Async rate limiter ``_emit_metric`` logging exercised natively."""

    @pytest.fixture
    async def limiter_with_failing_callback(self, async_redis_client, limiter_id):
        """Create an async limiter with a failing callback."""
        from tests.implementations.conftest import MinimalAsyncRateLimiter

        def failing_callback(event, data):
            raise RuntimeError("callback boom")

        limiter = MinimalAsyncRateLimiter(
            redis_client=async_redis_client,
            limiter_id=f"{limiter_id}_emit_metric_async",
            limit=5,
            window=60,
            max_concurrency=2,
            metrics_callback=failing_callback,
        )
        await limiter.start()
        yield limiter
        await limiter.shutdown()


class InitialDefaultTests:
    """Unified tests for the initial default values of freshly constructed limiters.

    Subclasses must provide a ``limiter`` fixture that returns either a
    ``SyncToAsyncLimiterAdapter``-wrapped sync limiter or a native async limiter.
    """

    @staticmethod
    async def test_fresh_limiter_has_expected_defaults(limiter):
        """Verify that a freshly constructed limiter exposes the correct initial defaults."""
        # Assert
        assert limiter._paused_until == pytest.approx(0.0), (
            "fresh limiter should not be paused"
        )
        assert limiter._config_version == 0, (
            "fresh limiter should start at config version 0"
        )
        assert limiter.jitter_enabled is True, (
            "fresh limiter should have jitter enabled by default"
        )
        assert limiter.jitter_min_pct == pytest.approx(0.02), (
            "fresh limiter should default to jitter_min_pct of 0.02"
        )
        assert limiter.jitter_max_pct == pytest.approx(0.08), (
            "fresh limiter should default to jitter_max_pct of 0.08"
        )
        assert limiter.drain_enabled is True, (
            "fresh limiter should have draining enabled by default"
        )


class TestSyncInitialDefaults(InitialDefaultTests):
    """Sync rate limiter initial defaults exercised through the async adapter."""

    @pytest.fixture
    def limiter(self, generic_limiter):
        """Wrap the sync generic limiter in an async adapter."""
        return SyncToAsyncLimiterAdapter(generic_limiter)


class TestAsyncInitialDefaults(InitialDefaultTests):
    """Async rate limiter initial defaults exercised natively."""

    @pytest.fixture
    def limiter(self, async_generic_limiter):
        """Provide the async generic limiter directly."""
        return async_generic_limiter
