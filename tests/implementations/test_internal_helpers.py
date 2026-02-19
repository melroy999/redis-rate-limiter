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
            and generic_limiter.id in record.message
            and "cleanup-test" in record.message
            and inflight_key in record.message
            for record in caplog.records
        ), "should emit a warning log containing the limiter id, task id, and inflight key"

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
            and generic_limiter.id in record.message
            and "cleanup-del" in record.message
            and inflight_key in record.message
            for record in caplog.records
        ), "should emit a debug log containing the limiter id, task id, and inflight key"

    @staticmethod
    def test_cleanup_inflight_key_handles_missing_key_gracefully(generic_limiter):
        """Verify that ``_cleanup_inflight_key`` does not raise when the key does not exist."""
        # Arrange
        inflight_key = f"{generic_limiter.id}:inflight:nonexistent"

        # Act & Assert
        # This invocation must not raise.
        generic_limiter._cleanup_inflight_key(inflight_key, "nonexistent")


class TestTokenRecoveryDelay:
    """Tests for the sliding-window token recovery delay calculation."""

    @staticmethod
    def test_token_recovery_returns_fractional_wait_when_decay_applies(generic_limiter):
        """Verify that a positive fractional delay is returned when previous-window decay can free a token."""
        # Arrange
        # A 1-second window is used so the arithmetic is straightforward to verify.
        generic_limiter.window = 1.0
        generic_limiter.limit = 5

        # Act
        delay = generic_limiter._calculate_token_recovery_delay(
            val_previous=5, val_current=3, reset_in_ms=500
        )

        # Assert
        assert delay > 0, (
            "delay should be positive when decay has not yet freed a token"
        )
        assert delay < 1.0, "delay should be less than the full window"

    @staticmethod
    def test_token_recovery_returns_immediate_when_decay_already_freed_token(
        generic_limiter,
    ):
        """Verify that an immediate retry is returned when previous-window decay has already freed a token."""
        # Arrange
        generic_limiter.window = 1.0
        generic_limiter.limit = 5

        # Act
        delay = generic_limiter._calculate_token_recovery_delay(
            val_previous=10, val_current=1, reset_in_ms=100
        )

        # Assert
        assert delay == 0.001, (
            "delay should be 0.001 when decay has already freed a token"
        )

    @staticmethod
    def test_token_recovery_fallback_when_val_previous_is_zero(generic_limiter):
        """Verify that the delay falls back to reset_in_ms when the previous window has no requests."""
        # Arrange
        generic_limiter.window = 1.0
        generic_limiter.limit = 5

        # Act
        delay = generic_limiter._calculate_token_recovery_delay(
            val_previous=0, val_current=3, reset_in_ms=500
        )

        # Assert
        # Fallback formula: reset_in_ms / 1000.0 + 0.001 = 0.501
        assert delay == pytest.approx(0.501), (
            "delay should equal reset_in_ms / 1000 + 0.001 when val_previous is zero"
        )

    @staticmethod
    def test_token_recovery_fallback_when_val_current_equals_limit(generic_limiter):
        """Verify that the delay falls back to reset_in_ms when the current window is at the limit."""
        # Arrange
        generic_limiter.window = 1.0
        generic_limiter.limit = 5

        # Act
        delay = generic_limiter._calculate_token_recovery_delay(
            val_previous=5, val_current=5, reset_in_ms=500
        )

        # Assert
        # Fallback formula: reset_in_ms / 1000.0 + 0.001 = 0.501
        assert delay == pytest.approx(0.501), (
            "delay should equal reset_in_ms / 1000 + 0.001 when val_current equals limit"
        )

    @staticmethod
    def test_token_recovery_primary_path_exact_value(generic_limiter):
        """Verify the exact delay value computed via the primary decay formula."""
        # Arrange
        generic_limiter.window = 1.0
        generic_limiter.limit = 5

        # Act
        delay = generic_limiter._calculate_token_recovery_delay(
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
    def test_token_recovery_primary_path_floor_when_wait_ms_zero(generic_limiter):
        """Verify that the 0.001 floor is returned when wait_ms is exactly zero."""
        # Arrange
        generic_limiter.window = 1.0
        generic_limiter.limit = 5

        # Act
        # t_needed_ms = 1000 * (1.0 - (5 - 3) / 5) = 600
        # time_passed_ms = 1000 - 400 = 600
        # wait_ms = 600 - 600 = 0 (<= 0)
        delay = generic_limiter._calculate_token_recovery_delay(
            val_previous=5, val_current=3, reset_in_ms=400
        )

        # Assert
        assert delay == 0.001, (
            "delay should be 0.001 when wait_ms is exactly zero"
        )


class _MinimalSyncLimiter(AbstractSyncRateLimiter):
    """Bare subclass that exposes ``_eval_script`` without any mixin logic."""

    pass


class _MinimalAsyncLimiter(AbstractAsyncRateLimiter):
    """Bare async subclass that exposes ``_eval_script`` without any mixin logic."""

    pass


class TestSyncEvalScript:
    """Tests for ``AbstractSyncRateLimiter._eval_script`` NOSCRIPT recovery."""

    @pytest.fixture
    def limiter(self, redis_client):
        """Create a minimal sync limiter with a pre-registered script."""
        _limiter = _MinimalSyncLimiter(
            redis_client=redis_client,
            limiter_id="eval_script_sync",
            limit=5,
            window=60,
        )
        _limiter._register_script("health.lua")
        return _limiter

    @staticmethod
    def test_eval_script_recovers_from_transient_noscript(limiter):
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
        _limiter = _MinimalAsyncLimiter(
            redis_client=async_redis_client,
            limiter_id="eval_script_async",
            limit=5,
            window=60,
        )
        await _limiter._register_script("health.lua")
        return _limiter

    @staticmethod
    async def test_eval_script_recovers_from_transient_noscript(limiter):
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
