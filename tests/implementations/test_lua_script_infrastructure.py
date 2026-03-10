"""Tests for Lua script loading, fallback, and NOSCRIPT eval recovery.

This module covers the ``_register_script`` caching and fallback mechanism,
the ``load_lua_script`` resource loader, and the ``_eval_script`` NOSCRIPT
recovery path for both sync and async implementations.

Fixture dependencies:
    - ``redis_client``, ``async_redis_client``: from ``tests/conftest.py``.
    - ``stub_limiter``: from ``tests/implementations/conftest.py``.
"""

import logging
from unittest.mock import patch

import pytest
import redis

from redis_rate_limiter.core.base import (
    AbstractAsyncRateLimiter,
    AbstractSyncRateLimiter,
)
from redis_rate_limiter.core.scripts import DEFAULT_RESOURCE_PACKAGES, load_lua_script
from tests.helpers.utils import assert_log_emitted

# ---------------------------------------------------------------------------
# Behavioral tests
# ---------------------------------------------------------------------------


@pytest.mark.behavior
@pytest.mark.parametrize(
    "lua_script",
    [
        "schedule.lua",
        # A fictional non-existent file for testing the failure path.
        "missing.lua",
    ],
    ids=["existing_script", "missing_script"],
)
class TestScriptRegistration:
    """Tests for ``_register_script`` caching and error handling."""

    @staticmethod
    def test_register_script_uses_cached_source(stub_limiter, lua_script):
        """Verify that script sources are loaded from disk only
        once and subsequently served from the cache.
        """
        # Arrange
        existing_content = "return 1"
        stub_limiter._script_sources[lua_script] = existing_content

        # Act
        with patch("redis_rate_limiter.core.scripts.resources.files") as mock_files:
            stub_limiter._register_script(lua_script)

            # Assert
            mock_files.assert_not_called()

        assert stub_limiter._script_sources[lua_script] == existing_content, (
            "cached script content should not be modified"
        )

    @staticmethod
    def test_register_script_raises_import_error_on_missing_source(
        stub_limiter, lua_script
    ):
        """Verify that an ImportError is raised when the Lua
        script cannot be loaded from any resource package.
        """
        # Arrange
        stub_limiter._script_sources.pop(lua_script, None)

        # Act & Assert
        with patch(
            "redis_rate_limiter.core.scripts.resources.files",
            side_effect=FileNotFoundError("File system error"),
        ) as mock_files:
            with pytest.raises(ImportError, match=f"Could not load {lua_script}"):
                stub_limiter._register_script(lua_script)

            # Verify that all configured package candidates were attempted.
            assert mock_files.call_count == len(DEFAULT_RESOURCE_PACKAGES), (
                "resource loader should try each configured package candidate"
            )


@pytest.mark.behavior
class TestScriptLoaderFallback:
    """Tests for the Lua script loader fallback mechanism."""

    @staticmethod
    def test_load_lua_script_falls_back_to_second_package(stub_limiter):
        """Verify that the script loader succeeds via the second
        package when the first raises ModuleNotFoundError.
        """
        # Arrange
        stub_limiter._script_sources.pop("schedule.lua", None)

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
            "redis_rate_limiter.core.scripts.resources.files",
            side_effect=selective_files,
        ):
            stub_limiter._register_script("schedule.lua")

        # Assert
        assert "schedule.lua" in stub_limiter._script_sources, (
            "script source should be loaded after fallback to second package"
        )
        assert call_count["n"] == 2, (
            "resource loader should be called twice (first fails, second succeeds)"
        )

    @staticmethod
    def test_load_lua_script_error_lists_all_packages_comma_separated():
        """Verify that the ImportError message joins per-package
        errors with a comma separator.
        """
        # Arrange
        packages = ("fake.package.alpha", "fake.package.beta")

        # Act
        with pytest.raises(ImportError) as exc_info:
            load_lua_script("nonexistent.lua", resource_packages=packages)

        # Assert
        message = str(exc_info.value)
        assert "fake.package.alpha" in message, (
            "error message should mention the first package attempted"
        )
        assert "fake.package.beta" in message, (
            "error message should mention the second package attempted"
        )
        assert ", fake.package.beta" in message, (
            "per-package errors should be joined by a comma-space separator"
        )


class _BareSyncLimiter(AbstractSyncRateLimiter):
    """Subclass of the sync base without mixin logic,
    used to test ``_eval_script`` in isolation.
    """

    pass


class _BareAsyncLimiter(AbstractAsyncRateLimiter):
    """Subclass of the async base without mixin logic,
    used to test ``_eval_script`` in isolation.
    """

    pass


@pytest.mark.behavior
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
    def test_eval_script_recovers_from_transient_noscript(limiter):
        """Verify that ``_eval_script`` re-registers and
        retries on a single NoScriptError.
        """
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
        """Verify that ``_eval_script`` raises RuntimeError
        when the script cannot be retained.
        """
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


@pytest.mark.behavior
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
    async def test_eval_script_recovers_from_transient_noscript(limiter):
        """Verify that the async ``_eval_script`` re-registers
        and retries on a single NoScriptError.
        """
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
        """Verify that the async ``_eval_script`` raises
        RuntimeError when the script cannot be retained.
        """

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


# ---------------------------------------------------------------------------
# Observability tests
# ---------------------------------------------------------------------------


@pytest.mark.observability
class TestScriptLoaderFallbackObservability:
    """Observability tests for the Lua script loader debug log emission."""

    @staticmethod
    def test_load_lua_script_logs_resource_package_on_success(caplog):
        """Verify that the debug log includes the resolved resource package name."""
        # Act
        with caplog.at_level(logging.DEBUG, logger="redis_rate_limiter.core.scripts"):
            load_lua_script("schedule.lua")

        # Assert
        # The log message must include both the script name and the package that
        # resolved it. Removing the resource_package argument from the logger.debug
        # call would cause the package name to be absent from the formatted message.
        debug_records = [
            r
            for r in caplog.records
            if r.levelname == "DEBUG" and "schedule.lua" in r.getMessage()
        ]
        assert len(debug_records) == 1, (
            "exactly one debug log should be emitted for a successful script load"
        )
        message = debug_records[0].getMessage()
        assert any(pkg in message for pkg in DEFAULT_RESOURCE_PACKAGES), (
            f"debug log must include the resolved resource package name, got: {message}"
        )


@pytest.mark.observability
class TestSyncEvalScriptObservability:
    """Observability tests for sync ``_eval_script`` NOSCRIPT recovery log emission."""

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
    def test_noscript_recovery_emits_warning_log(limiter, caplog):
        """Verify that NOSCRIPT recovery emits a WARNING log
        with limiter id and script name.
        """
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
        with caplog.at_level(logging.WARNING, logger="redis_rate_limiter.core.base"):
            with (
                patch.object(limiter.redis, "evalsha", side_effect=fail_once),
                patch.object(
                    limiter.redis, "script_load", side_effect=real_script_load
                ),
            ):
                limiter._eval_script(
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
        assert_log_emitted(
            caplog.records,
            level="WARNING",
            required_fragments=[f"limiter={limiter.id}", "script=health.lua"],
            message=(
                "should emit a warning log containing the limiter"
                " id and script name on NOSCRIPT recovery"
            ),
        )


@pytest.mark.observability
class TestAsyncEvalScriptObservability:
    """Observability tests for async ``_eval_script`` NOSCRIPT recovery log emission."""

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
    async def test_noscript_recovery_emits_warning_log(limiter, caplog):
        """Verify that async NOSCRIPT recovery emits a WARNING
        log with limiter id and script name.
        """
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
        with caplog.at_level(logging.WARNING, logger="redis_rate_limiter.core.base"):
            with (
                patch.object(limiter.redis, "evalsha", side_effect=fail_once),
                patch.object(
                    limiter.redis, "script_load", side_effect=real_script_load
                ),
            ):
                await limiter._eval_script(
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
        assert_log_emitted(
            caplog.records,
            level="WARNING",
            required_fragments=[f"limiter={limiter.id}", "script=health.lua"],
            message=(
                "should emit a warning log containing the limiter"
                " id and script name on async NOSCRIPT recovery"
            ),
        )
