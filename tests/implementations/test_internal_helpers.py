"""Tests for the internal helper methods of the rate limiter.

This module tests internal implementation details such as Lua script loading
via ``_register_script``, task data formatting, and other utility methods.
"""

from unittest.mock import patch

import pytest

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
    def test_cleanup_inflight_key_suppresses_redis_failure(generic_limiter):
        """Verify that ``_cleanup_inflight_key`` does not propagate Redis exceptions."""
        # Arrange
        inflight_key = f"{generic_limiter.id}:inflight:cleanup-test"

        # Act & Assert
        with patch.object(
            generic_limiter.redis, "delete", side_effect=ConnectionError("redis down")
        ):
            # This invocation must not raise.
            generic_limiter._cleanup_inflight_key(inflight_key, "cleanup-test")


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
