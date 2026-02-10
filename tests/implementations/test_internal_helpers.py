"""Tests for internal helper methods of the rate limiter.

This module tests internal implementation details like Lua script loading,
task data formatting, and other utility methods.
"""

from unittest.mock import patch

import pytest


@pytest.mark.parametrize(
    "lua_script, target_key",
    [
        ("schedule.lua", "_SCHEDULE_LUA_SCRIPT"),
        # Fictional non-existent file.
        ("missing.lua", "_MISSING_LUA_SCRIPT"),
    ],
    ids=["existing_script", "missing_script"],
)
class TestInternalHelpers:
    """Test internal helper methods and Lua script loading."""

    def test_load_lua_script_imports_only_once(self, generic_limiter, lua_script, target_key):
        """Verify Lua scripts are only loaded from disk once (cached)."""
        # Arrange
        # Simulate script already loaded.
        existing_content = "return 1"
        setattr(generic_limiter, target_key, existing_content)

        # Act
        # Mock the resource loader to track the number of calls.
        with patch("src.celery_rate_limiter.limiters.resources.files") as mock_files:
            generic_limiter._load_lua_script(lua_script=lua_script, key=target_key)

            # Assert
            mock_files.assert_not_called()

        assert getattr(generic_limiter, target_key) == existing_content, (
            "cached script content should not be modified"
        )

    def test_load_lua_script_raises_import_error_on_failure(
        self, generic_limiter, lua_script, target_key
    ):
        """Verify ImportError is raised when Lua script cannot be loaded."""
        # Arrange
        # Ensure the attribute doesn't exist.
        if hasattr(generic_limiter, target_key):
            delattr(generic_limiter, target_key)

        # Act & Assert
        # Mock the resource loader to simulate file system error.
        with patch(
            "src.celery_rate_limiter.limiters.resources.files",
            side_effect=Exception("File system error"),
        ) as mock_files:
            with pytest.raises(ImportError, match=f"Could not load {lua_script}"):
                generic_limiter._load_lua_script(lua_script=lua_script, key=target_key)

            # Verify only one attempt was made.
            mock_files.assert_called_once()


class TestTaskSignature:
    """Tests for task-signature helper behavior."""

    @staticmethod
    def test_task_signature_is_deterministic_across_key_orders(generic_limiter):
        """Verify _get_task_signature_str is deterministic across dict key order."""
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
