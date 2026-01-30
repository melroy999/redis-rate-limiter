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
        ("missing.lua", "_MISSING_LUA_SCRIPT"),  # Fictional non-existent file
    ],
    ids=["existing_script", "missing_script"],
)
class TestInternalHelpers:
    """Test internal helper methods and Lua script loading."""

    def test_load_lua_script_imports_only_once(self, limiter, lua_script, target_key):
        """Verify Lua scripts are only loaded from disk once (cached)."""
        # Arrange - Simulate script already loaded
        existing_content = "return 1"
        setattr(limiter, target_key, existing_content)

        # Act - Mock the resource loader to track the number of calls
        with patch("src.celery_rate_limiter.limiters.resources.files") as mock_files:
            limiter._load_lua_script(lua_script=lua_script, key=target_key)

            # Assert - Script should not be loaded since it already exists
            mock_files.assert_not_called()

        # Assert - Script content should remain unchanged
        assert getattr(limiter, target_key) == existing_content, (
            "Cached script content should not be modified"
        )

    def test_load_lua_script_raises_import_error_on_failure(
        self, limiter, lua_script, target_key
    ):
        """Verify ImportError is raised when Lua script cannot be loaded."""
        # Arrange - Ensure the attribute doesn't exist
        if hasattr(limiter, target_key):
            delattr(limiter, target_key)

        # Act & Assert - Mock the resource loader to simulate file system error
        with patch(
            "src.celery_rate_limiter.limiters.resources.files",
            side_effect=Exception("File system error"),
        ) as mock_files:
            with pytest.raises(ImportError, match=f"Could not load {lua_script}"):
                limiter._load_lua_script(lua_script=lua_script, key=target_key)

            # Verify only one attempt was made
            mock_files.assert_called_once()
