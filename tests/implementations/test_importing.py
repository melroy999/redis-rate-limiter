"""Tests for core import-string utility behavior."""

import json

import pytest

from celery_rate_limiter import import_string


class TestImportString:
    """Test suite for import_string() behavior."""

    def test_import_string_resolves_valid_function(self):
        """Verify import_string resolves valid callable import path."""
        # Act
        resolved = import_string("json.dumps")

        # Assert
        assert resolved is json.dumps, (
            "import_string should resolve json.dumps callable"
        )

    def test_import_string_raises_type_error_for_non_callable(self):
        """Verify import_string raises TypeError for non-callable targets."""
        # Act & Assert
        with pytest.raises(TypeError, match="not callable"):
            import_string("json.__doc__")

    def test_import_string_raises_on_invalid_module(self):
        """Verify import_string raises ModuleNotFoundError for missing module."""
        # Act & Assert
        with pytest.raises(ModuleNotFoundError):
            import_string("missing_module_for_tests.function_name")

    def test_import_string_raises_on_missing_attribute(self):
        """Verify import_string raises AttributeError for missing attribute."""
        # Act & Assert
        with pytest.raises(AttributeError):
            import_string("json.this_attribute_does_not_exist")

    @pytest.mark.parametrize(
        ("invalid_path", "expected_exception"),
        [
            ("", ValueError),
            ("no_dot_path", ValueError),
            ("  json.dumps  ", ModuleNotFoundError),
        ],
        ids=["empty_string", "no_dot", "whitespace_padded"],
    )
    def test_import_string_raises_on_malformed_path(
        self,
        invalid_path: str,
        expected_exception: type[Exception],
    ):
        """Verify import_string raises on structurally invalid import paths."""
        # Act & Assert
        with pytest.raises(expected_exception):
            import_string(invalid_path)
