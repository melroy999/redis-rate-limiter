"""Tests for the core import-string utility behavior."""

import json
import logging

import pytest

from celery_rate_limiter import import_string
from tests.helpers.utils import assert_log_emitted


class TestImportString:
    """Test suite for ``import_string()`` behavior."""

    @staticmethod
    def test_import_string_resolves_valid_function():
        """Verify that ``import_string()`` resolves a valid callable import path."""
        # Act
        resolved = import_string("json.dumps")

        # Assert
        assert resolved is json.dumps, (
            "import_string should resolve json.dumps callable"
        )

    @staticmethod
    def test_import_string_raises_type_error_for_non_callable():
        """Verify that ``import_string()`` raises a TypeError for non-callable targets."""
        # Act & Assert
        with pytest.raises(TypeError, match="not callable"):
            import_string("json.__doc__")

    @staticmethod
    def test_import_string_raises_on_invalid_module():
        """Verify that ``import_string()`` raises a ModuleNotFoundError for a missing module."""
        # Act & Assert
        with pytest.raises(ModuleNotFoundError):
            import_string("missing_module_for_tests.function_name")

    @staticmethod
    def test_import_string_raises_on_missing_attribute():
        """Verify that ``import_string()`` raises an AttributeError for a missing attribute."""
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
    @staticmethod
    def test_import_string_raises_on_malformed_path(
        invalid_path: str,
        expected_exception: type[Exception],
    ):
        """Verify that ``import_string()`` raises an exception on structurally invalid import paths."""
        # Act & Assert
        with pytest.raises(expected_exception):
            import_string(invalid_path)


# ---------------------------------------------------------------------------
# Observability tests
# ---------------------------------------------------------------------------


class TestImportStringObservability:
    """Observability tests for ``import_string()``."""

    @staticmethod
    def test_import_string_emits_debug_log_for_resolved_import(caplog):
        """Verify that ``import_string()`` emits a debug log with the import path, module, and callable."""
        # Act
        with caplog.at_level(
            logging.DEBUG, logger="celery_rate_limiter.core.importing"
        ):
            import_string("json.dumps")

        # Assert
        assert_log_emitted(
            caplog.records,
            "DEBUG",
            ["import_path=json.dumps", "module=json", "callable=dumps"],
            "should emit a debug log for the resolved import with import path, module, and callable",
        )
