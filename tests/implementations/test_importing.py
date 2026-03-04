"""Tests for the core import-string utility behavior."""

import functools
import json
import logging

import pytest

from redis_rate_limiter import import_string, resolve_import_path
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
        with pytest.raises(
            ModuleNotFoundError, match="No module named 'missing_module_for_tests'"
        ):
            import_string("missing_module_for_tests.function_name")

    @staticmethod
    def test_import_string_raises_on_missing_attribute():
        """Verify that ``import_string()`` raises an AttributeError for a missing attribute."""
        # Act & Assert
        with pytest.raises(
            AttributeError,
            match="module 'json' has no attribute 'this_attribute_does_not_exist'",
        ):
            import_string("json.this_attribute_does_not_exist")

    @staticmethod
    @pytest.mark.parametrize(
        ("invalid_path", "expected_exception", "expected_match"),
        [
            ("", ValueError, "not enough values to unpack"),
            ("no_dot_path", ValueError, "not enough values to unpack"),
            ("  json.dumps  ", ModuleNotFoundError, "No module named"),
        ],
        ids=["empty_string", "no_dot", "whitespace_padded"],
    )
    def test_import_string_raises_on_malformed_path(
        invalid_path: str,
        expected_exception: type[Exception],
        expected_match: str,
    ):
        """Verify that ``import_string()`` raises an exception on structurally invalid import paths."""
        # Act & Assert
        with pytest.raises(expected_exception, match=expected_match):
            import_string(invalid_path)


class TestResolveImportPath:
    """Test suite for ``resolve_import_path()`` behavior."""

    @staticmethod
    def test_resolves_module_level_function():
        """Verify that a module-level function resolves to its dotted import path."""
        # Act
        result = resolve_import_path(json.dumps)

        # Assert
        assert result == "json.dumps", (
            "resolve_import_path should return the dotted path for json.dumps"
        )

    @staticmethod
    def test_resolves_module_level_class():
        """Verify that a module-level class resolves to its dotted import path.

        Note: ``__module__`` reflects the defining module, not the re-export.
        ``json.JSONEncoder`` is defined in ``json.encoder``, so the resolved
        path is ``json.encoder.JSONEncoder``, not ``json.JSONEncoder``.
        """
        # Act
        result = resolve_import_path(json.JSONEncoder)

        # Assert
        assert result == "json.encoder.JSONEncoder", (
            "resolve_import_path should return the defining module path for json.JSONEncoder"
        )

    @staticmethod
    def test_round_trip_with_import_string():
        """Verify that the resolved path round-trips through ``import_string``."""
        # Arrange
        original = json.dumps

        # Act
        path = resolve_import_path(original)
        resolved = import_string(path)

        # Assert
        assert resolved is original, (
            "import_string of the resolved path should return the same object"
        )

    @staticmethod
    def test_rejects_lambda():
        """Verify that lambda functions are rejected with a ``ValueError``."""
        # Arrange
        fn = lambda x: x  # noqa: E731

        # Act & Assert
        with pytest.raises(ValueError, match="lambda"):
            resolve_import_path(fn)

    @staticmethod
    def test_rejects_nested_function():
        """Verify that nested (inner) functions are rejected with a ``ValueError``."""

        # Arrange
        def inner_function():
            pass

        # Act & Assert
        with pytest.raises(ValueError, match="nested function or closure"):
            resolve_import_path(inner_function)

    @staticmethod
    def test_rejects_bound_method():
        """Verify that bound methods are rejected with a ``ValueError``."""
        # Arrange
        encoder = json.JSONEncoder()

        # Act & Assert
        with pytest.raises(ValueError, match="class-bound callable"):
            resolve_import_path(encoder.encode)

    @staticmethod
    def test_rejects_static_method_reference():
        """Verify that a reference to a static method via class attribute is rejected."""

        # Arrange
        class Example:
            @staticmethod
            def helper():
                pass

        # Act & Assert
        with pytest.raises(ValueError, match="class-bound callable|nested function"):
            resolve_import_path(Example.helper)

    @staticmethod
    def test_rejects_functools_partial():
        """Verify that ``functools.partial`` objects are rejected with a ``ValueError``."""
        # Arrange
        fn = functools.partial(json.dumps, indent=2)

        # Act & Assert
        with pytest.raises(ValueError, match="missing __module__ or __qualname__"):
            resolve_import_path(fn)

    @staticmethod
    def test_rejects_callable_without_module_attribute():
        """Verify that a callable missing ``__module__`` is rejected."""

        # Arrange
        # Simulate a callable lacking __module__ by wrapping a function and
        # stripping the attribute. Standard classes in CPython 3.12+ have
        # immutable __module__, so a wrapper is necessary.
        def original():
            pass

        class Wrapper:
            """Callable wrapper that deliberately omits __module__."""

            __qualname__ = "Wrapper"

            def __call__(self):
                return original()

        fn = Wrapper()
        # Instance objects do not have __module__ by default; getattr falls
        # through to the class. Override at instance level to hide the class attr.
        fn.__module__ = None  # type: ignore[assignment]

        # Act & Assert
        with pytest.raises(ValueError, match="missing __module__ or __qualname__"):
            resolve_import_path(fn)

    @staticmethod
    def test_rejects_callable_without_qualname_attribute():
        """Verify that a callable missing ``__qualname__`` is rejected."""

        # Arrange
        # Simulate via a simple namespace-like callable that lacks __qualname__.
        class BareCallable:
            """Callable wrapper that deliberately omits __qualname__."""

            __module__ = "some.module"

            def __call__(self):
                pass

        fn = BareCallable()
        fn.__qualname__ = None  # type: ignore[assignment]

        # Act & Assert
        with pytest.raises(ValueError, match="missing __module__ or __qualname__"):
            resolve_import_path(fn)

    @staticmethod
    def test_rejects_callable_with_absent_module_attribute():
        """Verify that a callable whose ``__module__`` attribute is truly absent is rejected with ``ValueError``.

        Mutation target: ``None`` default in ``getattr(fn, "__module__", None)`` in ``_resolve_import_path()``.
        """

        # Arrange
        # CPython 3.12+ makes __module__ immutable on class objects, so
        # ``del`` is not possible. Instead, override ``__getattribute__`` to
        # make __module__ truly absent from getattr's perspective.
        class NoModuleCallable:
            """Callable that hides ``__module__`` via ``__getattribute__``."""

            def __call__(self):
                pass

            def __getattribute__(self, name: str) -> object:
                if name == "__module__":
                    raise AttributeError(name)
                return super().__getattribute__(name)

        fn = NoModuleCallable()

        # Act & Assert
        with pytest.raises(ValueError, match="missing __module__ or __qualname__"):
            resolve_import_path(fn)

    @staticmethod
    def test_rejects_callable_that_fails_round_trip_verification():
        """Verify that a callable whose derived path resolves to a different object is rejected."""

        # Arrange
        # A callable that claims to live at ``json.loads`` but is not ``json.loads``.
        class Impostor:
            """Callable that lies about its module and qualname."""

            __module__ = "json"
            __qualname__ = "loads"

            def __call__(self):
                pass

        # Act & Assert
        with pytest.raises(ValueError, match="Round-trip verification failed"):
            resolve_import_path(Impostor)


# ---------------------------------------------------------------------------
# Observability tests
# ---------------------------------------------------------------------------


class TestImportStringObservability:
    """Observability tests for ``import_string()``."""

    @staticmethod
    def test_import_string_emits_debug_log_for_resolved_import(caplog):
        """Verify that ``import_string()`` emits a debug log with the import path, module, and callable."""
        # Act
        with caplog.at_level(logging.DEBUG, logger="redis_rate_limiter.core.importing"):
            import_string("json.dumps")

        # Assert
        assert_log_emitted(
            caplog.records,
            "DEBUG",
            ["import_path=json.dumps", "module=json", "callable=dumps"],
            "should emit a debug log for the resolved import with import path, module, and callable",
        )


class TestResolveImportPathObservability:
    """Observability tests for ``resolve_import_path()``."""

    @staticmethod
    def test_emits_debug_log_on_success(caplog):
        """Verify that ``resolve_import_path()`` emits a debug log with the resolved path."""
        # Act
        with caplog.at_level(logging.DEBUG, logger="redis_rate_limiter.core.importing"):
            resolve_import_path(json.dumps)

        # Assert
        assert_log_emitted(
            caplog.records,
            "DEBUG",
            ["callable=", "import_path=json.dumps"],
            "should emit a debug log with the callable and resolved import path",
        )
