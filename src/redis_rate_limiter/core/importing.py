import importlib
import logging
from typing import Any, Callable, cast

logger = logging.getLogger(__name__)


def import_string(import_path: str) -> Callable[..., Any]:
    """Resolve a dotted import path of the form 'module.submodule.callable' into the corresponding callable object."""
    module_path, func_name = import_path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    func = getattr(module, func_name)
    if not callable(func):
        raise TypeError(f"Object at {import_path} is not callable.")

    logger.debug(
        "[ImportResolver] Dynamic import resolved: import_path=%s, module=%s, callable=%s.",
        import_path,
        module_path,
        func_name,
    )

    # noinspection PyUnnecessaryCast
    # This cast is, in fact, necessary to satisfy mypy type validation.
    # fmt: off
    return cast(  # pragma: no mutate
        Callable[..., Any], func
    )
    # fmt: on


def resolve_import_path(fn: Callable[..., Any]) -> str:
    """Derive the dotted import path for a callable so it can be passed to ``schedule_task``.

    This is the inverse of ``import_string``: given a module-level function or
    class, it returns a string of the form ``"module.submodule.name"`` that
    ``import_string`` can later resolve back to the same object.

    Only module-level named callables are supported. Lambdas, closures, nested
    functions, bound methods, and objects without ``__module__``/``__qualname__``
    (e.g., ``functools.partial``) are rejected with a ``ValueError``.

    Args:
        fn: The callable to resolve.

    Returns:
        The dotted import path string.

    Raises:
        ValueError: If the callable cannot be expressed as an importable dotted path.
    """
    module = getattr(fn, "__module__", None)
    qualname = getattr(fn, "__qualname__", None)

    if module is None or qualname is None:
        raise ValueError(
            f"Cannot resolve import path: {fn!r} is missing __module__ or __qualname__."
        )

    if "<lambda>" in qualname:
        raise ValueError(f"Cannot resolve import path for lambda: {fn!r}.")

    if "<locals>" in qualname:
        raise ValueError(
            f"Cannot resolve import path for nested function or closure: {fn!r} "
            f"(qualname={qualname!r})."
        )

    if "." in qualname:
        raise ValueError(
            f"Cannot resolve import path for class-bound callable: {fn!r} "
            f"(qualname={qualname!r}). Only module-level names are supported."
        )

    import_path = f"{module}.{qualname}"

    # Round-trip verification: ensure the derived path actually resolves back
    # to the same object.
    resolved = import_string(import_path)
    if resolved is not fn:
        raise ValueError(
            f"Round-trip verification failed for {fn!r}: "
            f"import_string({import_path!r}) resolved to {resolved!r}, "
            f"which is not the same object."
        )

    logger.debug(
        "[ImportResolver] Callable resolved to import path: callable=%r, import_path=%s.",
        fn,
        import_path,
    )

    return import_path
