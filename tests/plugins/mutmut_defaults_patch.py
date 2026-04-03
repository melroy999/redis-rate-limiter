"""Pytest plugin that patches trampoline ``__defaults__`` for mutmut default parameter mutations.

Problem
-------

Mutmut 3.5.0 uses a trampoline dispatch architecture. For each mutable function, it generates:

1. A **trampoline** that preserves the original function signature (including default parameter
   values) and dispatches to the original or mutant variant based on ``MUTANT_UNDER_TEST``.
2. **Mutant variants** (e.g., ``x_ǁDrainLoopǁwake__mutmut_1``) that are separate ``def``
   statements with the mutation applied.

When a mutation targets a default parameter value (e.g., ``delay: float = 0.0`` to
``delay: float = 1.0``), the mutant variant's ``__defaults__`` tuple contains the mutated value.
However, all calls go through the trampoline, which still has the original ``__defaults__``. When
a caller omits the argument, Python resolves the default from the trampoline's signature, then
passes it explicitly to the mutant, bypassing the mutant's own default entirely.

This makes default parameter mutations structurally undetectable by both behavioral tests and
``inspect.signature`` assertion tests.

Solution
--------

This plugin runs at ``pytest_configure`` time in mutmut child forks. It identifies the active
mutant function object via the ``__mutmut_mutants`` class/module variable, then copies the
mutant's ``__defaults__`` and ``__kwdefaults__`` onto the corresponding trampoline function. This
allows:

- ``inspect.signature`` tests to observe the mutated default value (and fail, killing the mutant).
- Behavioral tests that call the function without the defaulted argument to receive the mutated
  value, potentially causing assertion failures.

Scope
-----

This plugin is a no-op outside of mutmut child fork processes. It activates only when
``MUTANT_UNDER_TEST`` is set to a value other than ``stats`` or ``fail``.
"""

from __future__ import annotations

import logging
import os
import sys
from types import FunctionType, ModuleType

logger = logging.getLogger(__name__)

# Unicode separator used by mutmut to mangle class method names.
# See ``mutmut.trampoline_templates.CLASS_NAME_SEPARATOR``.
_CLASS_NAME_SEP = "ǁ"


def pytest_configure(config: object) -> None:
    """Patch trampoline defaults if running inside a mutmut child fork."""
    mutant_id = os.environ.get("MUTANT_UNDER_TEST", "")
    if not mutant_id or mutant_id in ("stats", "fail"):
        return
    _patch_trampoline_defaults(mutant_id)


def _patch_trampoline_defaults(mutant_id: str) -> None:
    """Find the active mutant's function object and copy its defaults onto the trampoline.

    Args:
        mutant_id: The ``MUTANT_UNDER_TEST`` environment variable value.
            Format for class methods: ``module.path.xǁClassǁmethod__mutmut_N``
            Format for top-level functions: ``module.path.x_method__mutmut_N``
    """
    # Find the longest matching module name. Multiple modules share a common prefix
    # (e.g., ``redis_rate_limiter``, ``redis_rate_limiter.backends.asgi``,
    # ``redis_rate_limiter.backends.asgi.middleware``), and all would match as prefixes
    # of the mutant ID. Only the longest (most specific) prefix yields the correct
    # local name for class/function lookup.
    best_mod: ModuleType | None = None
    best_prefix = ""

    for mod_name, mod in sys.modules.items():
        if mod is None or not mod_name.startswith("redis_rate_limiter"):
            continue
        prefix = mod_name + "."
        if mutant_id.startswith(prefix) and len(prefix) > len(best_prefix):
            best_mod = mod
            best_prefix = prefix

    if best_mod is None:
        return

    local_name = mutant_id[len(best_prefix) :]

    if _CLASS_NAME_SEP in local_name:
        _patch_class_method(best_mod, local_name)
    else:
        _patch_top_level_function(best_mod, local_name)


def _patch_class_method(mod: ModuleType, local_name: str) -> None:
    """Patch a class method trampoline with the mutant's defaults.

    Args:
        mod: The module containing the class.
        local_name: The local portion of the mutant ID (after the module prefix).
            Format: ``xǁClassNameǁmethod_name__mutmut_N``
    """
    parts = local_name.split(_CLASS_NAME_SEP)
    if len(parts) != 3:
        logger.debug(
            "mutmut_defaults_patch: unexpected mangled name format, parts=%s",
            parts,
        )
        return

    class_name = parts[1]
    method_and_suffix = parts[2]
    method_name = method_and_suffix.split("__mutmut_")[0]

    cls = getattr(mod, class_name, None)
    if cls is None:
        logger.debug(
            "mutmut_defaults_patch: class not found, module=%s, class=%s",
            mod.__name__,
            class_name,
        )
        return

    # Locate the mutants dictionary (a ClassVar on the class).
    # Note: mutmut uses "xǁClassǁmethod" for class methods (no underscore after x).
    mangled_base = f"x{_CLASS_NAME_SEP}{class_name}{_CLASS_NAME_SEP}{method_name}"
    mutants_attr = f"{mangled_base}__mutmut_mutants"
    mutants_dict = getattr(cls, mutants_attr, None)

    if not isinstance(mutants_dict, dict) or local_name not in mutants_dict:
        logger.debug(
            "mutmut_defaults_patch: mutants dict not found or missing key, "
            "attr=%s, local_name=%s",
            mutants_attr,
            local_name,
        )
        return

    mutant_func = mutants_dict[local_name]
    trampoline = _resolve_function(cls, method_name)

    if trampoline is None:
        logger.debug(
            "mutmut_defaults_patch: trampoline not found, class=%s, method=%s",
            class_name,
            method_name,
        )
        return

    _copy_defaults(trampoline, mutant_func, label=f"{class_name}.{method_name}")


def _patch_top_level_function(mod: ModuleType, local_name: str) -> None:
    """Patch a top-level function trampoline with the mutant's defaults.

    Args:
        mod: The module containing the function.
        local_name: The local portion of the mutant ID (after the module prefix).
            Format: ``x_func_name__mutmut_N``
    """
    mangled_base = local_name.split("__mutmut_")[0]
    func_name = mangled_base.removeprefix("x_")

    mutants_attr = f"{mangled_base}__mutmut_mutants"
    mutants_dict = getattr(mod, mutants_attr, None)

    if not isinstance(mutants_dict, dict) or local_name not in mutants_dict:
        logger.debug(
            "mutmut_defaults_patch: mutants dict not found or missing key, "
            "attr=%s, local_name=%s",
            mutants_attr,
            local_name,
        )
        return

    mutant_func = mutants_dict[local_name]
    trampoline = getattr(mod, func_name, None)

    if trampoline is None or not isinstance(trampoline, FunctionType):
        logger.debug(
            "mutmut_defaults_patch: trampoline not found, module=%s, func=%s",
            mod.__name__,
            func_name,
        )
        return

    _copy_defaults(trampoline, mutant_func, label=f"{mod.__name__}.{func_name}")


def _resolve_function(cls: type, method_name: str) -> FunctionType | None:
    """Resolve a class method name to its underlying function object.

    Handles regular methods, ``@staticmethod``, and ``@classmethod`` descriptors
    by unwrapping to the raw ``FunctionType`` whose ``__defaults__`` can be patched.

    Args:
        cls: The class to look up the method on.
        method_name: The unmangled method name.

    Returns:
        The underlying ``FunctionType``, or ``None`` if not found.
    """
    # Check the class __dict__ directly to avoid descriptor protocol invocation,
    # which would give us bound methods for classmethods.
    raw = cls.__dict__.get(method_name)

    if raw is None:
        # Fall back to MRO lookup.
        raw = getattr(cls, method_name, None)

    if isinstance(raw, (staticmethod, classmethod)):
        raw = raw.__func__

    if isinstance(raw, FunctionType):
        return raw

    return None


def _copy_defaults(
    trampoline: FunctionType,
    mutant: object,
    label: str,
) -> None:
    """Copy ``__defaults__`` and ``__kwdefaults__`` from the mutant onto the trampoline.

    Args:
        trampoline: The trampoline function whose defaults will be overwritten.
        mutant: The mutant function whose defaults will be read.
        label: A human-readable label for debug logging.
    """
    mutant_defaults = getattr(mutant, "__defaults__", None)
    mutant_kwdefaults = getattr(mutant, "__kwdefaults__", None)

    patched = False

    if mutant_defaults is not None and mutant_defaults != trampoline.__defaults__:
        logger.debug(
            "mutmut_defaults_patch: patching __defaults__ for %s: %s -> %s",
            label,
            trampoline.__defaults__,
            mutant_defaults,
        )
        trampoline.__defaults__ = mutant_defaults
        patched = True

    if mutant_kwdefaults is not None and mutant_kwdefaults != getattr(
        trampoline, "__kwdefaults__", None
    ):
        logger.debug(
            "mutmut_defaults_patch: patching __kwdefaults__ for %s: %s -> %s",
            label,
            getattr(trampoline, "__kwdefaults__", None),
            mutant_kwdefaults,
        )
        trampoline.__kwdefaults__ = mutant_kwdefaults
        patched = True

    if not patched:
        logger.debug(
            "mutmut_defaults_patch: no default difference for %s (mutation targets body, not defaults)",
            label,
        )
