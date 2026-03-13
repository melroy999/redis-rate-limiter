#!/usr/bin/env python3
"""Standalone verification script for the mutmut defaults patching mechanism.

This script simulates what mutmut does (trampoline + mutant variant generation) and
verifies that the patching logic in ``mutmut_defaults_patch.py`` correctly copies
mutated ``__defaults__`` onto the trampoline function.

Run directly:  python -m tests.plugins.verify_defaults_patch
Or via pytest:  pytest tests/plugins/verify_defaults_patch.py -v

This does NOT require mutmut, Docker, or Redis.
"""

from __future__ import annotations

import inspect
import sys
import types

# ---------------------------------------------------------------------------
# Simulate mutmut's trampoline code generation
# ---------------------------------------------------------------------------


def _build_simulated_module() -> types.ModuleType:
    """Create a fake module that mimics mutmut's trampoline output.

    Simulates the code mutmut would generate for::

        class DrainLoop:
            def wake(self, delay: float = 0.0) -> None:
                self.last_delay = delay

    With one mutant that changes ``delay: float = 0.0`` to ``delay: float = 1.0``.
    """
    mod = types.ModuleType("fake_redis_rate_limiter.core.limiters")
    mod.__name__ = "redis_rate_limiter.core.limiters"

    # The "original" function body (renamed by mutmut).
    def x_orig(self: object, delay: float = 0.0) -> None:
        self.last_delay = delay  # type: ignore[attr-defined]

    # The "mutant" function body (default changed to 1.0).
    def x_mutant(self: object, delay: float = 1.0) -> None:
        self.last_delay = delay  # type: ignore[attr-defined]

    # The trampoline (preserves original signature, dispatches to orig or mutant).
    def trampoline_wake(self: object, delay: float = 0.0) -> None:
        # In real mutmut, this calls _mutmut_trampoline. We simplify:
        # just call orig (the trampoline always forwards explicitly).
        x_orig(self, delay)

    # Build the class with mutmut's naming convention.
    sep = "ǁ"

    class DrainLoop:
        wake = trampoline_wake

    # Attach the mutants dict as a class variable (mimicking mutmut).
    mutants_attr = f"x{sep}DrainLoop{sep}wake__mutmut_mutants"
    mutants_dict = {f"x{sep}DrainLoop{sep}wake__mutmut_1": x_mutant}
    setattr(DrainLoop, mutants_attr, mutants_dict)

    # Also attach the orig (not strictly needed for the patch, but for completeness).
    orig_attr = f"x{sep}DrainLoop{sep}wake__mutmut_orig"
    setattr(DrainLoop, orig_attr, x_orig)

    mod.DrainLoop = DrainLoop  # type: ignore[attr-defined]
    return mod


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------


def test_defaults_before_patch() -> None:
    """Verify the trampoline has the original defaults before patching."""
    mod = _build_simulated_module()
    sig = inspect.signature(mod.DrainLoop.wake)  # type: ignore[attr-defined]
    assert sig.parameters["delay"].default == 0.0, (
        "trampoline should have original default before patch"
    )


def test_defaults_after_patch() -> None:
    """Verify the trampoline has mutated defaults after patching."""
    mod = _build_simulated_module()
    sep = "ǁ"
    mutant_id = f"redis_rate_limiter.core.limiters.x{sep}DrainLoop{sep}wake__mutmut_1"

    # Inject the fake module into sys.modules so the plugin can find it.
    sys.modules[mod.__name__] = mod
    try:
        from tests.plugins.mutmut_defaults_patch import _patch_trampoline_defaults

        _patch_trampoline_defaults(mutant_id)

        # After patching, inspect.signature should see the mutated default.
        sig = inspect.signature(mod.DrainLoop.wake)  # type: ignore[attr-defined]
        assert sig.parameters["delay"].default == 1.0, (
            "trampoline should have mutated default after patch"
        )

        # Behavioral test: calling without argument should use the mutated default.
        obj = mod.DrainLoop()  # type: ignore[attr-defined]
        mod.DrainLoop.wake(obj)  # type: ignore[attr-defined]
        assert obj.last_delay == 1.0, (  # type: ignore[attr-defined]
            "calling wake() without delay should use the mutated default 1.0"
        )
    finally:
        sys.modules.pop(mod.__name__, None)


def test_no_op_when_mutation_targets_body() -> None:
    """Verify no change when the active mutation does not target a default."""
    mod = _build_simulated_module()
    sep = "ǁ"

    # Add a body-only mutant (same defaults as original).
    def x_body_mutant(self: object, delay: float = 0.0) -> None:
        self.last_delay = delay + 999  # type: ignore[attr-defined]

    mutants_dict_attr = f"x{sep}DrainLoop{sep}wake__mutmut_mutants"
    getattr(mod.DrainLoop, mutants_dict_attr)[  # type: ignore[attr-defined]
        f"x{sep}DrainLoop{sep}wake__mutmut_2"
    ] = x_body_mutant

    mutant_id = f"redis_rate_limiter.core.limiters.x{sep}DrainLoop{sep}wake__mutmut_2"

    sys.modules[mod.__name__] = mod
    try:
        from tests.plugins.mutmut_defaults_patch import _patch_trampoline_defaults

        _patch_trampoline_defaults(mutant_id)

        # Defaults should remain unchanged.
        sig = inspect.signature(mod.DrainLoop.wake)  # type: ignore[attr-defined]
        assert sig.parameters["delay"].default == 0.0, (
            "defaults should not change for body-only mutations"
        )
    finally:
        sys.modules.pop(mod.__name__, None)


def test_nested_module_prefix_does_not_confuse_lookup() -> None:
    """Verify that a deeply nested module is matched correctly despite shorter prefixes."""
    sep = "ǁ"

    # Create a parent package module and a child module, both in sys.modules.
    parent_mod = types.ModuleType("redis_rate_limiter.backends.asgi")
    parent_mod.__name__ = "redis_rate_limiter.backends.asgi"

    child_mod = types.ModuleType("redis_rate_limiter.backends.asgi.middleware")
    child_mod.__name__ = "redis_rate_limiter.backends.asgi.middleware"

    # The mutant target lives on the child module.
    def trampoline_init(self: object, on_error: str = "fail_open") -> None:
        self.on_error = on_error  # type: ignore[attr-defined]

    def mutant_init(self: object, on_error: str = "XXfail_openXX") -> None:
        self.on_error = on_error  # type: ignore[attr-defined]

    class RateLimitMiddleware:
        __init__ = trampoline_init  # type: ignore[assignment]

    mutants_attr = f"x{sep}RateLimitMiddleware{sep}__init____mutmut_mutants"
    mutants_dict = {f"x{sep}RateLimitMiddleware{sep}__init____mutmut_1": mutant_init}
    setattr(RateLimitMiddleware, mutants_attr, mutants_dict)

    child_mod.RateLimitMiddleware = RateLimitMiddleware  # type: ignore[attr-defined]

    mutant_id = (
        f"redis_rate_limiter.backends.asgi.middleware."
        f"x{sep}RateLimitMiddleware{sep}__init____mutmut_1"
    )

    sys.modules[parent_mod.__name__] = parent_mod
    sys.modules[child_mod.__name__] = child_mod
    try:
        from tests.plugins.mutmut_defaults_patch import _patch_trampoline_defaults

        _patch_trampoline_defaults(mutant_id)

        sig = inspect.signature(RateLimitMiddleware.__init__)  # type: ignore[misc]
        assert sig.parameters["on_error"].default == "XXfail_openXX", (
            "plugin must resolve the longest module prefix to find the correct class"
        )
    finally:
        sys.modules.pop(parent_mod.__name__, None)
        sys.modules.pop(child_mod.__name__, None)


if __name__ == "__main__":
    test_defaults_before_patch()
    print("PASS: test_defaults_before_patch")

    test_defaults_after_patch()
    print("PASS: test_defaults_after_patch")

    test_no_op_when_mutation_targets_body()
    print("PASS: test_no_op_when_mutation_targets_body")

    test_nested_module_prefix_does_not_confuse_lookup()
    print("PASS: test_nested_module_prefix_does_not_confuse_lookup")

    print("\nAll verification tests passed.")
