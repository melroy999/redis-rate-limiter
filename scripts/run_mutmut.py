"""Wrapper that patches mutmut 3.5.0 to support mutation of decorated functions.

mutmut unconditionally skips all decorated functions/methods (issue #387).
This script patches the relevant functions at runtime before invoking the
CLI, enabling mutation of ``@classmethod``, ``@staticmethod``, and other
safe-decorated functions while preserving the skip for genuinely
incompatible decorators such as ``@property``.

Patches applied:

1. ``MutationVisitor._skip_node_and_children``: selective decorator skip
   instead of blanket skip. Also skips ``cst.Decorator`` nodes to prevent
   mutating decorator arguments.
2. ``function_trampoline_arrangement``: strips decorators from ``_orig``
   and ``_mutmut_N`` copies to avoid side effects and descriptor issues.
3. ``create_trampoline_wrapper``: generates correct wrapper bodies for
   ``@classmethod`` (uses ``cls`` and ``type.__getattribute__``) and
   ``@staticmethod`` (uses ``ClassName.attr`` lookups, no self_arg).
4. ``trampoline_impl``: adds ``orig_is_unbound`` parameter so the
   trampoline prepends ``cls`` to ``orig()`` calls for classmethods.
"""

from __future__ import annotations

import sys
from collections.abc import Iterable, Sequence
from typing import Union

import libcst as cst
from mutmut import file_mutation, trampoline_templates
from mutmut.file_mutation import (
    MODULE_STATEMENT,
    NEVER_MUTATE_FUNCTION_CALLS,
    NEVER_MUTATE_FUNCTION_NAMES,
    Mutation,
    deep_replace,
)
from mutmut.trampoline_templates import (
    create_trampoline_lookup,
    mangle_function_name,
)

# ---------------------------------------------------------------------------
# Decorator classification
# ---------------------------------------------------------------------------

# Decorators that are known to be safe with the trampoline mechanism.
# Unknown decorators are conservatively skipped to avoid side effects
# (e.g., @app.post("/foo") would register a route multiple times).
_COMPATIBLE_DECORATORS: set[str] = {
    "classmethod",
    "staticmethod",
    "abstractmethod",
    "override",
    "shared_task",
    "rate_limited",
    "functools.wraps",
    "functools.cache",
    "functools.lru_cache",
    "functools.cached_property",
}


def _get_decorator_name(decorator: cst.Decorator) -> str:
    """Extract the string name of a decorator.

    Handles simple names (``@classmethod``), dotted attributes
    (``@functools.wraps``), and calls (``@shared_task(...)``).
    """
    node = decorator.decorator
    # @shared_task(...) or @functools.wraps(fn)
    if isinstance(node, cst.Call):
        node = node.func
    # @functools.wraps
    if isinstance(node, cst.Attribute):
        parts: list[str] = []
        while isinstance(node, cst.Attribute):
            parts.append(node.attr.value)
            node = node.value  # type: ignore[assignment]
        if isinstance(node, cst.Name):
            parts.append(node.value)
        return ".".join(reversed(parts))
    # @classmethod, @staticmethod, @property
    if isinstance(node, cst.Name):
        return node.value
    return ""


def _should_skip_decorated_function(function: cst.FunctionDef) -> bool:
    """Return True if the function should be skipped from mutation.

    A function is skipped if any decorator is incompatible, or if any
    decorator is not on the compatible allowlist.
    """
    for decorator in function.decorators:
        name = _get_decorator_name(decorator)
        if name not in _COMPATIBLE_DECORATORS:
            return True
    return False


def _detect_descriptor_type(function: cst.FunctionDef) -> str:
    """Return ``'classmethod'``, ``'staticmethod'``, or ``'regular'``."""
    for decorator in function.decorators:
        name = _get_decorator_name(decorator)
        if name == "classmethod":
            return "classmethod"
        if name == "staticmethod":
            return "staticmethod"
    return "regular"


# ---------------------------------------------------------------------------
# Patch 1: _skip_node_and_children
# ---------------------------------------------------------------------------


def _patched_skip_node_and_children(
    self: file_mutation.MutationVisitor,
    node: cst.CSTNode,
) -> bool:
    if (
        isinstance(node, cst.Call)
        and isinstance(node.func, cst.Name)
        and node.func.value in NEVER_MUTATE_FUNCTION_CALLS
    ) or (
        isinstance(node, cst.FunctionDef)
        and node.name.value in NEVER_MUTATE_FUNCTION_NAMES
    ):
        return True

    # Ignore everything inside type annotations.
    if isinstance(node, cst.Annotation):
        return True

    # Default args are executed at definition time. Only allow simple
    # default values where mutations should not raise exceptions.
    if (
        isinstance(node, cst.Param)
        and node.default
        and not isinstance(node.default, (cst.Name, cst.BaseNumber, cst.BaseString))
    ):
        return True

    # Skip decorator nodes themselves (arguments like @shared_task(name=...)
    # should not be mutated), but allow visiting the function body.
    if isinstance(node, cst.Decorator):
        return True

    # Skip decorated classes (unchanged from upstream).
    if isinstance(node, cst.ClassDef) and len(node.decorators):
        return True

    # Skip functions with incompatible decorators (@property).
    if isinstance(node, cst.FunctionDef) and len(node.decorators):
        if _should_skip_decorated_function(node):
            return True
        # Allow all other decorated functions through.

    return False


# ---------------------------------------------------------------------------
# Patch 2: function_trampoline_arrangement
# ---------------------------------------------------------------------------


def _patched_function_trampoline_arrangement(
    function: cst.FunctionDef,
    mutants: Iterable[Mutation],
    class_name: Union[str, None],
) -> tuple[Sequence[MODULE_STATEMENT], Sequence[str]]:
    """Create mutated functions and a trampoline, stripping decorators from
    the orig and mutant copies to avoid side effects."""
    nodes: list[MODULE_STATEMENT] = []
    mutant_names: list[str] = []

    name = function.name.value
    mangled_name = mangle_function_name(name=name, class_name=class_name) + "__mutmut"

    # Trampoline wrapper keeps decorators (via function.with_changes(body=...)).
    nodes.append(_patched_create_trampoline_wrapper(function, mangled_name, class_name))

    # Strip decorators from the original copy and all mutant copies.
    function_no_decorators = function.with_changes(decorators=[])

    # Copy of original function (no decorators).
    nodes.append(
        function_no_decorators.with_changes(name=cst.Name(mangled_name + "_orig"))
    )

    # Mutated versions of the function (no decorators).
    for i, mutant in enumerate(mutants):
        mutant_name = f"{mangled_name}_{i + 1}"
        mutant_names.append(mutant_name)
        mutated_method = function_no_decorators.with_changes(name=cst.Name(mutant_name))
        mutated_method = deep_replace(
            mutated_method, mutant.original_node, mutant.mutated_node
        )
        nodes.append(mutated_method)  # type: ignore[arg-type]

    mutants_dict = list(
        cst.parse_module(
            create_trampoline_lookup(
                orig_name=name, mutants=mutant_names, class_name=class_name
            )
        ).body
    )
    mutants_dict[0] = mutants_dict[0].with_changes(leading_lines=[cst.EmptyLine()])

    nodes.extend(mutants_dict)

    return nodes, mutant_names


# ---------------------------------------------------------------------------
# Patch 3: create_trampoline_wrapper
# ---------------------------------------------------------------------------


def _patched_create_trampoline_wrapper(
    function: cst.FunctionDef,
    mangled_name: str,
    class_name: str | None,
) -> cst.FunctionDef:
    """Generate a trampoline wrapper that handles @classmethod/@staticmethod."""
    descriptor_type = _detect_descriptor_type(function)

    # ---- Collect positional args ----
    args: list[cst.Element | cst.StarredElement] = []
    for pos_only_param in function.params.posonly_params:
        args.append(cst.Element(pos_only_param.name))
    for param in function.params.params:
        args.append(cst.Element(param.name))
    if isinstance(function.params.star_arg, cst.Param):
        args.append(cst.StarredElement(function.params.star_arg.name))

    # For instance methods and classmethods inside a class, remove the
    # first arg (self or cls). For staticmethods, keep all args.
    if class_name is not None and descriptor_type != "staticmethod":
        args = args[1:]

    args_assignment = cst.Assign(
        [cst.AssignTarget(cst.Name(value="args"))], cst.List(args)
    )

    # ---- Collect keyword args ----
    kwargs: list[cst.DictElement | cst.StarredDictElement] = []
    for param in function.params.kwonly_params:
        kwargs.append(
            cst.DictElement(cst.SimpleString(f"'{param.name.value}'"), param.name)
        )
    if isinstance(function.params.star_kwarg, cst.Param):
        kwargs.append(cst.StarredDictElement(function.params.star_kwarg.name))

    kwargs_assignment = cst.Assign(
        [cst.AssignTarget(cst.Name(value="kwargs"))], cst.Dict(kwargs)
    )

    # ---- Build lookup expressions for orig and mutants ----
    def _get_local_name(func_name: str) -> cst.BaseExpression:
        if class_name is None:
            return cst.Name(func_name)

        if descriptor_type == "classmethod":
            # Use type.__getattribute__(cls, name) to access the raw
            # (undecorated) function on the class.
            first_param = function.params.params[0].name.value
            return cst.Call(
                func=cst.Attribute(cst.Name("type"), cst.Name("__getattribute__")),
                args=[
                    cst.Arg(cst.Name(first_param)),
                    cst.Arg(cst.SimpleString(f"'{func_name}'")),
                ],
            )

        if descriptor_type == "staticmethod":
            # Use ClassName.attr (the class is fully defined by call time).
            return cst.Attribute(cst.Name(class_name), cst.Name(func_name))

        # Regular instance method: object.__getattribute__(self, name).
        return cst.Call(
            func=cst.Attribute(cst.Name("object"), cst.Name("__getattribute__")),
            args=[
                cst.Arg(cst.Name("self")),
                cst.Arg(cst.SimpleString(f"'{func_name}'")),
            ],
        )

    # ---- Build self_arg and orig_is_unbound arguments ----
    if class_name is None:
        self_arg_node: cst.BaseExpression = cst.Name("None")
    elif descriptor_type == "staticmethod":
        self_arg_node = cst.Name("None")
    elif descriptor_type == "classmethod":
        first_param = function.params.params[0].name.value
        self_arg_node = cst.Name(first_param)
    else:
        self_arg_node = cst.Name("self")

    trampoline_args = [
        cst.Arg(_get_local_name(f"{mangled_name}_orig")),
        cst.Arg(_get_local_name(f"{mangled_name}_mutants")),
        cst.Arg(cst.Name("args")),
        cst.Arg(cst.Name("kwargs")),
        cst.Arg(self_arg_node),
    ]

    # For classmethods, the copies are plain functions (decorators stripped)
    # and need cls prepended explicitly. Pass orig_is_unbound=True.
    if descriptor_type == "classmethod":
        trampoline_args.append(cst.Arg(cst.Name("True")))

    result: cst.BaseExpression = cst.Call(
        func=cst.Name("_mutmut_trampoline"),
        args=trampoline_args,
    )

    # For non-async functions, simply return the value.
    result_statement: cst.BaseStatement = cst.SimpleStatementLine([cst.Return(result)])

    if function.asynchronous:
        is_generator = file_mutation._is_generator(function)
        if is_generator:
            result_statement = cst.For(
                target=cst.Name("i"),
                iter=result,
                body=cst.IndentedBlock(
                    [cst.SimpleStatementLine([cst.Expr(cst.Yield(cst.Name("i")))])]
                ),
                asynchronous=cst.Asynchronous(),
            )
        else:
            result_statement = cst.SimpleStatementLine([cst.Return(cst.Await(result))])

    type_ignore_whitespace = cst.TrailingWhitespace(
        comment=cst.Comment("# type: ignore")
    )

    return function.with_changes(
        body=cst.IndentedBlock(
            [
                cst.SimpleStatementLine(
                    [args_assignment],
                    trailing_whitespace=type_ignore_whitespace,
                ),
                cst.SimpleStatementLine(
                    [kwargs_assignment],
                    trailing_whitespace=type_ignore_whitespace,
                ),
                result_statement,
            ],
        ),
    )


# ---------------------------------------------------------------------------
# Patch 4: trampoline_impl
# ---------------------------------------------------------------------------


def _build_patched_trampoline_impl() -> str:
    """Return the patched _mutmut_trampoline implementation string.

    Adds the ``orig_is_unbound`` parameter. When True and ``self_arg``
    is not None, ``orig()`` is called with ``self_arg`` prepended (needed
    for @classmethod copies that lost their decorator).
    """
    return '''
from typing import Annotated
from typing import Callable
from typing import ClassVar

MutantDict = Annotated[dict[str, Callable], "Mutant"] # type: ignore


def _mutmut_trampoline(orig, mutants, call_args, call_kwargs, self_arg=None, orig_is_unbound=False): # type: ignore
    """Forward call to original or mutated function, depending on the environment""" # type: ignore
    import os # type: ignore
    mutant_under_test = os.environ['MUTANT_UNDER_TEST'] # type: ignore
    if mutant_under_test == 'fail': # type: ignore
        from mutmut.__main__ import MutmutProgrammaticFailException # type: ignore
        raise MutmutProgrammaticFailException('Failed programmatically')       # type: ignore
    elif mutant_under_test == 'stats': # type: ignore
        from mutmut.__main__ import record_trampoline_hit # type: ignore
        record_trampoline_hit(orig.__module__ + '.' + orig.__name__) # type: ignore
        if orig_is_unbound and self_arg is not None: # type: ignore
            result = orig(self_arg, *call_args, **call_kwargs) # type: ignore
        else: # type: ignore
            result = orig(*call_args, **call_kwargs) # type: ignore
        return result # type: ignore
    prefix = orig.__module__ + '.' + orig.__name__ + '__mutmut_' # type: ignore
    if not mutant_under_test.startswith(prefix): # type: ignore
        if orig_is_unbound and self_arg is not None: # type: ignore
            result = orig(self_arg, *call_args, **call_kwargs) # type: ignore
        else: # type: ignore
            result = orig(*call_args, **call_kwargs) # type: ignore
        return result # type: ignore
    mutant_name = mutant_under_test.rpartition('.')[-1] # type: ignore
    if self_arg is not None: # type: ignore
        result = mutants[mutant_name](self_arg, *call_args, **call_kwargs) # type: ignore
    else:
        result = mutants[mutant_name](*call_args, **call_kwargs) # type: ignore
    return result # type: ignore

'''


# ---------------------------------------------------------------------------
# Apply all patches
# ---------------------------------------------------------------------------


def _apply_patches() -> None:
    # Patch 1: selective decorator skip.
    file_mutation.MutationVisitor._skip_node_and_children = (  # type: ignore[assignment]
        _patched_skip_node_and_children
    )

    # Patch 2: strip decorators from copies.
    file_mutation.function_trampoline_arrangement = (  # type: ignore[assignment]
        _patched_function_trampoline_arrangement
    )

    # Patch 3: decorator-aware trampoline wrapper (called by Patch 2).
    file_mutation.create_trampoline_wrapper = (  # type: ignore[assignment]
        _patched_create_trampoline_wrapper
    )

    # Patch 4: updated trampoline implementation with orig_is_unbound.
    new_impl = _build_patched_trampoline_impl()
    trampoline_templates.trampoline_impl = new_impl  # type: ignore[attr-defined]
    file_mutation.trampoline_impl = new_impl  # type: ignore[attr-defined]
    new_cst = list(cst.parse_module(new_impl).body)
    new_cst[-1] = new_cst[-1].with_changes(
        leading_lines=[cst.EmptyLine(), cst.EmptyLine()]
    )
    file_mutation.trampoline_impl_cst = new_cst  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    _apply_patches()
    from mutmut.__main__ import cli

    cli(sys.argv[1:])


if __name__ == "__main__":
    main()
