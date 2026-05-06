"""Wrapper that patches mutmut 3.5.0 to support mutation of decorated functions
and killed-by test tracking.

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
5. ``PytestRunner.run_tests`` and ``SourceFileMutationData.register_result``:
   tracks which test killed each mutant via a pytest plugin and temp-file
   IPC between forked children and the parent process. Always runs with
   ``-x`` (first-killer mode) for fast blind spot analysis. Killed-by data
   is accumulated in memory and flushed to
   ``/tmp/mutmut_killed_by_results.json`` at exit.
6. ``mutate_file_contents`` and ``MutationVisitor._create_mutations``:
   captures the libcst operator name, source line, and a structural
   default-parameter flag for every mutation at generation time, keyed by
   the canonical mutmut mutant ID. Records are flushed to
   ``/tmp/mutmut_mutation_types.json`` at exit, where the classifier joins
   them in to use the operator name as ground truth instead of inferring
   it from a unified diff.
"""

from __future__ import annotations

import atexit
import json
import os
import signal
import sys
from collections.abc import Iterable, Sequence
from typing import Union

import libcst as cst
from libcst.metadata import PositionProvider
from mutmut import file_mutation, trampoline_templates
from mutmut.file_mutation import (
    MODULE_STATEMENT,
    NEVER_MUTATE_FUNCTION_CALLS,
    NEVER_MUTATE_FUNCTION_NAMES,
    Mutation,
    OuterFunctionProvider,
    deep_replace,
)
from mutmut.trampoline_templates import (
    create_trampoline_lookup,
    mangle_function_name,
)
from mutmut_shared import KilledByAccumulator
from mutmut_shared import KilledByCollector as _KilledByCollectorBase

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

    get_mutant_name = None
    if _current_source_file is not None:
        try:
            from mutmut.__main__ import get_mutant_name as _gmn

            get_mutant_name = _gmn
        except Exception:
            get_mutant_name = None

    # Mutated versions of the function (no decorators).
    for i, mutant in enumerate(mutants):
        mutant_name = f"{mangled_name}_{i + 1}"
        mutant_names.append(mutant_name)
        mutated_method = function_no_decorators.with_changes(name=cst.Name(mutant_name))
        mutated_method = deep_replace(
            mutated_method, mutant.original_node, mutant.mutated_node
        )
        nodes.append(mutated_method)  # type: ignore[arg-type]

        if get_mutant_name is not None and _current_source_file is not None:
            from pathlib import Path

            try:
                full_mutant_id = get_mutant_name(
                    Path(_current_source_file), mutant_name
                )
            except Exception:
                continue
            _record_mutation_metadata(mutant, full_mutant_id, function, class_name)

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
# Patch 5: killed-by test tracking
# ---------------------------------------------------------------------------

# Per-child temp directory for IPC and aggregated results file written by
# the parent. Both use absolute paths so they are independent of cwd.
_KILLED_BY_DIR = "/tmp/mutmut_killed_by"
_KILLED_BY_RESULTS = "/tmp/mutmut_killed_by_results.json"

_accumulator = KilledByAccumulator(_KILLED_BY_RESULTS, _KILLED_BY_DIR)

# Cache of ``@pytest.mark.signature`` test node IDs, populated once per
# process by ``_collect_signature_test_ids()``.
_signature_test_ids: list[str] | None = None


class KilledByCollector(_KilledByCollectorBase):
    """Pytest plugin that captures the nodeid of the first failing test.

    With ``-x``, pytest stops at the first failure, so at most one
    killer is recorded. Extends :class:`~mutmut_shared.KilledByCollector`
    with ``current_test`` tracking so the SIGXCPU handler can report
    which test was mid-execution when the process was killed.
    """

    def __init__(self, mutant_name: str | None = None) -> None:
        super().__init__(mutant_name, killed_by_dir=_KILLED_BY_DIR)
        self.current_test: str | None = None

    def _extra_payload(self) -> dict:
        if self.current_test is not None:
            return {"killed_during": self.current_test}
        return {}

    def pytest_runtest_call(self, item) -> None:  # type: ignore[no-untyped-def]
        """Track the test that is about to execute its call phase."""
        self.current_test = item.nodeid

    def pytest_runtest_makereport(self, item, call) -> None:  # type: ignore[no-untyped-def]
        super().pytest_runtest_makereport(item, call)
        if call.when == "call":
            self.current_test = None


def _collect_signature_test_ids() -> list[str]:
    """Return all test node IDs marked with ``@pytest.mark.signature``.

    Runs ``pytest --collect-only`` inside the mutants working directory so
    that paths match the node IDs mutmut uses during test execution.
    Collection results are cached in ``_signature_test_ids`` and reused
    across all mutants in the same process.
    """
    global _signature_test_ids
    if _signature_test_ids is not None:
        return _signature_test_ids

    import pytest
    from mutmut.__main__ import change_cwd

    collected: list[str] = []

    class _Collector:
        def pytest_collection_finish(self, session) -> None:  # type: ignore[no-untyped-def]
            for item in session.items:
                collected.append(item.nodeid)

    with change_cwd("mutants"):
        pytest.main(
            ["--collect-only", "-q", "--no-header", "-m", "signature"],
            plugins=[_Collector()],
        )

    _signature_test_ids = collected
    return _signature_test_ids


def _patched_run_tests(self, *, mutant_name, tests):  # type: ignore[no-untyped-def]
    """Replacement for ``PytestRunner.run_tests`` that injects the
    ``KilledByCollector`` plugin and writes killed-by data to a temp file.

    Always runs with ``-x`` so that pytest stops at the first failure
    (first-killer mode for fast blind spot analysis).
    """
    from mutmut.__main__ import change_cwd

    collector = KilledByCollector(mutant_name=mutant_name)

    # Register a SIGXCPU handler that flushes partial data before the
    # process is terminated by mutmut's RLIMIT_CPU backstop.
    original_sigxcpu = None
    if mutant_name is not None and hasattr(signal, "SIGXCPU"):
        original_sigxcpu = signal.getsignal(signal.SIGXCPU)

        def _sigxcpu_handler(signum, frame):  # type: ignore[no-untyped-def]
            collector.write_temp_file(partial=True)
            # Re-raise to let the default handler terminate the process.
            signal.signal(signal.SIGXCPU, signal.SIG_DFL)
            os.kill(os.getpid(), signal.SIGXCPU)

        signal.signal(signal.SIGXCPU, _sigxcpu_handler)

    pytest_args = ["-x", "-q", "-p", "no:randomly", "-p", "no:random-order"]
    if tests:
        pytest_args += list(tests)
        # Always append signature tests: coverage-based selection never includes
        # them because ``def func(...):`` lines execute at module import time
        # (before any test context is active), so they appear under no test
        # node ID in the coverage database. Default-parameter mutations on
        # functions that also have body coverage would therefore be tested
        # without ``inspect.signature`` assertions and survive.
        sig_ids = _collect_signature_test_ids()
        already = set(tests)
        pytest_args += [nid for nid in sig_ids if nid not in already]
    else:
        pytest_args += self._pytest_add_cli_args_test_selection
    with change_cwd("mutants"):
        result = int(self.execute_pytest(pytest_args, plugins=[collector]))

    # Restore original SIGXCPU handler.
    if original_sigxcpu is not None:
        signal.signal(signal.SIGXCPU, original_sigxcpu)

    # Write final (complete) killed-by data, overwriting any partial
    # flush from the incremental writes.
    collector.write_temp_file(partial=False)

    return result


_patched_sfmd_register_result = _accumulator.make_register_result_patch()


# ---------------------------------------------------------------------------
# Patch 6: capture mutation operator metadata at generation time
# ---------------------------------------------------------------------------

_MUTATION_TYPES_FILE = "/app/mutation-output/mutation-types.json"

_current_source_file: str | None = None
_operator_by_mutation_id: dict[int, dict[str, object]] = {}
_default_param_lines: set[int] = set()
_mutation_type_records: dict[str, dict[str, object]] = {}

_original_mutate_file_contents = file_mutation.mutate_file_contents


def _patched_mutate_file_contents(
    filename: str,
    code: str,
    covered_lines: Union[set[int], None] = None,
) -> tuple[str, Sequence[str]]:
    """Wrap ``mutate_file_contents`` to reset per-file scratch state and
    pre-scan default-parameter line ranges before mutmut's own parse runs."""
    global _current_source_file
    _current_source_file = str(filename)
    _operator_by_mutation_id.clear()
    _default_param_lines.clear()

    try:
        _populate_default_param_lines(code)
    except Exception:
        # A scan failure here only loses the structural is_default_param
        # signal; the regex fallback in classify_mutants still catches the
        # common single-line def case.
        pass

    return _original_mutate_file_contents(filename, code, covered_lines)  # type: ignore[no-any-return]


def _populate_default_param_lines(code: str) -> None:
    """Populate ``_default_param_lines`` with every line covered by a
    ``cst.Param.default`` subtree in the module."""
    module = cst.parse_module(code)
    wrapper = cst.metadata.MetadataWrapper(module)

    class _Scanner(cst.CSTVisitor):
        METADATA_DEPENDENCIES = (PositionProvider,)

        def visit_Param(self, node: cst.Param) -> None:
            if node.default is None:
                return
            try:
                pos = self.get_metadata(PositionProvider, node.default)
            except KeyError:
                return
            for line in range(pos.start.line, pos.end.line + 1):
                _default_param_lines.add(line)

    wrapper.visit(_Scanner())


def _patched_create_mutations(
    self: file_mutation.MutationVisitor,
    node: cst.CSTNode,
) -> None:
    """Create mutations and record per-Mutation operator metadata.

    Mirrors the upstream ``MutationVisitor._create_mutations`` body, but
    additionally captures ``operator.__name__``, the source line, the
    default-parameter flag, and the original/mutated CST node types into
    ``_operator_by_mutation_id`` keyed by ``id(mutation)``. The trampoline
    arrangement patch reads this side-channel when assigning mutant indices.
    """
    position = self.get_metadata(PositionProvider, node, None)
    line = position.start.line if position is not None else None
    is_default_param = line is not None and line in _default_param_lines

    for t, operator in self._operators:
        if isinstance(node, t):
            for mutated_node in operator(node):
                mutation = Mutation(
                    original_node=node,
                    mutated_node=mutated_node,
                    contained_by_top_level_function=self.get_metadata(  # type: ignore[arg-type]
                        OuterFunctionProvider, node, None
                    ),
                )
                _operator_by_mutation_id[id(mutation)] = {
                    "operator": operator.__name__,
                    "line": line,
                    "is_default_param": is_default_param,
                    "original_node_type": type(node).__name__,
                    "mutated_node_type": type(mutated_node).__name__,
                }
                self.mutations.append(mutation)


def _record_mutation_metadata(
    mutant: Mutation,
    full_mutant_id: str,
    function: cst.FunctionDef,
    class_name: str | None,
) -> None:
    """Emit a metadata record for a single mutant, keyed by its full ID.

    No-op if the side-channel is empty, which keeps the trampoline patch
    safe to import from callers that do not apply Patch 6 (e.g.
    ``analyze_superfluity.py``).
    """
    captured = _operator_by_mutation_id.get(id(mutant))
    if captured is None or _current_source_file is None:
        return

    function_label = (
        f"{class_name}.{function.name.value}"
        if class_name is not None
        else function.name.value
    )
    record: dict[str, object] = {
        "operator": captured.get("operator"),
        "line": captured.get("line"),
        "source_file": _current_source_file,
        "function": function_label,
        "is_default_param": captured.get("is_default_param", False),
        "original_node_type": captured.get("original_node_type"),
        "mutated_node_type": captured.get("mutated_node_type"),
    }
    try:
        empty_module = cst.Module(body=[])
        record["original"] = empty_module.code_for_node(mutant.original_node).strip()
        record["mutated"] = empty_module.code_for_node(mutant.mutated_node).strip()
    except Exception:
        # Some CST nodes (e.g. bare operators) are not standalone-printable.
        pass
    _mutation_type_records[full_mutant_id] = record


def _flush_mutation_types() -> None:
    """atexit-registered writer for the per-mutant metadata JSON."""
    if not _mutation_type_records:
        return
    os.makedirs(os.path.dirname(_MUTATION_TYPES_FILE), exist_ok=True)
    with open(_MUTATION_TYPES_FILE, "w") as f:
        json.dump(_mutation_type_records, f, indent=2)


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

    # Patch 5: killed-by test tracking via temp-file IPC.
    from mutmut.__main__ import PytestRunner, SourceFileMutationData

    SourceFileMutationData.register_result = _patched_sfmd_register_result  # type: ignore[assignment]
    PytestRunner.run_tests = _patched_run_tests  # type: ignore[assignment]
    atexit.register(_accumulator.flush)

    # Patch 6: capture mutation operator metadata at generation time.
    # mutmut.__main__ does ``from mutmut.file_mutation import mutate_file_contents``,
    # so patching only the file_mutation attribute does not propagate to the
    # call site at __main__.py:346. Patch both bindings.
    import mutmut.__main__ as _mutmut_main

    file_mutation.mutate_file_contents = _patched_mutate_file_contents  # type: ignore[assignment]
    _mutmut_main.mutate_file_contents = _patched_mutate_file_contents  # type: ignore[assignment]
    file_mutation.MutationVisitor._create_mutations = _patched_create_mutations  # type: ignore[assignment]
    atexit.register(_flush_mutation_types)


# ---------------------------------------------------------------------------
# Test-timeline configuration
# ---------------------------------------------------------------------------

# Shared file written by ``tests/plugins/mutmut_test_timeline.py`` from every
# mutmut child fork (and the parent's baseline pytest runs). One JSON line
# per event; ``O_APPEND`` keeps concurrent writes safe under PIPE_BUF.
_TEST_TIMELINE_FILE = "/tmp/mutmut_test_timeline.jsonl"


def _reset_test_timeline_file() -> None:
    """Truncate the timeline file at run start so artifacts only contain the
    current run's events. Children inherit the env var via ``fork()``.
    """
    os.environ["MUTMUT_TEST_TIMELINE_FILE"] = _TEST_TIMELINE_FILE
    try:
        with open(_TEST_TIMELINE_FILE, "w") as f:
            f.truncate(0)
    except OSError:
        pass


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    _reset_test_timeline_file()
    _apply_patches()
    from mutmut.__main__ import cli

    cli(sys.argv[1:])


if __name__ == "__main__":
    main()
