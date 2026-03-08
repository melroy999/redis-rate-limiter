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
   tracks which tests killed each mutant via a pytest plugin and temp-file
   IPC between forked children and the parent process. Configurable via the
   ``_USE_FAIL_FAST`` module-level flag: when True, runs with ``-x``
   (first-killer mode, fast); when False, runs all tests per mutant with
   per-test timeouts via ``pytest-timeout`` (full kill matrix, slower).
   Killed-by data is accumulated in memory and flushed to
   ``/tmp/mutmut_killed_by_results.json`` at exit.
6. ``timeout_checker``: replaces mutmut's wall-clock timeout checker with
   a higher multiplier when running in full-matrix mode (no ``-x``), since
   per-test timeouts handle individual hangs.
7. ``resource.setrlimit``: intercepts RLIMIT_CPU calls via a module proxy
   to inflate the CPU time limit in full-matrix mode, matching the higher
   wall-clock multiplier from Patch 6.
"""

from __future__ import annotations

import atexit
import json
import os
import signal
import sys
from collections.abc import Iterable, Sequence
from datetime import datetime
from time import sleep
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
# Patch 5: killed-by test tracking
# ---------------------------------------------------------------------------

# Per-child temp directory for IPC and aggregated results file written by
# the parent. Both use absolute paths so they are independent of cwd.
_KILLED_BY_DIR = "/tmp/mutmut_killed_by"
_KILLED_BY_RESULTS = "/tmp/mutmut_killed_by_results.json"

# When True, ``-x`` is prepended to pytest args so that each mutant run
# stops at the first failing test (fast, captures first killer only).
# When False (default), all tests run for each mutant with per-test
# timeouts via pytest-timeout (slower, captures full kill matrix).
_USE_FAIL_FAST: bool = False

# Per-test timeout multiplier: each test gets at most this many times its
# baseline duration before pytest-timeout aborts it. Generous enough to
# avoid false positives from trampoline overhead, but tight enough to
# prevent one hanging test from consuming the entire process budget.
_PER_TEST_TIMEOUT_MULTIPLIER: int = 10

# Absolute minimum per-test timeout in seconds, to handle tests with
# near-zero baseline durations.
_PER_TEST_TIMEOUT_FLOOR: float = 5.0

# Prefix of the failure message produced by pytest-timeout's signal method.
_PYTEST_TIMEOUT_PREFIX = "Timeout >"

# In-memory accumulator for killed-by data; written to disk once at exit.
# Values are dicts with "killed_by", "timed_out" (list[str]), "tests_run"
# (int), and "partial" (bool).
_killed_by_data: dict[str, dict[str, list[str] | int | bool]] = {}


def _compute_per_test_timeout(tests: list[str]) -> float:
    """Compute a per-test timeout from baseline test durations.

    Uses mutmut's ``duration_by_test`` mapping (populated during the
    baseline run) to find the slowest test in the current set, then
    applies ``_PER_TEST_TIMEOUT_MULTIPLIER`` to account for trampoline
    overhead and mutation-induced slowdowns.
    """
    import mutmut

    max_duration = 0.0
    for t in tests:
        d = mutmut.duration_by_test.get(t, 0.0)
        if d > max_duration:
            max_duration = d
    timeout = max(max_duration * _PER_TEST_TIMEOUT_MULTIPLIER, _PER_TEST_TIMEOUT_FLOOR)
    return round(timeout, 1)


def _write_killed_by_temp_file(
    mutant_name: str | None,
    collector: "KilledByCollector",
    *,
    partial: bool = False,
) -> None:
    """Write killed-by data to the child's temp file.

    Called after pytest finishes (``partial=False``) or from the SIGXCPU
    handler (``partial=True``). The parent reads this file in
    ``_patched_sfmd_register_result``.
    """
    if mutant_name is None:
        return
    if not collector.killed_by and not collector.timed_out:
        return
    os.makedirs(_KILLED_BY_DIR, exist_ok=True)
    path = os.path.join(_KILLED_BY_DIR, f"{os.getpid()}.json")
    with open(path, "w") as f:
        json.dump(
            {
                "mutant_name": mutant_name,
                "killed_by": collector.killed_by,
                "timed_out": collector.timed_out,
                "tests_run": collector.tests_run,
                "partial": partial,
            },
            f,
        )


class KilledByCollector:
    """Pytest plugin that captures the nodeids of failing tests.

    Records every test that fails during a mutant run, distinguishing
    clean assertion failures (``killed_by``) from pytest-timeout aborts
    (``timed_out``). When ``_USE_FAIL_FAST`` is True, only one failure
    is captured (pytest exits after the first); when False, all failures
    are captured.

    Also counts the total number of tests that completed their call
    phase, which is a proxy for how sensitive the mutant is to test
    ordering.

    When a ``mutant_name`` is provided, the collector flushes killed-by
    data to the temp file after every failure so that partial data
    survives a SIGXCPU process kill.
    """

    def __init__(self, mutant_name: str | None = None) -> None:
        self.killed_by: list[str] = []
        self.timed_out: list[str] = []
        self.tests_run: int = 0
        self._mutant_name = mutant_name

    def pytest_runtest_makereport(self, item, call) -> None:  # type: ignore[no-untyped-def]
        if call.when == "call":
            self.tests_run += 1
            if call.excinfo is not None:
                msg = str(call.excinfo.value) if call.excinfo.value else ""
                if msg.startswith(_PYTEST_TIMEOUT_PREFIX):
                    self.timed_out.append(item.nodeid)
                else:
                    self.killed_by.append(item.nodeid)
                # Flush incrementally so partial data survives SIGXCPU.
                if self._mutant_name is not None:
                    _write_killed_by_temp_file(
                        self._mutant_name, self, partial=False
                    )


def _patched_run_tests(self, *, mutant_name, tests):  # type: ignore[no-untyped-def]
    """Replacement for ``PytestRunner.run_tests`` that injects the
    ``KilledByCollector`` plugin and writes killed-by data to a temp file.

    When ``_USE_FAIL_FAST`` is True, runs with ``-x`` so that pytest
    stops at the first failure (fast, captures the first killer only).
    When False, runs without ``-x`` with per-test timeouts via
    ``pytest-timeout`` to capture all failing tests (full kill matrix).
    """
    from mutmut.__main__ import change_cwd

    collector = KilledByCollector(mutant_name=mutant_name)

    # Register a SIGXCPU handler that flushes partial data before the
    # process is terminated by mutmut's RLIMIT_CPU backstop.
    original_sigxcpu = None
    if mutant_name is not None and hasattr(signal, "SIGXCPU"):
        original_sigxcpu = signal.getsignal(signal.SIGXCPU)

        def _sigxcpu_handler(signum, frame):  # type: ignore[no-untyped-def]
            _write_killed_by_temp_file(mutant_name, collector, partial=True)
            # Re-raise to let the default handler terminate the process.
            signal.signal(signal.SIGXCPU, signal.SIG_DFL)
            os.kill(os.getpid(), signal.SIGXCPU)

        signal.signal(signal.SIGXCPU, _sigxcpu_handler)

    pytest_args = ["-q", "-p", "no:randomly", "-p", "no:random-order"]
    if _USE_FAIL_FAST:
        pytest_args.insert(0, "-x")
    elif tests:
        # Inject per-test timeout so individual hanging tests do not
        # consume the entire process budget. The signal method calls
        # pytest.fail() on timeout, allowing pytest to continue to the
        # next test. Uses SIGALRM, which does not conflict with
        # mutmut's SIGXCPU.
        per_test_timeout = _compute_per_test_timeout(list(tests))
        pytest_args += [
            f"--timeout={per_test_timeout}",
            "--timeout-method=signal",
        ]
    if tests:
        pytest_args += list(tests)
    else:
        pytest_args += self._pytest_add_cli_args_test_selection
    with change_cwd("mutants"):
        result = int(self.execute_pytest(pytest_args, plugins=[collector]))

    # Restore original SIGXCPU handler.
    if original_sigxcpu is not None:
        signal.signal(signal.SIGXCPU, original_sigxcpu)

    # Write final (complete) killed-by data, overwriting any partial
    # flush from the incremental writes.
    _write_killed_by_temp_file(mutant_name, collector, partial=False)

    return result


def _append_killed_by(
    mutant_name: str,
    test_nodeids: list[str],
    timed_out: list[str] | None = None,
    tests_run: int = 0,
    partial: bool = False,
) -> None:
    """Accumulate a killed-by entry in memory.

    Called in the parent process after reading the child's temp file.
    Data is written to disk once at exit via ``_flush_killed_by``.
    """
    _killed_by_data[mutant_name] = {
        "killed_by": test_nodeids,
        "timed_out": timed_out or [],
        "tests_run": tests_run,
        "partial": partial,
    }


def _flush_killed_by() -> None:
    """Write all accumulated killed-by data to disk in a single pass.

    Registered as an ``atexit`` handler so the JSON is written even if
    mutmut exits via ``SystemExit`` (which Click raises on completion).
    """
    if _killed_by_data:
        with open(_KILLED_BY_RESULTS, "w") as f:
            json.dump(_killed_by_data, f, indent=4)


def _patched_sfmd_register_result(self, *, pid, exit_code):  # type: ignore[no-untyped-def]
    """Replacement for ``SourceFileMutationData.register_result`` that reads
    the killed-by temp file (if present) and appends to the aggregated
    results file at ``_KILLED_BY_RESULTS``."""
    from mutmut.__main__ import START_TIMES_BY_PID_LOCK

    # Read killed-by data from the child process temp file.
    key = self.key_by_pid[pid]
    killed_by_path = os.path.join(_KILLED_BY_DIR, f"{pid}.json")
    if os.path.exists(killed_by_path):
        try:
            with open(killed_by_path) as f:
                data = json.load(f)
            killed_by = data.get("killed_by", [])
            timed_out = data.get("timed_out", [])
            tests_run = data.get("tests_run", 0)
            partial = data.get("partial", False)
            if killed_by or timed_out:
                _append_killed_by(key, killed_by, timed_out, tests_run, partial)
        except (json.JSONDecodeError, OSError):
            pass
        finally:
            try:
                os.unlink(killed_by_path)
            except OSError:
                pass

    # Reproduce the original register_result logic.
    assert self.key_by_pid[pid] in self.exit_code_by_key
    self.exit_code_by_key[key] = exit_code
    self.durations_by_key[key] = (
        datetime.now() - self.start_time_by_pid[pid]
    ).total_seconds()
    del self.key_by_pid[pid]
    with START_TIMES_BY_PID_LOCK:
        del self.start_time_by_pid[pid]
    self.save()


# ---------------------------------------------------------------------------
# Patch 6: wall-clock timeout checker with configurable multiplier
# ---------------------------------------------------------------------------


def _patched_timeout_checker(mutants):  # type: ignore[no-untyped-def]
    """Replacement for ``mutmut.__main__.timeout_checker`` that uses a
    higher wall-clock multiplier when running in full-matrix mode.

    The original uses ``(estimated_time + 1) * 15`` as the wall-clock
    limit. In full-matrix mode (``_USE_FAIL_FAST=False``), per-test
    timeouts handle individual hangs, so the process-level wall-clock
    limit is raised to 60x to avoid premature kills.
    """
    from mutmut.__main__ import START_TIMES_BY_PID_LOCK

    def inner_timeout_checker() -> None:
        multiplier = 15 if _USE_FAIL_FAST else 60
        while True:
            sleep(1)
            now = datetime.now()
            for m, mutant_name, result in mutants:
                with START_TIMES_BY_PID_LOCK:
                    start_times_by_pid = dict(m.start_time_by_pid)
                for pid, start_time in start_times_by_pid.items():
                    run_time = now - start_time
                    threshold = (
                        m.estimated_time_of_tests_by_mutant[mutant_name] + 1
                    ) * multiplier
                    if run_time.total_seconds() > threshold:
                        try:
                            os.kill(pid, signal.SIGXCPU)
                        except ProcessLookupError:
                            pass

    return inner_timeout_checker


# ---------------------------------------------------------------------------
# Patch 7: inflate RLIMIT_CPU for full-matrix mode
# ---------------------------------------------------------------------------

# The CPU time multiplier used by mutmut is 30x. In full-matrix mode,
# per-test timeouts (with _PER_TEST_TIMEOUT_FLOOR) can cause the total
# CPU time to far exceed 30x the estimated baseline. We intercept
# resource.setrlimit in the mutmut module to use a higher multiplier.
_CPU_TIME_MULTIPLIER_FULL_MATRIX: int = 120


class _ResourceProxy:
    """Proxy for the ``resource`` module that intercepts ``setrlimit``
    calls to inflate RLIMIT_CPU limits in full-matrix mode.

    The original mutmut code does::

        cpu_time_limit = ceil((estimated_time + 1) * 30 + process_time())
        resource.setrlimit(resource.RLIMIT_CPU, (cpu_time_limit, cpu_time_limit + 1))

    This proxy scales the limit by
    ``_CPU_TIME_MULTIPLIER_FULL_MATRIX / 30`` so that the CPU budget
    matches the higher wall-clock multiplier used in Patch 6.
    """

    def __init__(self, real_module):  # type: ignore[no-untyped-def]
        self._real = real_module

    def setrlimit(self, resource_id, limits):  # type: ignore[no-untyped-def]
        if resource_id == self._real.RLIMIT_CPU:
            scale = _CPU_TIME_MULTIPLIER_FULL_MATRIX / 30
            soft, hard = limits
            soft = int(soft * scale)
            hard = int(hard * scale)
            limits = (soft, hard)
        return self._real.setrlimit(resource_id, limits)

    def __getattr__(self, name):  # type: ignore[no-untyped-def]
        return getattr(self._real, name)


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
    atexit.register(_flush_killed_by)

    # Patch 6: wall-clock timeout checker with configurable multiplier.
    from mutmut import __main__ as mutmut_main

    mutmut_main.timeout_checker = _patched_timeout_checker  # type: ignore[attr-defined]

    # Patch 7: inflate RLIMIT_CPU for full-matrix mode.
    if not _USE_FAIL_FAST:
        mutmut_main.resource = _ResourceProxy(mutmut_main.resource)  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    _apply_patches()
    from mutmut.__main__ import cli

    cli(sys.argv[1:])


if __name__ == "__main__":
    main()
