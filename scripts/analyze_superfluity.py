"""Standalone class-local test superfluity analysis via mutation testing.

Runs a single test file's tests against the entire mutated project (without
``-x``, i.e., full matrix) and performs class-local superfluity analysis on
the killed-by results. Since only one test file's tests run per invocation
(typically 20-40 tests), the per-mutant test count is inherently small and
RLIMIT_CPU is not a problem.

Reuses decorator patches (1-4) from ``run_mutmut.py`` for correct handling
of decorated functions. Applies its own killed-by tracking (no ``-x``)
independently of ``run_mutmut.py``'s first-killer mode.

Supports two modes of operation:

- **Single-file mode** (``--test-file``): analyzes one test file and
  produces per-file reports.
- **Batch mode** (``--all``): discovers all test files under ``tests/``,
  runs each sequentially as a subprocess, and merges per-file results
  into a project-wide report.

Usage::

    # Single file:
    python scripts/analyze_superfluity.py \\
        --test-file tests/implementations/test_rate_limiter.py \\
        --output-dir mutmut-results/

    # All test files:
    python scripts/analyze_superfluity.py --all --output-dir mutmut-results/
"""

from __future__ import annotations

import argparse
import atexit
import json
import os
import shutil
import signal
import subprocess
import sys
from collections import defaultdict
from pathlib import Path

# ---------------------------------------------------------------------------
# Killed-by tracking (full matrix, no -x)
# ---------------------------------------------------------------------------

_KILLED_BY_DIR = "/tmp/mutmut_superfluity_killed_by"
_KILLED_BY_RESULTS = "/tmp/mutmut_superfluity_killed_by_results.json"

_killed_by_data: dict[str, dict[str, list[str] | int | bool]] = {}


def _write_killed_by_temp_file(
    mutant_name: str | None,
    collector: "KilledByCollector",
    *,
    partial: bool = False,
) -> None:
    """Write killed-by data to the child's temp file."""
    if mutant_name is None:
        return
    if not partial and not collector.killed_by:
        if not collector.tests_targeted:
            return
    os.makedirs(_KILLED_BY_DIR, exist_ok=True)
    path = os.path.join(_KILLED_BY_DIR, f"{os.getpid()}.json")
    with open(path, "w") as f:
        json.dump(
            {
                "mutant_name": mutant_name,
                "killed_by": collector.killed_by,
                "tests_run": collector.tests_run,
                "tests_targeted": collector.tests_targeted,
                "partial": partial,
            },
            f,
        )


class KilledByCollector:
    """Pytest plugin that captures the nodeids of all failing tests.

    Runs without ``-x`` so that all tests execute per mutant, capturing
    every test that kills the mutant (full kill matrix).
    """

    def __init__(self, mutant_name: str | None = None) -> None:
        self.killed_by: list[str] = []
        self.tests_run: int = 0
        self.tests_targeted: int = 0
        self._mutant_name = mutant_name

    def pytest_collection_modifyitems(self, items) -> None:  # type: ignore[no-untyped-def]
        """Record the number of tests pytest will execute for this run."""
        self.tests_targeted = len(items)

    def pytest_runtest_makereport(self, item, call) -> None:  # type: ignore[no-untyped-def]
        if call.when == "call":
            self.tests_run += 1
            if call.excinfo is not None:
                self.killed_by.append(item.nodeid)
                if self._mutant_name is not None:
                    _write_killed_by_temp_file(self._mutant_name, self, partial=False)


def _patched_run_tests(self, *, mutant_name, tests):  # type: ignore[no-untyped-def]
    """Replacement for ``PytestRunner.run_tests`` that runs all tests
    (no ``-x``) to capture the full kill matrix for superfluity analysis.
    """
    from mutmut.__main__ import change_cwd

    collector = KilledByCollector(mutant_name=mutant_name)

    original_sigxcpu = None
    if mutant_name is not None and hasattr(signal, "SIGXCPU"):
        original_sigxcpu = signal.getsignal(signal.SIGXCPU)

        def _sigxcpu_handler(signum, frame):  # type: ignore[no-untyped-def]
            _write_killed_by_temp_file(mutant_name, collector, partial=True)
            signal.signal(signal.SIGXCPU, signal.SIG_DFL)
            os.kill(os.getpid(), signal.SIGXCPU)

        signal.signal(signal.SIGXCPU, _sigxcpu_handler)

    # No -x: run all tests for full kill matrix.
    pytest_args = ["-q", "-p", "no:randomly", "-p", "no:random-order"]
    if tests:
        pytest_args += list(tests)
    else:
        pytest_args += self._pytest_add_cli_args_test_selection
    with change_cwd("mutants"):
        result = int(self.execute_pytest(pytest_args, plugins=[collector]))

    if original_sigxcpu is not None:
        signal.signal(signal.SIGXCPU, original_sigxcpu)

    _write_killed_by_temp_file(mutant_name, collector, partial=False)

    return result


def _append_killed_by(
    mutant_name: str,
    test_nodeids: list[str],
    tests_run: int = 0,
    tests_targeted: int = 0,
    partial: bool = False,
) -> None:
    """Accumulate a killed-by entry in memory."""
    _killed_by_data[mutant_name] = {
        "killed_by": test_nodeids,
        "tests_run": tests_run,
        "tests_targeted": tests_targeted,
        "partial": partial,
    }


def _flush_killed_by() -> None:
    """Write all accumulated killed-by data to disk."""
    if _killed_by_data:
        with open(_KILLED_BY_RESULTS, "w") as f:
            json.dump(_killed_by_data, f, indent=4)


def _patched_sfmd_register_result(self, *, pid, exit_code):  # type: ignore[no-untyped-def]
    """Read killed-by temp file from the child and accumulate results."""
    from datetime import datetime

    from mutmut.__main__ import START_TIMES_BY_PID_LOCK

    key = self.key_by_pid[pid]
    killed_by_path = os.path.join(_KILLED_BY_DIR, f"{pid}.json")
    if os.path.exists(killed_by_path):
        try:
            with open(killed_by_path) as f:
                data = json.load(f)
            killed_by = data.get("killed_by", [])
            tests_run = data.get("tests_run", 0)
            tests_targeted = data.get("tests_targeted", 0)
            partial = data.get("partial", False)
            if killed_by or tests_targeted:
                _append_killed_by(
                    key,
                    killed_by,
                    tests_run,
                    tests_targeted,
                    partial,
                )
        except (json.JSONDecodeError, OSError):
            pass
        finally:
            try:
                os.unlink(killed_by_path)
            except OSError:
                pass

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
# Patch application
# ---------------------------------------------------------------------------


def _apply_patches(test_file: str) -> None:
    """Apply mutmut patches for superfluity analysis.

    Reuses decorator patches (1-4) from ``run_mutmut`` and applies its
    own Patch 5 (full-matrix killed-by tracking). Overrides mutmut's
    test selection to use only the specified test file.
    """
    import mutmut
    from mutmut import file_mutation, trampoline_templates
    from mutmut.__main__ import (
        PytestRunner,
        SourceFileMutationData,
        ensure_config_loaded,
    )

    ensure_config_loaded()

    # Import shared decorator patches from run_mutmut.
    import libcst as cst
    from run_mutmut import (
        _build_patched_trampoline_impl,
        _patched_create_trampoline_wrapper,
        _patched_function_trampoline_arrangement,
        _patched_skip_node_and_children,
    )

    # Patch 1: selective decorator skip.
    file_mutation.MutationVisitor._skip_node_and_children = (  # type: ignore[assignment]
        _patched_skip_node_and_children
    )

    # Patch 2: strip decorators from copies.
    file_mutation.function_trampoline_arrangement = (  # type: ignore[assignment]
        _patched_function_trampoline_arrangement
    )

    # Patch 3: decorator-aware trampoline wrapper.
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

    # Patch 5: full-matrix killed-by tracking (no -x).
    SourceFileMutationData.register_result = _patched_sfmd_register_result  # type: ignore[assignment]
    PytestRunner.run_tests = _patched_run_tests  # type: ignore[assignment]
    atexit.register(_flush_killed_by)

    # Override test selection to use only the specified test file.
    mutmut.config.pytest_add_cli_args_test_selection = [test_file]

    # Only mutate lines covered by the specified test file. Uncovered mutants
    # always survive and contribute nothing to superfluity analysis.
    mutmut.config.mutate_only_covered_lines = True


# ---------------------------------------------------------------------------
# Superfluity analysis
# ---------------------------------------------------------------------------


def _extract_test_class(nodeid: str) -> str | None:
    """Extract the test class name from a pytest nodeid.

    Example: ``tests/foo/bar.py::TestClassName::test_method`` returns
    ``TestClassName``. Returns ``None`` for module-level tests (no class).
    """
    parts = nodeid.split("::")
    if len(parts) >= 3:
        return parts[1]
    return None


def _analyze_superfluity(
    killed_by_data: dict[str, dict[str, list[str] | int | bool]],
) -> dict:
    """Perform class-local superfluity analysis on killed-by data.

    For each test class, builds a mapping from test nodeid to the set of
    mutants it killed. A test is superfluous if its kill set is a strict
    subset of another test's kill set within the same class.
    """
    # Build per-class kill maps: {class_name: {test_nodeid: set(mutant_names)}}
    class_kill_maps: dict[str, dict[str, set[str]]] = defaultdict(
        lambda: defaultdict(set)
    )

    for mutant_name, entry in killed_by_data.items():
        killers = entry.get("killed_by", [])
        if not isinstance(killers, list):
            continue
        for test_nodeid in killers:
            test_class = _extract_test_class(test_nodeid)
            if test_class is None:
                test_class = "<module-level>"
            class_kill_maps[test_class][test_nodeid].add(mutant_name)

    # Analyze each class for superfluity.
    results: dict[str, dict] = {}
    total_superfluous = 0

    for class_name, kill_map in sorted(class_kill_maps.items()):
        tests = list(kill_map.items())
        superfluous: list[dict] = []
        zero_kill: list[str] = []

        for i, (test_a, kills_a) in enumerate(tests):
            if not kills_a:
                zero_kill.append(test_a)
                continue
            for j, (test_b, kills_b) in enumerate(tests):
                if i == j or not kills_b:
                    continue
                # test_a is superfluous if its kills are a strict subset
                # of test_b's kills.
                if kills_a < kills_b:
                    superfluous.append(
                        {
                            "test": test_a,
                            "kills": len(kills_a),
                            "subsumed_by": test_b,
                            "subsumed_by_kills": len(kills_b),
                        }
                    )
                    break  # Only report the first subsuming test.

        class_result: dict = {
            "total_tests": len(tests),
            "total_kills": sum(len(k) for k in kill_map.values()),
        }
        if superfluous:
            class_result["superfluous"] = superfluous
            total_superfluous += len(superfluous)
        if zero_kill:
            class_result["zero_kill_tests"] = zero_kill

        results[class_name] = class_result

    return {
        "total_classes": len(results),
        "total_superfluous_tests": total_superfluous,
        "classes": results,
    }


def _write_text_report(
    analysis: dict,
    txt_path: Path,
    *,
    title: str,
) -> None:
    """Write a human-readable superfluity report to a text file."""
    lines: list[str] = []
    lines.append(f"Superfluity Analysis: {title}")
    lines.append("=" * 60)
    lines.append(f"Total classes analyzed: {analysis['total_classes']}")
    lines.append(f"Total superfluous tests: {analysis['total_superfluous_tests']}")
    lines.append("")

    for class_name, class_data in analysis["classes"].items():
        superfluous = class_data.get("superfluous", [])
        zero_kill = class_data.get("zero_kill_tests", [])
        if not superfluous and not zero_kill:
            continue

        lines.append(f"{class_name}")
        lines.append("-" * 40)
        lines.append(
            f"  Tests: {class_data['total_tests']},"
            f" Mutation kills: {class_data['total_kills']}"
        )

        if superfluous:
            lines.append(f"  Superfluous tests ({len(superfluous)}):")
            for s in superfluous:
                test_short = s["test"].rpartition("::")[2]
                sub_short = s["subsumed_by"].rpartition("::")[2]
                lines.append(
                    f"    {test_short} ({s['kills']} kills)"
                    f" subsumed by {sub_short} ({s['subsumed_by_kills']} kills)"
                )

        if zero_kill:
            lines.append(f"  Zero-kill tests ({len(zero_kill)}):")
            for t in zero_kill:
                lines.append(f"    {t.rpartition('::')[2]}")

        lines.append("")

    txt_content = "\n".join(lines) + "\n"
    with open(txt_path, "w") as f:
        f.write(txt_content)


# ---------------------------------------------------------------------------
# Single-file mode
# ---------------------------------------------------------------------------


def _run_single(test_file: str, output_dir: Path) -> None:
    """Run superfluity analysis for a single test file.

    Applies mutmut patches, runs the mutation test with full matrix
    (no ``-x``), loads killed-by results, performs class-local superfluity
    analysis, and writes per-file JSON and text reports.
    """
    test_stem = Path(test_file).stem

    # Add scripts/ to sys.path so we can import run_mutmut.
    scripts_dir = str(Path(__file__).resolve().parent)
    if scripts_dir not in sys.path:
        sys.path.insert(0, scripts_dir)

    print(f"Analyzing superfluity for: {test_file}", flush=True)
    print(f"Output directory: {output_dir}", flush=True)

    # Apply patches and run mutmut.
    _apply_patches(test_file)
    from mutmut.__main__ import cli

    print("Running mutation testing (full matrix, no -x)...", flush=True)
    try:
        cli(["run"])
    except SystemExit:
        pass  # mutmut exits via SystemExit; we catch it.

    # Flush killed-by data accumulated in memory during the mutation run.
    # The atexit handler has not fired yet, so we flush explicitly.
    _flush_killed_by()

    # Use in-memory data directly; reading from disk would risk picking up
    # stale results from a previous subprocess invocation.
    if not _killed_by_data:
        print(
            "No killed-by results found. Did the mutation run complete?",
            file=sys.stderr,
        )
        sys.exit(1)

    print(f"  {len(_killed_by_data)} mutants with killed-by data.", flush=True)

    # Perform superfluity analysis.
    print("Analyzing class-local superfluity...", flush=True)
    analysis = _analyze_superfluity(_killed_by_data)

    # Write JSON output.
    json_path = output_dir / f"superfluity-{test_stem}.json"
    with open(json_path, "w") as f:
        json.dump(analysis, f, indent=2)
    print(f"  JSON report: {json_path}", flush=True)

    # Write human-readable output.
    txt_path = output_dir / f"superfluity-{test_stem}.txt"
    _write_text_report(analysis, txt_path, title=test_file)
    print(f"  Text report: {txt_path}", flush=True)

    # Summary.
    if analysis["total_superfluous_tests"] > 0:
        print(
            f"\nFound {analysis['total_superfluous_tests']} superfluous test(s)"
            f" across {analysis['total_classes']} class(es).",
            flush=True,
        )
    else:
        print("\nNo superfluous tests found.", flush=True)


# ---------------------------------------------------------------------------
# Batch mode (--all)
# ---------------------------------------------------------------------------


def _discover_test_files() -> list[str]:
    """Discover all test files under the ``tests/`` directory.

    Returns sorted paths relative to the project root, matching the
    pattern ``tests/**/test_*.py``.
    """
    project_root = Path(__file__).resolve().parent.parent
    tests_dir = project_root / "tests"
    test_files = sorted(
        str(p.relative_to(project_root)) for p in tests_dir.rglob("test_*.py")
    )
    return test_files


def _merge_reports(per_file_dir: Path) -> dict:
    """Merge per-file superfluity JSON reports into a project-wide report.

    Each per-file report contains class names that may collide across
    files (e.g., multiple files may have a ``TestDistributedLock`` class).
    To avoid collisions, each class is namespaced by the source file stem
    in the merged report.
    """
    combined_classes: dict[str, dict] = {}
    total_superfluous = 0

    for json_path in sorted(per_file_dir.glob("superfluity-*.json")):
        with open(json_path) as f:
            report = json.load(f)

        source_file = json_path.stem.removeprefix("superfluity-")
        for class_name, class_data in report["classes"].items():
            qualified_key = f"{source_file}::{class_name}"
            combined_classes[qualified_key] = class_data
            total_superfluous += len(class_data.get("superfluous", []))

    return {
        "total_classes": len(combined_classes),
        "total_superfluous_tests": total_superfluous,
        "classes": combined_classes,
    }


def _run_all(output_dir: Path) -> None:
    """Run superfluity analysis for all test files in the project.

    Discovers test files under ``tests/``, runs each sequentially as a
    subprocess (mutmut uses extensive global state that cannot be reset
    between runs within a single process), and merges per-file results
    into a combined project-wide report.
    """
    test_files = _discover_test_files()
    total = len(test_files)
    superfluity_dir = output_dir / "superfluity"
    superfluity_dir.mkdir(parents=True, exist_ok=True)

    print(f"Discovered {total} test file(s) under tests/.", flush=True)
    print(f"Output directory: {output_dir}", flush=True)
    print(f"Per-file reports: {superfluity_dir}", flush=True)
    print("", flush=True)

    script_path = str(Path(__file__).resolve())
    failed: list[str] = []

    # Determine the project root for cache cleanup between runs.
    project_root = Path(__file__).resolve().parent.parent

    for i, test_file in enumerate(test_files, start=1):
        # Clean mutmut's cached mutants and metadata so each test file
        # gets fresh mutant generation based on its own coverage data.
        for cache_dir in ("mutants", ".mutmut-cache"):
            cache_path = project_root / cache_dir
            if cache_path.is_dir():
                shutil.rmtree(cache_path)

        print(
            f"[{i}/{total}] {test_file}",
            flush=True,
        )
        result = subprocess.run(
            [
                sys.executable,
                script_path,
                "--test-file",
                test_file,
                "--output-dir",
                str(superfluity_dir),
            ],
            check=False,
        )
        if result.returncode != 0:
            failed.append(test_file)
            print(
                f"  Warning: subprocess exited with code {result.returncode}.",
                flush=True,
            )
        print("", flush=True)

    # Merge per-file results into a project-wide report.
    print("Merging per-file reports...", flush=True)
    combined = _merge_reports(superfluity_dir)

    json_path = output_dir / "superfluity-all.json"
    with open(json_path, "w") as f:
        json.dump(combined, f, indent=2)
    print(f"  Combined JSON report: {json_path}", flush=True)

    txt_path = output_dir / "superfluity-all.txt"
    _write_text_report(combined, txt_path, title="All test files")
    print(f"  Combined text report: {txt_path}", flush=True)

    # Summary.
    print(
        f"\nProject-wide: {combined['total_superfluous_tests']} superfluous"
        f" test(s) across {combined['total_classes']} class(es).",
        flush=True,
    )

    if failed:
        print(
            f"\n{len(failed)} file(s) failed during analysis:",
            flush=True,
        )
        for f_path in failed:
            print(f"  {f_path}", flush=True)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description=("Run class-local test superfluity analysis via mutation testing."),
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--test-file",
        help=(
            "Path to a single test file to analyze"
            " (e.g., tests/implementations/test_rate_limiter.py)."
        ),
    )
    group.add_argument(
        "--all",
        action="store_true",
        dest="run_all",
        help=(
            "Discover and analyze all test files under tests/."
            " Runs each file sequentially and produces a merged"
            " project-wide report."
        ),
    )
    parser.add_argument(
        "--output-dir",
        default="mutmut-results",
        help="Directory to write output files to (default: mutmut-results/).",
    )
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if args.run_all:
        _run_all(output_dir)
    else:
        _run_single(args.test_file, output_dir)


if __name__ == "__main__":
    main()
