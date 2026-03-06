"""Analyze test superfluity using mutation testing and coverage data.

Identifies tests whose removal would not reduce path coverage or mutation
coverage, i.e., tests that are strict subsets of other test collections.

Operates in two modes depending on available data:

1. **Function-level analysis** (always available): uses
   ``tests_by_mangled_function_name`` from ``stats.json`` and killed-by
   data from ``report-detail.json`` to produce a coarse tiered ranking.

2. **Line-level analysis** (when ``coverage-contexts.json`` is present):
   uses per-test line coverage from ``coverage.py --show-contexts`` to
   perform precise set-cover analysis.

Usage::

    python scripts/analyze_test_superfluity.py mutmut-results/
    python scripts/analyze_test_superfluity.py mutmut-results/ \\
        --coverage mutmut-results/coverage-contexts.json

Output files (written to the input directory):

- ``superfluity-report.json``: structured analysis for programmatic use
- ``superfluity-report.txt``: human-readable summary (also printed to stdout)
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import cast

# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass
class TestRecord:
    """Per-test superfluity analysis record."""

    test_nodeid: str
    tier: int
    kills: int
    duration_seconds: float
    functions_exercised: list[str] = field(default_factory=list)
    functions_uniquely_exercised: list[str] = field(default_factory=list)
    superset_test: str | None = None
    reason: str = ""
    lines_covered: int = 0
    lines_uniquely_covered: int = 0
    is_superfluous: bool = False


@dataclass
class SuperfluityReport:
    """Top-level report structure."""

    analysis_level: str
    total_tests: int
    killing_tests: int
    zero_kill_tests: int
    out_of_scope_tests: int
    function_subset_candidates: int
    unknown_tests: int
    line_superfluous_tests: int
    combined_superfluous_tests: int
    records: list[TestRecord] = field(default_factory=list)
    caveats: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Phase 1: Function-level subset analysis
# ---------------------------------------------------------------------------


def _invert_function_mapping(
    tests_by_function: dict[str, list[str]],
) -> dict[str, set[str]]:
    """Invert function -> tests mapping to test -> functions."""
    test_to_functions: dict[str, set[str]] = {}
    for func, tests in tests_by_function.items():
        for t in tests:
            test_to_functions.setdefault(t, set()).add(func)
    return test_to_functions


def _compute_kill_counts(
    killed_by_detail: dict[str, dict[str, list[str] | int]],
) -> Counter[str]:
    """Count how many mutants each test killed (as first killer)."""
    counts: Counter[str] = Counter()
    for entry in killed_by_detail.values():
        killers = entry.get("killed_by", [])
        if isinstance(killers, list):
            for t in killers:
                counts[t] += 1
    return counts


def _find_function_unique_tests(
    test_to_functions: dict[str, set[str]],
) -> dict[str, list[str]]:
    """For each test, find functions it is the sole exerciser of."""
    function_test_count: Counter[str] = Counter()
    for funcs in test_to_functions.values():
        for f in funcs:
            function_test_count[f] += 1

    unique: dict[str, list[str]] = {}
    for test, funcs in test_to_functions.items():
        sole = [f for f in funcs if function_test_count[f] == 1]
        if sole:
            unique[test] = sole
    return unique


def analyze_function_level(
    tests_by_function: dict[str, list[str]],
    killed_by_detail: dict[str, dict[str, list[str] | int]],
    duration_by_test: dict[str, float],
) -> list[TestRecord]:
    """Produce tiered superfluity records using function-level data only."""
    test_to_functions = _invert_function_mapping(tests_by_function)
    kill_counts = _compute_kill_counts(killed_by_detail)
    unique_functions = _find_function_unique_tests(test_to_functions)

    # All known test nodeids: union of function mapping, duration mapping,
    # and killed-by data (some tests appear only in killed-by).
    all_tests = (
        set(test_to_functions.keys())
        | set(duration_by_test.keys())
        | set(kill_counts.keys())
    )

    # Precompute killing tests and their function sets for subset checks.
    killing_tests = {t for t in all_tests if kill_counts[t] > 0}
    killing_test_funcs = {t: test_to_functions.get(t, set()) for t in killing_tests}

    records: list[TestRecord] = []
    for test in sorted(all_tests):
        kills = kill_counts[test]
        duration = duration_by_test.get(test, 0.0)
        funcs = test_to_functions.get(test, set())
        unique_funcs = unique_functions.get(test, [])

        if kills > 0:
            # Killing tests are not candidates for superfluity in
            # function-level analysis (they demonstrably catch mutations).
            records.append(
                TestRecord(
                    test_nodeid=test,
                    tier=0,
                    kills=kills,
                    duration_seconds=round(duration, 4),
                    functions_exercised=sorted(funcs),
                    functions_uniquely_exercised=sorted(unique_funcs),
                    reason="killing test",
                )
            )
            continue

        if not funcs:
            # Tier 1: test does not appear in the function mapping at all.
            records.append(
                TestRecord(
                    test_nodeid=test,
                    tier=1,
                    kills=0,
                    duration_seconds=round(duration, 4),
                    reason="out of mutation scope",
                )
            )
            continue

        if unique_funcs:
            # Test exercises functions no other test touches; it cannot
            # be a subset of any other test's function coverage.
            records.append(
                TestRecord(
                    test_nodeid=test,
                    tier=3,
                    kills=0,
                    duration_seconds=round(duration, 4),
                    functions_exercised=sorted(funcs),
                    functions_uniquely_exercised=sorted(unique_funcs),
                    reason="exercises unique functions",
                )
            )
            continue

        # Check if this test's function set is a subset of any single
        # killing test's function set.
        superset = None
        for kt, kt_funcs in killing_test_funcs.items():
            if funcs <= kt_funcs:
                superset = kt
                break

        if superset is not None:
            records.append(
                TestRecord(
                    test_nodeid=test,
                    tier=2,
                    kills=0,
                    duration_seconds=round(duration, 4),
                    functions_exercised=sorted(funcs),
                    superset_test=superset,
                    reason="function coverage is subset of a killing test",
                )
            )
        else:
            records.append(
                TestRecord(
                    test_nodeid=test,
                    tier=3,
                    kills=0,
                    duration_seconds=round(duration, 4),
                    functions_exercised=sorted(funcs),
                    reason="insufficient data to classify",
                )
            )

    return records


# ---------------------------------------------------------------------------
# Phase 3: Line-level superfluity detection
# ---------------------------------------------------------------------------


def _parse_coverage_contexts(
    coverage_data: dict[str, object],
) -> dict[str, set[tuple[str, int]]]:
    """Parse coverage.py --show-contexts JSON into test -> covered lines.

    The coverage JSON has the structure::

        {"files": {"path": {"contexts": {"line": ["ctx|phase", ...]}}}}

    Context strings have the format ``test_nodeid|phase`` where phase is
    typically ``run`` or ``setup``. We strip the phase suffix and normalize
    to match pytest nodeids.
    """
    test_coverage: dict[str, set[tuple[str, int]]] = {}
    files = cast(dict[str, dict[str, object]], coverage_data.get("files", {}))
    for filepath, filedata in files.items():
        contexts = cast(dict[str, list[str]], filedata.get("contexts", {}))
        for line_str, ctx_list in contexts.items():
            line = int(line_str)
            for ctx in ctx_list:
                # Strip the "|run" or "|setup" suffix.
                nodeid = ctx.rsplit("|", 1)[0] if "|" in ctx else ctx
                if not nodeid:
                    continue
                test_coverage.setdefault(nodeid, set()).add((filepath, line))
    return test_coverage


def analyze_line_level(
    test_coverage: dict[str, set[tuple[str, int]]],
) -> dict[str, tuple[int, int, bool]]:
    """Compute per-test line superfluity.

    Returns a dict mapping test_nodeid to (lines_covered,
    lines_uniquely_covered, is_line_superfluous).

    A test is line-superfluous if every line it covers is also covered
    by at least one other test.
    """
    # Count how many tests cover each line.
    line_count: Counter[tuple[str, int]] = Counter()
    for lines in test_coverage.values():
        for point in lines:
            line_count[point] += 1

    results: dict[str, tuple[int, int, bool]] = {}
    for test, lines in test_coverage.items():
        total = len(lines)
        unique = sum(1 for pt in lines if line_count[pt] == 1)
        is_superfluous = total > 0 and unique == 0
        results[test] = (total, unique, is_superfluous)
    return results


# ---------------------------------------------------------------------------
# Report building
# ---------------------------------------------------------------------------


_CAVEATS = [
    "No single tier is definitive. Function-level analysis identifies"
    " candidates; line-level analysis confirms or refutes most of them;"
    " full certainty requires branch coverage and a complete kill matrix"
    " (no -x).",
    "Under -x mode, killed_by records only the first killing test per"
    " mutant; test ordering determines kill credit, not true uniqueness."
    " A zero-kill test may be the only test capable of killing certain"
    " mutants.",
    "Line coverage does not capture branch-level differences within the"
    " same line; branch = True in coverage.py would close this gap.",
    "Contract tests (tests/contracts/) enforce interface guarantees even"
    " if coverage-redundant; review before removing.",
    "Out-of-scope tests (algorithms, Lua, properties) exercise code"
    " outside the mutation scope and are not superfluous by definition.",
]


def build_report(
    records: list[TestRecord],
    analysis_level: str,
) -> SuperfluityReport:
    """Aggregate records into a summary report."""
    killing = sum(1 for r in records if r.tier == 0)
    zero_kill = sum(1 for r in records if r.tier > 0)
    out_of_scope = sum(1 for r in records if r.tier == 1)
    fn_subset = sum(1 for r in records if r.tier == 2)
    unknown = sum(1 for r in records if r.tier == 3)
    line_superfluous = sum(1 for r in records if r.is_superfluous and r.tier != 1)
    combined = sum(1 for r in records if r.is_superfluous and r.tier == 2)

    return SuperfluityReport(
        analysis_level=analysis_level,
        total_tests=len(records),
        killing_tests=killing,
        zero_kill_tests=zero_kill,
        out_of_scope_tests=out_of_scope,
        function_subset_candidates=fn_subset,
        unknown_tests=unknown,
        line_superfluous_tests=line_superfluous,
        combined_superfluous_tests=combined,
        records=records,
        caveats=_CAVEATS,
    )


def format_text_report(report: SuperfluityReport) -> str:
    """Format the report as a human-readable text summary."""
    lines: list[str] = []
    lines.append("Test Superfluity Analysis")
    lines.append("=" * 60)
    lines.append("")
    lines.append(f"  Analysis level:             {report.analysis_level}")
    lines.append(f"  Total tests:                {report.total_tests}")
    lines.append("")
    lines.append("  Classification breakdown:")
    lines.append(
        f"    Not superfluous (killing): {report.killing_tests}"
        "  (killed at least one mutant)"
    )
    lines.append(
        f"    Not superfluous (scope):   {report.out_of_scope_tests}"
        "  (outside mutated source tree)"
    )
    if report.analysis_level == "line":
        lines.append(
            f"    Candidate + line-superfl.: {report.combined_superfluous_tests}"
            "  (function-subset + no unique lines)"
        )
    lines.append(
        f"    Candidates:                {report.function_subset_candidates}"
        "  (function-subset only, not confirmed)"
    )
    lines.append(
        f"    Unknown:                   {report.unknown_tests}"
        "  (insufficient data, not ordered vs candidates)"
    )
    lines.append("")
    if report.analysis_level == "function":
        lines.append(
            "  Note: function-level analysis can identify candidates but cannot"
        )
        lines.append("  confirm superfluity. Run with per-test line coverage for")
        lines.append(
            "  stronger evidence (see: docker compose --profile coverage-context up)."
        )
        lines.append("")

    # Strongest evidence: tier 2 candidates confirmed by line-level data.
    if report.analysis_level == "line":
        high_conf = sorted(
            [r for r in report.records if r.is_superfluous and r.tier == 2],
            key=lambda r: r.duration_seconds,
            reverse=True,
        )
        if high_conf:
            lines.append(
                f"Strongest Evidence ({len(high_conf)} tests)"
                " - function-subset + line-superfluous"
            )
            lines.append("-" * 60)
            lines.append(
                "  Both function coverage and line coverage are fully redundant."
            )
            lines.append(
                "  Strongest signal available, but not definitive: branch-level"
            )
            lines.append("  differences and -x kill credit bias remain (see caveats).")
            lines.append("")
            for r in high_conf:
                lines.append(f"  {r.duration_seconds:.4f}s  {r.test_nodeid}")
                if r.superset_test:
                    lines.append(f"           superset: {r.superset_test}")
                lines.append(
                    f"           lines: {r.lines_covered} covered,"
                    f" {r.lines_uniquely_covered} unique"
                )
                lines.append("")

    # Tier 2 candidates (function-subset), sorted by duration descending
    # so the most expensive candidates appear first.
    tier2 = sorted(
        [r for r in report.records if r.tier == 2],
        key=lambda r: r.duration_seconds,
        reverse=True,
    )
    # When line-level data is available, separate out the high-confidence
    # tests (already shown above) from the remaining candidates.
    if report.analysis_level == "line":
        tier2_remaining = [r for r in tier2 if not r.is_superfluous]
    else:
        tier2_remaining = tier2
    if tier2_remaining:
        if report.analysis_level == "line":
            lines.append(
                f"Candidates ({len(tier2_remaining)} tests)"
                " - function-subset only, NOT line-superfluous"
            )
            lines.append("-" * 60)
            lines.append(
                "  Function coverage is redundant, but these tests cover unique"
            )
            lines.append("  source lines. They are likely NOT superfluous.")
        else:
            lines.append(
                f"Candidates ({len(tier2_remaining)} tests)"
                " - function-subset, not confirmed"
            )
            lines.append("-" * 60)
            lines.append("  Function coverage is a strict subset of a killing test.")
            lines.append(
                "  Cannot confirm superfluity without line-level coverage data."
            )
        lines.append("  Sorted by duration (most expensive first).")
        lines.append("")
        for r in tier2_remaining:
            lines.append(f"  {r.duration_seconds:.4f}s  {r.test_nodeid}")
            if r.superset_test:
                lines.append(f"           superset: {r.superset_test}")
            if r.functions_exercised:
                lines.append(
                    f"           functions: {', '.join(r.functions_exercised)}"
                )
            if report.analysis_level == "line":
                lines.append(
                    f"           lines: {r.lines_covered} covered,"
                    f" {r.lines_uniquely_covered} unique"
                )
            lines.append("")

    # Line-superfluous tests that are NOT tier 2 (they exercise unique
    # functions or are tier 3, but still have no unique lines).
    if report.analysis_level == "line":
        line_only = sorted(
            [r for r in report.records if r.is_superfluous and r.tier not in (0, 1, 2)],
            key=lambda r: r.duration_seconds,
            reverse=True,
        )
        if line_only:
            lines.append(
                f"Line-Superfluous Only ({len(line_only)} tests)"
                " - no unique lines, but not function-subset"
            )
            lines.append("-" * 60)
            lines.append(
                "  Every source line these tests cover is also covered by another"
            )
            lines.append(
                "  test, but their function coverage is not a subset of any single"
            )
            lines.append(
                "  killing test. Weaker evidence than the strongest-evidence group."
            )
            lines.append("")
            for r in line_only:
                lines.append(
                    f"  {r.duration_seconds:.4f}s  {r.test_nodeid}"
                    f"  ({r.lines_covered} lines, {r.lines_uniquely_covered} unique)"
                )
            lines.append("")

    # Out-of-scope tests.
    tier1 = [r for r in report.records if r.tier == 1]
    if tier1:
        lines.append(f"Not Superfluous: Out of Mutation Scope ({len(tier1)} tests)")
        lines.append("-" * 60)
        lines.append("  These tests exercise code outside the mutated source tree")
        lines.append(
            "  (algorithms, Lua scripts, properties). Definitively not superfluous."
        )
        lines.append("")
        for r in sorted(tier1, key=lambda r: r.test_nodeid):
            lines.append(f"  {r.test_nodeid}")
        lines.append("")

    # Caveats.
    lines.append("Caveats")
    lines.append("-" * 60)
    for c in report.caveats:
        lines.append(f"  - {c}")
    lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# I/O and entry point
# ---------------------------------------------------------------------------


def _load_json(path: Path) -> dict[str, object]:
    """Load a JSON file, exiting with an error if it does not exist."""
    if not path.exists():
        print(f"error: {path} not found", file=sys.stderr)
        sys.exit(1)
    with open(path) as f:
        result: dict[str, object] = json.load(f)
    return result


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Analyze test superfluity from mutation testing data.",
    )
    parser.add_argument(
        "results_dir",
        type=Path,
        help="Directory containing mutmut report files (stats.json, report-detail.json).",
    )
    parser.add_argument(
        "--coverage",
        type=Path,
        default=None,
        help="Path to coverage-contexts.json (coverage.py --show-contexts output).",
    )
    args = parser.parse_args(argv)

    results_dir: Path = args.results_dir

    # Load required data.
    stats = _load_json(results_dir / "stats.json")
    detail = _load_json(results_dir / "report-detail.json")

    tests_by_function = cast(
        dict[str, list[str]],
        stats.get("tests_by_mangled_function_name", {}),
    )
    duration_by_test = cast(
        dict[str, float],
        stats.get("duration_by_test", {}),
    )
    killed_by_detail = cast(
        dict[str, dict[str, list[str] | int]],
        detail.get("killed_by_detail", {}),
    )

    # Phase 1: function-level analysis.
    records = analyze_function_level(
        tests_by_function,
        killed_by_detail,
        duration_by_test,
    )

    analysis_level = "function"

    # Phase 3: line-level analysis (when coverage data is available).
    coverage_path = args.coverage
    if coverage_path is None:
        # Auto-detect coverage-contexts.json in the results directory.
        auto_path = results_dir / "coverage-contexts.json"
        if auto_path.exists():
            coverage_path = auto_path

    if coverage_path is not None and coverage_path.exists():
        analysis_level = "line"
        coverage_data = _load_json(coverage_path)
        test_coverage = _parse_coverage_contexts(coverage_data)
        line_results = analyze_line_level(test_coverage)

        # Enrich records with line-level data.
        for record in records:
            info = line_results.get(record.test_nodeid)
            if info is not None:
                covered, unique, superfluous = info
                record.lines_covered = covered
                record.lines_uniquely_covered = unique
                record.is_superfluous = superfluous

    # Build and write report.
    report = build_report(records, analysis_level)

    json_path = results_dir / "superfluity-report.json"
    txt_path = results_dir / "superfluity-report.txt"

    json_output = asdict(report)
    # Exclude full records from the summary JSON; write them separately.
    summary = {k: v for k, v in json_output.items() if k != "records"}
    summary["candidates"] = [
        asdict(r) for r in report.records if r.tier == 2 or r.is_superfluous
    ]

    with open(json_path, "w") as f:
        json.dump(summary, f, indent=2)

    text = format_text_report(report)
    with open(txt_path, "w") as f:
        f.write(text)

    print(text)
    print(f"Reports written to {json_path} and {txt_path}")


if __name__ == "__main__":
    main()
