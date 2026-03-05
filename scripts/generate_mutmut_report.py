"""Generate a unified mutation testing report from mutmut artifacts.

Single-pass report generator that reads mutmut's ``.meta`` files and
trampolined source files to produce a unified JSON report and a
human-readable text summary. Parses each source file once via libcst to
extract diffs for all non-killed mutants in that file.

This is the primary post-processing entry point; it is called by the
``mutate`` service in ``docker-compose.yml`` after ``run_mutmut.py``
completes. It imports ``classify_mutants`` and ``extract_mutation_score``
as libraries.

Output files (written to ``--output-dir``):

- ``report.json``: structured data for programmatic consumption
- ``report.txt``: human-readable summary (also printed to stdout)
- ``mutation-score.txt``: score percentage for CI badge consumption
- ``stats.json``: copy of ``mutmut-stats.json`` (test mapping, durations)

Usage::

    python scripts/generate_mutmut_report.py \\
        --log /tmp/mutmut-run.log \\
        --output-dir /app/mutation-output \\
        --killed-by /tmp/mutmut_killed_by_results.json
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import sys
from collections import Counter
from dataclasses import asdict, dataclass, field
from difflib import unified_diff
from pathlib import Path

import libcst as cst

# Make sibling scripts importable.
sys.path.insert(0, str(Path(__file__).parent))

from classify_mutants import (  # noqa: E402
    ClassifiedMutation,
    MutationDiff,
    _SCORE_HEADERS,
    _SCORE_LABELS,
    _SCORE_NOTES,
    _classify,
    _extract_diff_lines,
    _find_mirrors,
    _match_known_benign,
    _normalize_for_mirror,
    _shorten_name,
)
from extract_mutation_score import extract_score  # noqa: E402


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass
class MutantRecord:
    """Complete record for a single mutant."""

    name: str
    short_name: str
    status: str
    source_file: str
    duration_seconds: float | None = None
    diff: str | None = None
    line_number: int | None = None
    killed_by: list[str] = field(default_factory=list)
    tests_run: int | None = None
    classification_score: int | None = None
    mutation_type: str | None = None
    classification_desc: str | None = None
    is_known_benign: bool = False
    benign_reason: str | None = None
    mirror_key: str | None = None


@dataclass
class KilledBySummary:
    """Aggregated killed-by statistics."""

    tracked_kills: int
    killing_tests: int
    zero_kill_tests: int
    total_tests: int


@dataclass
class ClassificationSummary:
    """Counts per classification score."""

    score_0_cosmetic: int = 0
    score_1_argument: int = 0
    score_2_logic: int = 0
    score_3_fork_immune: int = 0
    known_benign: int = 0


@dataclass
class FileSummary:
    """Per-source-file mutation statistics."""

    path: str
    total: int
    killed: int
    survived: int
    no_tests: int
    timeout: int
    survival_rate: float


@dataclass
class MutationTypeSummary:
    """Per-mutation-type statistics for non-killed mutants."""

    total: int
    survived: int


@dataclass
class TestEfficiency:
    """Per-test efficiency metrics."""

    test_nodeid: str
    kills: int
    unique_kills: int
    duration_seconds: float
    kills_per_second: float


@dataclass
class UnifiedReport:
    """Top-level report structure."""

    mutation_score: float
    total: int
    killed: int
    survived: int
    timeout: int
    suspicious: int
    no_tests: int
    skipped: int
    classification: ClassificationSummary
    killed_by_summary: KilledBySummary | None
    files: list[FileSummary]
    mutation_type_summary: dict[str, MutationTypeSummary]
    test_effectiveness: list[TestEfficiency] | None
    uncovered_functions: list[str]
    mutants: list[MutantRecord]


# ---------------------------------------------------------------------------
# Loading mutmut data
# ---------------------------------------------------------------------------


def _load_all_meta() -> dict[str, tuple[str, float | None, str]]:
    """Load all ``.meta`` files.

    Returns ``{mutant_name: (status, duration_seconds, source_path)}``.
    """
    import mutmut
    from mutmut.__main__ import (
        SourceFileMutationData,
        status_by_exit_code,
        walk_source_files,
    )

    result: dict[str, tuple[str, float | None, str]] = {}
    for path in walk_source_files():
        if mutmut.config.should_ignore_for_mutation(path):
            continue
        m = SourceFileMutationData(path=path)
        m.load()
        for key, exit_code in m.exit_code_by_key.items():
            status = status_by_exit_code[exit_code]
            duration = m.durations_by_key.get(key)
            result[key] = (status, duration, str(path))
    return result


def _load_killed_by_raw(path: Path) -> dict:
    """Load the raw killed-by JSON file."""
    try:
        with open(path) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def _parse_killed_by(
    raw: dict,
) -> tuple[dict[str, list[str]], dict[str, int]]:
    """Parse the killed-by data into separate mappings.

    Handles both legacy format (``{name: [tests]}``) and new format
    (``{name: {"killed_by": [tests], "tests_run": N}}``).

    Returns ``(killed_by, tests_run_data)`` where:
    - ``killed_by``: ``{mutant_name: [test_nodeids]}``
    - ``tests_run_data``: ``{mutant_name: tests_run_count}``
    """
    killed_by: dict[str, list[str]] = {}
    tests_run_data: dict[str, int] = {}

    for name, value in raw.items():
        if isinstance(value, list):
            # Legacy format: value is directly the list of test nodeids.
            killed_by[name] = value
        elif isinstance(value, dict):
            # New format: value is {"killed_by": [...], "tests_run": N}.
            killed_by[name] = value.get("killed_by", [])
            tests_run_data[name] = value.get("tests_run", 0)

    return killed_by, tests_run_data


def _load_all_test_nodeids(stats_path: Path) -> set[str]:
    """Load the full set of test nodeids from ``mutmut-stats.json``."""
    try:
        with open(stats_path) as f:
            stats = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return set()
    all_tests: set[str] = set()
    for test_list in stats.get("tests_by_mangled_function_name", {}).values():
        all_tests.update(test_list)
    return all_tests


# ---------------------------------------------------------------------------
# Batch diff extraction
# ---------------------------------------------------------------------------


def _generate_all_diffs(
    non_killed_names: set[str],
    all_meta: dict[str, tuple[str, float | None, str]],
) -> dict[str, tuple[str, list[str], list[str], list[str]]]:
    """Generate diffs for all non-killed mutants, parsing each source file once.

    Returns ``{mutant_name: (diff_text, old_lines, new_lines, context_lines)}``.
    """
    from mutmut.__main__ import (
        read_mutant_function,
        read_mutants_module,
        read_original_function,
    )

    # Group non-killed mutants by source file.
    by_source: dict[str, list[str]] = {}
    for name in non_killed_names:
        _, _, source_path = all_meta[name]
        by_source.setdefault(source_path, []).append(name)

    results: dict[str, tuple[str, list[str], list[str], list[str]]] = {}

    for source_path, mutant_names in by_source.items():
        try:
            module = read_mutants_module(Path(source_path))
        except (FileNotFoundError, OSError) as exc:
            for name in mutant_names:
                results[name] = (f"# error reading source: {exc}", [], [], [])
            continue

        for mutant_name in mutant_names:
            try:
                orig_fn = read_original_function(module, mutant_name)
                mutant_fn = read_mutant_function(module, mutant_name)
                orig_code = cst.Module(body=[orig_fn]).code.strip()
                mutant_code = cst.Module(body=[mutant_fn]).code.strip()

                diff_lines = list(
                    unified_diff(
                        orig_code.split("\n"),
                        mutant_code.split("\n"),
                        fromfile=source_path,
                        tofile=source_path,
                        lineterm="",
                    )
                )
                diff_text = "\n".join(diff_lines)
                old, new, ctx = _extract_diff_lines(diff_text)
                results[mutant_name] = (diff_text, old, new, ctx)
            except Exception as exc:
                results[mutant_name] = (f"# error: {exc}", [], [], [])

    return results


# ---------------------------------------------------------------------------
# Building records
# ---------------------------------------------------------------------------


_HUNK_LINE_RE = re.compile(r"@@ -(\d+)")


def _extract_line_number(diff_text: str) -> int | None:
    """Extract the source line number from the first ``@@`` hunk header."""
    m = _HUNK_LINE_RE.search(diff_text)
    return int(m.group(1)) if m else None


def _build_records(
    all_meta: dict[str, tuple[str, float | None, str]],
    diffs: dict[str, tuple[str, list[str], list[str], list[str]]],
    killed_by: dict[str, list[str]],
    tests_run_data: dict[str, int] | None = None,
) -> list[MutantRecord]:
    """Build a ``MutantRecord`` for every mutant."""
    records: list[MutantRecord] = []
    tests_run_data = tests_run_data or {}

    for name, (status, duration, source_path) in all_meta.items():
        short_name = _shorten_name(name)
        record = MutantRecord(
            name=name,
            short_name=short_name,
            status=status,
            source_file=source_path,
            duration_seconds=duration,
            killed_by=killed_by.get(name, []),
            tests_run=tests_run_data.get(name),
        )

        if name in diffs:
            diff_text, old_lines, new_lines, ctx_lines = diffs[name]
            record.diff = diff_text
            record.line_number = _extract_line_number(diff_text)

            if old_lines or new_lines:
                mutation_diff = MutationDiff(
                    name=name,
                    status=status,
                    body=diff_text,
                    old_lines=old_lines,
                    new_lines=new_lines,
                    context_lines=ctx_lines,
                )
                score, mutation_type, description = _classify(mutation_diff)
                record.classification_score = score
                record.mutation_type = mutation_type
                record.classification_desc = description

                reason = _match_known_benign(short_name, description)
                if reason is not None:
                    record.is_known_benign = True
                    record.benign_reason = reason

        records.append(record)

    return records


def _attach_mirror_keys(records: list[MutantRecord]) -> None:
    """Detect sync/async mirrors and attach ``mirror_key`` to each record."""
    # Build ClassifiedMutation wrappers for non-killed, classified records.
    classified_by_name: dict[str, ClassifiedMutation] = {}
    for r in records:
        if r.classification_score is not None and not r.is_known_benign:
            cm = ClassifiedMutation(
                diff=MutationDiff(
                    name=r.name,
                    status=r.status,
                    body=r.diff or "",
                ),
                score=r.classification_score,
                mutation_type=r.mutation_type or "unknown",
                description=r.classification_desc or "",
                short_name=r.short_name,
            )
            classified_by_name[r.name] = cm

    if not classified_by_name:
        return

    _, mirrored_names = _find_mirrors(list(classified_by_name.values()))

    # Assign mirror keys.
    for r in records:
        if r.name in classified_by_name and r.name in mirrored_names:
            cm = classified_by_name[r.name]
            r.mirror_key = (
                f"{_normalize_for_mirror(cm.short_name)}"
                f"|{cm.score}|{cm.mutation_type}"
            )


# ---------------------------------------------------------------------------
# Report building
# ---------------------------------------------------------------------------


def _build_file_summaries(
    all_meta: dict[str, tuple[str, float | None, str]],
) -> list[FileSummary]:
    """Aggregate mutation statistics per source file."""
    file_totals: Counter[str] = Counter()
    file_killed: Counter[str] = Counter()
    file_survived: Counter[str] = Counter()
    file_no_tests: Counter[str] = Counter()
    file_timeout: Counter[str] = Counter()

    for _, (status, _, source_path) in all_meta.items():
        file_totals[source_path] += 1
        if status == "killed":
            file_killed[source_path] += 1
        elif status == "survived":
            file_survived[source_path] += 1
        elif status == "no tests":
            file_no_tests[source_path] += 1
        elif status == "timeout":
            file_timeout[source_path] += 1

    summaries = []
    for path in sorted(file_totals, key=file_totals.get, reverse=True):
        total = file_totals[path]
        killed = file_killed[path]
        survived = file_survived[path]
        non_killed = total - killed
        survival_rate = round(non_killed / total * 100, 2) if total else 0.0
        summaries.append(
            FileSummary(
                path=path,
                total=total,
                killed=killed,
                survived=survived,
                no_tests=file_no_tests[path],
                timeout=file_timeout[path],
                survival_rate=survival_rate,
            )
        )
    return summaries


def _build_mutation_type_summary(
    records: list[MutantRecord],
) -> dict[str, MutationTypeSummary]:
    """Aggregate mutation type statistics from non-killed mutants."""
    type_total: Counter[str] = Counter()
    type_survived: Counter[str] = Counter()

    for r in records:
        if r.mutation_type is not None:
            mtype = r.mutation_type
            type_total[mtype] += 1
            if r.status == "survived" and not r.is_known_benign:
                type_survived[mtype] += 1

    return {
        mtype: MutationTypeSummary(total=type_total[mtype], survived=type_survived[mtype])
        for mtype in sorted(type_total, key=type_total.get, reverse=True)
    }


def _build_test_effectiveness(
    killed_by: dict[str, list[str]],
    duration_by_test: dict[str, float],
) -> list[TestEfficiency]:
    """Compute per-test efficiency: kills per second of test runtime."""
    kill_counts: Counter[str] = Counter()
    unique_kill_counts: Counter[str] = Counter()

    for test_list in killed_by.values():
        for t in test_list:
            kill_counts[t] += 1
    for test_list in killed_by.values():
        if len(test_list) == 1:
            unique_kill_counts[test_list[0]] += 1

    results = []
    for test_nodeid, kills in kill_counts.items():
        duration = duration_by_test.get(test_nodeid, 0.0)
        kps = kills / duration if duration > 0 else float("inf")
        results.append(
            TestEfficiency(
                test_nodeid=test_nodeid,
                kills=kills,
                unique_kills=unique_kill_counts.get(test_nodeid, 0),
                duration_seconds=round(duration, 4),
                kills_per_second=round(kps, 2),
            )
        )
    results.sort(key=lambda t: t.kills_per_second, reverse=True)
    return results


def _detect_uncovered_functions(
    all_meta: dict[str, tuple[str, float | None, str]],
    tests_by_function: dict[str, list[str]],
) -> list[str]:
    """Identify mutated functions that have zero test coverage.

    Extracts the mangled function name (without ``__mutmut_N`` suffix) from
    each mutant key and checks whether ``tests_by_mangled_function_name``
    has any tests mapped to it.
    """
    # Collect all mutated function names (strip __mutmut_N suffix).
    mutated_functions: set[str] = set()
    for name in all_meta:
        base = re.sub(r"__mutmut_\d+$", "", name)
        mutated_functions.add(base)

    uncovered = sorted(
        fn for fn in mutated_functions
        if fn not in tests_by_function or not tests_by_function[fn]
    )
    return uncovered


def _build_report(
    records: list[MutantRecord],
    score: float,
    killed_count: int,
    total_count: int,
    killed_by: dict[str, list[str]],
    all_test_nodeids: set[str],
    all_meta: dict[str, tuple[str, float | None, str]],
    stats_data: dict | None = None,
) -> UnifiedReport:
    """Assemble the unified report from records and summary data."""
    # Count statuses.
    status_counts: Counter[str] = Counter()
    for r in records:
        status_counts[r.status] += 1

    # Classification summary (survivors only).
    cls_summary = ClassificationSummary()
    for r in records:
        if r.is_known_benign:
            cls_summary.known_benign += 1
        elif r.classification_score == 0:
            cls_summary.score_0_cosmetic += 1
        elif r.classification_score == 1:
            cls_summary.score_1_argument += 1
        elif r.classification_score == 2:
            cls_summary.score_2_logic += 1
        elif r.classification_score == 3:
            cls_summary.score_3_fork_immune += 1

    # Killed-by summary.
    kb_summary = None
    if killed_by:
        kill_counts: Counter[str] = Counter()
        for test_list in killed_by.values():
            for t in test_list:
                kill_counts[t] += 1
        killing_tests = set(kill_counts.keys())
        zero_kill = len(all_test_nodeids - killing_tests) if all_test_nodeids else 0
        kb_summary = KilledBySummary(
            tracked_kills=len(killed_by),
            killing_tests=len(killing_tests),
            zero_kill_tests=zero_kill,
            total_tests=len(all_test_nodeids),
        )

    # Per-file summary.
    file_summaries = _build_file_summaries(all_meta)

    # Mutation type summary.
    mutation_type_summary = _build_mutation_type_summary(records)

    # Test effectiveness (requires stats.json duration data).
    test_effectiveness = None
    duration_by_test = (stats_data or {}).get("duration_by_test", {})
    if killed_by and duration_by_test:
        test_effectiveness = _build_test_effectiveness(killed_by, duration_by_test)

    # Uncovered functions.
    tests_by_function = (stats_data or {}).get("tests_by_mangled_function_name", {})
    uncovered = _detect_uncovered_functions(all_meta, tests_by_function)

    return UnifiedReport(
        mutation_score=round(score, 2),
        total=total_count,
        killed=killed_count,
        survived=status_counts.get("survived", 0),
        timeout=status_counts.get("timeout", 0),
        suspicious=status_counts.get("suspicious", 0),
        no_tests=status_counts.get("no tests", 0),
        skipped=status_counts.get("skipped", 0),
        classification=cls_summary,
        killed_by_summary=kb_summary,
        files=file_summaries,
        mutation_type_summary=mutation_type_summary,
        test_effectiveness=test_effectiveness,
        uncovered_functions=uncovered,
        mutants=records,
    )


# ---------------------------------------------------------------------------
# Text report formatting
# ---------------------------------------------------------------------------


def _format_text_report(report: UnifiedReport) -> str:
    """Generate a human-readable text report from the unified report."""
    lines: list[str] = []

    # Score summary.
    lines.append("Mutation Testing Report")
    lines.append("=" * 60)
    lines.append(f"Score: {report.mutation_score:.2f}% ({report.killed}/{report.total} killed)")
    lines.append("")
    lines.append(f"  Killed:     {report.killed:>5}")
    lines.append(f"  Survived:   {report.survived:>5}")
    lines.append(f"  Timeout:    {report.timeout:>5}")
    lines.append(f"  Suspicious: {report.suspicious:>5}")
    lines.append(f"  No tests:   {report.no_tests:>5}")
    if report.skipped:
        lines.append(f"  Skipped:    {report.skipped:>5}")
    lines.append("")

    # Classification summary.
    cls = report.classification
    lines.append("Classification (non-killed mutants)")
    lines.append("-" * 60)
    for score_val, label in _SCORE_LABELS.items():
        count = getattr(cls, f"score_{score_val}_{label}", 0)
        lines.append(f"  Score {score_val} ({label}): {count:>4}")
    if cls.known_benign:
        lines.append(f"  Known benign:        {cls.known_benign:>4}")
    lines.append("")

    # Per-score-group survivor listings.
    non_killed = [r for r in report.mutants if r.status != "killed"]
    classified: list[ClassifiedMutation] = []
    benign: list[tuple[ClassifiedMutation, str]] = []
    timeout_names: list[str] = []
    no_test_names: list[str] = []

    for r in non_killed:
        if r.status == "timeout":
            timeout_names.append(r.name)
            continue
        if r.status == "no tests":
            no_test_names.append(r.name)
            continue
        if r.classification_score is None:
            continue

        cm = ClassifiedMutation(
            diff=MutationDiff(name=r.name, status=r.status, body=r.diff or ""),
            score=r.classification_score,
            mutation_type=r.mutation_type or "unknown",
            description=r.classification_desc or "",
            short_name=r.short_name,
        )
        if r.is_known_benign and r.benign_reason:
            benign.append((cm, r.benign_reason))
        else:
            classified.append(cm)

    classified.sort(key=lambda m: (-m.score, m.short_name))
    mirrors, mirrored_names = _find_mirrors(classified)

    for score_val in (3, 2, 1, 0):
        score_mutations = [m for m in classified if m.score == score_val]
        if not score_mutations:
            continue

        header = _SCORE_HEADERS[score_val]
        note = _SCORE_NOTES.get(score_val)

        score_mirrors = [g for g in mirrors if g.members[0].score == score_val]
        non_mirrored = [m for m in score_mutations if m.diff.name not in mirrored_names]

        count = len(non_mirrored) + sum(len(g.members) for g in score_mirrors)
        lines.append(f"--- {header} ({count}) ---")
        if note:
            lines.append(f"  {note}")

        for group in score_mirrors:
            representative = group.members[0]
            method_part = _normalize_for_mirror(representative.short_name)
            lines.append(f"  [sync/async mirror] {method_part}")
            for member in group.members:
                lines.append(f"    {member.short_name}")
            lines.append(f"    {representative.description}")
            lines.append("")

        for m in non_mirrored:
            lines.append(f"  {m.short_name}")
            lines.append(f"    {m.description}")
            lines.append("")

    if timeout_names:
        lines.append(f"--- TIMEOUTS ({len(timeout_names)}) ---")
        for name in timeout_names:
            lines.append(f"  {_shorten_name(name)}")
        lines.append("")

    if no_test_names:
        lines.append(f"--- NO TESTS ({len(no_test_names)}) ---")
        for name in no_test_names:
            lines.append(f"  {_shorten_name(name)}")
        lines.append("")

    if benign:
        lines.append(f"--- KNOWN BENIGN ({len(benign)}) ---")
        for cm, reason in benign:
            lines.append(f"  {cm.short_name}")
            lines.append(f"    {cm.description}")
            lines.append(f"    reason: {reason}")
            lines.append("")

    # Killed-by summary.
    kb = report.killed_by_summary
    if kb is not None:
        lines.append("Killed-By Summary")
        lines.append("-" * 60)
        lines.append(f"  Mutants with kill data: {kb.tracked_kills}")
        lines.append(f"  Killing tests:         {kb.killing_tests}")
        if kb.total_tests:
            lines.append(f"  Zero-kill tests:       {kb.zero_kill_tests} of {kb.total_tests}")
        lines.append("")

        kill_counts: Counter[str] = Counter()
        unique_kill_counts: Counter[str] = Counter()
        killed_by_data = {
            r.name: r.killed_by
            for r in report.mutants
            if r.killed_by
        }
        for test_list in killed_by_data.values():
            for t in test_list:
                kill_counts[t] += 1
        for test_list in killed_by_data.values():
            if len(test_list) == 1:
                unique_kill_counts[test_list[0]] += 1

        if kill_counts:
            lines.append("  Top 20 killing tests:")
            for test_nodeid, count in kill_counts.most_common(20):
                unique = unique_kill_counts.get(test_nodeid, 0)
                lines.append(f"    {count:4d} kills ({unique:3d} unique)  {test_nodeid}")
            lines.append("")

    # Test effectiveness.
    if report.test_effectiveness:
        lines.append("Test Effectiveness (kills per second of test runtime)")
        lines.append("-" * 60)
        lines.append("  Top 10 most efficient:")
        for te in report.test_effectiveness[:10]:
            kps = f"{te.kills_per_second:.1f}" if te.kills_per_second != float("inf") else "inf"
            lines.append(
                f"    {kps:>7} k/s  {te.kills:4d} kills  {te.duration_seconds:.3f}s  {te.test_nodeid}"
            )
        lines.append("")

        zero_kill = [
            te for te in report.test_effectiveness if te.kills == 0
        ]
        if zero_kill:
            zero_kill.sort(key=lambda t: t.duration_seconds, reverse=True)
            lines.append(f"  Slowest zero-kill tests ({len(zero_kill)} total):")
            for te in zero_kill[:10]:
                lines.append(f"    {te.duration_seconds:.3f}s  {te.test_nodeid}")
            lines.append("")

    # Per-file survival rates.
    if report.files:
        non_perfect = [f for f in report.files if f.survival_rate > 0]
        if non_perfect:
            lines.append("Per-File Survival Rates")
            lines.append("-" * 60)
            for fs in non_perfect:
                lines.append(
                    f"  {fs.survival_rate:5.1f}%  {fs.survived:3d} survived  "
                    f"{fs.no_tests:3d} no tests  {fs.total:4d} total  {fs.path}"
                )
            lines.append("")

    # Mutation type distribution.
    if report.mutation_type_summary:
        lines.append("Mutation Type Distribution (non-killed)")
        lines.append("-" * 60)
        for mtype, mts in report.mutation_type_summary.items():
            lines.append(f"  {mts.total:4d} total  {mts.survived:3d} survived  {mtype}")
        lines.append("")

    # Uncovered functions.
    if report.uncovered_functions:
        lines.append(f"Uncovered Functions ({len(report.uncovered_functions)} with zero test coverage)")
        lines.append("-" * 60)
        for fn in report.uncovered_functions:
            lines.append(f"  {_shorten_name(fn)}")
        lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# JSON serialization
# ---------------------------------------------------------------------------


def _serialize_report(report: UnifiedReport, include_killed: bool = False) -> dict:
    """Serialize the report to a JSON-compatible dict.

    By default, killed mutants are excluded from the ``mutants`` list to
    keep the JSON manageable. Set ``include_killed=True`` to include them.

    Killed-by and tests-run data is always included in a separate
    ``killed_by_detail`` mapping so it is not lost when killed mutants are
    filtered from the ``mutants`` list.
    """
    d = asdict(report)

    # Extract killed-by / tests-run into a compact top-level mapping before
    # potentially dropping killed mutant records.
    detail: dict[str, dict] = {}
    for m in d["mutants"]:
        if m["killed_by"] or m["tests_run"] is not None:
            entry: dict = {}
            if m["killed_by"]:
                entry["killed_by"] = m["killed_by"]
            if m["tests_run"] is not None:
                entry["tests_run"] = m["tests_run"]
            detail[m["name"]] = entry
    d["killed_by_detail"] = detail

    if not include_killed:
        d["mutants"] = [m for m in d["mutants"] if m["status"] != "killed"]

    # Replace inf values in test_effectiveness (JSON does not support inf).
    if d.get("test_effectiveness"):
        for te in d["test_effectiveness"]:
            if te["kills_per_second"] == float("inf"):
                te["kills_per_second"] = -1  # sentinel for "instant kill"
    return d


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate a unified mutation testing report from mutmut artifacts."
    )
    parser.add_argument(
        "--log",
        default="/tmp/mutmut-run.log",
        help="path to the mutmut run log (default: /tmp/mutmut-run.log)",
    )
    parser.add_argument(
        "--output-dir",
        default="/app/mutation-output",
        help="output directory for report files (default: /app/mutation-output)",
    )
    parser.add_argument(
        "--killed-by",
        default="/tmp/mutmut_killed_by_results.json",
        help="path to the killed-by JSON (default: /tmp/mutmut_killed_by_results.json)",
    )
    parser.add_argument(
        "--include-killed",
        action="store_true",
        help="include killed mutants in the JSON output (increases file size)",
    )
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    from mutmut.__main__ import ensure_config_loaded

    ensure_config_loaded()

    # Load all .meta files.
    print("Loading mutation metadata...", flush=True)
    all_meta = _load_all_meta()
    if not all_meta:
        print("Error: no mutation metadata found. Did mutmut run complete?", file=sys.stderr)
        sys.exit(1)
    print(f"  {len(all_meta)} mutants found.", flush=True)

    # Load killed-by data.
    killed_by_raw = _load_killed_by_raw(Path(args.killed_by))
    killed_by, tests_run_data = _parse_killed_by(killed_by_raw)
    print(f"  {len(killed_by)} mutants with killed-by data.", flush=True)

    # Load mutmut-stats.json for test mapping and durations.
    stats_path = Path("mutants/mutmut-stats.json")
    stats_data: dict | None = None
    try:
        with open(stats_path) as f:
            stats_data = json.load(f)
        print(f"  Stats loaded: {len(stats_data.get('duration_by_test', {}))} test durations.", flush=True)
    except (FileNotFoundError, json.JSONDecodeError):
        print("  Warning: mutmut-stats.json not found; test effectiveness will be unavailable.", flush=True)

    all_test_nodeids = _load_all_test_nodeids(stats_path)

    # Extract mutation score from log.
    log_path = Path(args.log)
    try:
        log_text = log_path.read_text(encoding="utf-8")
        score, killed_count, total_count = extract_score(log_text)
    except (FileNotFoundError, ValueError) as exc:
        print(f"Warning: could not extract score from log: {exc}", file=sys.stderr)
        total_count = len(all_meta)
        killed_count = sum(1 for s, _, _ in all_meta.values() if s == "killed")
        score = killed_count / total_count * 100 if total_count else 0.0

    # Generate diffs for non-killed mutants.
    non_killed = {
        name
        for name, (status, _, _) in all_meta.items()
        if status != "killed"
    }
    print(f"  {len(non_killed)} non-killed mutants; generating diffs...", flush=True)
    diffs = _generate_all_diffs(non_killed, all_meta)
    print(f"  {len(diffs)} diffs generated.", flush=True)

    # Build records and attach mirror keys.
    records = _build_records(all_meta, diffs, killed_by, tests_run_data or None)
    _attach_mirror_keys(records)

    # Build unified report.
    report = _build_report(
        records, score, killed_count, total_count,
        killed_by, all_test_nodeids, all_meta, stats_data,
    )

    # Write output files.
    json_path = output_dir / "report.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(_serialize_report(report, include_killed=args.include_killed), f, indent=2)
    print(f"  JSON report written to {json_path}", flush=True)

    score_path = output_dir / "mutation-score.txt"
    score_path.write_text(f"{score:.2f}", encoding="utf-8")

    if stats_data is not None:
        shutil.copy2(stats_path, output_dir / "stats.json")

    text = _format_text_report(report)
    text_path = output_dir / "report.txt"
    text_path.write_text(text, encoding="utf-8")

    print("")
    print(text)


if __name__ == "__main__":
    main()
