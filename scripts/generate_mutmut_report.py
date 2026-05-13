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

- ``report.json``: structured summary for programmatic consumption
- ``report-detail.json``: large diagnostic data (killed-by mappings, test effectiveness)
- ``all-mutations.json``: every mutant with diffs (gitignored; for offline analysis)
- ``report.txt``: human-readable summary (also printed to stdout)
- ``mutation-score.txt``: score percentage for CI badge consumption
- ``test-timeline.jsonl``: raw per-test timing events (when a timeline file is supplied)
- ``test-timeline.txt``: human-readable timeline + cross-PID overlap analysis

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
import sys
from collections import Counter
from dataclasses import asdict, dataclass, field
from difflib import unified_diff
from pathlib import Path

import libcst as cst

# Make sibling scripts importable.
sys.path.insert(0, str(Path(__file__).parent))

from classify_mutants import (  # noqa: E402
    _SCORE_HEADERS,
    _SCORE_LABELS,
    _SCORE_NOTES,
    ClassifiedMutation,
    MutationDiff,
    MutationKind,
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
    partial_data: bool = False
    killed_during: str | None = None
    tests_selected: list[str] | None = None
    tests_ran: list[str] | None = None
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
    wall_clock_seconds: float | None = None


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


def _load_mutation_types(path: Path) -> dict[str, MutationKind]:
    """Load per-mutant operator metadata captured by run_mutmut.py Patch 6.

    A missing or unreadable file yields ``{}``, which falls the classifier
    back to its diff-based heuristics for every mutant.
    """
    try:
        with open(path) as f:
            raw = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}
    if not isinstance(raw, dict):
        return {}

    result: dict[str, MutationKind] = {}
    for mutant_id, entry in raw.items():
        if not isinstance(entry, dict):
            continue
        result[mutant_id] = MutationKind(
            operator=entry.get("operator"),
            line=entry.get("line"),
            source_file=entry.get("source_file"),
            function=entry.get("function"),
            is_default_param=bool(entry.get("is_default_param", False)),
            original_node_type=entry.get("original_node_type"),
            mutated_node_type=entry.get("mutated_node_type"),
            original=entry.get("original"),
            mutated=entry.get("mutated"),
        )
    return result


def _parse_killed_by(
    raw: dict[str, dict[str, object]],
) -> tuple[
    dict[str, list[str]],
    dict[str, list[str]],
    dict[str, list[str]],
    dict[str, bool],
    dict[str, str],
]:
    """Parse the killed-by data into separate mappings.

    Each value is a dict with keys ``killed_by``, ``tests_selected``,
    ``tests_ran``, ``partial``, and optionally ``killed_during``.

    Returns ``(killed_by, tests_selected_data, tests_ran_data,
    partial_data, killed_during_data)``.
    """
    killed_by: dict[str, list[str]] = {}
    tests_selected_data: dict[str, list[str]] = {}
    tests_ran_data: dict[str, list[str]] = {}
    partial_data: dict[str, bool] = {}
    killed_during_data: dict[str, str] = {}

    for name, value in raw.items():
        killed_by[name] = value.get("killed_by", [])
        tests_selected_data[name] = value.get("tests_selected", [])
        tests_ran_data[name] = value.get("tests_ran", [])
        partial_data[name] = value.get("partial", False)
        killed_during = value.get("killed_during")
        if killed_during is not None:
            killed_during_data[name] = killed_during

    return (
        killed_by,
        tests_selected_data,
        tests_ran_data,
        partial_data,
        killed_during_data,
    )


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
    tests_selected_data: dict[str, list[str]] | None = None,
    tests_ran_data: dict[str, list[str]] | None = None,
    partial_data: dict[str, bool] | None = None,
    killed_during_data: dict[str, str] | None = None,
    mutation_types: dict[str, MutationKind] | None = None,
) -> list[MutantRecord]:
    """Build a ``MutantRecord`` for every mutant."""
    records: list[MutantRecord] = []
    tests_selected_data = tests_selected_data or {}
    tests_ran_data = tests_ran_data or {}
    partial_data = partial_data or {}
    killed_during_data = killed_during_data or {}
    mutation_types = mutation_types or {}

    for name, (status, duration, source_path) in all_meta.items():
        short_name = _shorten_name(name)
        record = MutantRecord(
            name=name,
            short_name=short_name,
            status=status,
            source_file=source_path,
            duration_seconds=duration,
            killed_by=killed_by.get(name, []),
            partial_data=partial_data.get(name, False),
            killed_during=killed_during_data.get(name),
            tests_selected=tests_selected_data.get(name),
            tests_ran=tests_ran_data.get(name),
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
                captured = mutation_types.get(name)
                score, mutation_type, description = _classify(mutation_diff, captured)
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
                f"{_normalize_for_mirror(cm.short_name)}|{cm.score}|{cm.mutation_type}"
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
        mtype: MutationTypeSummary(
            total=type_total[mtype], survived=type_survived[mtype]
        )
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
        fn
        for fn in mutated_functions
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

    # Classification summary (non-killed only).
    cls_summary = ClassificationSummary()
    for r in records:
        if r.status == "killed":
            continue
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
    lines.append(
        f"Score: {report.mutation_score:.2f}% ({report.killed}/{report.total} killed)"
    )
    if report.wall_clock_seconds is not None:
        minutes, seconds = divmod(int(report.wall_clock_seconds), 60)
        lines.append(f"Duration: {minutes}m {seconds}s")
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
    suspicious_names: list[str] = []
    no_test_names: list[str] = []

    for r in non_killed:
        if r.status == "timeout":
            timeout_names.append(r.name)
            continue
        if r.status == "suspicious":
            suspicious_names.append(r.name)
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

    if suspicious_names:
        lines.append(f"--- SUSPICIOUS ({len(suspicious_names)}) ---")
        for name in suspicious_names:
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
            lines.append(
                f"  Zero-kill tests:       {kb.zero_kill_tests} of {kb.total_tests}"
            )
        lines.append("")

        kill_counts: Counter[str] = Counter()
        unique_kill_counts: Counter[str] = Counter()
        killed_by_data = {r.name: r.killed_by for r in report.mutants if r.killed_by}
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
                lines.append(
                    f"    {count:4d} kills ({unique:3d} unique)  {test_nodeid}"
                )
            lines.append("")

    # Partial data (SIGXCPU killed the process before test completed).
    partial_records = [r for r in report.mutants if r.partial_data]
    if partial_records:
        lines.append("Partial Data (SIGXCPU)")
        lines.append("-" * 60)
        lines.append(
            f"  {len(partial_records)} mutants with partial data"
            "  (process killed before test completed)"
        )
        for r in partial_records[:10]:
            if r.killed_during:
                lines.append(f"    {r.short_name}  (killed during: {r.killed_during})")
            else:
                lines.append(f"    {r.short_name}")
        if len(partial_records) > 10:
            lines.append(f"    ... and {len(partial_records) - 10} more")
        lines.append("")

    # Test effectiveness.
    if report.test_effectiveness:
        lines.append("Test Effectiveness (kills per second of test runtime)")
        lines.append("-" * 60)
        lines.append("  Top 10 most efficient:")
        for te in report.test_effectiveness[:10]:
            kps = (
                f"{te.kills_per_second:.1f}"
                if te.kills_per_second != float("inf")
                else "inf"
            )
            lines.append(
                f"    {kps:>7} k/s  {te.kills:4d} kills  {te.duration_seconds:.3f}s  {te.test_nodeid}"
            )
        lines.append("")

        zero_kill = [te for te in report.test_effectiveness if te.kills == 0]
        if zero_kill:
            zero_kill.sort(key=lambda t: t.duration_seconds, reverse=True)
            lines.append(f"  Slowest zero-kill tests ({len(zero_kill)} total):")
            for te in zero_kill[:10]:
                lines.append(f"    {te.duration_seconds:.3f}s  {te.test_nodeid}")
            lines.append("")

    # Per-file survival rates.
    if report.files:
        if report.files:
            lines.append("Per-File Survival Rates")
            lines.append("-" * 60)
            for fs in report.files:
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
        lines.append(
            f"Uncovered Functions ({len(report.uncovered_functions)} with zero test coverage)"
        )
        lines.append("-" * 60)
        for fn in report.uncovered_functions:
            lines.append(f"  {_shorten_name(fn)}")
        lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# JSON serialization
# ---------------------------------------------------------------------------


def _serialize_report(
    report: UnifiedReport, include_killed: bool = False
) -> tuple[dict, dict]:
    """Serialize the report into a summary dict and a detail dict.

    The summary dict (written to ``report.json``) contains scores,
    per-file stats, classification, mutation type distribution, uncovered
    functions, and non-killed mutant records. The detail dict (written to
    ``report-detail.json``) contains the large diagnostic sections:
    ``killed_by_detail`` and ``test_effectiveness``.

    By default, killed mutants are excluded from the ``mutants`` list in
    the summary to keep the JSON manageable. Set ``include_killed=True``
    to include them.
    """
    d = asdict(report)

    # Extract killed-by / test-selection into a compact mapping.
    killed_by_detail: dict[str, dict] = {}
    for m in d["mutants"]:
        has_data = (
            m["killed_by"]
            or m["tests_selected"] is not None
            or m["tests_ran"] is not None
        )
        if has_data:
            entry: dict = {}
            if m["killed_by"]:
                entry["killed_by"] = m["killed_by"]
            if m["tests_selected"] is not None:
                entry["tests_selected"] = m["tests_selected"]
                entry["tests_targeted"] = len(m["tests_selected"])
            if m["tests_ran"] is not None:
                entry["tests_ran"] = m["tests_ran"]
                entry["tests_run"] = len(m["tests_ran"])
            if m["partial_data"]:
                entry["partial"] = True
            if m.get("killed_during"):
                entry["killed_during"] = m["killed_during"]
            killed_by_detail[m["name"]] = entry

    if not include_killed:
        d["mutants"] = [m for m in d["mutants"] if m["status"] != "killed"]

    # Strip default-valued fields from mutant entries to reduce JSON size.
    # Fields that carry no information when holding their default value are
    # omitted entirely (e.g., killed_by is always empty for non-killed
    # mutants, partial_data is almost always false).
    _MUTANT_DEFAULTS: dict[str, object] = {
        "killed_by": [],
        "partial_data": False,
        "killed_during": None,
        "is_known_benign": False,
        "benign_reason": None,
        "mirror_key": None,
        "classification_score": None,
        "duration_seconds": None,
        "tests_selected": None,
        "tests_ran": None,
    }
    for m in d["mutants"]:
        for key, default in _MUTANT_DEFAULTS.items():
            if m.get(key) == default:
                m.pop(key, None)

    # Replace inf values in test_effectiveness (JSON does not support inf).
    test_effectiveness = d.pop("test_effectiveness", None)
    if test_effectiveness:
        for te in test_effectiveness:
            if te["kills_per_second"] == float("inf"):
                te["kills_per_second"] = -1  # sentinel for "instant kill"

    # Detail dict: large diagnostic data split into a separate file.
    detail = {
        "mutation_score": d["mutation_score"],
        "killed_by_detail": killed_by_detail,
        "test_effectiveness": test_effectiveness,
    }

    return d, detail


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def _load_timeline_events(timeline_path: Path) -> list[dict]:
    """Load raw events from the timeline JSONL produced by the pytest plugin."""
    if not timeline_path.exists():
        return []
    events: list[dict] = []
    with open(timeline_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                events.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return events


def _build_test_intervals(events: list[dict]) -> list[dict]:
    """Pair start events with their effective end (completed, last heartbeat, or process kill).

    Returns one record per ``(pid, mutant_id, nodeid)`` start event with:
    ``start_mono``, ``end_mono``, ``end_kind``, plus ``start_ts``/``end_ts`` for
    wall-clock display. Records are sorted by ``start_mono``.
    """
    by_pid: dict[int, list[dict]] = {}
    for ev in events:
        by_pid.setdefault(ev["pid"], []).append(ev)
    records: list[dict] = []
    for pid, pid_events in by_pid.items():
        pid_events.sort(key=lambda e: e["mono"])
        # Find the kill event for this PID, if any (last process_killed).
        kill_event = next(
            (e for e in reversed(pid_events) if e["event"] == "process_killed"),
            None,
        )
        # Walk through tests: for each ``start``, find the matching ``end`` or
        # fall back to the last heartbeat for that nodeid before the next start.
        i = 0
        while i < len(pid_events):
            ev = pid_events[i]
            if ev["event"] != "start":
                i += 1
                continue
            nodeid = ev.get("nodeid")
            mutant_id = ev["mutant_id"]
            end_event = None
            last_heartbeat = None
            j = i + 1
            while j < len(pid_events):
                later = pid_events[j]
                if later.get("nodeid") == nodeid and later["event"] == "end":
                    end_event = later
                    break
                if later.get("nodeid") == nodeid and later["event"] == "heartbeat":
                    last_heartbeat = later
                if later["event"] == "start" and later.get("nodeid") != nodeid:
                    break
                j += 1
            if end_event is not None:
                end_mono = end_event["mono"]
                end_ts = end_event["ts"]
                end_kind = "completed"
            elif kill_event is not None:
                end_mono = kill_event["mono"]
                end_ts = kill_event["ts"]
                end_kind = "parent_killed"
            elif last_heartbeat is not None:
                end_mono = last_heartbeat["mono"]
                end_ts = last_heartbeat["ts"]
                end_kind = "last_heartbeat"
            else:
                end_mono = ev["mono"]
                end_ts = ev["ts"]
                end_kind = "no_end"
            records.append(
                {
                    "pid": pid,
                    "mutant_id": mutant_id,
                    "nodeid": nodeid,
                    "start_mono": ev["mono"],
                    "start_ts": ev["ts"],
                    "end_mono": end_mono,
                    "end_ts": end_ts,
                    "end_kind": end_kind,
                    "duration_ms": (end_mono - ev["mono"]) * 1000.0,
                    "last_heartbeat_mono": (
                        last_heartbeat["mono"] if last_heartbeat else None
                    ),
                }
            )
            i = j
    records.sort(key=lambda r: r["start_mono"])
    return records


def _detect_cross_pid_overlaps(records: list[dict]) -> list[dict]:
    """Return pairs of intervals from different PIDs whose [start, end] overlap."""
    overlaps: list[dict] = []
    sorted_records = sorted(records, key=lambda r: r["start_mono"])
    for i, a in enumerate(sorted_records):
        for b in sorted_records[i + 1 :]:
            if b["start_mono"] >= a["end_mono"]:
                break
            if a["pid"] == b["pid"]:
                continue
            overlap_start = max(a["start_mono"], b["start_mono"])
            overlap_end = min(a["end_mono"], b["end_mono"])
            overlaps.append(
                {
                    "a_pid": a["pid"],
                    "a_mutant": a["mutant_id"],
                    "a_nodeid": a["nodeid"],
                    "b_pid": b["pid"],
                    "b_mutant": b["mutant_id"],
                    "b_nodeid": b["nodeid"],
                    "overlap_ms": (overlap_end - overlap_start) * 1000.0,
                }
            )
    return overlaps


def _format_timeline_report(
    records: list[dict], overlaps: list[dict], events: list[dict]
) -> str:
    """Build a human-readable timeline report.

    Per-mutant section lists tests in chronological order. The overlap
    section lists cross-PID overlaps sorted by overlap duration (longest first)
    so the most likely cross-influence pairs are immediately visible.
    """
    lines: list[str] = []
    lines.append("Test Timeline Report")
    lines.append("=" * 60)
    lines.append(f"Total events captured: {len(events)}")
    lines.append(f"Test intervals: {len(records)}")
    lines.append(f"Cross-PID overlaps: {len(overlaps)}")
    lines.append("")

    # Group by mutant for the per-mutant view.
    by_mutant: dict[str, list[dict]] = {}
    for r in records:
        by_mutant.setdefault(r["mutant_id"], []).append(r)
    lines.append("--- PER-MUTANT TIMELINES ---")
    lines.append("")
    for mutant_id in sorted(by_mutant):
        mutant_records = by_mutant[mutant_id]
        mutant_records.sort(key=lambda r: r["start_mono"])
        lines.append(f"  {mutant_id} (pid={mutant_records[0]['pid']})")
        first_start = mutant_records[0]["start_mono"]
        for r in mutant_records:
            offset_s = r["start_mono"] - first_start
            kind_tag = "" if r["end_kind"] == "completed" else f" [{r['end_kind']}]"
            lines.append(
                f"    {offset_s:>7.3f}s +{r['duration_ms']:>8.1f}ms{kind_tag}"
                f"  {r['nodeid']}"
            )
        lines.append("")

    if overlaps:
        lines.append("")
        lines.append("--- CROSS-PID OVERLAPS (longest first) ---")
        lines.append("")
        for o in sorted(overlaps, key=lambda x: -x["overlap_ms"])[:200]:
            lines.append(
                f"  {o['overlap_ms']:>8.1f}ms"
                f"  pid {o['a_pid']} ({o['a_mutant']}): {o['a_nodeid']}"
            )
            lines.append(
                f"              pid {o['b_pid']} ({o['b_mutant']}): {o['b_nodeid']}"
            )
        if len(overlaps) > 200:
            lines.append(f"  ... ({len(overlaps) - 200} additional overlaps truncated)")
    return "\n".join(lines) + "\n"


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
        "--mutation-types",
        default="/app/mutation-output/mutation-types.json",
        help=(
            "path to the per-mutant operator metadata JSON"
            " (default: /app/mutation-output/mutation-types.json);"
            " missing file falls back to diff heuristics"
        ),
    )
    parser.add_argument(
        "--include-killed",
        action="store_true",
        help="include killed mutants in the JSON output (increases file size)",
    )
    parser.add_argument(
        "--timeline",
        default="/tmp/mutmut_test_timeline.jsonl",
        help=(
            "path to the per-test timeline JSONL written by"
            " tests/plugins/mutmut_test_timeline.py;"
            " missing file skips the timeline report"
        ),
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
        print(
            "Error: no mutation metadata found. Did mutmut run complete?",
            file=sys.stderr,
        )
        sys.exit(1)
    print(f"  {len(all_meta)} mutants found.", flush=True)

    # Load killed-by data.
    killed_by_raw = _load_killed_by_raw(Path(args.killed_by))
    killed_by, tests_selected_data, tests_ran_data, partial_data, killed_during_data = (
        _parse_killed_by(killed_by_raw)
    )
    partial_count = sum(1 for v in partial_data.values() if v)
    print(f"  {len(killed_by)} mutants with killed-by data.", flush=True)
    if partial_count:
        print(f"  {partial_count} mutants with partial data (SIGXCPU).", flush=True)

    mutation_types = _load_mutation_types(Path(args.mutation_types))
    if mutation_types:
        print(
            f"  {len(mutation_types)} mutants with captured operator metadata.",
            flush=True,
        )
    else:
        print(
            "  No captured operator metadata; classifier will use diff heuristics.",
            flush=True,
        )

    # Load mutmut-stats.json for test mapping and durations.
    stats_path = Path("mutants/mutmut-stats.json")
    stats_data: dict | None = None
    try:
        with open(stats_path) as f:
            stats_data = json.load(f)
        print(
            f"  Stats loaded: {len(stats_data.get('duration_by_test', {}))} test durations.",
            flush=True,
        )
    except (FileNotFoundError, json.JSONDecodeError):
        print(
            "  Warning: mutmut-stats.json not found; test effectiveness will be unavailable.",
            flush=True,
        )

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

    # Generate diffs for all mutants (enables full analysis from the output
    # files alone, without access to the mutmut working directory).
    all_names = set(all_meta.keys())
    print(f"  {len(all_names)} mutants; generating diffs...", flush=True)
    diffs = _generate_all_diffs(all_names, all_meta)
    print(f"  {len(diffs)} diffs generated.", flush=True)

    # Build records and attach mirror keys.
    records = _build_records(
        all_meta,
        diffs,
        killed_by,
        tests_selected_data or None,
        tests_ran_data or None,
        partial_data or None,
        killed_during_data or None,
        mutation_types or None,
    )
    _attach_mirror_keys(records)

    # Build unified report.
    report = _build_report(
        records,
        score,
        killed_count,
        total_count,
        killed_by,
        all_test_nodeids,
        all_meta,
        stats_data,
    )

    # Write output files.
    summary, detail = _serialize_report(report, include_killed=args.include_killed)

    json_path = output_dir / "report.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    print(f"  JSON report written to {json_path}", flush=True)

    detail_path = output_dir / "report-detail.json"
    with open(detail_path, "w", encoding="utf-8") as f:
        json.dump(detail, f, indent=2)
    print(f"  Detail report written to {detail_path}", flush=True)

    score_path = output_dir / "mutation-score.txt"
    score_path.write_text(f"{score:.2f}", encoding="utf-8")

    # Write all-mutations.json: every mutant with diffs, for offline analysis.
    all_mutations_summary, _ = _serialize_report(report, include_killed=True)
    all_mutations_path = output_dir / "all-mutations.json"
    with open(all_mutations_path, "w", encoding="utf-8") as f:
        json.dump(all_mutations_summary, f, indent=2)
    print(f"  All mutations written to {all_mutations_path}", flush=True)

    timeline_path = Path(args.timeline)
    timeline_events = _load_timeline_events(timeline_path)
    if timeline_events:
        timestamps = [e.get("ts", 0.0) for e in timeline_events if "ts" in e]
        if timestamps:
            report.wall_clock_seconds = max(timestamps) - min(timestamps)

    text = _format_text_report(report)
    text_path = output_dir / "report.txt"
    text_path.write_text(text, encoding="utf-8")

    if timeline_events:
        timeline_records = _build_test_intervals(timeline_events)
        timeline_overlaps = _detect_cross_pid_overlaps(timeline_records)
        timeline_text = _format_timeline_report(
            timeline_records, timeline_overlaps, timeline_events
        )
        (output_dir / "test-timeline.txt").write_text(timeline_text, encoding="utf-8")
        # Copy the raw JSONL so the artifact lives alongside the analysis.
        (output_dir / "test-timeline.jsonl").write_text(
            timeline_path.read_text(encoding="utf-8"), encoding="utf-8"
        )
        print(
            f"  Timeline: {len(timeline_events)} events,"
            f" {len(timeline_records)} test intervals,"
            f" {len(timeline_overlaps)} cross-PID overlaps.",
            flush=True,
        )

    print("")
    print(text)


if __name__ == "__main__":
    main()
