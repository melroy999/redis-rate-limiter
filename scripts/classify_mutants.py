"""Classify mutmut survivors by relevancy to prioritize remediation effort.

Parses ``mutmut-results/results.txt`` and ``mutmut-results/diffs.txt``, then
assigns each surviving mutation a relevancy score (0 to 3) based on the type
of change. Mutations are grouped by score with the most actionable items
printed first. Sync/async mirror pairs are collapsed into a single entry.

Relevancy scores:

- **0 (log text)**: string mutations (XX wrap, lowercase, uppercase) inside
  a ``logger.debug/info/warning/error/exception`` call.
- **1 (cosmetic)**: string mutations in error messages, ``NotImplementedError``
  text, or other non-logger contexts where the change has no behavioral impact.
- **2 (argument)**: argument removal from a function or method call.
- **3 (logic)**: everything else, including operator swaps, numeric increments,
  index changes, value-to-None replacements, boolean swaps, keyword swaps,
  unary operator removal, and string mutations on dict keys or identifiers.

Usage::

    python scripts/classify_mutants.py
    python scripts/classify_mutants.py mutmut-results/diffs.txt
"""

from __future__ import annotations

import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass
class MutationDiff:
    """A single mutation extracted from diffs.txt."""

    name: str
    status: str
    body: str
    old_lines: list[str] = field(default_factory=list)
    new_lines: list[str] = field(default_factory=list)
    context_lines: list[str] = field(default_factory=list)


@dataclass
class ClassifiedMutation:
    """A mutation with its classification results."""

    diff: MutationDiff
    score: int
    mutation_type: str
    description: str
    short_name: str


@dataclass
class MirrorGroup:
    """A pair of sync/async mutations that represent the same logical change."""

    key: str
    members: list[ClassifiedMutation]


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------


def _parse_results(path: Path) -> dict[str, str]:
    """Parse results.txt into a {mutation_id: status} mapping."""
    results: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        # Format: "mutation_id: status"
        if ": " in line:
            name, status = line.rsplit(": ", 1)
            results[name.strip()] = status.strip()
    return results


def _parse_diffs(path: Path) -> list[MutationDiff]:
    """Parse diffs.txt into individual mutation diff blocks."""
    text = path.read_text(encoding="utf-8")
    blocks: list[MutationDiff] = []

    # Split on === mutation_id === headers.
    parts = re.split(r"^=== (.+?) ===$", text, flags=re.MULTILINE)
    # parts[0] is preamble, then alternating name/body.
    for i in range(1, len(parts), 2):
        name = parts[i].strip()
        body = parts[i + 1] if i + 1 < len(parts) else ""

        # Extract the status from the comment line.
        status = "survived"
        status_match = re.search(r"#\s*.+?:\s*(\w[\w ]*)", body)
        if status_match:
            status = status_match.group(1).strip()

        old_lines, new_lines, context_lines = _extract_diff_lines(body)
        blocks.append(
            MutationDiff(
                name=name,
                status=status,
                body=body,
                old_lines=old_lines,
                new_lines=new_lines,
                context_lines=context_lines,
            )
        )

    return blocks


def _extract_diff_lines(body: str) -> tuple[list[str], list[str], list[str]]:
    """Extract removed, added, and context lines from a unified diff body."""
    old: list[str] = []
    new: list[str] = []
    context: list[str] = []
    in_hunk = False

    for line in body.splitlines():
        if line.startswith("@@"):
            in_hunk = True
            continue
        if not in_hunk:
            continue
        if line.startswith("-") and not line.startswith("---"):
            old.append(line[1:])
        elif line.startswith("+") and not line.startswith("+++"):
            new.append(line[1:])
        elif line.startswith(" "):
            context.append(line[1:])

    return old, new, context


# ---------------------------------------------------------------------------
# String mutation detection
# ---------------------------------------------------------------------------


def _extract_quoted_string(line: str) -> tuple[str | None, str, str]:
    """Extract the first double-quoted string from a line.

    Returns (string_content, prefix, suffix) where prefix and suffix are the
    parts of the line outside the quoted string. Returns (None, "", "") if no
    quoted string is found.
    """
    match = re.search(r'"((?:[^"\\]|\\.)*)"', line)
    if not match:
        return None, "", ""
    return match.group(1), line[: match.start(1)], line[match.end(1) :]


def _detect_string_mutation(
    old_lines: list[str], new_lines: list[str]
) -> str | None:
    """Detect XX wrap, lowercase, or uppercase string mutations.

    Returns "xx_wrap", "lowercase", "uppercase", or None.
    """
    if len(old_lines) != 1 or len(new_lines) != 1:
        return None

    old_str, old_pre, old_suf = _extract_quoted_string(old_lines[0].strip())
    new_str, new_pre, new_suf = _extract_quoted_string(new_lines[0].strip())

    if old_str is None or new_str is None:
        return None

    # Verify that the rest of the line (outside the string) is unchanged.
    if old_pre != new_pre or old_suf != new_suf:
        return None

    # XX wrap: new == "XX" + old + "XX"
    if new_str == f"XX{old_str}XX":
        return "xx_wrap"

    # Lowercase first char.
    if len(old_str) > 0 and new_str == old_str[0].lower() + old_str[1:]:
        if old_str[0] != old_str[0].lower():
            return "lowercase"

    # Uppercase.
    if len(old_str) > 0 and new_str == old_str.upper() and old_str != new_str:
        return "uppercase"

    return None


# ---------------------------------------------------------------------------
# Context detection
# ---------------------------------------------------------------------------

_LOGGER_PATTERN = re.compile(
    r"logger\.(debug|info|warning|error|exception|critical)\("
)
_RAISE_PATTERN = re.compile(r"\braise\b|\bException\b|\bError\(")


def _has_logger_context(context_lines: list[str], old_lines: list[str]) -> bool:
    """Check if the diff is within a logger call."""
    all_lines = context_lines + old_lines
    return any(_LOGGER_PATTERN.search(line) for line in all_lines)


def _has_raise_context(context_lines: list[str], old_lines: list[str]) -> bool:
    """Check if the diff is within a raise statement or exception constructor."""
    all_lines = context_lines + old_lines
    return any(_RAISE_PATTERN.search(line) for line in all_lines)


# ---------------------------------------------------------------------------
# Argument removal detection
# ---------------------------------------------------------------------------


def _detect_argument_removal(
    old_lines: list[str], new_lines: list[str]
) -> str | None:
    """Detect argument removal (lines removed, none added or net reduction).

    Returns a description of the removed argument, or None.
    """
    if len(old_lines) == 0:
        return None

    # Net line removal: more lines removed than added.
    if len(old_lines) <= len(new_lines):
        return None

    # The removed lines should look like arguments (indented, possibly with
    # trailing commas or closing parens).
    removed_content = [line.strip().rstrip(",").strip() for line in old_lines]
    removed_content = [r for r in removed_content if r and r != ")"]

    if not removed_content:
        return None

    return f"removed {removed_content[0]} argument"


# ---------------------------------------------------------------------------
# Operator and value mutation detection
# ---------------------------------------------------------------------------

_OPERATOR_PAIRS = [
    ("<", "<="),
    ("<=", "<"),
    (">", ">="),
    (">=", ">"),
    ("==", "!="),
    ("!=", "=="),
    ("+", "-"),
    ("-", "+"),
    ("*", "/"),
    ("/", "*"),
    ("//", "/"),
    ("%", "/"),
    ("**", "*"),
    ("&", "|"),
    ("|", "&"),
    ("^", "&"),
    ("<<", ">>"),
    (">>", "<<"),
    ("and", "or"),
    ("or", "and"),
    ("+=", "-="),
    ("-=", "+="),
    ("*=", "/="),
    ("/=", "*="),
    ("//=", "/="),
    ("%=", "/="),
    ("<<=", ">>="),
    (">>=", "<<="),
    ("is not", "is"),
    ("is", "is not"),
    ("not in", "in"),
    ("in", "not in"),
]


def _detect_operator_swap(
    old_lines: list[str], new_lines: list[str]
) -> str | None:
    """Detect operator swaps on single-line diffs.

    Returns a description like '<= 0  ->  < 0' or None.
    """
    if len(old_lines) != 1 or len(new_lines) != 1:
        return None

    old = old_lines[0].strip()
    new = new_lines[0].strip()

    for old_op, new_op in _OPERATOR_PAIRS:
        # Replace the first occurrence of the old operator with the new one
        # and check if the result matches.
        candidate = old.replace(old_op, new_op, 1)
        if candidate == new and old != new:
            return f"{old_op}  ->  {new_op}"

    return None


def _detect_numeric_increment(
    old_lines: list[str], new_lines: list[str]
) -> str | None:
    """Detect numeric literal increments (N -> N+1).

    Returns a description like '0  ->  1' or None.
    """
    if len(old_lines) != 1 or len(new_lines) != 1:
        return None

    old = old_lines[0].strip()
    new = new_lines[0].strip()

    # Find all numeric literals in both lines.
    old_nums = re.findall(r"\b(\d+(?:\.\d+)?)\b", old)
    new_nums = re.findall(r"\b(\d+(?:\.\d+)?)\b", new)

    if len(old_nums) != len(new_nums):
        return None

    changed = []
    for o, n in zip(old_nums, new_nums):
        if o != n:
            try:
                if float(n) == float(o) + 1:
                    changed.append((o, n))
            except ValueError:
                pass

    if len(changed) == 1:
        o, n = changed[0]
        return f"{o}  ->  {n}"

    return None


def _detect_value_to_none(
    old_lines: list[str], new_lines: list[str]
) -> str | None:
    """Detect value-to-None replacements on single-line diffs.

    Returns a description like 'result["remaining"]  ->  None' or None.
    """
    if len(old_lines) != 1 or len(new_lines) != 1:
        return None

    old = old_lines[0].strip()
    new = new_lines[0].strip()

    # Check if the only difference is a value replaced with None.
    # Find the differing substring.
    # Simple approach: if `new` is `old` with some token replaced by `None`.
    if "None" not in new or "None" in old:
        return None

    # Try to find the replaced token by checking common replacement patterns.
    # mutmut replaces a single argument or expression with None.
    for match in re.finditer(r"\bNone\b", new):
        # Reconstruct what was at this position in old.
        prefix = new[: match.start()]
        suffix = new[match.end() :]

        if old.startswith(prefix) and old.endswith(suffix):
            replaced = old[len(prefix) : len(old) - len(suffix)] if suffix else old[len(prefix) :]
            if replaced and replaced.strip():
                return f"{replaced.strip()}  ->  None"

    return None


def _detect_boolean_swap(
    old_lines: list[str], new_lines: list[str]
) -> str | None:
    """Detect True/False swaps."""
    if len(old_lines) != 1 or len(new_lines) != 1:
        return None

    old = old_lines[0].strip()
    new = new_lines[0].strip()

    if old.replace("True", "False", 1) == new and "True" in old:
        return "True  ->  False"
    if old.replace("False", "True", 1) == new and "False" in old:
        return "False  ->  True"

    return None


def _detect_keyword_swap(
    old_lines: list[str], new_lines: list[str]
) -> str | None:
    """Detect keyword mutations (break->return, continue->break, etc.)."""
    if len(old_lines) != 1 or len(new_lines) != 1:
        return None

    old = old_lines[0].strip()
    new = new_lines[0].strip()

    keyword_pairs = [
        ("break", "return"),
        ("continue", "break"),
    ]

    for old_kw, new_kw in keyword_pairs:
        if old.replace(old_kw, new_kw, 1) == new:
            return f"{old_kw}  ->  {new_kw}"

    return None


def _detect_unary_removal(
    old_lines: list[str], new_lines: list[str]
) -> str | None:
    """Detect unary operator removal (not x -> x, ~x -> x)."""
    if len(old_lines) != 1 or len(new_lines) != 1:
        return None

    old = old_lines[0].strip()
    new = new_lines[0].strip()

    if old.replace("not ", "", 1) == new and "not " in old:
        return "removed 'not' operator"
    if old.replace("~", "", 1) == new and "~" in old:
        return "removed '~' operator"

    return None


def _detect_augmented_to_simple(
    old_lines: list[str], new_lines: list[str]
) -> str | None:
    """Detect augmented assignment to simple assignment (x += 1 -> x = 1)."""
    if len(old_lines) != 1 or len(new_lines) != 1:
        return None

    old = old_lines[0].strip()
    new = new_lines[0].strip()

    aug_ops = ["+=", "-=", "*=", "/=", "//=", "%=", "**=", "&=", "|=", "^=", "<<=", ">>="]
    for op in aug_ops:
        if op in old and old.replace(op, "=", 1) == new:
            return f"{op}  ->  ="

    return None


def _detect_string_method_swap(
    old_lines: list[str], new_lines: list[str]
) -> str | None:
    """Detect symmetric string method swaps (.lower() <-> .upper(), etc.)."""
    if len(old_lines) != 1 or len(new_lines) != 1:
        return None

    old = old_lines[0].strip()
    new = new_lines[0].strip()

    method_pairs = [
        ("lower", "upper"),
        ("upper", "lower"),
        ("lstrip", "rstrip"),
        ("rstrip", "lstrip"),
        ("find", "rfind"),
        ("rfind", "find"),
        ("ljust", "rjust"),
        ("rjust", "ljust"),
        ("index", "rindex"),
        ("rindex", "index"),
        ("removeprefix", "removesuffix"),
        ("removesuffix", "removeprefix"),
        ("partition", "rpartition"),
        ("rpartition", "partition"),
        ("split", "rsplit"),
        ("rsplit", "split"),
    ]

    for old_method, new_method in method_pairs:
        if (
            f".{old_method}(" in old
            and old.replace(f".{old_method}(", f".{new_method}(", 1) == new
        ):
            return f".{old_method}()  ->  .{new_method}()"

    return None


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------


def _classify(diff: MutationDiff) -> tuple[int, str, str]:
    """Classify a mutation and return (score, mutation_type, description)."""
    old = diff.old_lines
    new = diff.new_lines
    ctx = diff.context_lines

    # 1. String mutations (XX wrap, lowercase, uppercase).
    string_type = _detect_string_mutation(old, new)
    if string_type is not None:
        logger_level = _get_logger_level(ctx, old)
        if logger_level is not None:
            desc = f"{string_type.replace('_', ' ')} on logger.{logger_level} format string"
            return 0, f"string_{string_type}", desc
        if _has_raise_context(ctx, old):
            desc = f"{string_type.replace('_', ' ')} on error message"
            return 1, f"string_{string_type}", desc
        # String mutation outside logger/raise context is real logic.
        old_str = old[0].strip() if old else ""
        new_str = new[0].strip() if new else ""
        return 3, f"string_{string_type}", f"{old_str}  ->  {new_str}"

    # 2. Argument removal.
    arg_desc = _detect_argument_removal(old, new)
    if arg_desc is not None:
        return 2, "argument_removal", arg_desc

    # 3. Operator swap.
    op_desc = _detect_operator_swap(old, new)
    if op_desc is not None:
        return 3, "operator_swap", op_desc

    # 4. Boolean swap.
    bool_desc = _detect_boolean_swap(old, new)
    if bool_desc is not None:
        return 3, "boolean_swap", bool_desc

    # 5. Keyword swap.
    kw_desc = _detect_keyword_swap(old, new)
    if kw_desc is not None:
        return 3, "keyword_swap", kw_desc

    # 6. Unary removal.
    unary_desc = _detect_unary_removal(old, new)
    if unary_desc is not None:
        return 3, "unary_removal", unary_desc

    # 7. Augmented to simple assignment.
    aug_desc = _detect_augmented_to_simple(old, new)
    if aug_desc is not None:
        return 3, "augmented_assignment", aug_desc

    # 8. String method swap.
    method_desc = _detect_string_method_swap(old, new)
    if method_desc is not None:
        return 3, "string_method_swap", method_desc

    # 9. Value to None.
    none_desc = _detect_value_to_none(old, new)
    if none_desc is not None:
        return 3, "value_to_none", none_desc

    # 10. Numeric increment.
    num_desc = _detect_numeric_increment(old, new)
    if num_desc is not None:
        return 3, "numeric_increment", num_desc

    # 11. Fallback: show the raw diff lines.
    old_summary = old[0].strip() if old else "(empty)"
    new_summary = new[0].strip() if new else "(empty)"
    if len(old) > 1 or len(new) > 1:
        desc = f"{len(old)} line(s) removed, {len(new)} line(s) added"
    else:
        desc = f"{old_summary}  ->  {new_summary}"

    return 3, "unknown", desc


def _get_logger_level(
    context_lines: list[str], old_lines: list[str]
) -> str | None:
    """Extract the logger level from context if a logger call is present."""
    all_lines = context_lines + old_lines
    for line in all_lines:
        match = _LOGGER_PATTERN.search(line)
        if match:
            return match.group(1)
    return None


# ---------------------------------------------------------------------------
# Short name generation
# ---------------------------------------------------------------------------


def _shorten_name(mutation_id: str) -> str:
    """Transform a full mutation ID into a readable short name.

    Example::

        celery_rate_limiter.core.limiters.xǁAbstractDistributedRateLimiterǁ_cleanup_inflight_key__mutmut_13
        ->
        limiters.AbstractDistributedRateLimiter._cleanup_inflight_key__mutmut_13
    """
    # Strip the common package prefix.
    name = mutation_id
    prefix = "celery_rate_limiter."
    if name.startswith(prefix):
        name = name[len(prefix):]

    # Strip intermediate subpackage segments (keep last module before xǁ).
    xsep = ".\x01"  # placeholder
    name = name.replace(".xǁ", xsep)
    parts = name.split(xsep)
    if len(parts) == 2:
        module_path = parts[0]
        rest = parts[1]
        module_name = module_path.rsplit(".", 1)[-1]
        name = f"{module_name}.{rest}"
    else:
        name = name.rsplit(".", 1)[-1] if "." in name else name

    # Replace remaining ǁ with dots.
    name = name.replace("ǁ", ".")

    return name


# ---------------------------------------------------------------------------
# Mirror detection
# ---------------------------------------------------------------------------


def _normalize_for_mirror(short_name: str) -> str:
    """Normalize a short name for mirror matching.

    Strips module prefix, Async/Abstract prefixes from class names, and
    returns just the method + mutmut suffix.
    """
    # Remove module prefix (e.g., "limiters." or "async_limiters.").
    parts = short_name.split(".", 1)
    if len(parts) == 2:
        rest = parts[1]
    else:
        rest = short_name

    # Remove common class prefixes: AbstractAsync, Abstract, Async.
    rest = re.sub(r"^(AbstractAsync|Abstract|Async)", "", rest)

    return rest


def _find_mirrors(
    mutations: list[ClassifiedMutation],
) -> tuple[list[MirrorGroup], set[str]]:
    """Group sync/async mirror pairs.

    Returns (mirror_groups, mirrored_names) where mirrored_names is the set
    of mutation names that belong to a mirror group.
    """
    by_key: dict[str, list[ClassifiedMutation]] = {}

    for m in mutations:
        key = _normalize_for_mirror(m.short_name)
        # Also include the score and mutation_type to ensure mirrors are
        # the same kind of mutation.
        group_key = f"{key}|{m.score}|{m.mutation_type}"
        by_key.setdefault(group_key, []).append(m)

    groups: list[MirrorGroup] = []
    mirrored: set[str] = set()

    for key, members in by_key.items():
        if len(members) >= 2:
            # Check that members come from different modules.
            modules = {m.short_name.split(".")[0] for m in members}
            if len(modules) >= 2:
                groups.append(MirrorGroup(key=key, members=members))
                for m in members:
                    mirrored.add(m.diff.name)

    return groups, mirrored


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

_SCORE_LABELS = {
    0: "log text",
    1: "cosmetic",
    2: "argument",
    3: "logic",
}

_SCORE_HEADERS = {
    0: "SCORE 0: LOG TEXT",
    1: "SCORE 1: COSMETIC",
    2: "SCORE 2: ARGUMENT",
    3: "SCORE 3: LOGIC (needs tests)",
}


def _format_report(
    classified: list[ClassifiedMutation],
    timeouts: list[str],
    no_tests: list[str],
) -> str:
    """Format the classification report."""
    lines: list[str] = []

    # Find mirrors.
    mirrors, mirrored_names = _find_mirrors(classified)

    # Count by score.
    by_score: dict[int, list[ClassifiedMutation]] = {}
    for m in classified:
        by_score.setdefault(m.score, []).append(m)

    total_survived = len(classified)

    lines.append("Mutant Classification Report")
    lines.append("=" * 40)
    parts = [f"{total_survived} survived"]
    if timeouts:
        parts.append(f"{len(timeouts)} timeout")
    if no_tests:
        parts.append(f"{len(no_tests)} no tests")
    lines.append(f"Total: {', '.join(parts)}")
    lines.append("")

    for score in (3, 2, 1, 0):
        count = len(by_score.get(score, []))
        label = _SCORE_LABELS[score]
        lines.append(f"  Score {score} ({label}): {count:>4}")
    lines.append("")

    # Print each score group (highest first).
    for score in (3, 2, 1, 0):
        mutations = by_score.get(score, [])
        if not mutations:
            continue

        header = _SCORE_HEADERS[score]

        # Collect mirror groups at this score level.
        score_mirrors = [g for g in mirrors if g.members[0].score == score]
        # Non-mirrored mutations at this score.
        non_mirrored = [m for m in mutations if m.diff.name not in mirrored_names]

        count = len(non_mirrored) + sum(len(g.members) for g in score_mirrors)
        lines.append(f"--- {header} ({count}) ---")

        # Print mirror groups first.
        for group in score_mirrors:
            representative = group.members[0]
            method_part = _normalize_for_mirror(representative.short_name)
            # Strip the score/type suffix from the group key.
            lines.append(f"  [sync/async mirror] {method_part}")
            for member in group.members:
                lines.append(f"    {member.short_name}")
            lines.append(f"    {representative.description}")
            lines.append("")

        # Print non-mirrored mutations.
        for m in non_mirrored:
            lines.append(f"  {m.short_name}")
            lines.append(f"    {m.description}")
            lines.append("")

    # Timeouts.
    if timeouts:
        lines.append(f"--- TIMEOUTS ({len(timeouts)}) ---")
        for name in timeouts:
            lines.append(f"  {_shorten_name(name)}")
        lines.append("")

    # No tests.
    if no_tests:
        lines.append(f"--- NO TESTS ({len(no_tests)}) ---")
        for name in no_tests:
            lines.append(f"  {_shorten_name(name)}")
        lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def classify(
    diffs_path: Path, results_path: Path | None = None
) -> tuple[list[ClassifiedMutation], list[str], list[str]]:
    """Classify all mutations.

    Returns (classified_mutations, timeout_names, no_test_names).
    """
    if results_path is None:
        results_path = diffs_path.parent / "results.txt"

    # Parse results for status info.
    statuses: dict[str, str] = {}
    if results_path.exists():
        statuses = _parse_results(results_path)

    # Parse diffs.
    diffs = _parse_diffs(diffs_path)

    # Separate timeouts and no-tests (which may or may not have diffs).
    timeout_names: list[str] = []
    no_test_names: list[str] = []

    for name, status in statuses.items():
        if status == "timeout":
            timeout_names.append(name)
        elif status == "no tests":
            no_test_names.append(name)

    # Classify each diff that is a survivor.
    classified: list[ClassifiedMutation] = []
    for diff in diffs:
        # Skip timeouts and no-tests (already captured from results.txt).
        if diff.name in {n for n in timeout_names + no_test_names}:
            continue

        score, mutation_type, description = _classify(diff)
        short_name = _shorten_name(diff.name)
        classified.append(
            ClassifiedMutation(
                diff=diff,
                score=score,
                mutation_type=mutation_type,
                description=description,
                short_name=short_name,
            )
        )

    # Sort by score descending, then by short name.
    classified.sort(key=lambda m: (-m.score, m.short_name))

    return classified, timeout_names, no_test_names


def main() -> None:
    if len(sys.argv) > 1:
        diffs_path = Path(sys.argv[1])
    else:
        diffs_path = Path("mutmut-results/diffs.txt")

    if not diffs_path.exists():
        print(f"Error: {diffs_path} not found.", file=sys.stderr)
        sys.exit(1)

    classified, timeouts, no_tests = classify(diffs_path)
    report = _format_report(classified, timeouts, no_tests)
    print(report)


if __name__ == "__main__":
    main()
