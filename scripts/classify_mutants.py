"""Classify mutmut survivors by relevancy to prioritize remediation effort.

Assigns each surviving mutation a relevancy score (0 to 3) based on the type
of change. Sync/async mirror pairs are collapsed into a single entry.

This module is imported as a library by ``generate_mutmut_report.py``, which
calls ``_classify``, ``_find_mirrors``, ``_match_known_benign``, and related
helpers directly. The ``main`` entry point and ``_format_report`` function are
retained for standalone use with pre-generated ``diffs.txt`` / ``results.txt``
files.

Type resolution accepts an optional ``MutationKind`` record carrying the
libcst operator name captured at generation time by ``run_mutmut.py``
Patch 6. When present, the operator name supplies the ``mutation_type``
label as ground truth; the legacy diff-only cascade in
``_resolve_type_from_diff`` runs as a fallback so the standalone CLI keeps
working on artifacts produced before Patch 6 existed.

Relevancy scores:

- **0 (cosmetic)**: string mutations (XX wrap, lowercase, uppercase) inside
  logger calls, error messages, ``NotImplementedError`` text, ASGI response
  body text, or other non-behavioral text contexts. Also includes
  ``exc_info`` boolean swaps and value-to-None replacements on logger
  format arguments.
- **1 (argument)**: argument removal from a function or method call.
- **2 (logic)**: everything else, including operator swaps, numeric increments,
  index changes, value-to-None replacements, boolean swaps, keyword swaps,
  unary operator removal, and string mutations on dict keys or identifiers.
- **3 (fork-immune)**: mutations on default parameter values in ``def``
  signatures. These are false survivors: Python stores defaults in the
  function object's ``__defaults__`` tuple at import time, but mutmut's
  AST mutations only modify the code object inside forked children. The
  parent's ``__defaults__`` is inherited unchanged, so the test suite
  always sees the original default regardless of the mutation.

Additionally, a **known benign** allowlist (``_KNOWN_BENIGN``) holds mutations
that have been manually verified as producing identical behavior. Each entry
specifies a method pattern, the expected diff description, and a reason. These
are separated from scored mutations and listed in their own report section.

Usage (standalone)::

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
class MutationKind:
    """Captured mutation metadata loaded from ``mutmut_mutation_types.json``."""

    operator: str | None = None
    line: int | None = None
    source_file: str | None = None
    function: str | None = None
    is_default_param: bool = False
    original_node_type: str | None = None
    mutated_node_type: str | None = None
    original: str | None = None
    mutated: str | None = None


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


def _split_context_around_mutation(body: str) -> tuple[list[str], list[str]]:
    """Split context lines into those before and after the mutation."""
    before: list[str] = []
    after: list[str] = []
    in_hunk = False
    seen_mutation = False

    for line in body.splitlines():
        if line.startswith("@@"):
            in_hunk = True
            continue
        if not in_hunk:
            continue
        if line.startswith(("-", "+")) and not line.startswith(("---", "+++")):
            seen_mutation = True
        elif line.startswith(" "):
            if seen_mutation:
                after.append(line[1:])
            else:
                before.append(line[1:])

    return before, after


# ---------------------------------------------------------------------------
# String mutation detection
# ---------------------------------------------------------------------------


def _extract_quoted_strings(line: str) -> list[tuple[str, str, str]]:
    """Extract all double-quoted strings from a line.

    Returns a list of (string_content, prefix, suffix) tuples where prefix and
    suffix are the parts of the line outside each quoted string. Returns an
    empty list if no quoted string is found.
    """
    results: list[tuple[str, str, str]] = []
    for match in re.finditer(r'"((?:[^"\\]|\\.)*)"', line):
        results.append((match.group(1), line[: match.start(1)], line[match.end(1) :]))
    return results


def _detect_string_mutation(old_lines: list[str], new_lines: list[str]) -> str | None:
    """Detect XX wrap, lowercase, or uppercase string mutations.

    Returns "xx_wrap", "lowercase", "uppercase", or None. When a line contains
    multiple quoted strings, each pair is checked so that mutations on the
    second (or later) string are also detected.
    """
    if len(old_lines) != 1 or len(new_lines) != 1:
        return None

    old_strings = _extract_quoted_strings(old_lines[0].strip())
    new_strings = _extract_quoted_strings(new_lines[0].strip())

    if not old_strings or not new_strings or len(old_strings) != len(new_strings):
        return None

    for (old_str, old_pre, old_suf), (new_str, new_pre, new_suf) in zip(
        old_strings, new_strings
    ):
        # Skip pairs where the string content is identical.
        if old_str == new_str:
            continue

        # Verify that the rest of the line (outside the string) is unchanged.
        if old_pre != new_pre or old_suf != new_suf:
            continue

        # XX wrap: new == "XX" + old + "XX"
        if new_str == f"XX{old_str}XX":
            return "xx_wrap"

        # Lowercase: either first character or full string lowercased.
        if len(old_str) > 0:
            if (
                new_str == old_str[0].lower() + old_str[1:]
                and old_str[0] != old_str[0].lower()
            ):
                return "lowercase"
            if new_str == old_str.lower():
                return "lowercase"

        # Uppercase.
        if len(old_str) > 0 and new_str == old_str.upper():
            return "uppercase"

    return None


# ---------------------------------------------------------------------------
# Default parameter mutation detection
# ---------------------------------------------------------------------------

_DEF_SIGNATURE_PATTERN = re.compile(r"^\s*def\s+\w+\(")


def _is_default_param_mutation(old_lines: list[str], new_lines: list[str]) -> bool:
    """Detect whether a mutation changes a default parameter value in a ``def`` signature."""
    if len(old_lines) != 1 or len(new_lines) != 1:
        return False
    old = old_lines[0]
    new = new_lines[0]
    if not _DEF_SIGNATURE_PATTERN.search(old):
        return False
    # The only difference should be inside the parameter list (between parens).
    # Quick check: the function name and everything before '(' must be identical.
    old_paren = old.find("(")
    new_paren = new.find("(")
    if old_paren == -1 or new_paren == -1:
        return False
    return old[:old_paren] == new[:new_paren] and old != new


# ---------------------------------------------------------------------------
# Context detection
# ---------------------------------------------------------------------------

_LOGGER_PATTERN = re.compile(r"logger\.(debug|info|warning|error|exception|critical)\(")
_RAISE_PATTERN = re.compile(r"\braise\b|\bException\b|\bError\(")
_FORMAT_SPEC_PATTERN = re.compile(r"""['"].*%[sdfiexXog]""")


def _has_logger_context(context_lines: list[str], old_lines: list[str]) -> bool:
    """Check if the diff is within a logger call."""
    all_lines = context_lines + old_lines
    return any(_LOGGER_PATTERN.search(line) for line in all_lines)


def _old_is_format_string(old_lines: list[str]) -> bool:
    """Return True when the mutated line itself contains a printf-style format specifier.

    Used to distinguish mutations *on* a logger format string from mutations on
    adjacent code (e.g., a redis.set value or a logger argument like ``self.token``)
    that merely appear near a logger call in the diff context.
    """
    return any(_FORMAT_SPEC_PATTERN.search(line) for line in old_lines)


def _has_raise_context(context_lines: list[str], old_lines: list[str]) -> bool:
    """Check if the diff is within a raise statement or exception constructor."""
    all_lines = context_lines + old_lines
    return any(_RAISE_PATTERN.search(line) for line in all_lines)


_ASGI_BODY_PATTERN = re.compile(r'"type"\s*:\s*"http\.response\.body"')


def _has_asgi_body_context(
    context_lines: list[str],
    old_lines: list[str],
    new_lines: list[str],
) -> bool:
    """Check if the diff is a byte string content mutation within an ASGI response body.

    Returns ``True`` only when the surrounding context contains
    ``"type": "http.response.body"`` AND the mutated quoted string is a byte
    string literal (preceded by ``b``). This ensures only human-readable body
    text is classified as cosmetic, not structural ASGI keys or type values.
    """
    all_lines = context_lines + old_lines
    has_body_type = any(_ASGI_BODY_PATTERN.search(line) for line in all_lines)
    if not has_body_type:
        return False
    # Verify the mutated string is actually a byte string (preceded by 'b').
    if len(old_lines) != 1 or len(new_lines) != 1:
        return False
    return _is_byte_string_mutation(old_lines[0], new_lines[0])


_FORMAT_STRING_PATTERN = re.compile(r'["\'].*%[sd]')


def _has_logger_format_context(
    context_lines: list[str], old_lines: list[str], body: str = ""
) -> bool:
    """Check if the diff is a trailing argument to a logger format call.

    Only considers context lines BEFORE the mutation, so that a logger
    call appearing after the mutation does not cause a false positive.
    """
    if body:
        before, _ = _split_context_around_mutation(body)
    else:
        before = context_lines

    all_lines = before + old_lines
    has_format_string = any(
        re.search(r'["\'].*%[sdfiexXog]', line) for line in all_lines
    )
    if not has_format_string:
        return False
    for line in old_lines:
        stripped = line.strip()
        if stripped and (stripped.endswith(",") or stripped.endswith(")")):
            return True
    return False


def _is_byte_string_mutation(old_line: str, new_line: str) -> bool:
    """Check if the mutated quoted string has a ``b`` prefix (byte string literal).

    The prefix returned by ``_extract_quoted_strings`` includes the opening
    quote character, so a byte string prefix ends with ``b"``.
    """
    old_strings = _extract_quoted_strings(old_line.strip())
    new_strings = _extract_quoted_strings(new_line.strip())
    if not old_strings or not new_strings or len(old_strings) != len(new_strings):
        return False
    for (old_str, old_pre, _), (new_str, _, _) in zip(old_strings, new_strings):
        if old_str != new_str and old_pre.endswith('b"'):
            return True
    return False


def _is_exc_info_swap(old_lines: list[str]) -> bool:
    """Check if a boolean swap is on an ``exc_info=`` parameter."""
    return any("exc_info=" in line for line in old_lines)


# ---------------------------------------------------------------------------
# Argument removal detection
# ---------------------------------------------------------------------------


def _detect_argument_removal(old_lines: list[str], new_lines: list[str]) -> str | None:
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


def _detect_operator_swap(old_lines: list[str], new_lines: list[str]) -> str | None:
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


def _detect_numeric_increment(old_lines: list[str], new_lines: list[str]) -> str | None:
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


def _detect_value_to_none(old_lines: list[str], new_lines: list[str]) -> str | None:
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
            replaced = (
                old[len(prefix) : len(old) - len(suffix)]
                if suffix
                else old[len(prefix) :]
            )
            if replaced and replaced.strip():
                return f"{replaced.strip()}  ->  None"

    return None


def _detect_boolean_swap(old_lines: list[str], new_lines: list[str]) -> str | None:
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


def _detect_keyword_swap(old_lines: list[str], new_lines: list[str]) -> str | None:
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


def _detect_unary_removal(old_lines: list[str], new_lines: list[str]) -> str | None:
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

    aug_ops = [
        "+=",
        "-=",
        "*=",
        "/=",
        "//=",
        "%=",
        "**=",
        "&=",
        "|=",
        "^=",
        "<<=",
        ">>=",
    ]
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

# Operators that emit multiple distinct kinds (operator_string,
# operator_name, operator_assignment, operator_lambda) are not listed
# here; they are sub-resolved against the diff in ``_resolve_type``.
_OPERATOR_TO_TYPE: dict[str, str] = {
    "operator_number": "numeric_increment",
    "operator_swap_op": "operator_swap",
    "operator_arg_removal": "argument_removal",
    "operator_remove_unary_ops": "unary_removal",
    "operator_augmented_assignment": "augmented_assignment",
    "operator_symmetric_string_methods_swap": "string_method_swap",
    "operator_unsymmetrical_string_methods_swap": "string_method_swap",
    "operator_keywords": "keyword_swap",
    "operator_dict_arguments": "dict_keyword_xx_wrap",
    "operator_lambda": "lambda_body",
    "operator_match": "match_case_drop",
}


def _fallback_desc(diff: MutationDiff) -> str:
    """Return a one-line summary of the diff for use as a description."""
    old = diff.old_lines
    new = diff.new_lines
    old_summary = old[0].strip() if old else "(empty)"
    new_summary = new[0].strip() if new else "(empty)"
    if len(old) > 1 or len(new) > 1:
        return f"{len(old)} line(s) removed, {len(new)} line(s) added"
    return f"{old_summary}  ->  {new_summary}"


def _describe(diff: MutationDiff, mutation_type: str) -> str:
    """Render a description in the same format the legacy cascade produces,
    so that ``_KNOWN_BENIGN`` substring matches keep working unchanged."""
    old = diff.old_lines
    new = diff.new_lines

    if mutation_type == "argument_removal":
        return _detect_argument_removal(old, new) or _fallback_desc(diff)
    if mutation_type == "operator_swap":
        return _detect_operator_swap(old, new) or _fallback_desc(diff)
    if mutation_type == "string_method_swap":
        return _detect_string_method_swap(old, new) or _fallback_desc(diff)
    if mutation_type == "unary_removal":
        return _detect_unary_removal(old, new) or _fallback_desc(diff)
    if mutation_type == "augmented_assignment":
        return _detect_augmented_to_simple(old, new) or _fallback_desc(diff)
    if mutation_type == "keyword_swap":
        return _detect_keyword_swap(old, new) or _fallback_desc(diff)
    if mutation_type == "numeric_increment":
        return _detect_numeric_increment(old, new) or _fallback_desc(diff)
    if mutation_type.startswith("string_"):
        old_str = old[0].strip() if old else ""
        new_str = new[0].strip() if new else ""
        return f"{old_str}  ->  {new_str}"
    return _fallback_desc(diff)


def _resolve_type(diff: MutationDiff, captured: MutationKind | None) -> tuple[str, str]:
    """Return ``(mutation_type, base_description)`` for a mutation, preferring
    the captured operator name and falling back to diff heuristics."""
    if captured is not None and captured.operator:
        op = captured.operator

        if op in _OPERATOR_TO_TYPE:
            mutation_type = _OPERATOR_TO_TYPE[op]
            return mutation_type, _describe(diff, mutation_type)

        if op == "operator_string":
            sub = _detect_string_mutation(diff.old_lines, diff.new_lines)
            mutation_type = f"string_{sub}" if sub else "string_unknown"
            return mutation_type, _describe(diff, mutation_type)

        if op == "operator_name":
            # True <-> False, deepcopy <-> copy.
            bool_desc = _detect_boolean_swap(diff.old_lines, diff.new_lines)
            if bool_desc is not None:
                return "boolean_swap", bool_desc
            return "name_swap", _fallback_desc(diff)

        if op == "operator_assignment":
            # a = val -> a = None, or a = None -> a = "".
            none_desc = _detect_value_to_none(diff.old_lines, diff.new_lines)
            if none_desc is not None:
                return "value_to_none", none_desc
            return "value_to_empty_string", _fallback_desc(diff)

    return _resolve_type_from_diff(diff)


def _resolve_type_from_diff(diff: MutationDiff) -> tuple[str, str]:
    """Legacy diff-based type detection (used as fallback when no captured
    metadata is available)."""
    old = diff.old_lines
    new = diff.new_lines

    string_type = _detect_string_mutation(old, new)
    if string_type is not None:
        old_str = old[0].strip() if old else ""
        new_str = new[0].strip() if new else ""
        return f"string_{string_type}", f"{old_str}  ->  {new_str}"

    arg_desc = _detect_argument_removal(old, new)
    if arg_desc is not None:
        return "argument_removal", arg_desc

    op_desc = _detect_operator_swap(old, new)
    if op_desc is not None:
        return "operator_swap", op_desc

    bool_desc = _detect_boolean_swap(old, new)
    if bool_desc is not None:
        return "boolean_swap", bool_desc

    kw_desc = _detect_keyword_swap(old, new)
    if kw_desc is not None:
        return "keyword_swap", kw_desc

    unary_desc = _detect_unary_removal(old, new)
    if unary_desc is not None:
        return "unary_removal", unary_desc

    aug_desc = _detect_augmented_to_simple(old, new)
    if aug_desc is not None:
        return "augmented_assignment", aug_desc

    method_desc = _detect_string_method_swap(old, new)
    if method_desc is not None:
        return "string_method_swap", method_desc

    none_desc = _detect_value_to_none(old, new)
    if none_desc is not None:
        return "value_to_none", none_desc

    num_desc = _detect_numeric_increment(old, new)
    if num_desc is not None:
        return "numeric_increment", num_desc

    return "unknown", _fallback_desc(diff)


def _score(
    diff: MutationDiff,
    mutation_type: str,
    captured: MutationKind | None,
    base_description: str,
) -> tuple[int, str]:
    """Apply context-driven scoring rules and return ``(score, description)``.

    Cosmetic promotion (score 0) does not gate on the operator name; the
    logger / raise / ASGI / exc_info / value-to-None checks are run against
    the diff so that ``_KNOWN_BENIGN`` description substrings keep matching
    regardless of which operator produced the symptom.
    """
    old = diff.old_lines
    new = diff.new_lines
    ctx = diff.context_lines
    body = diff.body

    if (captured is not None and captured.is_default_param) or (
        captured is None and _is_default_param_mutation(old, new)
    ):
        old_summary = old[0].strip() if old else "(empty)"
        new_summary = new[0].strip() if new else "(empty)"
        return 3, f"{old_summary}  ->  {new_summary}"

    if mutation_type.startswith("string_") and mutation_type != "string_unknown":
        sub = mutation_type[len("string_") :]
        readable = sub.replace("_", " ")
        if _old_is_format_string(old):
            logger_level = _get_logger_level(ctx, old, body)
            if logger_level is not None:
                return 0, f"{readable} on logger.{logger_level} format string"
        if _has_raise_context(ctx, old):
            return 0, f"{readable} on error message"
        if _has_asgi_body_context(ctx, old, new):
            return 0, f"{readable} on ASGI response body text"
        if _has_logger_format_context(ctx, old, body):
            return 0, f"{readable} on logger format argument"
        return 2, base_description

    if mutation_type == "boolean_swap" and _is_exc_info_swap(old):
        return 0, "exc_info boolean swap (cosmetic)"

    # Run on the diff, not the operator name, so that an arg-replaced-with-None
    # mutation hits the same cosmetic-in-logger rule as an assignment-to-None.
    none_desc = _detect_value_to_none(old, new)
    if none_desc is not None:
        logger_level = _get_logger_level(ctx, old, body)
        if logger_level is not None:
            if _old_is_format_string(old):
                return 0, f"format string  ->  None on logger.{logger_level}"
            return 0, f"{none_desc} (logger.{logger_level} argument)"
        if _has_logger_format_context(ctx, old, body):
            return 0, f"{none_desc} (logger format argument)"

    if mutation_type == "argument_removal":
        return 1, base_description

    return 2, base_description


def _classify(
    diff: MutationDiff, captured: MutationKind | None = None
) -> tuple[int, str, str]:
    """Classify a mutation and return ``(score, mutation_type, description)``."""
    mutation_type, base_description = _resolve_type(diff, captured)
    score, description = _score(diff, mutation_type, captured, base_description)
    return score, mutation_type, description


def _get_logger_level(
    context_lines: list[str], old_lines: list[str], body: str = ""
) -> str | None:
    """Extract the logger level if the mutated line is inside a logger call.

    Only considers the old lines themselves and context lines that appear
    BEFORE the mutation in the diff hunk. A logger call that appears only
    AFTER the mutation means the mutation is adjacent, not inside.
    """
    # Check the mutated line itself first.
    for line in old_lines:
        match = _LOGGER_PATTERN.search(line)
        if match:
            return match.group(1)

    # Check context before the mutation.
    if body:
        before, _ = _split_context_around_mutation(body)
    else:
        before = context_lines

    for line in before:
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
    for prefix in ("redis_rate_limiter.", "celery_rate_limiter."):
        if name.startswith(prefix):
            name = name[len(prefix) :]
            break

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
    0: "cosmetic",
    1: "argument",
    2: "logic",
    3: "fork-immune",
}

_SCORE_HEADERS = {
    0: "SCORE 0: COSMETIC",
    1: "SCORE 1: ARGUMENT",
    2: "SCORE 2: LOGIC (needs tests)",
    3: "FORK-IMMUNE",
}

_SCORE_NOTES = {
    3: (
        "mutmut cannot exercise these: Python stores default parameter\n"
        "  values in __defaults__ at import time, but mutmut's AST mutations\n"
        "  only modify the code object inside forked children. The parent's\n"
        "  __defaults__ tuple is inherited unchanged, so the test suite always\n"
        "  sees the original default regardless of the mutation."
    ),
}


def _format_report(
    classified: list[ClassifiedMutation],
    timeouts: list[str],
    no_tests: list[str],
    benign: list[tuple[ClassifiedMutation, str]] | None = None,
) -> str:
    """Format the classification report."""
    lines: list[str] = []
    benign = benign or []

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
    if benign:
        parts.append(f"{len(benign)} benign")
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
        note = _SCORE_NOTES.get(score)

        # Collect mirror groups at this score level.
        score_mirrors = [g for g in mirrors if g.members[0].score == score]
        # Non-mirrored mutations at this score.
        non_mirrored = [m for m in mutations if m.diff.name not in mirrored_names]

        count = len(non_mirrored) + sum(len(g.members) for g in score_mirrors)
        lines.append(f"--- {header} ({count}) ---")
        if note:
            lines.append(f"  {note}")

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

    # Known benign.
    if benign:
        lines.append(f"--- KNOWN BENIGN ({len(benign)}) ---")
        for cm, reason in benign:
            lines.append(f"  {cm.short_name}")
            lines.append(f"    {cm.description}")
            lines.append(f"    reason: {reason}")
            lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Known benign mutations
# ---------------------------------------------------------------------------

# Mutations that have been manually verified as producing identical behavior.
# Each entry is (method_pattern, description, reason). A mutation matches when
# method_pattern is a substring of the short name and description matches exactly.
# If the source code changes such that the mutation no longer exists or shifts,
# the entry harmlessly stops matching.
_KNOWN_BENIGN: list[tuple[str, str, str]] = [
    (
        "DrainLoop.wake",
        "<  ->  <=",
        "when target equals _next_wake, the assignment writes the same value back;"
        " the wake time does not change",
    ),
    (
        "AsyncDrainLoop._wake_async",
        "<  ->  <=",
        "when target equals _next_wake, the assignment writes the same value back;"
        " the wake time does not change",
    ),
    (
        "DrainLoop._run",
        ">  ->  >=",
        "when remaining equals zero, wait(timeout=0) returns immediately and the"
        " next iteration falls through to drain unchanged",
    ),
    (
        "AsyncDrainLoop._run",
        ">  ->  >=",
        "when remaining equals zero, wait(timeout=0) returns immediately and the"
        " next iteration falls through to drain unchanged",
    ),
    (
        "TaskLifecycle.__init__",
        "self._thread: Optional[Thread] = None"
        '  ->  self._thread: Optional[Thread] = ""',
        "both None and empty string are falsy; the only check is"
        " `if self._thread and self._thread.is_alive()` which"
        " short-circuits identically for both",
    ),
    (
        "_calculate_token_recovery_delay",
        "<=  ->  <",
        "when wait_ms equals zero, the fallthrough path returns 0 / 1000.0 = 0.0,"
        " identical to the early return of 0.0",
    ),
    (
        "_calculate_token_recovery_delay",
        ">=  ->  >",
        "when val_current equals limit, the primary decay formula computes"
        " reset_in_ms / 1000.0, identical to the fallback return value",
    ),
    (
        "schedule_task",
        'redis.set(inflight_key, "1"',
        "the inflight key value is never read back; only the key's existence"
        ' matters (NX flag). any non-empty string is equivalent to "1"',
    ),
    (
        "schedule_task",
        'max_age or ""',
        'passed to the Lua script as ARGV[3]; tonumber("") and tonumber("XXXX")'
        " both return nil, so the max_age override branch is skipped identically",
    ),
    (
        "DistributedLock.__enter__",
        "self.token  ->  None (logger.debug argument)",
        "changes the token field in the acquired-lock debug log from the lock"
        " token UUID to None; no test asserts the token value in lock log"
        " messages, and the lock acquisition/release logic is unaffected",
    ),
    (
        "AsyncDistributedLock.__aenter__",
        "self.token  ->  None (logger.debug argument)",
        "async mirror of DistributedLock.__enter__ token log argument; same"
        " reasoning applies",
    ),
    (
        "DistributedLock.__exit__",
        "self.token  ->  None (logger.debug argument)",
        "changes the token field in the released/expired-lock debug logs from"
        " the lock token UUID to None; no test asserts the token value in lock"
        " log messages, and the release logic is unaffected",
    ),
    (
        "AsyncDistributedLock.__aexit__",
        "self.token  ->  None (logger.debug argument)",
        "async mirror of DistributedLock.__exit__ token log argument; same"
        " reasoning applies",
    ),
    (
        "HeartbeatScheduler._renew_one",
        "==  ->  !=",
        "inverts the kill/warn branch; reliably caught by"
        " test_heartbeat_failure_kill_mode_terminates_worker but the mutated"
        " process occasionally receives SIGTERM before pytest can report the"
        " failure, causing a suspicious classification instead of killed",
    ),
]


def _match_known_benign(short_name: str, description: str) -> str | None:
    """Return the reason if the mutation matches a known benign entry, else None.

    ``method_pattern`` is matched as a substring of ``short_name``.
    ``desc_pattern`` is matched as a substring of ``description``, so entries
    can use a concise identifying fragment rather than the full classifier output.
    """
    for method_pattern, desc_pattern, reason in _KNOWN_BENIGN:
        if method_pattern in short_name and desc_pattern in description:
            return reason
    return None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def classify(
    diffs_path: Path,
    results_path: Path | None = None,
    mutation_types: dict[str, MutationKind] | None = None,
) -> tuple[
    list[ClassifiedMutation],
    list[str],
    list[str],
    list[tuple[ClassifiedMutation, str]],
]:
    """Classify all mutations.

    Returns (classified_mutations, timeout_names, no_test_names, benign_mutations).
    Each benign entry is a (mutation, reason) pair.

    When ``mutation_types`` is supplied, per-mutant operator metadata captured
    by ``scripts/run_mutmut.py`` Patch 6 is passed into ``_classify`` so the
    type label comes from mutmut's libcst dispatch instead of diff heuristics.
    A missing or empty mapping triggers the legacy diff-based cascade,
    preserving compatibility with artifacts produced before Patch 6 existed.
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
    benign: list[tuple[ClassifiedMutation, str]] = []
    for diff in diffs:
        # Skip timeouts and no-tests (already captured from results.txt).
        if diff.name in {n for n in timeout_names + no_test_names}:
            continue

        captured = mutation_types.get(diff.name) if mutation_types else None
        score, mutation_type, description = _classify(diff, captured)
        short_name = _shorten_name(diff.name)
        cm = ClassifiedMutation(
            diff=diff,
            score=score,
            mutation_type=mutation_type,
            description=description,
            short_name=short_name,
        )

        reason = _match_known_benign(short_name, description)
        if reason is not None:
            benign.append((cm, reason))
        else:
            classified.append(cm)

    # Sort by score descending, then by short name.
    classified.sort(key=lambda m: (-m.score, m.short_name))

    return classified, timeout_names, no_test_names, benign


def main() -> None:
    if len(sys.argv) > 1:
        diffs_path = Path(sys.argv[1])
    else:
        diffs_path = Path("mutmut-results/diffs.txt")

    if not diffs_path.exists():
        print(f"Error: {diffs_path} not found.", file=sys.stderr)
        sys.exit(1)

    classified, timeouts, no_tests, benign = classify(diffs_path)
    report = _format_report(classified, timeouts, no_tests, benign)
    print(report)


if __name__ == "__main__":
    main()
