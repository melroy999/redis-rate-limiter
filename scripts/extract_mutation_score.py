"""Extract the mutation score from captured mutmut run output.

Parses the final progress line printed by ``mutmut run`` to extract the
total and killed counts, then computes the mutation score percentage.

The ``extract_score`` function is imported by ``generate_mutmut_report.py``
as a library. The ``main`` entry point is retained for standalone use.

The progress line format (mutmut 3.x) is::

    N/N  🎉 KILLED 🫥 NO_TESTS  ⏰ TIMEOUT  🤔 SUSPICIOUS  🙁 SURVIVED  🔇 SKIPPED

Usage::

    python scripts/extract_mutation_score.py <log_file> <output_file>
"""

from __future__ import annotations

import re
import sys


def extract_score(log_text: str) -> tuple[float, int, int]:
    """Extract the mutation score from mutmut run output.

    Returns a (score_percentage, killed, total) tuple. The score is
    computed as ``killed / total * 100``.
    """
    # Match the progress line: "N/N  🎉 KILLED 🫥 ..."
    # Use a broad pattern that captures the progress fraction and the
    # killed count (first number after 🎉).
    pattern = r"(\d+)/(\d+)\s+🎉\s+(\d+)"
    matches = re.findall(pattern, log_text)
    if not matches:
        raise ValueError("no mutmut progress line found in log output")

    # Take the last match (the final progress update).
    _, total_str, killed_str = matches[-1]
    total = int(total_str)
    killed = int(killed_str)

    if total == 0:
        raise ValueError("total mutation count is zero")

    score = killed / total * 100
    return score, killed, total


def main() -> None:
    if len(sys.argv) != 3:
        print(
            "Usage: python scripts/extract_mutation_score.py <log_file> <output_file>",
            file=sys.stderr,
        )
        sys.exit(1)

    log_path = sys.argv[1]
    output_path = sys.argv[2]

    with open(log_path, encoding="utf-8") as f:
        log_text = f.read()

    score, killed, total = extract_score(log_text)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(f"{score:.2f}")

    print(f"Mutation score: {score:.2f}% ({killed}/{total} killed)")


if __name__ == "__main__":
    main()
