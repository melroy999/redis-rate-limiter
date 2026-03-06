"""Collect per-test line coverage and export for superfluity analysis.

Wrapper script that runs pytest with ``--cov --cov-context=test`` and
exports the coverage database to a ``coverage-contexts.json`` file
consumable by ``analyze_test_superfluity.py``.

This script is intended for use inside the Docker ``coverage-context``
service, but can be run manually on the host if Redis is available::

    python scripts/collect_per_test_coverage.py --output-dir mutmut-results/

Alternatively, use Docker Compose::

    docker compose --profile coverage-context up \\
        --abort-on-container-exit --exit-code-from coverage-context
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Collect per-test coverage contexts.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("mutmut-results"),
        help="Directory to write coverage-contexts.json (default: mutmut-results/).",
    )
    parser.add_argument(
        "--include-slow",
        action="store_true",
        help="Include slow tests in coverage collection.",
    )
    args = parser.parse_args(argv)

    output_dir: Path = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    # Step 1: Run pytest with coverage context tracking.
    pytest_args = [
        sys.executable,
        "-m",
        "pytest",
        "--cov",
        "--cov-context=test",
        "tests/",
        "-v",
        "-s",
        "--ignore=tests/properties/",
    ]
    if args.include_slow:
        pytest_args.append("--override-ini=addopts=--import-mode=importlib")
    else:
        pytest_args.extend(
            [
                "-m",
                "not slow",
                "--import-mode=importlib",
            ]
        )

    print("Running pytest with per-test coverage tracking...")
    result = subprocess.run(pytest_args)
    if result.returncode != 0:
        print(
            f"warning: pytest exited with code {result.returncode};"
            " coverage data may be incomplete",
            file=sys.stderr,
        )

    # Step 2: Export coverage database to JSON with contexts.
    output_path = output_dir / "coverage-contexts.json"
    print(f"Exporting coverage contexts to {output_path}...")
    export_result = subprocess.run(
        [
            sys.executable,
            "-m",
            "coverage",
            "json",
            "--show-contexts",
            "-o",
            str(output_path),
        ]
    )
    if export_result.returncode != 0:
        print("error: coverage json export failed", file=sys.stderr)
        sys.exit(1)

    print(f"Per-test coverage data written to {output_path}")


if __name__ == "__main__":
    main()
