"""Analyze the resource-snapshot JSONL produced by ``mutmut_resource_snapshot``.

Reads ``/tmp/mutmut_resource_snapshot.jsonl`` (or a path passed as the first
positional argument), groups events by PID, and reports:

1. **Per-PID peak**: highest ``rss_kb`` observed for each writing process,
   along with the timestamp and the nodeid that was active at the peak.
2. **Concurrent peak**: across all PIDs, the timestamp window where the
   sum of the latest ``rss_kb`` per active PID was largest, plus the set
   of tests running on each PID at that moment.
3. **Top per-test peaks**: the highest ``rss_kb`` ever recorded with a
   given nodeid as the active test, grouped by nodeid.

The output is plain text on stdout. No charts; the goal is to answer
"which tests / which moments correspond to memory peaks" without
speculation.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from typing import Optional


@dataclass
class _Sample:
    ts: float
    pid: int
    rss_kb: int
    threads: int
    fds: int
    nodeid: Optional[str]
    event: str


def _load(path: str) -> list[_Sample]:
    samples: list[_Sample] = []
    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            samples.append(
                _Sample(
                    ts=float(obj["ts"]),
                    pid=int(obj["pid"]),
                    rss_kb=int(obj.get("rss_kb", 0)),
                    threads=int(obj.get("threads", 0)),
                    fds=int(obj.get("fds", 0)),
                    nodeid=obj.get("nodeid"),
                    event=str(obj.get("event", "")),
                )
            )
    samples.sort(key=lambda s: s.ts)
    return samples


def _per_pid_peak(samples: list[_Sample]) -> None:
    by_pid: dict[int, _Sample] = {}
    for s in samples:
        existing = by_pid.get(s.pid)
        if existing is None or s.rss_kb > existing.rss_kb:
            by_pid[s.pid] = s

    print(f"\n{'='*60}\nPer-PID peak RSS\n{'='*60}")
    print(f"{'pid':>8s} {'rss_mb':>8s} {'threads':>8s} {'fds':>6s}  test_at_peak")
    for pid in sorted(by_pid, key=lambda p: -by_pid[p].rss_kb):
        s = by_pid[pid]
        print(
            f"{pid:>8d} {s.rss_kb / 1024:>8.1f} {s.threads:>8d} {s.fds:>6d}"
            f"  {s.nodeid or '(no test)'}"
        )


def _concurrent_peak(samples: list[_Sample]) -> None:
    # Walk samples in order; maintain "latest rss per pid" map. At each
    # step compute the sum of currently-known RSS values across PIDs that
    # have not yet seen a session_end. Track the maximum.
    last_rss: dict[int, int] = {}
    last_node: dict[int, Optional[str]] = {}
    last_threads: dict[int, int] = {}
    alive: set[int] = set()
    peak_sum_kb = 0
    peak_ts = 0.0
    peak_state: dict[int, tuple[int, Optional[str]]] = {}

    for s in samples:
        if s.event == "session_start":
            alive.add(s.pid)
        last_rss[s.pid] = s.rss_kb
        last_node[s.pid] = s.nodeid
        last_threads[s.pid] = s.threads
        if s.event in ("session_end", "process_killed"):
            alive.discard(s.pid)
            continue

        current_sum = sum(last_rss[p] for p in alive)
        if current_sum > peak_sum_kb:
            peak_sum_kb = current_sum
            peak_ts = s.ts
            peak_state = {p: (last_rss[p], last_node[p]) for p in alive}

    print(f"\n{'='*60}\nConcurrent RSS peak across all PIDs\n{'='*60}")
    print(f"peak_total_mb : {peak_sum_kb / 1024:.1f}")
    print(f"peak_ts       : {peak_ts:.3f}")
    print(f"alive_pids    : {len(peak_state)}")
    print()
    print(f"{'pid':>8s} {'rss_mb':>8s}  test_at_peak")
    for pid in sorted(peak_state, key=lambda p: -peak_state[p][0]):
        rss_kb, node = peak_state[pid]
        print(f"{pid:>8d} {rss_kb / 1024:>8.1f}  {node or '(no test)'}")


def _per_test_peak(samples: list[_Sample], top: int) -> None:
    by_node: dict[str, _Sample] = {}
    for s in samples:
        if not s.nodeid:
            continue
        existing = by_node.get(s.nodeid)
        if existing is None or s.rss_kb > existing.rss_kb:
            by_node[s.nodeid] = s

    print(f"\n{'='*60}\nTop {top} tests by peak RSS while active\n{'='*60}")
    print(f"{'rss_mb':>8s} {'threads':>8s} {'fds':>6s}  nodeid")
    ranked = sorted(by_node.values(), key=lambda s: -s.rss_kb)[:top]
    for s in ranked:
        print(
            f"{s.rss_kb / 1024:>8.1f} {s.threads:>8d} {s.fds:>6d}  {s.nodeid}"
        )


def _summary(samples: list[_Sample]) -> None:
    if not samples:
        print("No samples found.")
        return
    pids = {s.pid for s in samples}
    test_starts = sum(1 for s in samples if s.event == "start")
    duration = samples[-1].ts - samples[0].ts
    print(f"\n{'='*60}\nSummary\n{'='*60}")
    print(f"distinct_pids   : {len(pids)}")
    print(f"sample_events   : {sum(1 for s in samples if s.event == 'sample')}")
    print(f"test_starts     : {test_starts}")
    print(f"wall_clock_s    : {duration:.1f}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "path",
        nargs="?",
        default="/tmp/mutmut_resource_snapshot.jsonl",
        help="Path to the resource-snapshot JSONL file.",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=20,
        help="Number of tests to show in the per-test peak section.",
    )
    args = parser.parse_args()

    try:
        samples = _load(args.path)
    except FileNotFoundError:
        print(f"File not found: {args.path}", file=sys.stderr)
        sys.exit(1)

    _summary(samples)
    _per_pid_peak(samples)
    _per_test_peak(samples, args.top)
    _concurrent_peak(samples)


if __name__ == "__main__":
    main()
