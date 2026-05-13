"""Pytest plugin that records per-test process memory via ``/proc/self/status``.

For each test, read VmRSS (resident set size) and VmPeak (high-water mark)
before and after the test, then write the delta to a JSONL file. This
captures OS-level memory changes including thread stacks and glibc malloc
arenas, which tracemalloc cannot observe.

The plugin is a no-op when ``MEMORY_PROFILE_FILE`` is unset, so
normal pytest runs do no extra work.

Output schema (one JSON object per line):

- ``pid``: writing process PID
- ``nodeid``: pytest test node id
- ``vm_rss_before_kb``: VmRSS at test start (KB)
- ``vm_rss_after_kb``: VmRSS at test end (KB)
- ``vm_rss_delta_kb``: change in VmRSS during the test (KB)
- ``vm_peak_before_kb``: VmPeak at test start (KB)
- ``vm_peak_after_kb``: VmPeak at test end (KB)
- ``vm_size_before_kb``: VmSize (virtual) at test start (KB)
- ``vm_size_after_kb``: VmSize (virtual) at test end (KB)
- ``threads_before``: thread count at test start
- ``threads_after``: thread count at test end
"""

from __future__ import annotations

import json
import os
import threading
from typing import Any, Optional

_TRACE_FD: Optional[int] = None
_test_snapshots: dict[str, dict[str, int]] = {}


def _read_proc_status() -> dict[str, int]:
    """Read memory metrics from ``/proc/self/status``."""
    result: dict[str, int] = {}
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith(("VmRSS:", "VmPeak:", "VmSize:")):
                    parts = line.split()
                    result[parts[0].rstrip(":")] = int(parts[1])
    except OSError:
        pass
    result["Threads"] = threading.active_count()
    return result


def _emit(payload: dict) -> None:
    if _TRACE_FD is None:
        return
    line = (json.dumps(payload) + "\n").encode("utf-8")
    try:
        os.write(_TRACE_FD, line)
    except OSError:
        pass


def pytest_configure(config: Any) -> None:
    global _TRACE_FD
    path = os.environ.get("MEMORY_PROFILE_FILE")
    if not path:
        return
    _TRACE_FD = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o644)


def pytest_runtest_logstart(nodeid: str, location: Any) -> None:
    if _TRACE_FD is None:
        return
    _test_snapshots[nodeid] = _read_proc_status()


def pytest_runtest_logfinish(nodeid: str, location: Any) -> None:
    if _TRACE_FD is None:
        return
    before = _test_snapshots.pop(nodeid, None)
    if before is None:
        return
    after = _read_proc_status()
    _emit(
        {
            "pid": os.getpid(),
            "nodeid": nodeid,
            "vm_rss_before_kb": before.get("VmRSS", 0),
            "vm_rss_after_kb": after.get("VmRSS", 0),
            "vm_rss_delta_kb": after.get("VmRSS", 0) - before.get("VmRSS", 0),
            "vm_peak_before_kb": before.get("VmPeak", 0),
            "vm_peak_after_kb": after.get("VmPeak", 0),
            "vm_size_before_kb": before.get("VmSize", 0),
            "vm_size_after_kb": after.get("VmSize", 0),
            "threads_before": before.get("Threads", 0),
            "threads_after": after.get("Threads", 0),
        }
    )


def pytest_unconfigure(config: Any) -> None:
    global _TRACE_FD
    if _TRACE_FD is None:
        return
    try:
        os.close(_TRACE_FD)
    except OSError:
        pass
    _TRACE_FD = None
