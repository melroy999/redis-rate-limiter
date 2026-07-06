"""Pytest plugin that records per-process resource usage for mutmut diagnosis.

Each line in the output is one JSON object capturing a snapshot of the
writing process's resident set, virtual size, kernel thread count, and
open file descriptor count, plus the currently running pytest nodeid.

Two snapshot triggers:

1. **Periodic sampler** (background daemon thread, default 100 ms): runs
   for the duration of the pytest session and emits one ``sample`` event
   per tick. Captures intra-test peaks that test-boundary hooks miss.
2. **Test boundary hooks**: ``start`` and ``end`` events flank each test,
   so any sustained peak can be tied to whichever test was running.

Combined with ``mutmut_test_timeline``'s timeline, the resulting file
makes "how many mutmut child processes were simultaneously high-RSS at
time T" directly answerable: sort all events by ``ts``, group by
``pid``, and sum each PID's most recent ``rss_kb`` over a sliding
window.

The plugin is a no-op when ``MUTMUT_RESOURCE_SNAPSHOT_FILE`` is unset,
so behavioral pytest runs do no extra work.

Output schema
-------------

Each line is one JSON object with these fields:

- ``ts``: ``time.time()`` wall-clock seconds (UTC).
- ``mono``: ``time.monotonic()`` seconds, useful for duration math.
- ``pid``: the writing process's PID.
- ``mutant_id``: the value of ``MUTANT_UNDER_TEST`` (mutmut's child env
  var), or ``"baseline"`` outside mutmut child forks.
- ``event``: one of ``session_start``, ``start``, ``sample``, ``end``,
  ``session_end``, ``process_killed``.
- ``nodeid``: the pytest test node id of the most recently started test,
  or ``null`` between tests / before the first test.
- ``rss_kb``: ``VmRSS`` from ``/proc/self/status`` (kilobytes).
- ``vmsize_kb``: ``VmSize`` from ``/proc/self/status`` (kilobytes).
- ``threads``: ``Threads`` count from ``/proc/self/status``.
- ``fds``: number of entries in ``/proc/self/fd`` (open file descriptors).
"""

from __future__ import annotations

import json
import os
import signal
import threading
import time
from typing import Any, Optional

_SNAPSHOT_FD: Optional[int] = None
_MUTANT_ID: str = "baseline"
_SAMPLE_INTERVAL_S: float = 0.1
_active_nodeid: Optional[str] = None
_sampler: Optional["_Sampler"] = None


class _Sampler:
    """Background daemon thread that emits ``sample`` events at a fixed interval.

    One instance per pytest session; started in ``pytest_sessionstart`` and
    stopped in ``pytest_unconfigure``. Daemon-flagged so it never blocks
    process exit; if the process is killed mid-run, the last sample
    written before the kill is the diagnostic anchor.
    """

    def __init__(self) -> None:
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def _run(self) -> None:
        while not self._stop.wait(_SAMPLE_INTERVAL_S):
            _emit("sample")


def _read_proc_status() -> tuple[int, int, int]:
    """Return ``(rss_kb, vmsize_kb, threads)`` from ``/proc/self/status``.

    Returns zeros on any read error so the sampler never crashes the
    pytest worker.
    """
    rss_kb = 0
    vmsize_kb = 0
    threads = 0
    try:
        with open("/proc/self/status", "r") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    rss_kb = int(line.split()[1])
                elif line.startswith("VmSize:"):
                    vmsize_kb = int(line.split()[1])
                elif line.startswith("Threads:"):
                    threads = int(line.split()[1])
    except OSError:
        pass
    return rss_kb, vmsize_kb, threads


def _count_fds() -> int:
    """Return the number of entries in ``/proc/self/fd``."""
    try:
        return len(os.listdir("/proc/self/fd"))
    except OSError:
        return 0


def _emit(event: str, **fields: Any) -> None:
    """Append one event line to the snapshot file.

    ``os.write`` on an ``O_APPEND`` file descriptor is atomic for line
    sizes below ``PIPE_BUF`` (4 KB), so concurrent writes from sibling
    mutmut child processes do not interleave.
    """
    if _SNAPSHOT_FD is None:
        return
    rss_kb, vmsize_kb, threads = _read_proc_status()
    fds = _count_fds()
    payload = {
        "ts": time.time(),
        "mono": time.monotonic(),
        "pid": os.getpid(),
        "mutant_id": _MUTANT_ID,
        "event": event,
        "nodeid": _active_nodeid,
        "rss_kb": rss_kb,
        "vmsize_kb": vmsize_kb,
        "threads": threads,
        "fds": fds,
        **fields,
    }
    line = (json.dumps(payload) + "\n").encode("utf-8")
    try:
        os.write(_SNAPSHOT_FD, line)
    except OSError:
        pass


def _install_sigxcpu_chain() -> None:
    """Chain a ``process_killed`` emitter onto whatever SIGXCPU handler is set.

    Mutmut bounds per-mutant CPU time via ``RLIMIT_CPU``. Writing one
    final snapshot from the kernel's ``SIGXCPU`` delivery captures the
    resource state at the moment of kill, which would otherwise be lost.
    """
    if not hasattr(signal, "SIGXCPU"):
        return
    previous = signal.getsignal(signal.SIGXCPU)

    def _handler(signum: int, frame: Any) -> None:
        _emit("process_killed", signal="SIGXCPU")
        if callable(previous):
            previous(signum, frame)
        else:
            signal.signal(signal.SIGXCPU, signal.SIG_DFL)
            os.kill(os.getpid(), signal.SIGXCPU)

    signal.signal(signal.SIGXCPU, _handler)


def pytest_configure(config: Any) -> None:
    global _SNAPSHOT_FD, _MUTANT_ID
    snapshot_path = os.environ.get("MUTMUT_RESOURCE_SNAPSHOT_FILE")
    if not snapshot_path:
        return
    _MUTANT_ID = os.environ.get("MUTANT_UNDER_TEST", "baseline")
    _SNAPSHOT_FD = os.open(
        snapshot_path,
        os.O_WRONLY | os.O_APPEND | os.O_CREAT,
        0o644,
    )
    _install_sigxcpu_chain()


def pytest_sessionstart(session: Any) -> None:
    global _sampler
    _emit("session_start")
    if _SNAPSHOT_FD is None:
        return
    _sampler = _Sampler()
    _sampler.start()


def pytest_runtest_logstart(nodeid: str, location: Any) -> None:
    global _active_nodeid
    _active_nodeid = nodeid
    _emit("start", nodeid=nodeid)


def pytest_runtest_logfinish(nodeid: str, location: Any) -> None:
    global _active_nodeid
    _emit("end", nodeid=nodeid)
    _active_nodeid = None


def pytest_unconfigure(config: Any) -> None:
    global _SNAPSHOT_FD, _sampler
    if _sampler is not None:
        _sampler.stop()
        _sampler = None
    _emit("session_end")
    if _SNAPSHOT_FD is not None:
        try:
            os.close(_SNAPSHOT_FD)
        except OSError:
            pass
        _SNAPSHOT_FD = None
