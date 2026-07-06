"""Pytest plugin that records per-test wall-clock timing for mutmut diagnosis.

Each event (session start, test start, test end, periodic heartbeat, session
end, and process kill) is appended as one JSON line to a shared timeline file.
The file is opened with ``O_APPEND`` so concurrent appends from parallel mutmut
child processes do not interleave (POSIX guarantees atomicity for writes
smaller than ``PIPE_BUF`` = 4 KB; each event is well below that).

The plugin is a no-op when ``MUTMUT_TEST_TIMELINE_FILE`` is unset.

Why heartbeats
--------------

Mutmut enforces a per-mutant CPU-time budget via ``RLIMIT_CPU``. When a child
process exceeds the budget, the kernel sends ``SIGXCPU`` and the process dies.
``pytest_runtest_logfinish`` does not fire in that case, so for a test killed
mid-run we lose the end timestamp. A 100 ms heartbeat from a daemon thread
provides a reliable lower bound on "test was still alive at this time" without
depending on the asyncio event loop (which may be starved by the very
mutation under investigation).

Output schema
-------------

Each line is one JSON object with these fields:

- ``ts``: ``time.time()`` wall-clock seconds (UTC).
- ``mono``: ``time.monotonic()`` seconds, useful for duration math without
  worrying about clock skew.
- ``pid``: the writing process's PID.
- ``mutant_id``: the value of ``MUTANT_UNDER_TEST`` (mutmut's child env var),
  or ``"baseline"`` outside mutmut child forks.
- ``event``: one of ``session_start``, ``start``, ``heartbeat``, ``end``,
  ``session_end``, ``process_killed``.
- ``nodeid``: the pytest test node id (omitted on session-level events).
"""

from __future__ import annotations

import json
import os
import signal
import threading
import time
from typing import Any, Optional

_TIMELINE_FD: Optional[int] = None
_MUTANT_ID: str = "baseline"
_HEARTBEAT_INTERVAL_S: float = 0.1


class _Heartbeat:
    """Background daemon thread that emits heartbeat events at a fixed interval.

    One instance per active test; started in ``pytest_runtest_logstart`` and
    stopped in ``pytest_runtest_logfinish``. The thread is daemon so it does
    not block process exit; if the process is killed mid-test, the last
    heartbeat written before the kill is the diagnostic anchor.
    """

    def __init__(self, nodeid: str) -> None:
        self._nodeid = nodeid
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def _run(self) -> None:
        while not self._stop.wait(_HEARTBEAT_INTERVAL_S):
            _emit("heartbeat", nodeid=self._nodeid)


_active_heartbeat: Optional[_Heartbeat] = None


def _emit(event: str, **fields: Any) -> None:
    """Append one event line to the timeline file.

    ``os.write`` on an ``O_APPEND`` file descriptor is atomic for line sizes
    below ``PIPE_BUF`` (4 KB), so concurrent writes from sibling mutmut child
    processes do not interleave. The ``encode``/``write`` path is sync and
    avoids any Python-level buffering.
    """
    if _TIMELINE_FD is None:
        return
    payload = {
        "ts": time.time(),
        "mono": time.monotonic(),
        "pid": os.getpid(),
        "mutant_id": _MUTANT_ID,
        "event": event,
        **fields,
    }
    line = (json.dumps(payload) + "\n").encode("utf-8")
    try:
        os.write(_TIMELINE_FD, line)
    except OSError:
        pass


def _install_sigxcpu_chain() -> None:
    """Chain a ``process_killed`` emitter onto whatever SIGXCPU handler is set.

    Mutmut uses ``RLIMIT_CPU`` to bound per-mutant CPU time; when the child
    exceeds the budget, the kernel raises ``SIGXCPU``. Writing one final event
    here gives us the upper bound for the test that was running at the moment
    of kill, even though ``pytest_runtest_logfinish`` will not fire.
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
    global _TIMELINE_FD, _MUTANT_ID
    timeline_path = os.environ.get("MUTMUT_TEST_TIMELINE_FILE")
    if not timeline_path:
        return
    _MUTANT_ID = os.environ.get("MUTANT_UNDER_TEST", "baseline")
    _TIMELINE_FD = os.open(
        timeline_path,
        os.O_WRONLY | os.O_APPEND | os.O_CREAT,
        0o644,
    )
    _install_sigxcpu_chain()


def pytest_sessionstart(session: Any) -> None:
    _emit("session_start")


def pytest_runtest_logstart(nodeid: str, location: Any) -> None:
    global _active_heartbeat
    _emit("start", nodeid=nodeid)
    _active_heartbeat = _Heartbeat(nodeid)
    _active_heartbeat.start()


def pytest_runtest_logfinish(nodeid: str, location: Any) -> None:
    global _active_heartbeat
    if _active_heartbeat is not None:
        _active_heartbeat.stop()
        _active_heartbeat = None
    _emit("end", nodeid=nodeid)


def pytest_unconfigure(config: Any) -> None:
    global _TIMELINE_FD, _active_heartbeat
    if _active_heartbeat is not None:
        _active_heartbeat.stop()
        _active_heartbeat = None
    _emit("session_end")
    if _TIMELINE_FD is not None:
        try:
            os.close(_TIMELINE_FD)
        except OSError:
            pass
        _TIMELINE_FD = None
