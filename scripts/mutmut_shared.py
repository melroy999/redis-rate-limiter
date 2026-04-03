"""Shared utilities for mutmut wrapper scripts.

Provides two shared classes:

:class:`KilledByCollector`
    A pytest plugin used by both ``run_mutmut.py`` (first-killer mode,
    with ``-x``) and ``analyze_superfluity.py`` (full-matrix mode,
    without ``-x``) to record which tests kill each mutant and write
    that data to a per-child temp file for IPC with the parent process.

:class:`KilledByAccumulator`
    Parent-process state manager that reads per-child temp files and
    accumulates killed-by data in memory, then flushes to a JSON results
    file at exit. Also provides a factory for the
    ``SourceFileMutationData.register_result`` monkey-patch.

Usage::

    from mutmut_shared import KilledByCollector, KilledByAccumulator

    # Collector: subclass and supply the temp directory.
    class _MyCollector(KilledByCollector):
        def __init__(self, mutant_name=None):
            super().__init__(mutant_name, killed_by_dir=_KILLED_BY_DIR)

    # Accumulator: create once at module level.
    _accumulator = KilledByAccumulator(_KILLED_BY_RESULTS, _KILLED_BY_DIR)
"""

from __future__ import annotations

import json
import os


class KilledByCollector:
    """Pytest plugin that records which tests kill each mutant.

    Writes killed-by data to a per-child temp file inside ``killed_by_dir``
    for later collection by the parent process. Subclasses may override
    :meth:`_extra_payload` to add additional fields to the payload.

    Args:
        mutant_name: The name of the mutant under test, or ``None`` for
            baseline runs where no temp-file output is required.
        killed_by_dir: Absolute path to the directory used for IPC temp
            files. Each child process writes one file named ``{pid}.json``.
    """

    def __init__(self, mutant_name: str | None = None, *, killed_by_dir: str) -> None:
        self.killed_by: list[str] = []
        self.tests_selected: list[str] = []
        self.tests_ran: list[str] = []
        self._mutant_name = mutant_name
        self._killed_by_dir = killed_by_dir

    # ------------------------------------------------------------------
    # Subclass extension point
    # ------------------------------------------------------------------

    def _extra_payload(self) -> dict:  # type: ignore[return]
        """Return additional fields to merge into the temp-file payload.

        Override in subclasses to add script-specific data (e.g.,
        ``killed_during`` for in-progress test tracking).
        """
        return {}

    # ------------------------------------------------------------------
    # Temp-file I/O
    # ------------------------------------------------------------------

    def write_temp_file(self, *, partial: bool = False) -> None:
        """Write killed-by data to this child process's temp file.

        Called after pytest finishes (``partial=False``) or from a SIGXCPU
        handler (``partial=True``). The parent process reads the file in its
        patched ``SourceFileMutationData.register_result``.

        Args:
            partial: When ``True``, the write is a safety flush triggered
                by an impending SIGXCPU kill. When ``False``, it is the
                final authoritative write after the test run completes.
        """
        if self._mutant_name is None:
            return
        if not partial and not self.killed_by:
            # Final write with no failures: still write if tests were
            # selected so the parent can record the selection for
            # survived mutants. Skip if nothing useful to report.
            if not self.tests_selected:
                return
        os.makedirs(self._killed_by_dir, exist_ok=True)
        path = os.path.join(self._killed_by_dir, f"{os.getpid()}.json")
        payload: dict[str, object] = {
            "mutant_name": self._mutant_name,
            "killed_by": self.killed_by,
            "tests_selected": self.tests_selected,
            "tests_ran": self.tests_ran,
            "partial": partial,
        }
        payload.update(self._extra_payload())
        with open(path, "w") as f:
            json.dump(payload, f)

    # ------------------------------------------------------------------
    # Pytest hooks
    # ------------------------------------------------------------------

    def pytest_collection_modifyitems(self, items) -> None:  # type: ignore[no-untyped-def]
        """Record the tests pytest will execute for this run."""
        self.tests_selected = [item.nodeid for item in items]

    def pytest_runtest_makereport(self, item, call) -> None:  # type: ignore[no-untyped-def]
        if call.when == "call":
            self.tests_ran.append(item.nodeid)
            if call.excinfo is not None:
                self.killed_by.append(item.nodeid)
                if self._mutant_name is not None:
                    self.write_temp_file(partial=False)


class KilledByAccumulator:
    """Parent-process accumulator for killed-by IPC data.

    Reads per-child temp files written by :class:`KilledByCollector`,
    accumulates the data in memory, and flushes to a JSON results file
    at exit. Also provides a factory for the patched
    ``SourceFileMutationData.register_result`` method so that both
    ``run_mutmut.py`` and ``analyze_superfluity.py`` share the same
    read-and-accumulate logic.

    Args:
        results_path: Absolute path to the output JSON file.
        killed_by_dir: Absolute path to the per-child IPC temp directory.
    """

    def __init__(self, results_path: str, killed_by_dir: str) -> None:
        self._results_path = results_path
        self._killed_by_dir = killed_by_dir
        self._data: dict[str, dict[str, list[str] | int | bool | None]] = {}

    @property
    def data(self) -> dict[str, dict[str, list[str] | int | bool | None]]:
        """Return the accumulated killed-by data (read-only view)."""
        return self._data

    def append(
        self,
        mutant_name: str,
        killed_by: list[str],
        tests_selected: list[str] | None = None,
        tests_ran: list[str] | None = None,
        partial: bool = False,
        killed_during: str | None = None,
    ) -> None:
        """Accumulate a killed-by entry in memory.

        Called in the parent process after reading the child's temp file.
        Data is written to disk once at exit via :meth:`flush`.

        Args:
            mutant_name: The name of the mutant that was tested.
            killed_by: Node IDs of tests that killed the mutant.
            tests_selected: Node IDs of tests pytest collected for the run.
            tests_ran: Node IDs of tests that completed their call phase.
            partial: Whether the data is partial (SIGXCPU interrupted).
            killed_during: Node ID of the test that was running when the
                process was killed (first-killer mode only; ``None``
                otherwise).
        """
        entry: dict[str, object] = {
            "killed_by": killed_by,
            "tests_selected": tests_selected or [],
            "tests_ran": tests_ran or [],
            "partial": partial,
        }
        if killed_during is not None:
            entry["killed_during"] = killed_during
        self._data[mutant_name] = entry

    def flush(self) -> None:
        """Write all accumulated data to the results file.

        Registered as an ``atexit`` handler so the JSON is written even if
        mutmut exits via ``SystemExit`` (which Click raises on completion).
        """
        if self._data:
            with open(self._results_path, "w") as f:
                json.dump(self._data, f, indent=4)

    def make_register_result_patch(self):  # type: ignore[no-untyped-def]
        """Return a ``SourceFileMutationData.register_result`` replacement.

        The returned function reads the per-child killed-by temp file (if
        present) and appends its data to this accumulator, then reproduces
        the original ``register_result`` bookkeeping.
        """
        from datetime import datetime

        killed_by_dir = self._killed_by_dir
        accumulator = self

        def _patched_sfmd_register_result(self, *, pid, exit_code):  # type: ignore[no-untyped-def]
            from mutmut.__main__ import START_TIMES_BY_PID_LOCK

            key = self.key_by_pid[pid]
            killed_by_path = os.path.join(killed_by_dir, f"{pid}.json")
            if os.path.exists(killed_by_path):
                try:
                    with open(killed_by_path) as f:
                        data = json.load(f)
                    killed_by = data.get("killed_by", [])
                    tests_selected = data.get("tests_selected", [])
                    tests_ran = data.get("tests_ran", [])
                    partial = data.get("partial", False)
                    killed_during = data.get("killed_during")
                    if killed_by or tests_selected:
                        accumulator.append(
                            key,
                            killed_by,
                            tests_selected,
                            tests_ran,
                            partial,
                            killed_during,
                        )
                except (json.JSONDecodeError, OSError):
                    pass
                finally:
                    try:
                        os.unlink(killed_by_path)
                    except OSError:
                        pass

            assert self.key_by_pid[pid] in self.exit_code_by_key
            self.exit_code_by_key[key] = exit_code
            self.durations_by_key[key] = (
                datetime.now() - self.start_time_by_pid[pid]
            ).total_seconds()
            del self.key_by_pid[pid]
            with START_TIMES_BY_PID_LOCK:
                del self.start_time_by_pid[pid]
            self.save()

        return _patched_sfmd_register_result
