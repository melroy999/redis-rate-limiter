"""Reusable live dashboard for any rate limiter demonstration.

This module renders concurrency, buffer, rate limit, and dispatcher status
to the terminal.  Per-window history is tracked automatically across
successive render calls.

Usage::

    from examples.dashboard import Dashboard

    dashboard = Dashboard(limiter_id="my_limiter", limit=10)
    while True:
        status = limiter.get_status()
        dashboard.render(status)
        time.sleep(0.1)
"""

import os
import time


class Dashboard:
    """A live terminal dashboard driven by the dictionaries returned from ``get_status()``."""

    def __init__(self, limiter_id: str, limit: int) -> None:
        self._limiter_id = limiter_id
        self._limit = limit
        self._start = time.time()
        self._window_history: list[int] = []
        self._last_val_current: int | None = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def render(self, status: dict) -> None:
        """Clear the terminal and render the current status snapshot.

        Args:
            status: The dictionary returned by ``limiter.get_status()``.
        """
        elapsed = time.time() - self._start

        # Detect a window rotation: val_current drops below the last observed value.
        val_current = status["rate_limit"]["val_current"]
        if self._last_val_current is not None and val_current < self._last_val_current:
            self._window_history.append(self._last_val_current)
        self._last_val_current = val_current

        os.system("clear" if os.name == "posix" else "cls")
        self._print(status, elapsed, val_current)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _print(self, status: dict, elapsed: float, val_current: int) -> None:
        bar_width = 20

        print(f"=== {self._limiter_id} ===")
        print(f"Time: {time.strftime('%H:%M:%S')}  (elapsed: {elapsed:.1f}s)")

        # Concurrency bar
        c = status["concurrency"]
        filled = int((c["current"] / c["max"]) * bar_width) if c["max"] > 0 else 0
        bar = "#" * filled + "-" * (bar_width - filled)
        print(f"\nCONCURRENCY: [{bar}] {c['current']}/{c['max']}")

        # Buffer
        b = status["buffer"]
        print(f"BUFFER:      {b['count']} tasks waiting")

        # Rate limit bar (current window usage)
        r = status["rate_limit"]
        limit = r["limit"]
        usage_pct = min(1.0, val_current / limit) if limit > 0 else 0
        filled = int(usage_pct * bar_width)
        bar = "#" * filled + "-" * (bar_width - filled)
        print(f"RATE LIMIT:  [{bar}] {val_current}/{limit} (resets in {r['reset_in_ms']}ms)")
        print(f"             previous: {r['val_previous']}, estimated: {r['tokens_used']:.1f}")

        # Dispatcher lock status
        lock = "BUSY" if status["dispatcher"]["is_locked"] else "IDLE"
        print(f"DISPATCHER:  {lock}")

        # Per-window history
        print(f"\nPER WINDOW:  (limit={self._limit})")
        if self._window_history:
            for i, count in enumerate(self._window_history, 1):
                marker = " !" if count > self._limit else ""
                print(f"  window {i:>2}: {count}{marker}")
        if val_current > 0 or not self._window_history:
            label = len(self._window_history) + 1
            print(f"  window {label:>2}: {val_current} (in progress)")

        print("\n" + "-" * 40)
        print("Press [Ctrl+C] to exit")
