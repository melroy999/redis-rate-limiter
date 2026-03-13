"""Shared test utilities and helper functions.

This module provides utility functions that are used across multiple test files.
"""

import asyncio
import json
import logging
import math
import time
from contextlib import contextmanager
from threading import Timer
from typing import Generator

import pytest


@contextmanager
def shutdown_timer(
    obj: object,
    timeout: float = 0.5,
    attr: str = "_shutdown",
) -> Generator[Timer, None, None]:
    """Set a shutdown flag after *timeout* seconds so that a ``_run()``
    loop under test exits cleanly.

    This is the primary shutdown mechanism for tests that invoke ``_run()``
    directly rather than going through ``start()`` / ``shutdown()``.
    """
    timer = Timer(timeout, lambda: setattr(obj, attr, True))
    timer.start()
    try:
        yield timer
    finally:
        timer.cancel()


async def wait_for_key_expiry(
    redis_client, key: str, deadline_seconds: float = 5.0
) -> None:
    """Poll until a Redis key expires, using wall-clock time for the deadline.

    Relies on ``time.monotonic()`` rather than an iteration count to ensure
    the budget is honoured even when ``asyncio.sleep`` returns early or late
    (as observed under mutmut's trampoline overhead).

    Args:
        redis_client: An async Redis client.
        key: The Redis key to monitor.
        deadline_seconds: Maximum wall-clock seconds to wait before failing.

    Raises:
        pytest.fail: If the key has not expired within the deadline. The
            failure message includes the key's current PTTL for diagnosis.
    """
    end = time.monotonic() + deadline_seconds
    while time.monotonic() < end:
        if await redis_client.exists(key) == 0:
            return
        await asyncio.sleep(0.05)
    ttl = await redis_client.pttl(key)
    pytest.fail(f"key {key!r} did not expire within {deadline_seconds}s (pttl={ttl}ms)")


def dict_equals_approx(left, right, relative_tolerance=1e-9, absolute_tolerance=1e-9):
    """Determine whether two values are approximately equal, with support for nested structures.

    For float values, approximate equality is evaluated using a configurable tolerance.
    For nested dictionaries and lists, the comparison is performed recursively.
    For all other types, exact equality is used.

    Args:
        left: The first value to compare.
        right: The second value to compare.
        relative_tolerance: The relative tolerance applied to float comparisons.
        absolute_tolerance: The absolute tolerance applied to float comparisons.

    Returns:
        True if the values are approximately equal, False otherwise.
    """
    # Handle the case where both values are None.
    if left is None and right is None:
        return True
    if left is None or right is None:
        return False

    # Handle the case where the types differ.
    if type(left) is not type(right):
        return False

    # Handle float values with approximate equality.
    if isinstance(left, float):
        return math.isclose(
            left, right, rel_tol=relative_tolerance, abs_tol=absolute_tolerance
        )

    # Handle dictionaries recursively.
    if isinstance(left, dict):
        if set(left.keys()) != set(right.keys()):
            return False
        return all(
            dict_equals_approx(
                left[key], right[key], relative_tolerance, absolute_tolerance
            )
            for key in left.keys()
        )

    # Handle lists recursively.
    if isinstance(left, list):
        if len(left) != len(right):
            return False
        return all(
            dict_equals_approx(
                left[i], right[i], relative_tolerance, absolute_tolerance
            )
            for i in range(len(left))
        )

    # For all remaining types (i.e., int, str, bool), exact equality is used.
    return left == right


def assert_log_emitted(
    caplog_records: list,
    level: str,
    label: str,
    required_fragments: list[str],
    message: str,
) -> None:
    """Assert that at least one log record matches the given level, starts with ``label``, and
    contains all ``required_fragments`` as substrings.
    """

    def _matches(record: logging.LogRecord) -> bool:
        if record.levelname != level:
            return False
        if not record.message.startswith(label):
            return False
        if not all(fragment in record.message for fragment in required_fragments):
            return False
        # First word after the label prefix must be capitalized.
        body = record.message[len(label) :]
        first_alpha = next((c for c in body if c.isalpha()), None)
        if first_alpha is not None and not first_alpha.isupper():
            return False
        return True

    assert any(_matches(record) for record in caplog_records), message


def find_task_in_buffer(redis_client, buffer_key: str, task_id: str) -> dict | None:
    """Find and parse a single task by ID from the buffer sorted set (sync).

    Returns the parsed task dictionary, or ``None`` if no match exists.
    Raises ``AssertionError`` if more than one entry matches.
    """
    all_members = redis_client.zrange(buffer_key, 0, -1)
    results = [m for m in all_members if f'"{task_id}"' in m]
    if not results:
        return None
    assert len(results) == 1, (
        f"task with ID {task_id} found {len(results)} times in buffer, expected at most 1"
    )
    return json.loads(results[0])


async def async_find_task_in_buffer(
    redis_client, buffer_key: str, task_id: str
) -> dict | None:
    """Async equivalent of :func:`find_task_in_buffer`."""
    all_members = await redis_client.zrange(buffer_key, 0, -1)
    results = [m for m in all_members if f'"{task_id}"' in m]
    if not results:
        return None
    assert len(results) == 1, (
        f"task with ID {task_id} found {len(results)} times in buffer, expected at most 1"
    )
    return json.loads(results[0])


def clear_limiter_keys(redis_client, limiter) -> None:
    """Delete all Redis keys belonging to the given limiter instance."""
    keys = redis_client.keys(f"{limiter.id}:*")
    if keys:
        redis_client.delete(*keys)


def schedule_n_tasks(
    limiter,
    n: int,
    func_path: str = "rate_limiter.test.task.function",
) -> list[str]:
    """Preload the limiter buffer with ``n`` unique tasks and return their IDs."""
    task_ids: list[str] = []
    for i in range(n):
        scheduled, task_id = limiter.schedule_task(func_path, {"seq": i})
        assert scheduled, f"failed to schedule task {i}"
        task_ids.append(task_id)
    return task_ids


def is_subset(target: dict, superset: dict):
    """Determine whether the given target dictionary is a recursive subset of the given superset.

    Args:
        target: The dictionary that is considered the subset in the comparison.
        superset: The dictionary that is considered the superset in the comparison.

    Returns:
        True if ``target`` is a recursive subset of ``superset``, False otherwise.
    """
    for key, value in target.items():
        if key not in superset:
            return False
        if isinstance(value, dict):
            if not is_subset(value, superset.get(key, {})):
                return False
        elif value != superset[key]:
            return False
    return True
