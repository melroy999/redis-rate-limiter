"""Shared test utilities and helper functions.

This module provides utility functions that are used across multiple test files.
"""

import asyncio
import json
import logging
import math
import time
from contextlib import contextmanager
from threading import Thread, Timer
from typing import Any, Callable, Generator, Optional

import pytest


def shutdown_completes_within(subject: Any, timeout: float = 1.0) -> bool:
    """Return ``True`` if ``subject.shutdown()`` completes within ``timeout`` seconds.

    Runs ``shutdown`` on a daemon thread so the caller is never blocked
    longer than the timeout. Designed for ``timeout_safety_net`` tests
    that catch mutations which prevent the subject's internal stop
    mechanism from firing.
    """
    thread = Thread(target=subject.shutdown, daemon=True)
    thread.start()
    thread.join(timeout=timeout)
    return not thread.is_alive()


async def async_shutdown_completes_within(subject: Any, timeout: float = 1.0) -> bool:
    """Return ``True`` if ``await subject.shutdown()`` completes within ``timeout`` seconds.

    Measures elapsed wall-clock time rather than relying on
    ``asyncio.wait_for`` to raise ``TimeoutError``: a ``shutdown``
    that catches ``CancelledError`` (common pattern) returns normally
    even when cancelled, defeating ``wait_for``'s exception path.
    """
    start = time.monotonic()
    try:
        await asyncio.wait_for(subject.shutdown(), timeout=timeout)
    except asyncio.TimeoutError:
        return False
    return (time.monotonic() - start) < timeout


@contextmanager
def cap_iterations(
    target: Any,
    attr: str,
    *,
    return_value: Any = None,
    side_effect: Optional[Callable[..., Any]] = None,
    cap: int = 10000,
) -> Generator[Callable[[], int], None, None]:
    """Patch ``target.attr`` with a counting stub for spin-class mutation detection.

    Replaces the named attribute with a stub that records each invocation
    and returns either ``side_effect(*args, **kwargs)`` (when supplied) or
    ``return_value``. After ``cap`` calls the stub raises
    ``AssertionError`` to terminate runaway loops cheaply.

    The stub flavor is selected by introspecting the original attribute:
    coroutine functions get an ``async def`` stub, others get a plain
    function. When the original is async and ``side_effect`` returns a
    coroutine, the stub awaits it.

    Yields a zero-argument callable that returns the current invocation
    count, so background-loop tests can assert on iteration rate after a
    bounded sleep:

        rate_limited = {"success": False, ..., "remaining_tasks": 1}
        with cap_iterations(limiter, "consume", return_value=rate_limited) as count:
            limiter.trigger_consume()
            time.sleep(0.5)
        assert count() < 50, f"drain spun: {count()} iterations in 0.5s"

    Foreground tests where the AssertionError propagates can rely on the
    cap as the failure mechanism directly: install the wrapped callable on
    a synchronous code path and expect ``AssertionError`` (or whichever
    exception the surrounding code raises first).
    """
    state = {"count": 0}
    original = getattr(target, attr)
    is_async = asyncio.iscoroutinefunction(original)
    label = f"{type(target).__name__}.{attr}"

    def _resolve(args: tuple, kwargs: dict) -> Any:
        if side_effect is not None:
            return side_effect(*args, **kwargs)
        return return_value

    if is_async:

        async def _stub(*args: Any, **kwargs: Any) -> Any:
            state["count"] += 1
            if state["count"] > cap:
                raise AssertionError(f"call count exceeded cap of {cap} on {label}")
            result = _resolve(args, kwargs)
            if asyncio.iscoroutine(result):
                result = await result
            return result
    else:

        def _stub(*args: Any, **kwargs: Any) -> Any:
            state["count"] += 1
            if state["count"] > cap:
                raise AssertionError(f"call count exceeded cap of {cap} on {label}")
            return _resolve(args, kwargs)

    setattr(target, attr, _stub)
    try:
        yield lambda: state["count"]
    finally:
        setattr(target, attr, original)


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


def _log_record_matches(
    record: logging.LogRecord,
    level: str,
    label: str,
    required_fragments: list[str],
) -> bool:
    """Return ``True`` if *record* matches the given level, label prefix, fragment, and
    capitalization constraints.
    """
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
    assert any(
        _log_record_matches(record, level, label, required_fragments)
        for record in caplog_records
    ), message


def assert_log_emitted_with_exc_info(
    caplog_records: list,
    level: str,
    label: str,
    required_fragments: list[str],
    message: str,
) -> None:
    """Assert that at least one log record matches the given level, label, and fragments,
    and has ``exc_info`` attached (i.e., the log call included exception context).
    """
    matching = [
        record
        for record in caplog_records
        if _log_record_matches(record, level, label, required_fragments)
    ]
    assert matching, message
    assert any(record.exc_info is not None for record in matching), (
        f"{message} (matched record found, but exc_info was not set)"
    )


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
    """Delete non-expiring Redis keys belonging to the given limiter instance."""
    redis_client.delete(
        f"{limiter.id}:buffer",
        f"{limiter.id}:concurrency",
        f"{limiter.id}:dlq",
    )


@contextmanager
def property_test_cleanup(
    redis_client: Any,
    limiter: Any,
    task_ids: list[str] | None = None,
) -> Generator[list[str], None, None]:
    """Context manager that ensures clean Redis state for property-based tests.

    On entry, clears the limiter's non-expiring keys to ensure a clean
    starting state. On exit, clears the same keys again and deletes all
    in-flight keys for the collected task IDs.

    Args:
        redis_client: A sync Redis client.
        limiter: The rate limiter instance whose keys should be cleaned.
        task_ids: Optional pre-populated list of task IDs. If ``None``,
            a fresh empty list is created. Callers should append task IDs
            to this list as tasks are scheduled during the test.

    Yields:
        A mutable list of task IDs. The caller should append IDs of any
        tasks scheduled during the test body.
    """
    ids: list[str] = task_ids if task_ids is not None else []
    clear_limiter_keys(redis_client, limiter)
    try:
        yield ids
    finally:
        clear_limiter_keys(redis_client, limiter)
        for tid in ids:
            redis_client.delete(limiter.get_inflight_key(tid))


def cleanup_managed_limiter(redis_client, limiter_id: str) -> None:
    """Delete all Redis state for a managed limiter created via ``create()``.

    Removes the buffer, concurrency, and DLQ keys, as well as the limiter's
    entries in the shared registry hashes. This is the canonical teardown for
    fixtures that call ``LimiterClass.create()``.
    """
    redis_client.delete(
        f"{limiter_id}:buffer",
        f"{limiter_id}:concurrency",
        f"{limiter_id}:dlq",
    )
    redis_client.hdel("rl:registry:configs", limiter_id)
    redis_client.hdel("rl:registry:versions", limiter_id)


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
