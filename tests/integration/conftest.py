"""Shared helpers for integration tests.

This module provides two helpers used throughout the integration test suite:

- ``consume_and_complete``: consumes a task and immediately completes its
  lifecycle, thereby releasing the concurrency slot and isolating rate
  limiting behaviour from concurrency limiting.
- ``precise_sleep``: an active-polling sleep that works around the coarse
  timer resolution on Windows (~15ms). All integration tests that require
  sub-second timing precision must use this function instead of
  ``time.sleep()`` (see TESTING_GUIDELINES.md Section 5.5).
"""

import time


def consume_and_complete(limiter) -> dict:
    """Consume a task and immediately complete its lifecycle.

    This function simulates a task that is consumed and executed
    instantaneously, thereby releasing the concurrency slot. It is
    intended for tests that require the isolation of rate limiting
    behaviour from concurrency limiting.

    Args:
        limiter: The rate limiter instance.

    Returns:
        A dictionary containing the consume result.
    """
    result = limiter.consume()
    if result["success"]:
        task_id = result["task"]["id"]
        with limiter.task_lifecycle(task_id):
            pass
    return result


def precise_sleep(duration_seconds: float) -> None:
    """Sleep for a precise duration by means of active polling.

    This function serves as a workaround for the unreliability of
    ``time.sleep()`` on Windows for sub-second durations. On Windows,
    ``time.sleep()`` may deviate by as much as 10x for small durations
    owing to the coarse timer resolution (~15ms).

    Args:
        duration_seconds: The duration to sleep, specified in seconds.
    """
    target_time = time.time() + duration_seconds
    # Use 1ms sleep intervals to avoid busy-waiting while maintaining precision.
    while time.time() < target_time:
        time.sleep(0.001)
