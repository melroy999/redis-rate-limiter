"""Shared fixtures and helpers for direct Lua script unit tests.

These tests call ``redis.eval()`` directly with controlled Redis state,
bypassing the Python wrapper layer entirely. The Lua scripts are atomic
server-side operations; sync and async Python clients exercise identical
Lua code paths, so only the sync ``redis_client`` is used.

Fixture dependencies:
    - ``redis_client``, ``limiter_id``, ``lock_key``: from ``tests/conftest.py``.
"""

import json
import time

import pytest
from redis import Redis

from redis_rate_limiter.core.limiters import (
    LOCK_ACQUIRE_SCRIPT,
    LOCK_RELEASE_SCRIPT,
    LOCK_SIMPLE_RELEASE_SCRIPT,
)
from redis_rate_limiter.core.scripts import load_lua_script

__all__ = ["LOCK_ACQUIRE_SCRIPT", "LOCK_RELEASE_SCRIPT", "LOCK_SIMPLE_RELEASE_SCRIPT"]

CONSUME_SOURCE: str = load_lua_script("consume.lua")
ACQUIRE_SOURCE: str = load_lua_script("acquire.lua")
HEALTH_SOURCE: str = load_lua_script("health.lua")
RENEW_SOURCE: str = load_lua_script("renew.lua")
SCHEDULE_SOURCE: str = load_lua_script("schedule.lua")

# Default window size for sliding window tests (seconds).
WINDOW_SIZE: int = 10

# Default test parameters.
LIMIT: int = 10
MAX_CONCURRENCY: int = 5
MAX_AGE: int = 3600
LEASE_DURATION: int = 30
TIMEOUT_MS: int = 5000
COOLDOWN_MS: int = 1000


def get_window_keys(
    redis_client: Redis,
    base_key: str,
    window_size: int = WINDOW_SIZE,
) -> tuple[str, str]:
    """Return ``(current_key, previous_key)`` with a window-safety guarantee.

    Uses Redis ``TIME`` to compute the current and previous window keys,
    matching the Lua scripts' key derivation formula:
    ``current_window_start = floor(now_ms / window_size_ms) * window_size_ms``.

    If less than 500 ms remain in the current window, sleeps until the next
    window starts, then recomputes. This prevents window boundary crossings
    between key pre-population and ``eval()`` invocation.

    Args:
        redis_client: A sync Redis client instance.
        base_key: The base key prefix (e.g., ``"rl:test"``).
        window_size: The window size in seconds. Defaults to ``WINDOW_SIZE``.

    Returns:
        A tuple of ``(current_window_key, previous_window_key)``.
    """
    window_size_ms = window_size * 1000
    min_remaining_ms = 500

    redis_time = redis_client.time()
    now_ms = redis_time[0] * 1000 + redis_time[1] // 1000
    current_window_start = (now_ms // window_size_ms) * window_size_ms
    remaining_ms = current_window_start + window_size_ms - now_ms

    if remaining_ms < min_remaining_ms:
        time.sleep(remaining_ms / 1000 + 0.01)
        redis_time = redis_client.time()
        now_ms = redis_time[0] * 1000 + redis_time[1] // 1000
        current_window_start = (now_ms // window_size_ms) * window_size_ms

    previous_window_start = current_window_start - window_size_ms
    current_key = f"{base_key}:{current_window_start}"
    previous_key = f"{base_key}:{previous_window_start}"
    return current_key, previous_key


def get_redis_timestamp(redis_client: Redis) -> int:
    """Return the current Redis server timestamp in seconds.

    Args:
        redis_client: A sync Redis client instance.

    Returns:
        The server timestamp as an integer (seconds since epoch).
    """
    redis_time = redis_client.time()
    return redis_time[0]


def _build_task_json(
    base_key: str,
    task_id: str,
    func_path: str = "test.task",
    payload: dict | None = None,
    inflight_key: str | None = None,
    arrived_at_ms: int | None = None,
) -> str:
    """Build a JSON task string suitable for ``ZADD`` into the buffer.

    For ``consume.lua`` tests that require pre-populated buffer entries with
    metadata already present, pass ``arrived_at_ms`` explicitly. For
    ``schedule.lua`` tests, omit it (the script injects ``__meta_arrived_at``).

    Args:
        base_key: The test's base key prefix, used to namespace auto-generated
            inflight keys (e.g., ``"rl:limiter_test_abc_12345678"``).
        task_id: Unique task identifier.
        func_path: Dotted function path for task dispatch.
        payload: Task payload dictionary. Defaults to an empty dict.
        inflight_key: The Redis key used for deduplication. When not provided,
            derived as ``"{base_key}:inflight:{task_id}"``.
        arrived_at_ms: The arrival timestamp in milliseconds. If provided,
            included as ``__meta_arrived_at`` in the JSON.

    Returns:
        A JSON-encoded string representing the task.
    """
    data: dict = {
        "id": task_id,
        "func_path": func_path,
        "payload": payload or {},
        "inflight_key": inflight_key or f"{base_key}:inflight:{task_id}",
    }
    if arrived_at_ms is not None:
        data["__meta_arrived_at"] = arrived_at_ms
    return json.dumps(data, sort_keys=True)


@pytest.fixture
def build_task_json(base_key: str):
    """Provide a factory for building JSON task strings with
    namespace-isolated inflight keys.

    The returned callable has the same signature as the underlying
    ``_build_task_json`` helper, but with ``base_key`` pre-bound from the
    test's fixture scope.
    """
    from functools import partial

    return partial(_build_task_json, base_key)


@pytest.fixture
def base_key(limiter_id: str) -> str:
    """Provide a unique base key prefix for sliding window counter keys."""
    return f"rl:{limiter_id}"


@pytest.fixture
def buffer_key(limiter_id: str) -> str:
    """Provide a unique buffer sorted set key."""
    return f"rl:{limiter_id}:buffer"


@pytest.fixture
def concurrency_key(limiter_id: str) -> str:
    """Provide a unique concurrency sorted set key."""
    return f"rl:{limiter_id}:concurrency"


@pytest.fixture
def dlq_key(limiter_id: str) -> str:
    """Provide a unique dead letter queue list key."""
    return f"rl:{limiter_id}:dlq"
