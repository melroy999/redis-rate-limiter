from __future__ import annotations

import hashlib
import json
import logging
import math
import os
import random
import signal
import time
import uuid
from abc import ABC, abstractmethod
from importlib import resources
from threading import Condition, Event, Lock, Thread
from typing import (
    Any,
    Callable,
    ClassVar,
    ContextManager,
    Dict,
    Literal,
    Optional,
    TypedDict,
    cast,
)

import redis
from redis import Redis

logger = logging.getLogger(__name__)


class TaskData(TypedDict):
    """Structured representation of the task data returned by the buffer during consumption."""

    id: str  # The unique identifier of the task.
    func_path: str  # The Python dotted path to the function to be executed.
    payload: dict  # The parameters to be forwarded to the function.
    inflight_key: str  # The Redis key used to track de-duplication and in-flight state.


class ConsumeResult(TypedDict):
    """Structured representation of the result returned by the consume Lua script."""

    success: bool  # Indicates whether a task was successfully consumed.
    expired: bool  # Indicates whether the task has expired.
    task: Optional[TaskData]  # The deserialized task data from Redis, or None if no task was consumed.
    remaining_tokens: int  # The number of remaining rate limit tokens in the current window.
    active_concurrency: int  # The number of concurrency slots currently in use.
    reset_in_ms: int  # The time in milliseconds until the current window expires.
    remaining_tasks: int  # The number of tasks remaining in the buffer awaiting processing.
    val_previous: int  # The raw counter value for the previous fixed window.
    val_current: int  # The raw counter value for the current fixed window.


class DistributedLock:
    """Context manager for the distributed dispatch lock.

    The lock is acquired via a Redis-backed mechanism prior to draining and
    is released upon exit, thereby ensuring that only one drainer operates
    at any given time.
    """

    def __init__(self, redis_client: Redis, lock_key: str, timeout_ms: int):
        """Initialize the distributed lock manager.

        Args:
            redis_client: The Redis client instance used for lock operations.
            lock_key: The Redis key under which the lock is stored.
            timeout_ms: The lock timeout in milliseconds, after which the lock expires automatically.
        """
        self.redis = redis_client
        self.lock_key = lock_key
        self.timeout_ms = timeout_ms
        self.token = str(uuid.uuid4())
        self.acquired = False

    def __enter__(self) -> bool:
        """Attempt to acquire the dispatch lock via the Redis SET NX command.

        Returns:
            True if the lock was successfully acquired, or False if the lock is held by another drainer.
        """
        # Acquire the lock.
        self.acquired = bool(
            self.redis.set(self.lock_key, self.token, px=self.timeout_ms, nx=True)
        )
        if self.acquired:
            logger.debug(
                "Dispatch lock acquired: key=%s, token=%s, timeout_ms=%d.",
                self.lock_key,
                self.token,
                self.timeout_ms,
            )
        else:
            logger.debug(
                "Dispatch lock contended: key=%s (another drainer holds the lock).",
                self.lock_key,
            )
        return bool(self.acquired)

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Release the dispatch lock, provided it is still owned by this instance."""
        if self.acquired:
            # The lock is only deleted if the stored token matches the local token.
            # This prevents the inadvertent deletion of locks created after a timeout.
            script = """
                if redis.call("get", KEYS[1]) == ARGV[1] then
                    return redis.call("del", KEYS[1])
                else
                    return 0
                end
                """
            result = self.redis.eval(script, 1, self.lock_key, self.token)
            if result:
                logger.debug(
                    "Dispatch lock released: key=%s, token=%s.",
                    self.lock_key,
                    self.token,
                )
            else:
                logger.debug(
                    "Dispatch lock already expired before release: key=%s, token=%s.",
                    self.lock_key,
                    self.token,
                )


class TaskLifecycle:
    """Context manager responsible for concurrency slot cleanup upon task completion."""

    def __init__(
        self,
        limiter: AbstractDistributedRateLimiter,
        task_id: str,
        on_heartbeat_failure: Literal["warn", "kill"] = "warn",
    ):
        """Initialize a lifecycle context manager that ensures concurrency slot cleanup.

        Args:
            limiter: The rate limiter instance to be observed.
            task_id: The identifier of the task whose active state is to be cleared.
            on_heartbeat_failure: The strategy for handling heartbeat failures, either ``"warn"`` or ``"kill"``.
        """
        self.limiter = limiter
        self.task_id = task_id
        self.interval = self.limiter.lease_duration / 2

        # Threading controls for the heartbeat loop.
        self._stop_event: Event = Event()
        self._thread: Optional[Thread] = None

        # Health monitoring controls.
        self.on_failure_action = on_heartbeat_failure
        self.is_healthy = True

    def _heartbeat_loop(self) -> None:
        """Background task that periodically renews the lease on a concurrency slot."""
        while not self._stop_event.wait(timeout=self.interval):
            try:
                # Extend the lease.
                self.limiter.extend_lease(self.task_id, self.limiter.lease_duration)

                # Indicate that the worker has restored proper functioning.
                if not self.is_healthy:
                    logger.info(
                        "Heartbeat connection restored for task %s on limiter %s.",
                        self.task_id,
                        self.limiter.id,
                    )
                    self.is_healthy = True
            except Exception as e:
                # Mark the lifecycle as unhealthy to signal to the worker that an error has occurred.
                self.is_healthy = False

                if self.on_failure_action == "kill":
                    # Terminate the worker process and halt the heartbeat thread.
                    logger.critical(
                        "Heartbeat failed for task %s: %s - terminating worker.",
                        self.task_id,
                        e,
                    )
                    os.kill(os.getpid(), signal.SIGTERM)
                    break
                else:
                    logger.critical(
                        "Heartbeat failed for task %s: %s - flagged as unhealthy.",
                        self.task_id,
                        e,
                    )

    def __enter__(self) -> TaskLifecycle:
        """Start the heartbeat thread that periodically renews the concurrency lease."""
        # Start the keep-alive thread.
        self._thread = Thread(target=self._heartbeat_loop, daemon=True)
        self._thread.start()
        logger.debug(
            "Task lifecycle entered: limiter=%s, task_id=%s, heartbeat_interval_s=%.3f.",
            self.limiter.id,
            self.task_id,
            self.interval,
        )
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Stop the heartbeat thread, release the concurrency slot, and trigger a follow-up drain."""
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=1.0)

        # Perform resource cleanup.
        try:
            # Release the concurrency slot.
            removed_concurrency = self.limiter.redis.zrem(
                self.limiter.concurrency_key, self.task_id
            )

            # Clear the in-flight marker associated with the task.
            inflight_removed = 0
            if self.task_id:
                inflight_key = self.limiter.get_inflight_key(self.task_id)

                # noinspection PyUnnecessaryCast
                # This cast is necessary for mypy type validation.
                inflight_removed = cast(int, self.limiter.redis.delete(inflight_key))

            logger.debug(
                "Concurrency slot released and inflight key cleared: limiter=%s, task_id=%s, removed_concurrency=%s, removed_inflight=%s.",
                self.limiter.id,
                self.task_id,
                removed_concurrency,
                inflight_removed,
            )
        finally:
            logger.debug(
                "Task lifecycle exited, triggering follow-up consume: limiter=%s, task_id=%s.",
                self.limiter.id,
                self.task_id,
            )
            # Re-trigger the dispatcher to fill the newly vacated slot.
            self.limiter.trigger_consume()


class DrainLoop:
    """Single persistent thread that executes ``drain()`` according to a managed schedule.

    Multiple ``wake()`` calls are naturally coalesced; if an earlier drain is
    already pending, subsequent requests are treated as no-ops. A watchdog timeout
    triggers periodic drains even when no explicit ``wake()`` call is received,
    thereby enabling recovery from crashed tasks, lost recovery chains, or stale
    concurrency slots.
    """

    def __init__(
        self,
        limiter: AbstractDistributedRateLimiter,
        watchdog_interval: float,
    ) -> None:
        self._limiter = limiter
        self._lock = Lock()
        self._condition = Condition(self._lock)
        self._next_wake: float | None = None
        self._shutdown = False
        self._watchdog_interval = watchdog_interval
        self._thread: Thread | None = None

    def wake(self, delay: float = 0.0) -> None:
        """Request that a drain be performed ``delay`` seconds from the current time.

        If an earlier drain is already pending, this call is a no-op.
        The drain thread is lazily started upon the first invocation of ``wake()``.
        """
        target = time.monotonic() + delay
        with self._condition:
            self._ensure_started()
            if self._next_wake is None or target < self._next_wake:
                self._next_wake = target
                self._condition.notify()

    def shutdown(self) -> None:
        """Signal the drain thread to terminate and wait for it to complete."""
        with self._condition:
            self._shutdown = True
            self._condition.notify()
        if self._thread is not None:
            self._thread.join(timeout=5.0)

    def _ensure_started(self) -> None:
        """Lazily initialize and start the drain thread upon the first wake request.

        This method must be called while holding ``self._condition``.
        """
        if self._thread is None:
            self._thread = Thread(target=self._run, daemon=True)
            self._thread.start()

    def _run(self) -> None:
        """Execute the main loop, sleeping until the next scheduled wake or the watchdog timeout elapses."""
        while True:
            with self._condition:
                if self._shutdown:
                    return
                if self._next_wake is None:
                    # No work is scheduled; the watchdog timeout is used so that the
                    # loop periodically checks for stranded buffer items.
                    self._condition.wait(timeout=self._watchdog_interval)
                    if self._next_wake is not None:
                        # A real wake arrived during the wait.
                        continue
                else:
                    remaining = self._next_wake - time.monotonic()
                    if remaining > 0:
                        self._condition.wait(timeout=remaining)
                        continue
                self._next_wake = None
            # Execute the drain operation outside the condition lock.
            self._limiter.drain()


class AbstractDistributedRateLimiter(ABC):
    """Abstract base class for distributed rate limiting of task execution.

    The term "distributed" refers to the rate limiting state, not to task execution
    itself: multiple processes and machines sharing the same limiter identifier are
    collectively rate-limited via Redis. The manner in which tasks are dispatched
    (e.g., Celery, threads, asyncio) is determined by the concrete backend subclass.

    Rate limiting is performed through atomic Lua scripts executed on a single Redis
    instance. All rate limit state (i.e., window counters, the task buffer, and the
    concurrency set) must reside on the same Redis node to guarantee correctness.

    Redis configuration requirements:
        - A single Redis instance, or a master-only setup in which all reads and writes
          are directed to the same node. Read replicas introduce replication lag that
          may cause the rate limit to be exceeded, as a replica may serve stale window
          counters.
        - Redis Cluster is not supported. The limiter utilizes multiple keys (window
          counters, buffer, concurrency set, dispatch lock) that must be co-located on
          the same shard. Key hash tags are not applied; hence, Redis Cluster may
          distribute them across different nodes and violate atomicity.
    """

    _CONSUME_LUA_SCRIPT: str
    _SCHEDULE_LUA_SCRIPT: str
    _HEALTH_LUA_SCRIPT: str
    _RENEW_LUA_SCRIPT: str

    # Preferred package paths for Lua resources.
    # The first entry supports installed wheels; the latter serves as a fallback for source-tree imports.
    resource_packages: tuple[str, ...] = (
        "celery_rate_limiter.lua",
        "src.celery_rate_limiter.lua",
    )

    def __init__(
        self,
        redis_client: Redis,
        limiter_id: str,
        limit: int,
        window: float,
        max_concurrency: int,
        max_age: int = 3600,
        lease_duration: int = 30,
        on_heartbeat_failure: Literal["warn", "kill"] = "warn",
        jitter_enabled: bool = True,
        jitter_min_pct: float = 0.02,
        jitter_max_pct: float = 0.08,
        metrics_callback: Optional[Callable[[str, dict], None]] = None,
    ):
        """Initialize the rate limiter with the specified parameters.

        Args:
            redis_client: The Redis client instance to be used for all operations.
            limiter_id: The unique identifier of the rate limiter to be created.
            limit: The maximum number of tasks permitted per time window.
            window: The time window in seconds to which the rate limit is applied.
            max_concurrency: The maximum number of tasks that may execute concurrently.
            max_age: The maximum duration in seconds that a task may reside in the queue before it expires.
            lease_duration: The duration in seconds after which a concurrency slot lease expires.
            on_heartbeat_failure: The strategy for handling heartbeat failures. ``"warn"`` allows the
                job to proceed, whereas ``"kill"`` forces the worker to terminate its execution.
            jitter_enabled: Whether randomized jitter is added to retry delays to mitigate the
                thundering herd problem. Recommended: ``True`` (default).
            jitter_min_pct: The minimum jitter as a percentage of the window size (default: 2%,
                i.e., 20ms for a 1s window). The lower bound ensures some spread even under low load.
            jitter_max_pct: The maximum jitter as a percentage of the window size (default: 8%,
                i.e., 80ms for a 1s window). The upper bound prevents excessive delays under high load.
            metrics_callback: An optional callback invoked after consume and schedule operations.
                It receives an event name string (``"consume"`` or ``"schedule"``) and a dictionary
                containing event data. Any exceptions raised by the callback are caught and logged
                to avoid disrupting the limiter.
        """
        self.redis = redis_client
        self.id = limiter_id
        self.buffer_key = f"{self.id}:buffer"
        self.concurrency_key = f"{self.id}:concurrency"
        self.lock_key = f"{self.id}:dispatch_lock"
        self.dlq_key = f"{self.id}:dlq"
        self.limit = limit
        self.window = window
        self.max_concurrency = max_concurrency
        self.max_age = max_age
        self.lease_duration = lease_duration
        self.on_heartbeat_failure = on_heartbeat_failure
        self.jitter_enabled = jitter_enabled
        self.jitter_min_pct = jitter_min_pct
        self.jitter_max_pct = jitter_max_pct
        self.metrics_callback = metrics_callback
        self._config_version: int = 0
        self._paused_until: float = 0.0
        self._consecutive_drain_failures: int = 0
        self._drain_loop = DrainLoop(self, watchdog_interval=self.window * 2)
        logger.info(
            "Rate limiter initialized: id=%s, limit=%d, window_s=%g, max_concurrency=%d, max_age_s=%d, lease_duration_s=%d, heartbeat_failure=%s, jitter_enabled=%s, jitter_min_pct=%.3f, jitter_max_pct=%.3f, metrics_callback=%s.",
            self.id,
            self.limit,
            self.window,
            self.max_concurrency,
            self.max_age,
            self.lease_duration,
            self.on_heartbeat_failure,
            self.jitter_enabled,
            self.jitter_min_pct,
            self.jitter_max_pct,
            "enabled" if self.metrics_callback else "disabled",
        )

        # Load the Lua scripts from disk.
        self._load_lua_script("consume.lua", "_CONSUME_LUA_SCRIPT")
        self._load_lua_script("schedule.lua", "_SCHEDULE_LUA_SCRIPT")
        self._load_lua_script("health.lua", "_HEALTH_LUA_SCRIPT")
        self._load_lua_script("renew.lua", "_RENEW_LUA_SCRIPT")

        # Optimize performance by caching the scripts on the Redis server.
        self.consume_script_sha: str = str(
            self.redis.script_load(self._CONSUME_LUA_SCRIPT)
        )
        logger.debug(
            "Lua script cached: limiter=%s, script=%s, sha=%s.",
            self.id,
            "consume.lua",
            self.consume_script_sha,
        )
        self.schedule_script_sha: str = str(
            self.redis.script_load(self._SCHEDULE_LUA_SCRIPT)
        )
        logger.debug(
            "Lua script cached: limiter=%s, script=%s, sha=%s.",
            self.id,
            "schedule.lua",
            self.schedule_script_sha,
        )
        self.health_script_sha: str = str(
            self.redis.script_load(self._HEALTH_LUA_SCRIPT)
        )
        logger.debug(
            "Lua script cached: limiter=%s, script=%s, sha=%s.",
            self.id,
            "health.lua",
            self.health_script_sha,
        )
        self.renew_script_sha: str = str(self.redis.script_load(self._RENEW_LUA_SCRIPT))
        logger.debug(
            "Lua script cached: limiter=%s, script=%s, sha=%s.",
            self.id,
            "renew.lua",
            self.renew_script_sha,
        )

    def _load_lua_script(self, lua_script: str, key: str) -> None:
        """Load a Lua script from disk into the specified instance attribute.

        Args:
            lua_script: The filename of the Lua script to be loaded.
            key: The instance attribute name under which the script source is stored.
        """
        if getattr(self, key, None) is None:
            errors: list[str] = []
            for resource_package in self.resource_packages:
                try:
                    source = resources.files(resource_package).joinpath(lua_script)
                    setattr(self, key, source.read_text(encoding="utf-8"))
                    logger.debug(
                        "Lua script loaded from disk: limiter=%s, script=%s, attr=%s, package=%s.",
                        self.id,
                        lua_script,
                        key,
                        resource_package,
                    )
                    return
                except (ModuleNotFoundError, OSError) as error:
                    errors.append(f"{resource_package}: {error}")

            raise ImportError(
                f"Could not load {lua_script}; attempted packages: {', '.join(errors)}"
            )

    @staticmethod
    def _get_task_signature_str(func_path: str, payload: dict) -> str:
        """Return the signature of a task as a deterministic JSON string."""
        return json.dumps({"path": func_path, "payload": payload}, sort_keys=True)

    def _get_task_data(self, task_id: str, func_path: str, payload: dict) -> dict:
        """Return the structured data dictionary for a task."""
        task_data = {
            "id": task_id,
            "func_path": func_path,
            "payload": payload,
            "inflight_key": self.get_inflight_key(task_id),
        }

        return task_data

    def _get_task_data_str(self, task_id: str, func_path: str, payload: dict) -> str:
        """Return the structured data of a task as a JSON string."""
        return json.dumps(
            self._get_task_data(task_id, func_path, payload), sort_keys=True
        )

    def _get_inflight_ttl(self, max_age_override: Optional[int] = None) -> int:
        """Return a conservative TTL for in-flight deduplication keys.

        The TTL must cover queue residence (``max_age``) plus sufficient time for dispatch and cleanup.
        """
        effective_max_age = (
            self.max_age if max_age_override is None else max_age_override
        )
        ttl_seconds = (
            max(1.0, float(effective_max_age))
            + max(1.0, float(self.lease_duration))
            + max(1.0, float(self.window))
        )
        return int(math.ceil(ttl_seconds))

    def _cleanup_inflight_key(self, inflight_key: str, task_id: str) -> None:
        """Perform a best-effort cleanup of an in-flight key following a scheduling failure."""
        try:
            removed = self.redis.delete(inflight_key)
            logger.debug(
                "Inflight cleanup attempted: limiter=%s, task_id=%s, inflight_key=%s, removed=%s.",
                self.id,
                task_id,
                inflight_key,
                removed,
            )
        except Exception as cleanup_error:
            logger.warning(
                "Failed to cleanup inflight key after schedule failure: limiter=%s, task_id=%s, inflight_key=%s, error=%s.",
                self.id,
                task_id,
                inflight_key,
                cleanup_error,
            )

    def get_inflight_key(self, task_id: str) -> str:
        """Return the in-flight key for the specified task.

        The in-flight key tracks a task from the moment it is scheduled
        through to completion, thereby preventing duplicate scheduling.

        Args:
            task_id: The unique identifier of the task.

        Returns:
            The Redis key used for tracking the in-flight status of the task.
        """
        return f"{self.id}:inflight:{task_id}"

    def schedule_task(
        self,
        func_path: str,
        payload: dict,
        priority: int = 100,
        max_age: Optional[int] = None,
        retry: bool = True,
    ) -> tuple[bool, str]:
        """Schedule a task for execution once the rate limit permits.

        Args:
            func_path: The dotted Python path of the function to be scheduled.
            payload: The payload dictionary for the task in question.
            priority: The priority of the task (default: 100).
            max_age: An optional override for the maximum age of the task, in seconds.
            retry: Whether to retry the scheduling operation upon a NoScriptError (e.g., Redis restart).

        Returns:
            A tuple of (``was_scheduled``, ``task_id``). The ``was_scheduled`` value is ``False``
            if the task was skipped because it is already in-flight.

        Raises:
            RuntimeError: If the required Lua scripts cannot be (re)loaded.
        """
        # Generate a unique identifier derived from the task name and payload.
        task_signature = self._get_task_signature_str(func_path, payload)
        task_id = hashlib.md5(task_signature.encode()).hexdigest()
        logger.debug(
            "Scheduling task attempt: limiter=%s, task_id=%s, func_path=%s, priority=%d, max_age=%s.",
            self.id,
            task_id,
            func_path,
            priority,
            max_age,
        )

        # Atomically claim the scheduling right using SET NX. Only one caller
        # may succeed; all subsequent callers observe the key and return early.
        inflight_key = self.get_inflight_key(task_id)
        inflight_ttl = self._get_inflight_ttl(max_age_override=max_age)
        if not self.redis.set(inflight_key, "1", ex=inflight_ttl, nx=True):
            logger.debug(
                "Task already in-flight, skipping schedule: limiter=%s, task_id=%s, inflight_key=%s, inflight_ttl_s=%d.",
                self.id,
                task_id,
                inflight_key,
                inflight_ttl,
            )
            self._emit_metric("schedule", {"scheduled": False, "task_id": task_id})
            return False, task_id

        # Serialize the task data with the assigned priority for priority queue behavior.
        full_data = self._get_task_data_str(task_id, func_path, payload)

        try:
            # Attempt to schedule the task via the Lua script.
            self.redis.evalsha(
                self.schedule_script_sha,
                1,
                # KEYS: [buffer]
                self.buffer_key,
                # ARGV: [task_json, priority, max age]
                full_data,
                priority,
                max_age or "",
            )
            logger.info(
                "Task scheduled: limiter=%s, task_id=%s, func_path=%s, priority=%d.",
                self.id,
                task_id,
                func_path,
                priority,
            )

        except redis.exceptions.NoScriptError:
            # The Redis script cache is volatile; hence, the SHA may become invalid unexpectedly.
            # Determine whether a retry should be performed; raise a runtime error if not.
            if not retry:
                # Clean up the in-flight key to avoid an orphaned lock.
                self._cleanup_inflight_key(inflight_key, task_id)
                raise RuntimeError(
                    "Redis failed to retain the Lua script after a reload attempt."
                )

            # Reload the script SHA and reattempt the operation.
            logger.warning(
                "Lua script cache miss during schedule; reloading script: limiter=%s, script=%s, task_id=%s.",
                self.id,
                "schedule.lua",
                task_id,
            )
            self.schedule_script_sha = str(
                self.redis.script_load(self._SCHEDULE_LUA_SCRIPT)
            )
            # Release the claim so that the retry can re-acquire it.
            self._cleanup_inflight_key(inflight_key, task_id)
            return self.schedule_task(
                func_path, payload, priority, max_age=max_age, retry=False
            )
        except Exception:
            # Any non-NOSCRIPT scheduling failure must release the claim so that retries
            # from callers are not blocked by a stale in-flight marker.
            self._cleanup_inflight_key(inflight_key, task_id)
            raise

        # Attempt to consume a task from the buffer.
        self.trigger_consume()
        self._emit_metric("schedule", {"scheduled": True, "task_id": task_id})
        return True, task_id

    def consume(self, retry: bool = True) -> ConsumeResult:
        """Attempt to consume a task from the queue.

        Args:
            retry: Whether to retry the consumption upon a NoScriptError.

        Returns:
            A result containing the task data if consumption was successful, or an empty result otherwise.

        Raises:
            RuntimeError: If the required Lua scripts cannot be (re)loaded.
        """
        logger.debug("Consume attempt started: limiter=%s.", self.id)
        try:
            # Execute the consume Lua script and obtain the result.
            result = cast(
                list[str],
                cast(
                    object,
                    self.redis.evalsha(
                        self.consume_script_sha,
                        4,
                        # KEYS: [base, buffer, concurrency, dlq]
                        self.id,
                        self.buffer_key,
                        self.concurrency_key,
                        self.dlq_key,
                        # ARGV: [window, limit, max_concurrency, max_age, lease_duration]
                        self.window,
                        self.limit,
                        self.max_concurrency,
                        self.max_age,
                        self.lease_duration,
                    ),
                ),
            )

            # Parse and structure the result.
            consume_result: ConsumeResult = {
                "success": int(result[0]) == 1,
                "expired": int(result[0]) == -1,
                "task": cast(TaskData, json.loads(result[1])) if result[1] else None,
                "remaining_tokens": int(result[2]),
                "active_concurrency": int(result[3]),
                "reset_in_ms": int(result[4]),
                "remaining_tasks": int(result[5]),
                "val_previous": int(result[6]),
                "val_current": int(result[7]),
            }
            task_id = consume_result["task"]["id"] if consume_result["task"] else None
            logger.debug(
                "Consume result: limiter=%s, success=%s, expired=%s, task_id=%s, remaining_tokens=%d, active_concurrency=%d, remaining_tasks=%d, reset_in_ms=%d.",
                self.id,
                consume_result["success"],
                consume_result["expired"],
                task_id,
                consume_result["remaining_tokens"],
                consume_result["active_concurrency"],
                consume_result["remaining_tasks"],
                consume_result["reset_in_ms"],
            )
            self._emit_metric(
                "consume",
                {
                    "success": consume_result["success"],
                    "expired": consume_result["expired"],
                    "remaining_tokens": consume_result["remaining_tokens"],
                    "active_concurrency": consume_result["active_concurrency"],
                    "reset_in_ms": consume_result["reset_in_ms"],
                    "remaining_tasks": consume_result["remaining_tasks"],
                },
            )
            return consume_result

        except redis.exceptions.NoScriptError:
            # The Redis script cache is volatile; hence, the SHA may become invalid unexpectedly.
            # Determine whether a retry should be performed; raise a runtime error if not.
            if not retry:
                raise RuntimeError(
                    "Redis failed to retain the Lua script after a reload attempt."
                )

            # Reload the script SHA and reattempt the operation.
            logger.warning(
                "Lua script cache miss during consume; reloading script: limiter=%s, script=%s.",
                self.id,
                "consume.lua",
            )
            self.consume_script_sha = str(
                self.redis.script_load(self._CONSUME_LUA_SCRIPT)
            )
            return self.consume(retry=False)

    def extend_lease(self, task_id: str, duration: int, retry: bool = True) -> None:
        """Extend the lease on a concurrency slot.

        A lease-based concurrency system is employed such that proper cleanup can be performed
        by other workers upon system failure. By extending the lease, the worker notifies the
        distributed system that it is still active; this in turn ensures that a concurrency slot
        may be repurposed if a worker becomes unresponsive and its lease expires.

        Args:
            task_id: The identifier of the task whose lease is to be extended.
            duration: The number of seconds by which to extend the lease. This value is decoupled
                from the actual lease duration such that it may be set to a fraction thereof,
                ensuring that the lease is always refreshed well before expiration.
            retry: An internal flag indicating whether the operation should be reattempted if a
                script error occurs.

        Raises:
            KeyError: If the task identifier is not present in the concurrency set.
            RuntimeError: If the renew Lua script cannot be reloaded after a NoScriptError.
        """
        try:
            # noinspection PyUnnecessaryCast
            # This cast is necessary for mypy type validation.
            renewed = int(
                cast(
                    str,
                    self.redis.evalsha(
                        self.renew_script_sha,
                        1,
                        # KEYS: [concurrency]
                        self.concurrency_key,
                        # ARGV: [task_id, duration]
                        task_id,
                        duration,
                    ),
                )
            )

            logger.debug(
                "Lease extension result: limiter=%s, task_id=%s, duration_s=%d, renewed=%s.",
                self.id,
                task_id,
                duration,
                renewed == 1,
            )
            if renewed != 1:
                raise KeyError(
                    f"Could not extend lease for task '{task_id}' on limiter '{self.id}': "
                    "task id was not found in the concurrency set."
                )
        except redis.exceptions.NoScriptError:
            if not retry:
                raise RuntimeError(
                    "Redis failed to retain the Lua script after a reload attempt."
                )

            # Reload the script SHA and reattempt the operation.
            logger.warning(
                "Lua script cache miss during lease extension; reloading script: limiter=%s, script=%s, task_id=%s.",
                self.id,
                "renew.lua",
                task_id,
            )
            self.renew_script_sha = str(self.redis.script_load(self._RENEW_LUA_SCRIPT))
            self.extend_lease(task_id, duration, retry=False)

    def _emit_metric(self, event: str, data: dict) -> None:
        """Safely invoke the metrics callback, if one has been configured.

        Any exception raised by the callback is caught and logged to prevent a
        user-provided callback failure from disrupting the limiter.

        Args:
            event: The event name (e.g., ``"consume"`` or ``"schedule"``).
            data: A dictionary containing event-specific data.
        """
        if self.metrics_callback is None:
            return

        try:
            self.metrics_callback(event, data)
        except Exception as e:
            logger.warning(
                "Metrics callback raised an exception: limiter=%s, event=%s, error=%s.",
                self.id,
                event,
                e,
            )

    def _calculate_token_recovery_delay(
        self,
        val_previous: int,
        val_current: int,
        reset_in_ms: int,
    ) -> float:
        """Calculate the duration until the next rate limit token becomes available.

        The sliding window estimate decays linearly as time passes::

            estimated = val_previous * weight + val_current

        where ``weight`` decreases from ``reset_in_ms / window_ms`` to 0.

        This method solves for the earliest time at which ``estimated < limit``,
        i.e., the point at which one token is freed via previous-window decay.

        The method falls back to ``reset_in_ms`` when decay cannot free a token
        within the current window (i.e., when ``val_previous == 0`` or
        ``val_current >= limit``).

        Args:
            val_previous: The raw counter value for the previous fixed window.
            val_current: The raw counter value for the current fixed window.
            reset_in_ms: The number of milliseconds until the current window expires.

        Returns:
            The delay in seconds until the next token is expected to become available.
        """
        window_ms = self.window * 1000

        # No previous window exists to decay, or the current window alone is at the limit.
        # In either case, wait for the next window.
        if val_previous <= 0 or val_current >= self.limit:
            return (reset_in_ms / 1000.0) + 0.001

        # Determine the earliest point at which previous-window decay frees a token.
        #   estimated = val_previous * (window_ms - t) / window_ms + val_current
        #   estimated < limit  =>  t > window_ms * (1 - (limit - val_current) / val_previous)
        time_passed_ms = window_ms - reset_in_ms
        t_needed_ms = window_ms * (1.0 - (self.limit - val_current) / val_previous)
        wait_ms = t_needed_ms - time_passed_ms

        if wait_ms <= 0:
            # Decay has already freed a token; a retry may be performed immediately.
            return 0.001

        return wait_ms / 1000.0

    def _calculate_smart_jitter(
        self,
        remaining_tasks: int,
        remaining_tokens: int,
        active_concurrency: int,
    ) -> float:
        """Calculate adaptive jitter to mitigate the thundering herd problem at window resets.

        Jitter prevents all workers from waking simultaneously when the rate limit resets.
        The strategy scales with both the window size and the system load:
            - Window proportional: a 1s window yields 20--80ms of jitter; a 60s window yields 1.2--4.8s.
            - Load adaptive: high contention produces a larger jitter spread; low contention produces a smaller spread.

        Args:
            remaining_tasks: The number of tasks waiting in the buffer.
            remaining_tokens: The number of rate limit tokens currently available.
            active_concurrency: The number of tasks currently executing.

        Returns:
            The jitter amount in seconds to be added to the base delay.
        """
        if not self.jitter_enabled:
            return 0.0

        # The base jitter range scales proportionally with the window size.
        min_jitter = self.window * self.jitter_min_pct
        max_jitter = self.window * self.jitter_max_pct

        # Compute the load pressure factor (0.0 = low contention, 1.0 = high contention).
        if remaining_tasks <= 0:
            load_pressure = 0.0
        elif remaining_tasks < 10:
            load_pressure = 0.2
        elif remaining_tasks < 50:
            load_pressure = 0.5
        elif remaining_tasks < 100:
            load_pressure = 0.7
        else:
            load_pressure = 1.0

        # Compute the concurrency pressure factor (0.0 = many free slots, 1.0 = at capacity).
        concurrency_pressure = active_concurrency / max(1, self.max_concurrency)

        # Combine the pressures, weighting queue load more heavily than concurrency.
        combined_pressure = (load_pressure * 0.7) + (concurrency_pressure * 0.3)

        # Scale the jitter range based on combined pressure.
        # High pressure results in a larger jitter range (greater worker spread).
        # Low pressure results in a smaller jitter range (faster processing, less spread).
        # The resulting scale factor ranges from 0.3 to 1.0.
        jitter_scale = 0.3 + (combined_pressure * 0.7)

        # Compute the final jitter value with randomization.
        jitter_range_size = (max_jitter - min_jitter) * jitter_scale
        jitter = min_jitter + (jitter_range_size * random.random())

        rounded_jitter = round(jitter, 3)
        logger.debug(
            "Smart jitter calculated: limiter=%s, remaining_tasks=%d, remaining_tokens=%d, active_concurrency=%d, load_pressure=%.3f, concurrency_pressure=%.3f, jitter_s=%.3f.",
            self.id,
            remaining_tasks,
            remaining_tokens,
            active_concurrency,
            load_pressure,
            concurrency_pressure,
            rounded_jitter,
        )
        return rounded_jitter

    def get_buffer_count(self) -> int:
        """Return the number of items currently in the buffer."""
        return int(str(self.redis.zcard(self.buffer_key)))

    # noinspection PyBroadException
    def drain(self) -> None:
        """Attempt to drain an item from the queue.

        This method wraps ``_drain_inner`` with exception handling such that a failure
        in any step (consume, dispatch, or schedule) does not permanently terminate the
        drain loop. Upon failure, a recovery drain is scheduled with exponential backoff
        (100ms, 200ms, 400ms, ... capped at ``window``).
        """
        # Check for dynamic configuration updates before draining.
        if hasattr(self, "refresh_config"):
            self.refresh_config()

        # Respect the window-change pause: skip draining until the pause expires,
        # but schedule a follow-up so that the drain loop resumes automatically.
        if hasattr(self, "_paused_until") and time.time() < self._paused_until:
            remaining = self._paused_until - time.time()
            logger.debug(
                "Drain deferred: limiter=%s is paused for %.3fs for window transition.",
                self.id,
                remaining,
            )
            self._schedule_drain(delay=remaining)
            return

        try:
            self._drain_inner()
            self._consecutive_drain_failures = 0
        except Exception:
            self._consecutive_drain_failures += 1
            delay = min(
                self.window,
                0.1 * (2 ** (self._consecutive_drain_failures - 1)),
            )
            logger.error(
                "Drain failed (attempt #%d), scheduling recovery in %.3fs: limiter=%s.",
                self._consecutive_drain_failures,
                delay,
                self.id,
                exc_info=True,
            )
            try:
                self._schedule_drain(delay=delay)
            except Exception:
                logger.critical(
                    "Recovery scheduling also failed: limiter=%s. "
                    "Drain loop will resume on next trigger_consume() or task completion.",
                    self.id,
                    exc_info=True,
                )

    def _drain_inner(self) -> None:
        """Execute the core drain logic: consume, dispatch, and schedule a follow-up.

        This method is separated from ``drain()`` so that the outer method can catch
        and recover from exceptions without duplicating the pause and configuration-refresh
        preamble.
        """
        logger.debug("Drain loop start: limiter=%s.", self.id)
        # Acquire the execution lock to avoid the thundering herd problem.
        with self.execution_lock() as acquired:
            logger.debug(
                "Drain lock acquisition result: limiter=%s, acquired=%s.",
                self.id,
                acquired,
            )
            if not acquired:
                # Another drainer is already executing an attempt. A backup drain
                # is scheduled so that the loop is not lost if the holder fails.
                logger.debug(
                    "Drain skipped because lock is held by another drainer: limiter=%s.",
                    self.id,
                )
                self._schedule_backup_drain()
                return

            # Attempt to consume a task from the buffer.
            result = self.consume()

            # Check whether the task has expired.
            if result["expired"]:
                logger.warning(
                    "Expired task moved to DLQ during consume: limiter=%s.",
                    self.id,
                )

            # Dispatch the task if the consumption was successful.
            if result["success"] and result["task"]:
                task = result["task"]
                task_id = task.get("id", "")

                # Dispatch the consumed task to the execution backend.
                self._dispatch_task(
                    func_path=task["func_path"],
                    payload=task["payload"],
                    task_id=task_id,
                )
                logger.info(
                    "Task dispatched: limiter=%s, task_id=%s, func_path=%s.",
                    self.id,
                    task_id,
                    task["func_path"],
                )

                # Schedule a follow-up drain only if there are items still waiting in the buffer.
                # This prevents the dispatcher from running indefinitely.
                if result["remaining_tasks"] > 0:
                    logger.debug(
                        "More tasks remain, scheduling immediate follow-up drain: limiter=%s, remaining_tasks=%d.",
                        self.id,
                        result["remaining_tasks"],
                    )
                    self._schedule_drain()

            elif result["remaining_tasks"] == 0:
                # Stop: no remaining tasks. The next drain will be triggered when a new task is added.
                logger.debug("Drain stopped: buffer empty for limiter=%s.", self.id)

            elif result["active_concurrency"] >= self.max_concurrency:
                # Stop: the next drain will be triggered upon worker completion.
                logger.debug(
                    "Drain stopped: concurrency at capacity for limiter=%s (active=%d, max=%d).",
                    self.id,
                    result["active_concurrency"],
                    self.max_concurrency,
                )

            elif result["remaining_tokens"] <= 0:
                # Calculate when the next token becomes available via sliding
                # window decay, rather than waiting for the full window reset.
                val_previous = result["val_previous"]
                val_current = result["val_current"]
                base_delay = self._calculate_token_recovery_delay(
                    val_previous=val_previous,
                    val_current=val_current,
                    reset_in_ms=result.get("reset_in_ms", 0),
                )

                # Jitter is only added on the fallback path (full window reset),
                # where multiple workers may wake simultaneously. On the
                # token-recovery path, drains are already naturally staggered
                # by the sliding window position; adding jitter would only
                # reduce throughput.
                is_fallback = val_previous <= 0 or val_current >= self.limit
                if is_fallback:
                    jitter = self._calculate_smart_jitter(
                        remaining_tasks=result["remaining_tasks"],
                        remaining_tokens=result["remaining_tokens"],
                        active_concurrency=result["active_concurrency"],
                    )
                else:
                    jitter = 0.0

                delay_seconds = round(max(0.001, base_delay + jitter), 3)
                logger.info(
                    "Rate limited, scheduling retry: limiter=%s, delay_s=%.3f, base_delay_s=%.3f, jitter_s=%.3f, remaining_tasks=%d, val_previous=%d, val_current=%d, fallback=%s.",
                    self.id,
                    delay_seconds,
                    base_delay,
                    jitter,
                    result["remaining_tasks"],
                    val_previous,
                    val_current,
                    is_fallback,
                )
                self._schedule_drain(delay=delay_seconds)

    def _schedule_backup_drain(self) -> None:
        """Schedule a safety-net drain after failing to acquire the dispatch lock.

        The delay is set to one token interval (``window / limit``), which is
        sufficiently long for the lock holder to finish yet short enough to maintain
        throughput. The ``DrainLoop`` naturally coalesces multiple backup requests.
        """
        token_interval = self.window / self.limit if self.limit > 0 else self.window
        logger.debug(
            "Backup drain scheduled: limiter=%s, delay_s=%.3f.",
            self.id,
            token_interval,
        )
        self._schedule_drain(delay=token_interval)

    @abstractmethod
    def _dispatch_task(self, func_path: str, payload: dict, task_id: str) -> None:
        """Dispatch the task to the concrete execution backend (e.g., Celery worker, thread).

        Args:
            func_path: The dotted Python path to the function to be executed.
            payload: The task payload dictionary.
            task_id: The unique task identifier.
        """
        pass  # pragma: no cover

    def _schedule_drain(self, delay: float = 0.0) -> None:
        """Schedule the drain method to execute again after ``delay`` seconds.

        The default implementation wakes the ``DrainLoop``. Subclasses used in
        testing may override this method to record calls without starting the loop.
        """
        self._drain_loop.wake(delay)

    def trigger_consume(self) -> None:
        """Trigger consumption from the task queue.

        The drain loop is woken to check for available work. The ``DrainLoop``
        naturally coalesces near-simultaneous triggers.
        """
        logger.debug("Trigger consume scheduling drain: limiter=%s.", self.id)
        self._schedule_drain()

    def shutdown(self) -> None:
        """Stop the drain loop to facilitate a clean shutdown."""
        self._drain_loop.shutdown()

    def execution_lock(self, timeout_ms: int = 5000) -> ContextManager[bool]:
        """Acquire the dispatch lock and perform cleanup after task completion.

        Args:
            timeout_ms: The lock timeout in milliseconds.

        Returns:
            A context manager that yields the lock acquisition status, indicating whether the task should proceed.
        """
        return DistributedLock(
            redis_client=self.redis, lock_key=self.lock_key, timeout_ms=timeout_ms
        )

    def task_lifecycle(
        self,
        task_id: str,
        on_heartbeat_failure_override: Optional[Literal["warn", "kill"]] = None,
    ) -> TaskLifecycle:
        """Create a context manager that ensures the concurrency slot is released.

        The concurrency slot is guaranteed to be released regardless of the outcome
        of task execution.

        Args:
            task_id: The identifier of the task to be managed.
            on_heartbeat_failure_override: An optional override for the heartbeat failure
                strategy to be passed to the ``TaskLifecycle`` constructor.

        Returns:
            A ``TaskLifecycle`` context manager instance.
        """
        strategy = on_heartbeat_failure_override or self.on_heartbeat_failure

        return TaskLifecycle(
            limiter=self, task_id=task_id, on_heartbeat_failure=strategy
        )

    def get_status(self, retry: bool = True) -> dict:
        """Return a snapshot of the current state of the limiter.

        Args:
            retry: Whether to retry the operation upon a script error.

        Returns:
            A dictionary containing all status information for the limiter.

        Raises:
            RuntimeError: If the required Lua scripts cannot be (re)loaded.
        """
        try:
            result = cast(
                list[str],
                cast(
                    object,
                    self.redis.evalsha(
                        self.health_script_sha,
                        3,
                        # KEYS: [base, buffer, concurrency]
                        self.id,
                        self.buffer_key,
                        self.concurrency_key,
                        # ARGV: [window, limit, max_concurrency]
                        self.window,
                        self.limit,
                        self.max_concurrency,
                    ),
                ),
            )

            # Map the result list to a structured dictionary.
            return {
                "limiter_id": self.id,
                "concurrency": {
                    "current": result[3],
                    "max": self.max_concurrency,
                    "available": max(0, self.max_concurrency - int(result[3])),
                },
                "buffer": {
                    "count": result[5],
                },
                "rate_limit": {
                    "val_previous": result[0],
                    "val_current": result[1],
                    "tokens_used": float(result[2]),  # Estimated count is a float
                    "limit": self.limit,
                    "window": self.window,
                    "reset_in_ms": result[4],
                },
                "dispatcher": {
                    "is_locked": self.redis.exists(f"{self.id}:dispatch_lock")
                },
            }
        except redis.exceptions.NoScriptError:
            # The Redis script cache is volatile; hence, the SHA may become invalid unexpectedly.
            # Determine whether a retry should be performed; raise a runtime error if not.
            if not retry:
                raise RuntimeError(
                    "Redis failed to retain the Lua script after a reload attempt."
                )

            # Reload the script SHA and reattempt the operation.
            logger.warning(
                "Lua script cache miss during status fetch; reloading script: limiter=%s, script=%s.",
                self.id,
                "health.lua",
            )
            self.health_script_sha = str(
                self.redis.script_load(self._HEALTH_LUA_SCRIPT)
            )
            return self.get_status(retry=False)


class AbstractRedisManagedRateLimiter(AbstractDistributedRateLimiter, ABC):
    """Shared class-level API for Redis-backed limiter implementations.

    This base class provides singleton-style instance management and Redis-backed
    configuration persistence, such that concrete implementations need only define
    the backend-specific context setup (e.g., Celery app, thread pool).
    """

    _REGISTRY_KEY: ClassVar[str] = "rl:registry:configs"
    _VERSION_KEY: ClassVar[str] = "rl:registry:versions"
    _SENTINEL: ClassVar[object] = object()
    _redis_client: ClassVar[Optional[Redis]] = None
    _instances: ClassVar[Dict[str, "AbstractRedisManagedRateLimiter"]] = {}

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Ensure that each subclass receives isolated class-level state."""
        super().__init_subclass__(**kwargs)
        cls._SENTINEL = object()
        cls._redis_client = None
        cls._instances = {}

    @classmethod
    @abstractmethod
    def _configure_backend(cls, **backend_context: Any) -> None:
        """Store the backend-specific class context during ``configure()``."""

    @classmethod
    @abstractmethod
    def _has_backend_context(cls) -> bool:
        """Determine whether the backend-specific class context has been configured."""

    @classmethod
    @abstractmethod
    def _get_instance_context(cls) -> dict[str, Any]:
        """Return the backend context to be forwarded to concrete instance constructors."""

    @classmethod
    @abstractmethod
    def _reset_backend_context(cls) -> None:
        """Clear the backend-specific class context, intended for testing and resets."""

    @classmethod
    @abstractmethod
    def _configure_hint(cls) -> str:
        """Return a human-readable ``configure()`` usage hint suitable for error messages."""

    @classmethod
    def configure(cls, redis_client: Redis, **backend_context: Any) -> None:
        """Configure the shared Redis client and backend context for class-level API usage."""
        cls._redis_client = redis_client
        cls._configure_backend(**backend_context)
        logger.info("%s configured.", cls.__name__)

    @classmethod
    def _require_internal_construction(cls, sentinel: Any) -> None:
        """Reject direct constructor invocations that bypass the managed class API."""
        if sentinel is not cls._SENTINEL:
            raise RuntimeError(
                f"Direct {cls.__name__}() construction is not supported. "
                f"Use {cls._configure_hint()} then {cls.__name__}.create() or {cls.__name__}.get()."
            )

    def __init__(
        self,
        redis_client: Redis,
        *args: Any,
        _sentinel: Any = None,
        **kwargs: Any,
    ) -> None:
        """Construct a managed limiter instance via internal class API flows."""
        self.__class__._require_internal_construction(_sentinel)
        super().__init__(redis_client, *args, **kwargs)

    @classmethod
    def create(
        cls,
        limiter_id: str,
        limit: int,
        window: float,
        max_concurrency: int,
        max_age: int = 3600,
        lease_duration: int = 30,
        override: bool = False,
        persist: bool = True,
        **kwargs: Any,
    ) -> "AbstractRedisManagedRateLimiter":
        """Create and cache a limiter instance, optionally persisting the configuration to Redis."""
        cls._require_configured()

        if not override and limiter_id in cls._instances:
            raise ValueError(
                f"Limiter '{limiter_id}' already exists. Use override=True to replace it."
            )

        assert cls._redis_client is not None  # Guaranteed by _require_configured.
        instance = cls(
            redis_client=cls._redis_client,
            limiter_id=limiter_id,
            limit=limit,
            window=window,
            max_concurrency=max_concurrency,
            max_age=max_age,
            lease_duration=lease_duration,
            _sentinel=cls._SENTINEL,
            **cls._get_instance_context(),
            **kwargs,
        )
        cls._instances[limiter_id] = instance

        if persist:
            cls._persist_config(instance)

        logger.info(
            "%s created: limiter_id=%s, window_s=%g, limit=%d, max_concurrency=%d, persist=%s.",
            cls.__name__,
            limiter_id,
            window,
            limit,
            max_concurrency,
            persist,
        )
        return instance

    @classmethod
    def get(cls, limiter_id: str) -> "AbstractRedisManagedRateLimiter":
        """Retrieve a limiter by its identifier from the local cache or the Redis registry."""
        if limiter_id in cls._instances:
            logger.debug(
                "%s resolved from local cache: limiter_id=%s.",
                cls.__name__,
                limiter_id,
            )
            return cls._instances[limiter_id]

        cls._require_configured()
        assert cls._redis_client is not None  # Guaranteed by _require_configured.

        raw_config = cls._redis_client.hget(cls._REGISTRY_KEY, limiter_id)
        if raw_config is None:
            raise ValueError(
                f"Limiter '{limiter_id}' not found in local cache or Redis. "
                f"Ensure it was created via {cls.__name__}.create()."
            )

        config = json.loads(
            raw_config.decode("utf-8")
            if isinstance(raw_config, bytes)
            else str(raw_config)
        )
        instance = cls(
            redis_client=cls._redis_client,
            limiter_id=limiter_id,
            _sentinel=cls._SENTINEL,
            **cls._get_instance_context(),
            **config,
        )

        raw_version = cls._redis_client.hget(cls._VERSION_KEY, limiter_id)
        if raw_version is not None:
            # noinspection PyUnnecessaryCast
            # This cast is necessary for mypy type validation.
            version_value = cast(str | bytes | int, raw_version)
            instance._config_version = int(
                version_value.decode("utf-8")
                if isinstance(version_value, bytes)
                else version_value
            )

        cls._instances[limiter_id] = instance
        logger.debug("%s hydrated from Redis: limiter_id=%s.", cls.__name__, limiter_id)
        return instance

    @classmethod
    def update(
        cls,
        limiter_id: str,
        limit: Optional[int] = None,
        window: Optional[float] = None,
        max_concurrency: Optional[int] = None,
        max_age: Optional[int] = None,
        lease_duration: Optional[int] = None,
    ) -> "AbstractRedisManagedRateLimiter":
        """Update the limiter configuration, persist it to Redis, and increment the version counter."""
        instance = cls.get(limiter_id)

        if window is not None and window != instance.window:
            pause_duration = max(instance.window, window)
            instance._paused_until = time.time() + pause_duration
            instance.window = window
            logger.info(
                "Window changed for limiter %s: new_window=%g, paused_for_s=%g.",
                limiter_id,
                window,
                pause_duration,
            )

        if limit is not None:
            instance.limit = limit
        if max_concurrency is not None:
            instance.max_concurrency = max_concurrency
        if max_age is not None:
            instance.max_age = max_age
        if lease_duration is not None:
            instance.lease_duration = lease_duration

        cls._persist_config(instance)

        logger.info(
            "%s updated: limiter_id=%s, limit=%d, window=%g, max_concurrency=%d.",
            cls.__name__,
            limiter_id,
            instance.limit,
            instance.window,
            instance.max_concurrency,
        )
        return instance

    @classmethod
    def _reset(cls) -> None:
        """Clear all class-level singleton state. This method is intended for use in tests."""
        cls._instances.clear()
        cls._redis_client = None
        cls._reset_backend_context()

    @classmethod
    def _require_configured(cls) -> None:
        """Raise an error if ``configure()`` has not been called with the required context."""
        if cls._redis_client is None or not cls._has_backend_context():
            raise RuntimeError(
                f"{cls._configure_hint()} must be called before create() or get()."
            )

    @classmethod
    def _persist_config(cls, instance: "AbstractRedisManagedRateLimiter") -> None:
        """Write the limiter configuration to Redis and increment its version counter."""
        assert cls._redis_client is not None
        config = {
            "limit": instance.limit,
            "window": instance.window,
            "max_concurrency": instance.max_concurrency,
            "max_age": instance.max_age,
            "lease_duration": instance.lease_duration,
        }
        cls._redis_client.hset(cls._REGISTRY_KEY, instance.id, json.dumps(config))
        cls._redis_client.hincrby(cls._VERSION_KEY, instance.id, 1)

        raw_version = cls._redis_client.hget(cls._VERSION_KEY, instance.id)
        if raw_version is not None:
            # noinspection PyUnnecessaryCast
            # This cast is necessary for mypy type validation.
            version_value = cast(str | bytes | int, raw_version)
            instance._config_version = int(
                version_value.decode("utf-8")
                if isinstance(version_value, bytes)
                else version_value
            )

    def refresh_config(self) -> bool:
        """Apply a newer persisted configuration from Redis when a version change is detected."""
        raw_version = self.redis.hget(self.__class__._VERSION_KEY, self.id)
        if raw_version is None:
            return False

        # noinspection PyUnnecessaryCast
        # This cast is necessary for mypy type validation.
        version_value = cast(str | bytes | int, raw_version)
        remote_version = int(
            version_value.decode("utf-8")
            if isinstance(version_value, bytes)
            else version_value
        )
        if remote_version <= self._config_version:
            return False

        raw_config = self.redis.hget(self.__class__._REGISTRY_KEY, self.id)
        if raw_config is None:
            return False

        try:
            config = json.loads(
                raw_config.decode("utf-8")
                if isinstance(raw_config, bytes)
                else str(raw_config)
            )
        except (json.JSONDecodeError, TypeError) as error:
            logger.warning(
                "Config refresh skipped due to malformed persisted config: limiter=%s, error=%s.",
                self.id,
                error,
            )
            return False

        new_window = config.get("window", self.window)
        if new_window != self.window:
            pause_duration = max(self.window, new_window)
            self._paused_until = time.time() + pause_duration
            self.window = new_window
            logger.info(
                "Window change detected via refresh for limiter %s: new_window=%g, paused_for_s=%g.",
                self.id,
                new_window,
                pause_duration,
            )

        self.limit = config.get("limit", self.limit)
        self.max_concurrency = config.get("max_concurrency", self.max_concurrency)
        self.max_age = config.get("max_age", self.max_age)
        self.lease_duration = config.get("lease_duration", self.lease_duration)
        self._config_version = remote_version
        logger.info(
            "Config refreshed for limiter %s: version=%d, limit=%d, window=%g, max_concurrency=%d.",
            self.id,
            remote_version,
            self.limit,
            self.window,
            self.max_concurrency,
        )
        return True
