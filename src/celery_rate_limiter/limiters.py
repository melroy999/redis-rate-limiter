from __future__ import annotations

import hashlib
import json
import logging
import os
import random
import signal
import time
import uuid
import warnings
from abc import ABC, abstractmethod
from importlib import resources
from threading import Event, Thread
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
from celery import Celery
from redis import Redis

logger = logging.getLogger(__name__)


class TaskData(TypedDict):
    """A class that holds the task data format."""

    id: str # The id of the task.
    func_path: str # Python path to the function to execute.
    payload: dict # The parameters to pass on to the function.


class ConsumeResult(TypedDict):
    """A class to hold the result of a consume.lua call."""

    success: bool  # Whether a task was actually consumed.
    expired: bool  # Whether a task has expired.
    task: Optional[TaskData]  # The raw JSON string from Redis.
    remaining_tokens: int  # Rate limit telemetry.
    active_concurrency: int  # Concurrency telemetry.
    reset_in_ms: int  # Time until window shift.
    remaining_tasks: int  # The number of tasks that remain to be processed.


class DistributedLock:
    """A dedicated class for the execution lock (instead of a @contextmanager) to keep the IDE happy.

    Requests a dispatch lock and perform cleanup after task completion.
    """

    def __init__(self, redis_client: Redis, lock_key: str, timeout_ms: int):
        """Initialize the lock manager.

        Args:
            redis_client: The client to use to connect to the redis server.
            lock_key: The name of the key the lock is stored under.
            timeout_ms: The timeout for the lock in milliseconds.
        """
        self.redis = redis_client
        self.lock_key = lock_key
        self.timeout_ms = timeout_ms
        self.token = str(uuid.uuid4())
        self.acquired = False

    def __enter__(self) -> bool:
        """Enter the context manager.

        Returns:
            The status of the lock such that the task knows if it should proceed.
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
        """Leave the context manager."""
        # This runs even if the task crashes.
        if self.acquired:
            # Only delete if locks match.
            # This prevents deleting locks created after a timeout.
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
    """Context manager that handles concurrency slot cleanup."""

    def __init__(
        self,
        limiter: AbstractDistributedRateLimiter,
        task_id: str,
        on_heartbeat_failure: Literal["warn", "kill"] = "warn",
    ):
        """Create a lifecycle context manager that cleans up concurrency slots.

        Args:
            limiter: The limiter to observe.
            task_id: The id of the task to clear the active state for.
            on_heartbeat_failure: How to handle heartbeat failures.
        """
        self.limiter = limiter
        self.task_id = task_id
        self.interval = self.limiter.lease_duration / 2

        # Threading controls.
        self._stop_event: Event = Event()
        self._thread: Optional[Thread] = None

        # Health controls.
        self.on_failure_action = on_heartbeat_failure
        self.is_healthy = True

    def _heartbeat_loop(self) -> None:
        """Background task that renews the lease over a concurrency slot."""
        while not self._stop_event.wait(timeout=self.interval):
            try:
                # Extend the lease.
                self.limiter.extend_lease(self.task_id, self.limiter.lease_duration)

                # Indicate that the worker has restored its proper functioning.
                if not self.is_healthy:
                    logger.info(
                        "Heartbeat connection restored for task %s on limiter %s.",
                        self.task_id,
                        self.limiter.id,
                    )
                    self.is_healthy = True
            except Exception as e:
                # Mark as unhealthy to signal to the worker job that something is wrong.
                self.is_healthy = False

                if self.on_failure_action == "kill":
                    # Log that the worker is now dead and break the loop so the child-thread stops as well.
                    logger.critical(
                        "Heartbeat failed for task %s: %s - terminating worker (suicide pact).",
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
        """Enter the context manager."""
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
        """Leave the context manager."""
        # Stop the heartbeat.
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=1.0)

        # Perform cleanup.
        try:
            # Always decrement the counter when leaving the 'with' block.
            removed_concurrency = self.limiter.redis.zrem(
                self.limiter.concurrency_key, self.task_id
            )

            # Clear the active lock of the task.
            active_removed = 0
            if self.task_id:
                active_key = self.limiter.get_active_key(self.task_id)
                active_removed = self.limiter.redis.delete(active_key)

            logger.debug(
                "Concurrency slot released and active key cleared: limiter=%s, task_id=%s, removed_concurrency=%s, removed_active=%s.",
                self.limiter.id,
                self.task_id,
                removed_concurrency,
                active_removed,
            )
        finally:
            logger.debug(
                "Task lifecycle exited, triggering follow-up consume: limiter=%s, task_id=%s.",
                self.limiter.id,
                self.task_id,
            )
            # Re-trigger the dispatcher to fill the empty slot.
            self.limiter.trigger_consume()


class AbstractDistributedRateLimiter(ABC):
    """A class that rate limits celery tasks.

    Rate limiting relies on atomic Lua scripts executed on a single Redis instance.
    All rate limit state (window counters, buffer, concurrency set) must reside on the
    same Redis node to guarantee correctness.

    Redis configuration requirements:
        - Single Redis instance, or a master-only setup where all reads and writes go
          to the same node. Read replicas introduce replication lag that can cause the
          rate limit to be exceeded, because a replica may serve stale window counters.
        - Redis Cluster is not supported. The limiter uses multiple keys (window counters,
          buffer, concurrency set, dispatch lock) that must be co-located on the same
          shard. Key hash tags are not applied, so Redis Cluster may distribute them
          across different nodes and break atomicity.
    """

    _CONSUME_LUA_SCRIPT: str
    _SCHEDULE_LUA_SCRIPT: str
    _HEALTH_LUA_SCRIPT: str
    _RENEW_LUA_SCRIPT: str

    # Get the location of the lua package.
    resource_package = "src.celery_rate_limiter.lua"

    def __init__(
        self,
        redis_client: Redis,
        limiter_id: str,
        limit: int,
        window: int,
        max_concurrency: int,
        max_age: int = 3600,
        lease_duration: int = 30,
        on_heartbeat_failure: Literal["warn", "kill"] = "warn",
        jitter_enabled: bool = True,
        jitter_min_pct: float = 0.02,
        jitter_max_pct: float = 0.08,
        metrics_callback: Optional[Callable[[str, dict], None]] = None,
    ):
        """Create an abstract rate limiter instance with the given parameters and import the appropriate lua scripts.

        Args:
            redis_client: The redis client to use.
            limiter_id: The id of the rate limiter to create.
            limit: The maximum number of tasks per time window.
            window: The time window in seconds that the limit is applied to.
            max_concurrency: The maximum number of concurrent tasks.
            max_age: The maximum time a task may exist in the queue before it expires.
            lease_duration: The time in seconds after which the lease to a concurrency slot will expire.
            on_heartbeat_failure: How to handle heartbeat failures. 'warn' lets the job proceed,
                whereas 'kill' makes the worker forcefully exit its execution, effectively killing it.
            jitter_enabled: Whether to add randomized jitter to retry delays to reduce thundering herd.
                Recommended: True (default).
            jitter_min_pct: Minimum jitter as percentage of window size (default: 2% = 20ms for 1s window).
                Lower bound ensures some spread even under low load.
            jitter_max_pct: Maximum jitter as percentage of window size (default: 8% = 80ms for 1s window).
                Upper bound prevents excessive delays under high load.
            metrics_callback: Optional callback invoked after consume and schedule operations.
                Receives an event name string ("consume" or "schedule") and a dict with event data.
                Exceptions raised by the callback are caught and logged to avoid breaking the limiter.
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
        logger.info(
            "Rate limiter initialized: id=%s, limit=%d, window_s=%d, max_concurrency=%d, max_age_s=%d, lease_duration_s=%d, heartbeat_failure=%s, jitter_enabled=%s, jitter_min_pct=%.3f, jitter_max_pct=%.3f, metrics_callback=%s.",
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

        # Import the scripts.
        self._load_lua_script("consume.lua", "_CONSUME_LUA_SCRIPT")
        self._load_lua_script("schedule.lua", "_SCHEDULE_LUA_SCRIPT")
        self._load_lua_script("health.lua", "_HEALTH_LUA_SCRIPT")
        self._load_lua_script("renew.lua", "_RENEW_LUA_SCRIPT")

        # Optimize performance by caching the scripts on the server.
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
        """Load a lua script from disk.

        Args:
            lua_script: The name of the script to load.
            key: The attribute key to store the script under.
        """
        if getattr(self, key, None) is None:
            try:
                source = resources.files(self.resource_package).joinpath(lua_script)
                setattr(self, key, source.read_text(encoding="utf-8"))
                logger.debug(
                    "Lua script loaded from disk: limiter=%s, script=%s, attr=%s.",
                    self.id,
                    lua_script,
                    key,
                )
            except Exception as e:
                raise ImportError(
                    f"Could not load {lua_script} from {self.resource_package}: {e}"
                )

    @staticmethod
    def _get_task_signature_str(func_path: str, payload: dict) -> str:
        """Get the signature of a task as a JSON string."""
        return json.dumps({"path": func_path, "payload": payload}, sort_keys=True)

    @staticmethod
    def _get_task_data(task_id: str, func_path: str, payload: dict) -> dict:
        """Get the data of a task."""
        task_data = {"id": task_id, "func_path": func_path, "payload": payload}

        return task_data

    def _get_task_data_str(self, task_id: str, func_path: str, payload: dict) -> str:
        """Get the data of a task as a JSON string."""
        return json.dumps(
            self._get_task_data(task_id, func_path, payload), sort_keys=True
        )

    def get_active_key(self, task_id: str) -> str:
        """Get the active key for the given task.

        Args:
            task_id: The id of the task.

        Returns:
            The Redis key for tracking active status of this task.
        """
        return f"{self.id}:active:{task_id}"

    def schedule_task(
        self,
        func_path: str,
        payload: dict,
        priority: int = 100,
        max_age: Optional[int] = None,
        retry: bool = True,
    ) -> tuple[bool, str]:
        """Schedule a task to run once rate limiting allows for it.

        Args:
            func_path: The name of the function to schedule.
            payload: The payload for the task in question.
            priority: The priority of the task (100 default).
            max_age: An optional override for the maximum age of the task in seconds.
            retry: Whether to retry the scheduling on no script error (Redis outage).

        Returns:
            A tuple of (was_scheduled, task_id). was_scheduled is False if the task was skipped
            because it's already in-flight.

        Raises:
            RuntimeError: If the necessary lua scripts cannot be (re)loaded.
        """
        # Generate a unique id for the task name and payload.
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

        # Track active tasks--skip if it is already active.
        active_key = self.get_active_key(task_id)
        if self.redis.exists(active_key):
            logger.debug(
                "Task already in-flight, skipping schedule: limiter=%s, task_id=%s, active_key=%s.",
                self.id,
                task_id,
                active_key,
            )
            self._emit_metric("schedule", {"scheduled": False, "task_id": task_id})
            return False, task_id

        # Add a priority for priority queue behavior.
        full_data = self._get_task_data_str(task_id, func_path, payload)

        try:
            # Attempt to schedule.
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

            # Mark as active only after scheduling.
            self.redis.set(active_key, "1", ex=3600)
            logger.info(
                "Task scheduled: limiter=%s, task_id=%s, func_path=%s, priority=%d.",
                self.id,
                task_id,
                func_path,
                priority,
            )

        except redis.exceptions.NoScriptError:
            # Redis cache is volatile, and hence, the sha may become invalid unexpectedly.
            # Check if we should retry or not; throw a runtime error if not.
            if not retry:
                raise RuntimeError(
                    "Redis failed to retain the Lua script after a reload attempt."
                )

            # Fetch the script sha again and reattempt.
            logger.warning(
                "Lua script cache miss during schedule; reloading script: limiter=%s, script=%s, task_id=%s.",
                self.id,
                "schedule.lua",
                task_id,
            )
            self.schedule_script_sha = str(
                self.redis.script_load(self._SCHEDULE_LUA_SCRIPT)
            )
            return self.schedule_task(
                func_path, payload, priority, max_age=max_age, retry=False
            )

        # Attempt a consume.
        self.trigger_consume()
        self._emit_metric("schedule", {"scheduled": True, "task_id": task_id})
        return True, task_id

    def consume(self, retry: bool = True) -> ConsumeResult:
        """Attempt to consume a task from the queue.

        Args:
            retry: Whether to retry the consumption on no script error.

        Returns:
            A payload with a task if successfully consumed. Empty payload otherwise.

        Raises:
            RuntimeError: If the necessary lua scripts cannot be (re)loaded.
        """
        logger.debug("Consume attempt started: limiter=%s.", self.id)
        try:
            # Fetch the result.
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

            # Attempt to parse the result.
            consume_result: ConsumeResult = {
                "success": int(result[0]) == 1,
                "expired": int(result[0]) == -1,
                "task": cast(TaskData, json.loads(result[1])) if result[1] else None,
                "remaining_tokens": int(result[2]),
                "active_concurrency": int(result[3]),
                "reset_in_ms": int(result[4]),
                "remaining_tasks": int(result[5]),
            }
            logger.debug(
                "Consume result: limiter=%s, success=%s, expired=%s, task_id=%s, remaining_tokens=%d, active_concurrency=%d, remaining_tasks=%d, reset_in_ms=%d.",
                self.id,
                consume_result["success"],
                consume_result["expired"],
                (consume_result["task"] or {}).get("id"),
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
            # Redis cache is volatile, and hence, the sha may become invalid unexpectedly.
            # Check if we should retry or not; throw a runtime error if not.
            if not retry:
                raise RuntimeError(
                    "Redis failed to retain the Lua script after a reload attempt."
                )

            # Fetch the script sha again and reattempt.
            logger.warning(
                "Lua script cache miss during consume; reloading script: limiter=%s, script=%s.",
                self.id,
                "consume.lua",
            )
            self.consume_script_sha = str(
                self.redis.script_load(self._CONSUME_LUA_SCRIPT)
            )
            return self.consume(retry=False)

    def extend_lease(self, task_id: str, duration: int, retry: bool = True) -> bool:
        """Extend the lease on a concurrency slot.

        A lease-based concurrency system is used such that proper cleanup can be performed by other
        workers on system failure. By extending the lease, the worker notifies the distributed system
        it is still alive; this in turn ensures that a concurrency slot can be repurposed if a worker
        falls quiet and expires.

        Args:
            task_id: The id of the task to extend the lease of.
            duration: The number of seconds to extend the lease by. This is decoupled such that it
                can be a fraction of the actual lease duration, such that it is always refreshed
                well before expiration.
            retry: Internal flag to perform the operation again if a script error occurs.

        Returns:
            The result of the lua renew script.
        """
        try:
            renewed = cast(
                bool,
                cast(
                    object,
                    self.redis.evalsha(
                        self.renew_script_sha,
                        1,
                        # KEYS: [concurrency]
                        self.concurrency_key,
                        # ARGV: [task_id, duration]
                        task_id,
                        duration,
                    ),
                ),
            )
            logger.debug(
                "Lease extension result: limiter=%s, task_id=%s, duration_s=%d, renewed=%s.",
                self.id,
                task_id,
                duration,
                renewed,
            )
            return renewed
        except redis.exceptions.NoScriptError:
            if not retry:
                raise RuntimeError(
                    "Redis failed to retain the Lua script after a reload attempt."
                )

            # Fetch the script sha again and reattempt.
            logger.warning(
                "Lua script cache miss during lease extension; reloading script: limiter=%s, script=%s, task_id=%s.",
                self.id,
                "renew.lua",
                task_id,
            )
            self.renew_script_sha = str(self.redis.script_load(self._RENEW_LUA_SCRIPT))
            return self.extend_lease(task_id, duration, retry=False)

    def _emit_metric(self, event: str, data: dict) -> None:
        """Safely invoke the metrics callback if one is configured.

        Catches and logs any exception raised by the callback to avoid breaking
        the limiter if a user-provided callback fails.

        Args:
            event: The event name (e.g. "consume", "schedule").
            data: A dict with event-specific data.
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

    def _calculate_smart_jitter(
        self,
        remaining_tasks: int,
        remaining_tokens: int,
        active_concurrency: int,
    ) -> float:
        """Calculate adaptive jitter to reduce thundering herd at window resets.

        Jitter prevents all workers from waking simultaneously when rate limit resets.
        Strategy scales with both window size and system load:
            - Window proportional: 1s window = 20-80ms jitter, 60s window = 1.2-4.8s jitter.
            - Load adaptive: High contention = larger jitter spread, low contention = smaller spread.

        Args:
            remaining_tasks: Number of tasks waiting in buffer.
            remaining_tokens: Number of rate limit tokens available.
            active_concurrency: Number of currently active tasks.

        Returns:
            Jitter amount in seconds to add to base delay.
        """
        if not self.jitter_enabled:
            return 0.0

        # Base jitter range scales with window size.
        min_jitter = self.window * self.jitter_min_pct
        max_jitter = self.window * self.jitter_max_pct

        # Calculate load pressure (0.0 = low contention, 1.0 = high contention).
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

        # Calculate concurrency pressure (0.0 = many free slots, 1.0 = at capacity).
        concurrency_pressure = active_concurrency / max(1, self.max_concurrency)

        # Combine pressures: weight queue load more heavily than concurrency.
        combined_pressure = (load_pressure * 0.7) + (concurrency_pressure * 0.3)

        # Scale jitter range based on pressure.
        # High pressure = use more of jitter range (spread workers out more).
        # Low pressure = use less of jitter range (process faster, less spread).
        # Range: 0.3 to 1.0.
        jitter_scale = 0.3 + (combined_pressure * 0.7)

        # Calculate final jitter with randomization.
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
        """Get the number of items in the buffer."""
        return int(str(self.redis.zcard(self.buffer_key)))

    def drain(self) -> None:
        """Attempt to drain an item from the queue."""
        # Check for dynamic config updates before draining.
        if hasattr(self, "refresh_config"):
            self.refresh_config()

        # Respect window-change pause: skip draining until the pause expires,
        # but schedule a follow-up so the drain loop resumes automatically.
        if hasattr(self, "_paused_until") and time.time() < self._paused_until:
            remaining = self._paused_until - time.time()
            logger.debug(
                "Drain deferred: limiter=%s is paused for %.3fs for window transition.",
                self.id,
                remaining,
            )
            self._schedule_drain(delay=remaining)
            return

        logger.debug("Drain loop start: limiter=%s.", self.id)
        # Lock the execution to avoid the thundering herd problem.
        with self.execution_lock() as acquired:
            logger.debug(
                "Drain lock acquisition result: limiter=%s, acquired=%s.",
                self.id,
                acquired,
            )
            if not acquired:
                # Someone is already executing an attempt; hence skip.
                logger.debug(
                    "Drain skipped because lock is held by another drainer: limiter=%s.",
                    self.id,
                )
                return

            # Perform a consume.
            result = self.consume()

            # Check if the task has expired.
            if result["expired"]:
                logger.warning(
                    "Expired task moved to DLQ during consume: limiter=%s.",
                    self.id,
                )

            # Execute the task if the green light is given.
            if result["success"] and result["task"]:
                task = result["task"]
                task_id = task.get("id", "")

                # Send to the generic worker.
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

                # ONLY pulse if there are still items waiting in the buffer.
                # This prevents the dispatcher from running forever.
                if result["remaining_tasks"] > 0:
                    logger.debug(
                        "More tasks remain, scheduling immediate follow-up drain: limiter=%s, remaining_tasks=%d.",
                        self.id,
                        result["remaining_tasks"],
                    )
                    self._schedule_drain()

            elif result["remaining_tasks"] == 0:
                # Stop: No remaining tasks. Next drain will be triggered by a new task being added.
                logger.debug("Drain stopped: buffer empty for limiter=%s.", self.id)

            elif result["active_concurrency"] >= self.max_concurrency:
                # Stop: The next drain will be triggered by worker completion.
                logger.debug(
                    "Drain stopped: concurrency at capacity for limiter=%s (active=%d, max=%d).",
                    self.id,
                    result["active_concurrency"],
                    self.max_concurrency,
                )

            elif result["remaining_tokens"] <= 0:
                # Wait for the rate window to reset.
                ms_to_reset = result.get("reset_in_ms", 0)
                base_delay = (ms_to_reset / 1000.0) + 0.001

                # Add smart jitter to prevent thundering herd when window resets.
                jitter = self._calculate_smart_jitter(
                    remaining_tasks=result["remaining_tasks"],
                    remaining_tokens=result["remaining_tokens"],
                    active_concurrency=result["active_concurrency"],
                )

                delay_seconds = round(max(0.001, base_delay + jitter), 3)
                logger.info(
                    "Rate limited, scheduling retry: limiter=%s, delay_s=%.3f, reset_in_ms=%d, jitter_s=%.3f, remaining_tasks=%d.",
                    self.id,
                    delay_seconds,
                    ms_to_reset,
                    jitter,
                    result["remaining_tasks"],
                )
                self._schedule_drain(delay=delay_seconds)

    @abstractmethod
    def _dispatch_task(self, func_path: str, payload: dict, task_id: str) -> None:
        """Send the task to the actual worker (Celery worker, Thread, etc.).

        Args:
            func_path: The path to the function to execute.
            payload: The task payload.
            task_id: The unique task identifier.
        """
        pass  # pragma: no cover

    @abstractmethod
    def _schedule_drain(self, delay: float = 0.0) -> None:
        """Schedule the drain method to run again after delay seconds.

        Args:
            delay: The amount of time to sleep before scheduling.
        """
        pass  # pragma: no cover

    def trigger_consume(self) -> None:
        """Trigger the consumption of the task queue."""
        if self.redis.exists(self.lock_key):
            logger.debug(
                "Trigger consume skipped because dispatch lock is currently held: limiter=%s, lock_key=%s.",
                self.id,
                self.lock_key,
            )
            return

        logger.debug("Trigger consume scheduling drain: limiter=%s.", self.id)
        self._schedule_drain()

    def execution_lock(self, timeout_ms: int = 5000) -> ContextManager[bool]:
        """Request the dispatch lock and perform cleanup after task completion.

        Args:
            timeout_ms: The timeout in milliseconds.

        Returns:
            A context manager that yields the lock status indicating if the task should proceed.
        """
        return DistributedLock(
            redis_client=self.redis, lock_key=self.lock_key, timeout_ms=timeout_ms
        )

    def task_lifecycle(
        self,
        task_id: str,
        on_heartbeat_failure_override: Optional[Literal["warn", "kill"]] = None,
    ) -> TaskLifecycle:
        """Create a context manager to ensure the concurrency slot is released.

        Ensures the concurrency slot is released no matter what happens during task execution.

        Args:
            task_id: The id of the task to lifecycle.
            on_heartbeat_failure_override: An optional override to pass to the task lifecycle function.

        Returns:
            A TaskLifecycle context manager.
        """
        strategy = on_heartbeat_failure_override or self.on_heartbeat_failure

        return TaskLifecycle(
            limiter=self, task_id=task_id, on_heartbeat_failure=strategy
        )

    def get_status(self, retry: bool = True) -> dict:
        """Get a snapshot of the current state of the limiter.

        Args:
            retry: Whether to retry on script error.

        Returns:
            A JSON formatted result containing all status information.

        Raises:
            RuntimeError: If the necessary lua scripts cannot be (re)loaded.
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

            # Map the list to our dictionary.
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
            # Redis cache is volatile, and hence, the sha may become invalid unexpectedly.
            # Check if we should retry or not; throw a runtime error if not.
            if not retry:
                raise RuntimeError(
                    "Redis failed to retain the Lua script after a reload attempt."
                )

            # Fetch the script sha again and reattempt.
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
    configuration persistence, so concrete implementations only need to define
    backend-specific context setup (e.g. Celery app, thread pool, etc.).
    """

    _REGISTRY_KEY: ClassVar[str] = "rl:registry:configs"
    _VERSION_KEY: ClassVar[str] = "rl:registry:versions"
    _SENTINEL: ClassVar[object] = object()
    _redis_client: ClassVar[Optional[Redis]] = None
    _instances: ClassVar[Dict[str, "AbstractRedisManagedRateLimiter"]] = {}

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Ensure each subclass gets isolated class-level state."""
        super().__init_subclass__(**kwargs)
        cls._SENTINEL = object()
        cls._redis_client = None
        cls._instances = {}

    @classmethod
    @abstractmethod
    def _configure_backend(cls, **backend_context: Any) -> None:
        """Store backend-specific class context during configure()."""

    @classmethod
    @abstractmethod
    def _has_backend_context(cls) -> bool:
        """Check if backend-specific class context has been configured."""

    @classmethod
    @abstractmethod
    def _get_instance_context(cls) -> dict[str, Any]:
        """Return backend context forwarded to concrete instance constructors."""

    @classmethod
    @abstractmethod
    def _reset_backend_context(cls) -> None:
        """Clear backend-specific class context for tests/reset."""

    @classmethod
    @abstractmethod
    def _configure_hint(cls) -> str:
        """Return a human-readable configure() usage hint for error messages."""

    @classmethod
    def configure(cls, redis_client: Redis, **backend_context: Any) -> None:
        """Configure shared Redis client and backend context for class API usage."""
        cls._redis_client = redis_client
        cls._configure_backend(**backend_context)
        logger.info("%s configured.", cls.__name__)

    @classmethod
    def create(
        cls,
        limiter_id: str,
        limit: int,
        window: int,
        max_concurrency: int,
        max_age: int = 3600,
        lease_duration: int = 30,
        override: bool = False,
        persist: bool = True,
        **kwargs: Any,
    ) -> "AbstractRedisManagedRateLimiter":
        """Create and cache a limiter instance, optionally persisting config."""
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
            **cls._get_instance_context(),
            **kwargs,
        )
        cls._instances[limiter_id] = instance

        if persist:
            cls._persist_config(instance)

        logger.info(
            "%s created: limiter_id=%s, window_s=%d, limit=%d, max_concurrency=%d, persist=%s.",
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
        """Retrieve a limiter by id from local cache or Redis registry."""
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
            raw_config.decode("utf-8") if isinstance(raw_config, bytes) else str(raw_config)
        )
        instance = cls(
            redis_client=cls._redis_client,
            limiter_id=limiter_id,
            **cls._get_instance_context(),
            **config,
        )

        raw_version = cls._redis_client.hget(cls._VERSION_KEY, limiter_id)
        if raw_version is not None:
            instance._config_version = int(raw_version)

        cls._instances[limiter_id] = instance
        logger.debug("%s hydrated from Redis: limiter_id=%s.", cls.__name__, limiter_id)
        return instance

    @classmethod
    def update(
        cls,
        limiter_id: str,
        limit: Optional[int] = None,
        window: Optional[int] = None,
        max_concurrency: Optional[int] = None,
        max_age: Optional[int] = None,
        lease_duration: Optional[int] = None,
    ) -> "AbstractRedisManagedRateLimiter":
        """Update limiter config, persist it to Redis, and bump version."""
        instance = cls.get(limiter_id)

        if window is not None and window != instance.window:
            pause_duration = max(instance.window, window)
            instance._paused_until = time.time() + pause_duration
            instance.window = window
            logger.info(
                "Window changed for limiter %s: new_window=%d, paused_for_s=%d.",
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
            "%s updated: limiter_id=%s, limit=%d, window=%d, max_concurrency=%d.",
            cls.__name__,
            limiter_id,
            instance.limit,
            instance.window,
            instance.max_concurrency,
        )
        return instance

    @classmethod
    def _reset(cls) -> None:
        """Clear class-level singleton state. Intended for tests."""
        cls._instances.clear()
        cls._redis_client = None
        cls._reset_backend_context()

    @classmethod
    def _require_configured(cls) -> None:
        """Raise if configure() has not been called with required context."""
        if cls._redis_client is None or not cls._has_backend_context():
            raise RuntimeError(
                f"{cls._configure_hint()} must be called before create() or get()."
            )

    @classmethod
    def _persist_config(cls, instance: "AbstractRedisManagedRateLimiter") -> None:
        """Write limiter config to Redis and bump its version counter."""
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
            instance._config_version = int(raw_version)

    def refresh_config(self) -> bool:
        """Apply newer persisted config from Redis when version changes."""
        raw_version = self.redis.hget(self.__class__._VERSION_KEY, self.id)
        if raw_version is None:
            return False

        remote_version = int(raw_version)
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
                "Window change detected via refresh for limiter %s: new_window=%d, paused_for_s=%d.",
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
            "Config refreshed for limiter %s: version=%d, limit=%d, window=%d, max_concurrency=%d.",
            self.id,
            remote_version,
            self.limit,
            self.window,
            self.max_concurrency,
        )
        return True


class CeleryRateLimiter(AbstractRedisManagedRateLimiter):
    """A rate limiter that dispatches tasks via Celery.

    Use the classmethods ``configure``, ``create``, ``get``, and ``update``
    instead of constructing instances directly.
    """

    _celery_app: ClassVar[Optional[Celery]] = None

    @classmethod
    def configure(cls, redis_client: Redis, **backend_context: Any) -> None:
        """Configure shared Redis and Celery app context for class API usage.

        Requires ``celery_app`` as a keyword argument
        (e.g. ``CeleryRateLimiter.configure(redis, celery_app=app)``).
        """
        super().configure(redis_client, **backend_context)

    @classmethod
    def _configure_backend(cls, **backend_context: Any) -> None:
        """Store backend-specific context for Celery-backed limiter instances."""
        celery_app = backend_context.get("celery_app")
        if celery_app is None:
            raise RuntimeError(
                "CeleryRateLimiter.configure(redis_client, celery_app) "
                "must be called before create() or get()."
            )
        cls._celery_app = celery_app

    @classmethod
    def _has_backend_context(cls) -> bool:
        """Check if Celery app context has been configured."""
        return cls._celery_app is not None

    @classmethod
    def _get_instance_context(cls) -> dict[str, Any]:
        """Expose constructor context for concrete instance creation."""
        assert cls._celery_app is not None
        return {"celery_app": cls._celery_app, "_sentinel": cls._SENTINEL}

    @classmethod
    def _reset_backend_context(cls) -> None:
        """Clear Celery app class context."""
        cls._celery_app = None

    @classmethod
    def _configure_hint(cls) -> str:
        """Return configure usage for runtime errors."""
        return "CeleryRateLimiter.configure(redis_client, celery_app)"

    # ------------------------------------------------------------------
    # Instance construction
    # ------------------------------------------------------------------

    def __init__(
        self,
        redis_client: Redis,
        celery_app: Celery,
        *args: Any,
        _sentinel: Any = None,
        **kwargs: Any,
    ):
        """Create a Celery rate limiter instance.

        Deprecated:
            Direct construction is deprecated.  Use ``CeleryRateLimiter.create()`` or
            ``CeleryRateLimiter.get()`` instead.

        Args:
            redis_client: The Redis client.
            celery_app: The Celery app to use for task dispatch.
            _sentinel: Internal — passed by classmethods to suppress the deprecation warning.

        Other parameters are inherited from AbstractDistributedRateLimiter.
        """
        if _sentinel is not self.__class__._SENTINEL:
            warnings.warn(
                "Direct CeleryRateLimiter() construction is deprecated. "
                "Use CeleryRateLimiter.configure() + .create() or .get() instead.",
                DeprecationWarning,
                stacklevel=2,
            )
        super().__init__(redis_client, *args, **kwargs)
        self.app = celery_app

    @staticmethod
    def _get_enhanced_payload(payload: dict, use_executor: bool) -> dict:
        """Get the enhanced payload with metadata.

        Args:
            payload: The original task payload.
            use_executor: Whether to use the generic executor.

        Returns:
            The enhanced payload with metadata.
        """
        return {"data": payload, "meta": {"use_executor": use_executor}}

    def schedule_task(
        self,
        func_path: str,
        payload: dict,
        priority: int = 100,
        max_age: Optional[int] = None,
        retry: bool = True,
        use_executor: bool = True,
    ) -> tuple[bool, str]:
        # Add the use executor flag to the payload.
        # Only add this if we aren't re-trying--the payload is already present otherwise.
        enhanced_payload = payload
        if retry:
            enhanced_payload = self._get_enhanced_payload(payload, use_executor)

        # Call the parent scheduler.
        return super().schedule_task(
            func_path, enhanced_payload, priority, max_age, retry
        )

    def _dispatch_task(self, func_path: str, payload: dict, task_id: str) -> None:
        # Check if the built-in worker should be used.
        use_executor = payload.get("meta", {}).get("use_executor", True)
        data = payload.get("data", {})

        if use_executor:
            # Dispatch the task to the generic worker.
            self.app.send_task(
                "celery_rate_limiter.generic_worker",
                kwargs={
                    "limiter_id": self.id,
                    "func_path": func_path,
                    "payload": data,
                    "_rate_limit_task_id": task_id,
                },
            )
            logger.debug(
                "Celery task sent to generic worker: limiter=%s, task_id=%s, func_path=%s.",
                self.id,
                task_id,
                func_path,
            )
        else:
            # Use the custom user task.
            self.app.send_task(
                func_path, args=[data], kwargs={"_rate_limit_task_id": task_id}
            )
            logger.debug(
                "Celery task sent to custom worker: limiter=%s, task_id=%s, func_path=%s.",
                self.id,
                task_id,
                func_path,
            )

    def _schedule_drain(self, delay: float = 0.0) -> None:
        # Schedule an attempt at consuming a token.
        self.app.send_task(
            "celery_rate_limiter.attempt_consume", args=[self.id], countdown=delay
        )
        logger.debug(
            "Drain scheduled via Celery: limiter=%s, countdown_s=%.3f.",
            self.id,
            delay,
        )

