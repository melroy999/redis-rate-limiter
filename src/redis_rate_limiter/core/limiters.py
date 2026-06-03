from __future__ import annotations

import hashlib
import heapq
import json
import logging
import math
import os
import random
import signal
import time
import uuid
import warnings
from dataclasses import dataclass
from threading import Condition, Event, Lock, Thread
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Literal,
    Optional,
    TypedDict,
    cast,
)

import redis
from redis import Redis

from redis_rate_limiter.core.base import (
    AbstractRateLimiter,
    AbstractSyncRateLimiter,
    AcquireTimeout,
)

logger = logging.getLogger(__name__)

_ACQUIRE_MARKER_PATH = "__redis_rate_limiter_acquire_marker__"


class TaskData(TypedDict):
    """Structured representation of the task data returned by the buffer during consumption."""

    id: str  # The unique identifier of the task.
    func_path: str  # The Python dotted path to the function to be executed.
    payload: dict  # The parameters to be forwarded to the function.
    inflight_key: str  # The Redis key used to track de-duplication and in-flight state.


class ConsumeResult(TypedDict):
    """Structured representation of the result returned by the consume Lua script."""

    success: bool  # Indicates whether a task was successfully consumed.
    expired: bool  # Indicates whether the task has expired (moved to the DLQ).
    marker_skipped: bool  # Indicates whether an acquire marker was silently dropped because its deadline had elapsed.
    yielded: (
        bool  # Indicates that consume yielded to give competing workers a fair turn.
    )
    task: Optional[
        TaskData
    ]  # The deserialized task data from Redis, or None if no task was consumed.
    remaining_tokens: (
        int  # The number of remaining rate limit tokens in the current window.
    )
    active_concurrency: int  # The number of concurrency slots currently in use.
    reset_in_ms: int  # The time in milliseconds until the current window expires.
    remaining_tasks: (
        int  # The number of tasks remaining in the buffer awaiting processing.
    )
    val_previous: int  # The raw counter value for the previous fixed window.
    val_current: int  # The raw counter value for the current fixed window.


def build_enhanced_payload(payload: dict, use_executor: bool) -> dict:
    """Wrap a task payload with executor dispatch metadata.

    Backends that support the ``use_executor`` flag (e.g., Celery, RQ) call
    this function before passing the payload to the base
    ``schedule_task()`` so that ``_dispatch_task`` can later recover the
    original payload and the routing decision.

    Args:
        payload: The original task payload provided by the caller.
        use_executor: Whether the generic rate-limit executor task should be
            used for dispatch (``True``) or the function should be dispatched
            directly to the user-supplied worker (``False``).

    Returns:
        A dictionary of the form ``{"data": payload, "meta": {"use_executor": use_executor}}``.
    """
    return {"data": payload, "meta": {"use_executor": use_executor}}


# noinspection PyUnnecessaryCast
@dataclass
class HeartbeatEntry:
    """Per-task state tracked by ``HeartbeatScheduler``.

    The ``generation`` counter implements lazy heap deletion: a popped
    heap tuple whose generation does not match the live entry is treated
    as stale and skipped, avoiding O(N) ``heap.remove()`` calls.
    """

    task_id: str
    on_failure_action: str  # "warn" or "kill"
    is_healthy: bool = True
    generation: int = 0


class HeartbeatScheduler:
    """Single background thread that renews concurrency leases for many tasks.

    Tasks are registered on lifecycle entry and deregistered on exit.
    Renewal happens on the shared thread at ``lease_duration / 2``.
    Renewals run outside the lock so a slow Redis call cannot block
    registrations or deregistrations on the fast path.
    """

    def __init__(self, limiter: AbstractDistributedRateLimiter) -> None:
        self._limiter = limiter
        self._interval = limiter.lease_duration / 2
        self._condition: Condition = Condition(Lock())
        self._entries: dict[str, HeartbeatEntry] = {}
        self._heap: list[tuple[float, str, int]] = []
        self._shutdown = False
        self._thread: Optional[Thread] = None

    def register(
        self,
        task_id: str,
        on_failure_action: str,
    ) -> HeartbeatEntry:
        """Register a task for periodic lease renewal.

        Returns the live ``HeartbeatEntry`` so the caller can read its
        ``is_healthy`` field. An empty ``task_id`` creates the entry but
        schedules no renewal, so callers may safely manage tasks that
        never reached the in-flight stage.
        """
        with self._condition:
            entry = HeartbeatEntry(
                task_id=task_id,
                on_failure_action=on_failure_action,
            )
            self._entries[task_id] = entry
            if task_id:
                due = time.monotonic() + self._interval
                heapq.heappush(self._heap, (due, task_id, entry.generation))
                self._ensure_started_locked()
                self._condition.notify()
            return entry

    def deregister(self, task_id: str) -> None:
        """Stop receiving renewals for the given task.

        Heap tuples for the deregistered task are left in place and
        dropped lazily by ``_run`` when popped.
        """
        with self._condition:
            self._entries.pop(task_id, None)

    def get_entry(self, task_id: str) -> Optional[HeartbeatEntry]:
        """Return the live entry for ``task_id``, or ``None`` if not registered."""
        with self._condition:
            return self._entries.get(task_id)

    def shutdown(self) -> None:
        """Signal the worker thread to stop and wait for it to exit."""
        with self._condition:
            self._shutdown = True
            self._condition.notify_all()
        if self._thread is not None:
            self._thread.join(timeout=5.0)

    def _ensure_started_locked(self) -> None:
        """Lazily spawn (or respawn) the worker thread.

        Must be called with ``self._condition`` held.
        """
        if self._thread is None or not self._thread.is_alive():
            self._shutdown = False
            self._thread = Thread(
                target=self._run,
                name=f"HeartbeatScheduler-{self._limiter.id}",
                daemon=True,
            )
            self._thread.start()

    def _run(self) -> None:
        """Sleep until the next due renewal, perform it, repeat."""
        while True:
            with self._condition:
                if self._shutdown:
                    return
                if not self._heap:
                    self._condition.wait(timeout=self._interval)
                    continue

                due, task_id, generation = self._heap[0]
                now = time.monotonic()
                if due > now:
                    self._condition.wait(timeout=due - now)
                    continue

                heapq.heappop(self._heap)
                entry = self._entries.get(task_id)
                if entry is None or entry.generation != generation:
                    # Stale heap entry: task was deregistered or already
                    # rescheduled at a later time. Drop without renewing.
                    continue

                task_id_snapshot = entry.task_id
                on_failure = entry.on_failure_action

            self._renew_one(task_id_snapshot, on_failure)

            with self._condition:
                entry = self._entries.get(task_id_snapshot)
                if entry is not None:
                    entry.generation += 1
                    next_due = time.monotonic() + self._interval
                    heapq.heappush(
                        self._heap,
                        (next_due, task_id_snapshot, entry.generation),
                    )

    def _renew_one(self, task_id: str, on_failure_action: str) -> None:
        """Perform one lease renewal and update the entry's health state."""
        try:
            self._limiter.extend_lease(task_id, self._limiter.lease_duration)
        except redis.RedisError as e:
            with self._condition:
                entry = self._entries.get(task_id)
                if entry is not None:
                    entry.is_healthy = False

            if on_failure_action == "kill":
                logger.critical(
                    "[TaskLifecycle] Heartbeat failed for task %s: %s, terminating worker.",
                    task_id,
                    e,
                )
                os.kill(os.getpid(), signal.SIGTERM)
                return
            logger.critical(
                "[TaskLifecycle] Heartbeat failed for task %s: %s, flagged as unhealthy.",
                task_id,
                e,
            )
            return

        with self._condition:
            entry = self._entries.get(task_id)
            if entry is not None and not entry.is_healthy:
                entry.is_healthy = True
                logger.info(
                    "[TaskLifecycle] Heartbeat connection restored for task %s on limiter %s.",
                    task_id,
                    self._limiter.id,
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
        self.on_failure_action = on_heartbeat_failure.lower()
        self._entry: Optional[HeartbeatEntry] = None

    @property
    def is_healthy(self) -> bool:
        """Report whether the most recent heartbeat for this task succeeded.

        Before ``__enter__`` and after ``__exit__`` the task has no live
        scheduler entry, so ``True`` is returned.
        """
        if self._entry is None:
            return True
        return self._entry.is_healthy

    @is_healthy.setter
    def is_healthy(self, value: bool) -> None:
        if self._entry is not None:
            self._entry.is_healthy = value

    def __enter__(self) -> TaskLifecycle:
        """Register the task with the shared heartbeat scheduler."""
        self._entry = self.limiter._heartbeat_scheduler.register(
            self.task_id, self.on_failure_action
        )
        logger.debug(
            "[TaskLifecycle] Task lifecycle entered: limiter=%s, task_id=%s, heartbeat_interval_s=%.3f.",
            self.limiter.id,
            self.task_id,
            self.interval,
        )
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Deregister heartbeat, release concurrency + inflight, publish drain signal, and wake the local drain loop.

        The release script (KEYS[3] = drain channel, ARGV[2] = worker id)
        also publishes the cross-process wake-up, so the only step left
        in Python is the local ``_schedule_drain`` notification of this
        worker's own drain loop.
        """
        self.limiter._heartbeat_scheduler.deregister(self.task_id)

        try:
            inflight_key = (
                self.limiter.get_inflight_key(self.task_id) if self.task_id else ""
            )
            # fmt: off
            result = cast(  # pragma: no mutate
                list[int],
                self.limiter._eval_script(
                    "release.lua",
                    3,
                    # KEYS: [concurrency, inflight, drain_channel]
                    self.limiter.concurrency_key,
                    inflight_key,
                    self.limiter._drain_signal_channel,
                    # ARGV: [task_id, worker_id]
                    self.task_id,
                    self.limiter._worker_id,
                ),
            )
            # fmt: on
            removed_concurrency = int(result[0])
            inflight_removed = int(result[1])

            logger.debug(
                "[TaskLifecycle] Concurrency slot released and inflight key cleared: limiter=%s, task_id=%s, removed_concurrency=%s, removed_inflight=%s.",
                self.limiter.id,
                self.task_id,
                removed_concurrency == 1,
                inflight_removed == 1,
            )
        finally:
            logger.debug(
                "[TaskLifecycle] Task lifecycle exited, triggering follow-up consume: limiter=%s, task_id=%s.",
                self.limiter.id,
                self.task_id,
            )
            self.limiter._schedule_drain()


class DrainLoop:
    """Single persistent thread that executes ``drain()`` according to a managed schedule.

    Multiple ``wake()`` calls are naturally coalesced; if an earlier drain is
    already pending, subsequent requests are treated as no-ops. A watchdog timeout
    triggers periodic drains even when no explicit ``wake()`` call is received,
    serving as a safety net for missed Pub/Sub signals, crashed tasks, lost
    recovery chains, or stale concurrency slots.
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
        If a previous drain thread has died (e.g., due to an unhandled exception),
        a new thread is created to replace it.
        """
        if self._thread is None or not self._thread.is_alive():
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
            try:
                self._limiter.drain()
            except Exception:
                logger.exception(
                    "[DrainLoop] Unhandled exception escaped drain(): limiter=%s.",
                    self._limiter.id,
                )


class DrainSignalSubscriber:
    """Subscribes to a Redis Pub/Sub channel for cross-process drain signals.

    When a task is scheduled or a concurrency slot is freed, the originating
    process publishes a drain signal. This subscriber receives the signal and
    wakes the local ``DrainLoop``, enabling immediate cross-process task
    consumption without relying on the watchdog timer.

    Messages originating from the local process are ignored (filtered by
    ``worker_id``) to prevent redundant in-process wake signals.
    """

    def __init__(self, limiter: AbstractDistributedRateLimiter) -> None:
        self._limiter = limiter
        self._channel = f"{limiter.id}:drain_signal"
        self._pubsub = limiter.redis.pubsub()
        self._thread: Thread | None = None
        self._shutdown = False

    def start(self) -> None:
        """Subscribe to the drain signal channel and start the listener thread."""
        self._pubsub.subscribe(self._channel)
        self._thread = Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self) -> None:
        """Listen for drain signals and wake the local drain loop on receipt."""
        while not self._shutdown:
            try:
                message = self._pubsub.get_message(timeout=0.5)
                if message is not None and message["type"] == "message":
                    sender_id = message["data"]
                    if sender_id != self._limiter._worker_id:
                        self._limiter._schedule_drain()
            except Exception:
                if self._shutdown:
                    return
                logger.exception(
                    "[DrainSignalSubscriber] Drain signal subscriber error: limiter=%s.",
                    self._limiter.id,
                )
                time.sleep(1.0)

    def shutdown(self) -> None:
        """Stop the subscriber thread.

        Publishes a sentinel message to the subscriber's own channel so
        the listener's ``get_message`` returns immediately instead of
        waiting out its poll timeout.
        """
        self._shutdown = True
        try:
            self._limiter.redis.publish(self._channel, "")
        except Exception:
            pass
        if self._thread is not None:
            self._thread.join(timeout=5.0)


class BackendHealthMonitor:
    """Periodically checks whether the execution backend is operational.

    This monitor runs as a daemon background thread and calls
    ``_check_backend_health()`` on the limiter at a configurable interval.
    It uses state-transition logging: a WARNING is emitted when the backend
    transitions from healthy to unhealthy, and an INFO is emitted on
    recovery. Consecutive unhealthy states do not produce repeated warnings.
    """

    def __init__(
        self,
        limiter: AbstractDistributedRateLimiter,
        interval: float,
    ) -> None:
        self._limiter = limiter
        self._interval = interval
        self._healthy = True
        self._shutdown_event = Event()
        self._thread: Thread | None = None

    @property
    def is_healthy(self) -> bool:
        return self._healthy

    def start(self) -> None:
        self._thread = Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self) -> None:
        while not self._shutdown_event.wait(timeout=self._interval):
            self._run_once()

    def _run_once(self) -> None:
        try:
            healthy = self._limiter._check_backend_health()
        except Exception:
            logger.debug(
                "[BackendHealthMonitor] Backend health check raised an exception: limiter=%s.",
                self._limiter.id,
                exc_info=True,
            )
            healthy = False

        if self._healthy and not healthy:
            # fmt: off
            logger.warning(
                "[BackendHealthMonitor] Backend health check failed: limiter=%s. Workers may be unavailable; dispatched tasks will not complete until the backend recovers.",
                self._limiter.id,
            )
            # fmt: on
        elif not self._healthy and healthy:
            # fmt: off
            logger.info(
                "[BackendHealthMonitor] Backend health check recovered: limiter=%s. Workers are available again.",
                self._limiter.id,
            )
            # fmt: on

        self._healthy = healthy

    def shutdown(self) -> None:
        """Signal the health check thread to terminate and wait for it to complete."""
        self._shutdown_event.set()
        if self._thread is not None:
            self._thread.join(timeout=5.0)


class DistributedRateLimiterMixin(AbstractRateLimiter):
    """Shared domain logic for distributed rate limiters (sync and async).

    The term "distributed" refers to the rate limiting state, not to task execution itself: multiple processes and machines sharing the same limiter identifier are collectively rate-limited via Redis. The manner in which tasks are dispatched (e.g., Celery, threads, asyncio) is determined by the concrete backend subclass.

    Rate limiting is performed through atomic Lua scripts executed on a single Redis instance. All rate limit state (i.e., window counters, the task buffer, and the concurrency set) must reside on the same Redis node to guarantee correctness.

    Redis configuration requirements:
        - A single Redis instance, or a master-only setup in which all reads and writes are directed to the same node. Read replicas introduce replication lag that may cause the rate limit to be exceeded, as a replica may serve stale window counters.
        - Redis Cluster is not supported. The limiter utilizes multiple keys (window counters, buffer, concurrency set, dispatch lock) that must be co-located on the same shard. Key hash tags are not applied; hence, Redis Cluster may distribute them across different nodes and violate atomicity.

    This mixin provides configuration storage, Redis key construction, task data helpers, algorithm calculations (token recovery delay, adaptive jitter), and metric emission. It deliberately excludes all Redis I/O and threading/asyncio primitives so that both ``AbstractDistributedRateLimiter`` (sync) and ``AbstractAsyncDistributedRateLimiter`` (async) can reuse it via multiple inheritance.

    The cooperative ``__init__`` accepts distributed-specific keyword arguments, stores them as instance attributes, and forwards the remaining keyword arguments to the next class in the MRO (typically ``AbstractSyncRateLimiter`` or ``AbstractAsyncRateLimiter``).
    """

    if TYPE_CHECKING:
        id: str
        limit: int
        window: float

        def _schedule_drain(self, delay: float = 0.0) -> None: ...

    def __init__(
        self,
        *args: Any,
        max_concurrency: int,
        max_age: int = 3600,
        lease_duration: int = 30,
        on_heartbeat_failure: Literal["warn", "kill"] = "warn",
        jitter_enabled: bool = True,
        jitter_min_pct: float = 0.02,
        jitter_max_pct: float = 0.08,
        metrics_callback: Optional[Callable[[str, dict], None]] = None,
        drain_enabled: bool = True,
        **kwargs: Any,
    ) -> None:
        if max_concurrency < 1:
            raise ValueError(
                f"max_concurrency must be a positive integer, got {max_concurrency}"
            )
        if max_age < 1:
            raise ValueError(f"max_age must be a positive integer, got {max_age}")
        if lease_duration < 1:
            raise ValueError(
                f"lease_duration must be a positive integer, got {lease_duration}"
            )

        super().__init__(*args, **kwargs)
        self.max_concurrency = max_concurrency
        self.max_age = max_age
        self.lease_duration = lease_duration
        self.on_heartbeat_failure = on_heartbeat_failure
        self.jitter_enabled = jitter_enabled
        self.jitter_min_pct = jitter_min_pct
        self.jitter_max_pct = jitter_max_pct
        self.metrics_callback = metrics_callback
        self.drain_enabled = drain_enabled

        self._worker_id: str = str(uuid.uuid4())
        self._consecutive_drain_failures: int = 0
        self._last_refresh_at: float = 0.0

        # self.id is set by AbstractRateLimiter in the MRO.
        self.buffer_key = f"{self.id}:buffer"
        self.concurrency_key = f"{self.id}:concurrency"
        self.dlq_key = f"{self.id}:dlq"
        self._drain_signal_channel = f"{self.id}:drain_signal"

    # ---------------------------------------------------------------------------
    # Backend health check
    # ---------------------------------------------------------------------------

    def _check_backend_health(self) -> bool:
        """Check whether the execution backend is operational.

        The base implementation returns ``True`` (always healthy), which is
        correct for in-process backends. Distributed backends (e.g., Celery,
        RQ) should override this method to verify that remote workers are
        available.
        """
        return True

    # ---------------------------------------------------------------------------
    # Task data helpers
    # ---------------------------------------------------------------------------

    @staticmethod
    def _get_task_signature_str(func_path: str, payload: dict) -> str:
        return json.dumps({"path": func_path, "payload": payload}, sort_keys=True)

    def _get_task_data(self, task_id: str, func_path: str, payload: dict) -> dict:
        task_data = {
            "id": task_id,
            "func_path": func_path,
            "payload": payload,
            "inflight_key": self.get_inflight_key(task_id),
        }
        return task_data

    def _get_task_data_str(self, task_id: str, func_path: str, payload: dict) -> str:
        return json.dumps(self._get_task_data(task_id, func_path, payload))

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

    # ---------------------------------------------------------------------------
    # Algorithm helpers
    # ---------------------------------------------------------------------------

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
            return reset_in_ms / 1000.0

        # Determine the earliest point at which previous-window decay frees a token.
        #   estimated = val_previous * (window_ms - t) / window_ms + val_current
        #   estimated < limit  =>  t > window_ms * (1 - (limit - val_current) / val_previous)
        time_passed_ms = window_ms - reset_in_ms
        t_needed_ms = window_ms * (1.0 - (self.limit - val_current) / val_previous)
        wait_ms = t_needed_ms - time_passed_ms

        if wait_ms <= 0:
            # Decay has already freed a token; no waiting is needed.
            return 0.0

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

        min_jitter = self.window * self.jitter_min_pct
        max_jitter = self.window * self.jitter_max_pct

        # Compute the load pressure factor (0.0 = low contention, 1.0 = high contention).
        # These thresholds and pressure values are tuning constants validated through
        # behavioral properties (monotonicity, scaling) rather than exact-value tests.
        # Mutating them does not break correctness: it merely shifts the retry distribution.
        if remaining_tasks <= 0:  # pragma: no mutate
            load_pressure = 0.0  # pragma: no mutate
        elif remaining_tasks < 10:  # pragma: no mutate
            load_pressure = 0.2  # pragma: no mutate
        elif remaining_tasks < 50:  # pragma: no mutate
            load_pressure = 0.5  # pragma: no mutate
        elif remaining_tasks < 100:  # pragma: no mutate
            load_pressure = 0.7  # pragma: no mutate
        else:
            load_pressure = 1.0  # pragma: no mutate

        # Compute the concurrency pressure factor (0.0 = many free slots, 1.0 = at capacity).
        # fmt: off
        concurrency_pressure = active_concurrency / max(1, self.max_concurrency)  # pragma: no mutate
        # fmt: on

        # Combine the pressures, weighting queue load more heavily than concurrency.
        # fmt: off
        combined_pressure = (load_pressure * 0.7) + (concurrency_pressure * 0.3)  # pragma: no mutate
        # fmt: on

        # Scale factor ranges from 0.3 (low contention) to 1.0 (high contention).
        jitter_scale = 0.3 + (combined_pressure * 0.7)  # pragma: no mutate

        jitter_range_size = (max_jitter - min_jitter) * jitter_scale
        jitter = min_jitter + (jitter_range_size * random.random())

        rounded_jitter = round(jitter, 3)
        logger.debug(
            "[%s] Smart jitter calculated: limiter=%s, remaining_tasks=%d, remaining_tokens=%d, active_concurrency=%d, load_pressure=%.3f, concurrency_pressure=%.3f, jitter_s=%.3f.",
            type(self).__name__,
            self.id,
            remaining_tasks,
            remaining_tokens,
            active_concurrency,
            load_pressure,
            concurrency_pressure,
            rounded_jitter,
        )
        return rounded_jitter

    # ---------------------------------------------------------------------------
    # Metrics
    # ---------------------------------------------------------------------------

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
                "[%s] Metrics callback raised an exception: limiter=%s, event=%s, error=%s.",
                type(self).__name__,
                self.id,
                event,
                e,
            )

    # ---------------------------------------------------------------------------
    # Scheduling helpers
    # ---------------------------------------------------------------------------

    @property
    def _token_interval(self) -> float:
        """The duration between successive rate limit tokens: ``window / limit``."""
        return self.window / self.limit if self.limit > 0 else self.window

    def _has_local_capacity(self) -> bool:
        """Check whether the local execution environment can accept another dispatched task.

        The base implementation always returns ``True``. Backends with bounded
        local execution capacity (e.g., a ``ThreadPoolExecutor`` with a fixed
        number of workers) should override this method to prevent the consumer
        from acquiring Redis concurrency slots for tasks that would only be
        queued locally.
        """
        return True

    # ---------------------------------------------------------------------------
    # Config persistence hooks
    # ---------------------------------------------------------------------------

    def _build_persist_config(self) -> dict[str, Any]:
        config: dict[str, Any] = super()._build_persist_config()
        config.update(
            {
                "max_concurrency": self.max_concurrency,
                "max_age": self.max_age,
                "lease_duration": self.lease_duration,
            }
        )
        return config

    def _apply_config_overrides(self, overrides: dict[str, Any]) -> None:
        super()._apply_config_overrides(overrides)
        if "max_concurrency" in overrides:
            self.max_concurrency = overrides["max_concurrency"]
        if "max_age" in overrides:
            self.max_age = overrides["max_age"]
        if "lease_duration" in overrides:
            self.lease_duration = overrides["lease_duration"]


# noinspection PyUnnecessaryCast
class AbstractDistributedRateLimiter(
    DistributedRateLimiterMixin, AbstractSyncRateLimiter
):
    """Synchronous implementation of the distributed rate limiter.

    Provides synchronous Redis operations, threading-based drain loop and signal subscriber, thread-based task lifecycle heartbeat, and a synchronous distributed lock. See ``DistributedRateLimiterMixin`` for distributed semantics, Redis requirements, and shared algorithm logic.
    """

    _last_refresh_at: float

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
        drain_enabled: bool = True,
    ):
        """Initialize the rate limiter with the specified parameters.

        Args:
            redis_client: The Redis client instance to be used for all operations.
            limiter_id: The unique identifier of the rate limiter to be created.
            limit: The maximum number of tasks permitted per time window.
            window: The time window in seconds to which the rate limit is applied.
            max_concurrency: The maximum number of tasks that may execute concurrently.
                For external broker backends (Celery, RQ, Dramatiq, Huey), this value
                should match the total worker capacity of the fleet. Dispatched tasks
                hold a concurrency lease while waiting in the broker queue; if
                ``max_concurrency`` exceeds the actual worker capacity, tasks accumulate
                and their leases expire, causing unbounded queue growth.
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
            drain_enabled: Whether the drain loop is created and active. Set to ``False`` for
                scheduler-only instances that push tasks into the buffer without consuming them
                (e.g., a traffic generator in a multi-process deployment). Defaults to ``True``.
        """
        # Cooperative __init__: chains through AbstractSyncRateLimiter (consumes redis),
        # AbstractRateLimiter (consumes id, limit, window), and DistributedRateLimiterMixin
        # (consumes max_concurrency, keys, etc.).
        super().__init__(
            redis_client,
            limiter_id=limiter_id,
            limit=limit,
            window=window,
            max_concurrency=max_concurrency,
            max_age=max_age,
            lease_duration=lease_duration,
            on_heartbeat_failure=on_heartbeat_failure,
            jitter_enabled=jitter_enabled,
            jitter_min_pct=jitter_min_pct,
            jitter_max_pct=jitter_max_pct,
            metrics_callback=metrics_callback,
            drain_enabled=drain_enabled,
        )

        self._register_script("consume.lua")
        self._register_script("schedule.lua")
        self._register_script("health.lua")
        self._register_script("renew.lua")
        self._register_script("release.lua")

        self._heartbeat_scheduler: HeartbeatScheduler = HeartbeatScheduler(self)

        # Detect whether the concrete subclass provides a custom health check
        # (i.e., overrides the default no-op) before starting any threads,
        # so the log message can report the final configuration.
        has_health_monitor = (
            drain_enabled
            and type(self)._check_backend_health
            is not DistributedRateLimiterMixin._check_backend_health
        )

        logger.info(
            "[%s] Rate limiter initialized: id=%s, limit=%d, window_s=%g, max_concurrency=%d, max_age_s=%d, lease_duration_s=%d, heartbeat_failure=%s, jitter_enabled=%s, jitter_min_pct=%.3f, jitter_max_pct=%.3f, metrics_callback=%s, drain_enabled=%s, backend_health_monitor=%s.",
            type(self).__name__,
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
            self.drain_enabled,
            "enabled" if has_health_monitor else "disabled",
        )

        if drain_enabled:
            self._drain_loop: DrainLoop | None = DrainLoop(
                self,
                watchdog_interval=max(5.0, self.window * 2),
            )
            self._drain_signal_subscriber: DrainSignalSubscriber | None = (
                DrainSignalSubscriber(self)
            )
            self._drain_signal_subscriber.start()
        else:
            self._drain_loop = None
            self._drain_signal_subscriber = None

        if has_health_monitor:
            self._backend_health_monitor: BackendHealthMonitor | None = (
                BackendHealthMonitor(self, interval=float(self.lease_duration))
            )
            self._backend_health_monitor.start()
        else:
            self._backend_health_monitor = None

    # ---------------------------------------------------------------------------
    # Task scheduling
    # ---------------------------------------------------------------------------

    def _cleanup_inflight_key(self, inflight_key: str, task_id: str) -> None:
        """Perform a best-effort cleanup of an in-flight key following a scheduling failure."""
        try:
            removed = self.redis.delete(inflight_key)
            logger.debug(
                "[%s] Inflight cleanup attempted: limiter=%s, task_id=%s, inflight_key=%s, removed=%s.",
                type(self).__name__,
                self.id,
                task_id,
                inflight_key,
                removed,
            )
        except Exception as cleanup_error:
            logger.warning(
                "[%s] Failed to cleanup inflight key after schedule failure: limiter=%s, task_id=%s, inflight_key=%s, error=%s.",
                type(self).__name__,
                self.id,
                task_id,
                inflight_key,
                cleanup_error,
            )

    def schedule_task(
        self,
        func_path: str,
        payload: dict,
        priority: int = 100,
        max_age: Optional[int] = None,
    ) -> tuple[bool, str]:
        """Schedule a task for execution once the rate limit permits.

        Args:
            func_path: The dotted Python path of the function to be scheduled.
            payload: The payload dictionary for the task in question.
            priority: The priority of the task (default: 100).
            max_age: An optional override for the maximum age of the task, in seconds.

        Returns:
            A tuple of (``was_scheduled``, ``task_id``). The ``was_scheduled`` value is ``False``
            if the task was skipped because it is already in-flight.

        Raises:
            RuntimeError: If the required Lua scripts cannot be (re)loaded.
            ValueError: If ``priority`` is not a finite number.
        """
        if not math.isfinite(priority):
            raise ValueError(f"priority must be a finite number, got {priority}")

        task_signature = self._get_task_signature_str(func_path, payload)
        task_id = hashlib.md5(task_signature.encode()).hexdigest()
        logger.debug(
            "[%s] Scheduling task attempt: limiter=%s, task_id=%s, func_path=%s, priority=%d, max_age=%s.",
            type(self).__name__,
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
                "[%s] Task already in-flight, skipping schedule: limiter=%s, task_id=%s, inflight_key=%s, inflight_ttl_s=%d.",
                type(self).__name__,
                self.id,
                task_id,
                inflight_key,
                inflight_ttl,
            )
            self._emit_metric("schedule", {"scheduled": False, "task_id": task_id})
            return False, task_id

        full_data = self._get_task_data_str(task_id, func_path, payload)

        try:
            self._eval_script(
                "schedule.lua",
                1,
                # KEYS: [buffer]
                self.buffer_key,
                # ARGV: [task_json, priority, max age]
                full_data,
                priority,
                max_age or "",
            )
            logger.info(
                "[%s] Task scheduled: limiter=%s, task_id=%s, func_path=%s, priority=%d.",
                type(self).__name__,
                self.id,
                task_id,
                func_path,
                priority,
            )
        except Exception:
            # Any scheduling failure must release the claim so that retries
            # from callers are not blocked by a stale in-flight marker.
            self._cleanup_inflight_key(inflight_key, task_id)
            raise

        self.trigger_consume()
        self._emit_metric("schedule", {"scheduled": True, "task_id": task_id})
        return True, task_id

    # ---------------------------------------------------------------------------
    # Inline slot reservation
    # ---------------------------------------------------------------------------

    def acquire(self, timeout: float, priority: int = 100) -> TaskLifecycle:
        """Block up to ``timeout`` seconds for a rate and concurrency slot, then return a context manager that releases it.

        Schedules a marker into the priority buffer so that inline callers
        compete for the rate and concurrency budget on the same footing as
        buffered tasks. The wait is implemented as ``BLPOP`` against a per-call
        signal list that ``consume.lua`` populates atomically with the lease
        registration. An embedded deadline in the marker payload lets
        ``consume.lua`` refuse admission for callers whose timeout has already
        elapsed, preventing concurrency slot leaks.

        Args:
            timeout: Maximum time in seconds to wait for admission. Must be positive.
            priority: Priority for the marker; lower scores are dequeued first.

        Returns:
            A ``TaskLifecycle`` whose ``__enter__`` confirms the slot and whose
            ``__exit__`` releases it via ``release.lua``.

        Raises:
            AcquireTimeout: The timeout expired before ``consume.lua`` admitted the marker.
            ValueError: ``timeout`` is not positive.
            RuntimeError: ``drain_enabled=False`` (no drain loop is running to admit the marker).
        """
        if timeout <= 0:
            raise ValueError(f"timeout must be positive, got {timeout}")
        if self._drain_loop is None:
            raise RuntimeError("acquire() requires drain_enabled=True")

        timeout_ms = int(timeout * 1000)
        scheduled, task_id = self.schedule_task(
            func_path=_ACQUIRE_MARKER_PATH,
            payload={"_uuid": uuid.uuid4().hex, "_acquire_timeout_ms": timeout_ms},
            priority=priority,
            max_age=max(1, math.ceil(timeout)),
        )
        if not scheduled:
            raise RuntimeError(
                f"acquire marker rejected by buffer: limiter={self.id}, task_id={task_id}"
            )

        signal_key = f"{self.id}:acquire:{task_id}"
        if self.redis.blpop(signal_key, timeout=timeout) is None:
            raise AcquireTimeout(
                f"Acquire on limiter '{self.id}' timed out after {timeout}s"
            )
        return self.task_lifecycle(task_id)

    # ---------------------------------------------------------------------------
    # Task consumption and lease management
    # ---------------------------------------------------------------------------

    def consume(self) -> ConsumeResult:
        """Attempt to consume a task from the queue.

        Returns:
            A result containing the task data if consumption was successful, or an empty result otherwise.

        Raises:
            RuntimeError: If the required Lua scripts cannot be (re)loaded.
        """
        logger.debug(
            "[%s] Consume attempt started: limiter=%s.", type(self).__name__, self.id
        )

        # fmt: off
        result = cast(  # pragma: no mutate
            list[str],
            self._eval_script(
                "consume.lua",
                4,
                # KEYS: [base, buffer, concurrency, dlq]
                self.id,
                self.buffer_key,
                self.concurrency_key,
                self.dlq_key,
                # ARGV: [window, limit, max_concurrency, max_age, lease_duration, worker_id]
                self.window,
                self.limit,
                self.max_concurrency,
                self.max_age,
                self.lease_duration,
                self._worker_id,
            ),
        )
        # fmt: on

        consume_result: ConsumeResult = {
            "success": int(result[0]) == 1,
            "expired": int(result[0]) == -1,
            "marker_skipped": int(result[0]) == -2,
            "yielded": int(result[0]) == -3,
            # fmt: off
            "task": cast(  # pragma: no mutate
                TaskData, json.loads(result[1])
            )
            # fmt: on
            if result[1]
            else None,
            "remaining_tokens": int(result[2]),
            "active_concurrency": int(result[3]),
            "reset_in_ms": int(result[4]),
            "remaining_tasks": int(result[5]),
            "val_previous": int(result[6]),
            "val_current": int(result[7]),
        }
        task_id = consume_result["task"]["id"] if consume_result["task"] else None
        logger.debug(
            "[%s] Consume result: limiter=%s, success=%s, expired=%s, task_id=%s, remaining_tokens=%d, active_concurrency=%d, remaining_tasks=%d, reset_in_ms=%d.",
            type(self).__name__,
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

    def extend_lease(self, task_id: str, duration: int) -> None:
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

        Raises:
            KeyError: If the task identifier is not present in the concurrency set.
            RuntimeError: If the renew Lua script cannot be reloaded after a NoScriptError.
        """
        # fmt: off
        renewed = int(
            cast(  # pragma: no mutate
                str,
                self._eval_script(
                    "renew.lua",
                    1,
                    # KEYS: [concurrency]
                    self.concurrency_key,
                    # ARGV: [task_id, duration]
                    task_id,
                    duration,
                ),
            )
        )
        # fmt: on

        logger.debug(
            "[%s] Lease extension result: limiter=%s, task_id=%s, duration_s=%d, renewed=%s.",
            type(self).__name__,
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

    # ---------------------------------------------------------------------------
    # Drain orchestration
    # ---------------------------------------------------------------------------

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
        try:
            now = time.monotonic()
            if hasattr(self, "refresh_config") and now - self._last_refresh_at >= 1.0:
                self.refresh_config()
                self._last_refresh_at = now

            # Respect the window-change pause: skip draining until the pause expires,
            # but schedule a follow-up so that the drain loop resumes automatically.
            if (
                hasattr(self, "_drain_paused_until")
                and time.time() < self._drain_paused_until
            ):
                remaining = self._drain_paused_until - time.time()
                logger.debug(
                    "[%s] Drain deferred: limiter=%s is paused for %.3fs for window transition.",
                    type(self).__name__,
                    self.id,
                    remaining,
                )
                self._schedule_drain(delay=remaining)
                return

            self._drain_inner()
            self._consecutive_drain_failures = 0
        except Exception:
            self._consecutive_drain_failures += 1
            delay = min(
                self.window,
                0.1 * (2 ** (self._consecutive_drain_failures - 1)),
            )
            logger.error(
                "[%s] Drain failed (attempt #%d), scheduling recovery in %.3fs: limiter=%s.",
                type(self).__name__,
                self._consecutive_drain_failures,
                delay,
                self.id,
                exc_info=True,
            )
            try:
                self._schedule_drain(delay=delay)
            except Exception:
                # fmt: off
                logger.critical(
                    "[%s] Recovery scheduling also failed: limiter=%s. Drain loop will resume on next trigger_consume() or task completion.",
                    type(self).__name__,
                    self.id,
                    exc_info=True,
                )
                # fmt: on

    def _drain_inner(self) -> None:
        """Execute the core drain logic: consume, dispatch, and schedule a follow-up."""
        logger.debug("[%s] Drain loop start: limiter=%s.", type(self).__name__, self.id)

        # Check local execution capacity before calling consume.lua.
        # This prevents acquiring Redis concurrency slots for tasks that would
        # only be queued in the local execution environment (e.g., a thread pool).
        if not self._has_local_capacity():
            logger.debug(
                "[%s] Drain deferred: local execution capacity reached for limiter=%s.",
                type(self).__name__,
                self.id,
            )
            self._schedule_drain(delay=self._token_interval)
            return

        result = self.consume()

        if result["yielded"]:
            logger.debug(
                "[%s] Drain yielded for round-robin fairness: limiter=%s.",
                type(self).__name__,
                self.id,
            )
            self._schedule_drain(delay=self._token_interval)
            return

        if result["expired"]:
            logger.warning(
                "[%s] Expired task moved to DLQ during consume: limiter=%s.",
                type(self).__name__,
                self.id,
            )

        if result["success"]:
            task = result["task"]
            assert task is not None, "task must be present when success is True"
            task_id = task.get("id")

            if task["func_path"] != _ACQUIRE_MARKER_PATH:
                self._dispatch_task(
                    func_path=task["func_path"],
                    payload=task["payload"],
                    task_id=task_id,
                )
                logger.info(
                    "[%s] Task dispatched: limiter=%s, task_id=%s, func_path=%s.",
                    type(self).__name__,
                    self.id,
                    task_id,
                    task["func_path"],
                )

            if result["remaining_tasks"] > 0:
                logger.debug(
                    "[%s] More tasks remain, scheduling immediate follow-up drain: limiter=%s, remaining_tasks=%d.",
                    type(self).__name__,
                    self.id,
                    result["remaining_tasks"],
                )
                self._schedule_drain()

        elif result["remaining_tasks"] == 0:
            logger.debug(
                "[%s] Drain stopped: buffer empty for limiter=%s.",
                type(self).__name__,
                self.id,
            )

        elif result["active_concurrency"] >= self.max_concurrency:
            logger.debug(
                "[%s] Drain stopped: concurrency at capacity for limiter=%s (active=%d, max=%d).",
                type(self).__name__,
                self.id,
                result["active_concurrency"],
                self.max_concurrency,
            )

        elif result["marker_skipped"]:
            if result["remaining_tasks"] > 0:
                self._schedule_drain()

        elif result["remaining_tokens"] <= 0:
            val_previous = result["val_previous"]
            val_current = result["val_current"]
            base_delay = self._calculate_token_recovery_delay(
                val_previous=val_previous,
                val_current=val_current,
                reset_in_ms=result["reset_in_ms"],
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
                "[%s] Rate limited, scheduling retry: limiter=%s, delay_s=%.3f, base_delay_s=%.3f, jitter_s=%.3f, remaining_tasks=%d, val_previous=%d, val_current=%d, fallback=%s.",
                type(self).__name__,
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

    def _dispatch_task(self, func_path: str, payload: dict, task_id: str) -> None:
        """Dispatch the task to the concrete execution backend (e.g., Celery worker, thread).

        Args:
            func_path: The dotted Python path to the function to be executed.
            payload: The task payload dictionary.
            task_id: The unique task identifier.
        """
        raise NotImplementedError("Subclasses must implement _dispatch_task")

    def _schedule_drain(self, delay: float = 0.0) -> None:
        """Schedule the drain method to execute again after ``delay`` seconds.

        The default implementation wakes the ``DrainLoop``. Subclasses used in
        testing may override this method to record calls without starting the loop.
        When ``drain_enabled`` is ``False``, this method is a no-op.
        """
        if self._drain_loop is not None:
            self._drain_loop.wake(delay)

    # ---------------------------------------------------------------------------
    # Cross-process signaling
    # ---------------------------------------------------------------------------

    def trigger_consume(self) -> None:
        """Trigger consumption from the task queue.

        Wakes the local drain loop and publishes a cross-process drain signal
        so that other workers sharing the same limiter identifier can also
        attempt to consume.
        """
        logger.debug(
            "[%s] Trigger consume scheduling drain: limiter=%s.",
            type(self).__name__,
            self.id,
        )
        self._schedule_drain()
        self._publish_drain_signal()

    def _publish_drain_signal(self) -> None:
        """Publish a drain signal for cross-process notification.

        The message payload is the sender's ``worker_id``, which subscribers
        use to filter out self-notifications. This method fires regardless of
        ``drain_enabled``, so that scheduler-only instances (e.g., a traffic
        generator) can notify consumer workers.
        """
        try:
            self.redis.publish(self._drain_signal_channel, self._worker_id)
        except Exception:
            logger.debug(
                "[%s] Failed to publish drain signal: limiter=%s.",
                type(self).__name__,
                self.id,
            )

    # ---------------------------------------------------------------------------
    # Lifecycle
    # ---------------------------------------------------------------------------

    def shutdown(self) -> None:
        """Stop the drain loop and signal subscriber to facilitate a clean shutdown.

        This method must be called when a limiter instance is no longer needed.
        Each limiter created with ``drain_enabled=True`` (the default) runs
        background threads for the drain loop, the Redis Pub/Sub signal
        subscriber, and (when the backend overrides ``_check_backend_health``)
        the backend health monitor. Failing to call ``shutdown()`` will leak
        these threads.
        """
        self._shutdown_called = True
        if self._backend_health_monitor is not None:
            self._backend_health_monitor.shutdown()
        if self._drain_loop is not None:
            self._drain_loop.shutdown()
        if self._drain_signal_subscriber is not None:
            self._drain_signal_subscriber.shutdown()
        self._heartbeat_scheduler.shutdown()

    def __del__(self) -> None:
        """Emit a warning if shutdown() was not called.

        Calling ``shutdown()`` from ``__del__`` is intentionally avoided
        because the garbage collector may invoke this method at unpredictable
        times (e.g., between in-process test iterations), and joining threads
        or closing Pub/Sub connections during GC can interfere with active
        Redis state.
        """
        if getattr(self, "_drain_loop", None) is not None and not getattr(
            self, "_shutdown_called", False
        ):
            warnings.warn(
                f"limiter={self.id!r} was not shut down; "
                "call shutdown() to stop background threads",
                ResourceWarning,
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

    # ---------------------------------------------------------------------------
    # Status and monitoring
    # ---------------------------------------------------------------------------

    def get_status(self) -> dict:
        """Return a snapshot of the current state of the limiter.

        Returns:
            A dictionary containing all status information for the limiter.

        Raises:
            RuntimeError: If the required Lua scripts cannot be (re)loaded.
        """
        # fmt: off
        result = cast(  # pragma: no mutate
            list[str],
            self._eval_script(
                "health.lua",
                3,
                # KEYS: [base, buffer, concurrency]
                self.id,
                self.buffer_key,
                self.concurrency_key,
                # ARGV: [window]
                self.window,
            ),
        )
        # fmt: on

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
        }
