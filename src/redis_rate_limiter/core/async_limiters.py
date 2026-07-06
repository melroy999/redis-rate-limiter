"""Async helper classes and abstract base for distributed rate limiting.

This module provides asyncio-based counterparts for the synchronous helpers in
``limiters.py``. Each sync class has a 1:1 async mirror; the Lua scripts and
Redis key structure are identical, and only the I/O and coordination primitives
differ (``asyncio.Task``, ``asyncio.Condition``, ``redis.asyncio.Redis``).
"""

from __future__ import annotations

import asyncio
import hashlib
import heapq
import json
import logging
import math
import os
import signal
import time
import uuid
import warnings
from typing import (
    Any,
    Awaitable,
    Callable,
    Literal,
    Optional,
    cast,
)

import redis.asyncio

from redis_rate_limiter.core.base import AbstractAsyncRateLimiter, AcquireTimeout
from redis_rate_limiter.core.limiters import (
    _ACQUIRE_MARKER_PATH,
    ConsumeResult,
    DistributedRateLimiterMixin,
    HeartbeatEntry,
    TaskData,
)

logger = logging.getLogger(__name__)


class AsyncHeartbeatScheduler:
    """Single asyncio task that renews concurrency leases for many tasks.

    Async mirror of ``HeartbeatScheduler`` in ``limiters.py``. Tasks are
    registered on lifecycle entry and deregistered on exit. Renewal happens
    on the shared ``asyncio.Task`` at the limiter-wide heartbeat interval
    (``lease_duration / 2``). Renewals run outside the lock so a slow
    Redis call cannot block other registrations or deregistrations.
    """

    def __init__(self, limiter: AbstractAsyncDistributedRateLimiter) -> None:
        self._limiter = limiter
        self._interval = limiter.lease_duration / 2
        self._lock: asyncio.Lock = asyncio.Lock()
        self._wakeup: asyncio.Event = asyncio.Event()
        self._entries: dict[str, HeartbeatEntry] = {}
        self._heap: list[tuple[float, str, int]] = []
        self._shutdown = False
        self._task: Optional[asyncio.Task[None]] = None

    async def register(
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
        async with self._lock:
            entry = HeartbeatEntry(
                task_id=task_id,
                on_failure_action=on_failure_action,
            )
            self._entries[task_id] = entry
            if task_id:
                due = time.monotonic() + self._interval
                heapq.heappush(self._heap, (due, task_id, entry.generation))
                self._ensure_started_locked()
                self._wakeup.set()
            return entry

    async def deregister(self, task_id: str) -> None:
        """Stop receiving renewals for the given task.

        Heap tuples for the deregistered task are left in place and
        dropped lazily by ``_run`` when popped.
        """
        async with self._lock:
            self._entries.pop(task_id, None)

    async def get_entry(self, task_id: str) -> Optional[HeartbeatEntry]:
        """Return the live entry for ``task_id``, or ``None`` if not registered."""
        async with self._lock:
            return self._entries.get(task_id)

    async def shutdown(self) -> None:
        """Signal the worker task to stop and wait for it to exit."""
        async with self._lock:
            self._shutdown = True
            self._wakeup.set()
        if self._task is not None and not self._task.done():
            try:
                await asyncio.wait_for(self._task, timeout=5.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                self._task.cancel()
                logger.warning(
                    "[AsyncHeartbeatScheduler] Shutdown timed out, cancelling task: limiter=%s.",
                    self._limiter.id,
                )

    def _ensure_started_locked(self) -> None:
        """Lazily spawn (or respawn) the worker task.

        Must be called with ``self._lock`` held.
        """
        if self._task is None or self._task.done():
            self._shutdown = False
            self._task = asyncio.create_task(self._run())

    async def _run(self) -> None:
        """Sleep until the next due renewal, perform it, repeat."""
        while True:
            async with self._lock:
                if self._shutdown:
                    return
                if not self._heap:
                    self._wakeup.clear()
                    wait_for_event = self._wakeup.wait()
                    timeout = self._interval
                else:
                    due, task_id, generation = self._heap[0]
                    now = time.monotonic()
                    if due > now:
                        self._wakeup.clear()
                        wait_for_event = self._wakeup.wait()
                        timeout = due - now
                    else:
                        wait_for_event = None

            if wait_for_event is not None:
                try:
                    await asyncio.wait_for(wait_for_event, timeout=timeout)
                except asyncio.TimeoutError:
                    pass
                continue

            async with self._lock:
                if not self._heap:
                    continue
                due, task_id, generation = self._heap[0]
                if due > time.monotonic():
                    continue
                heapq.heappop(self._heap)
                entry = self._entries.get(task_id)
                if entry is None or entry.generation != generation:
                    # Stale heap entry: task was deregistered or already
                    # rescheduled at a later time. Drop without renewing.
                    continue

                task_id_snapshot = entry.task_id
                on_failure = entry.on_failure_action

            await self._renew_one(task_id_snapshot, on_failure)

            async with self._lock:
                entry = self._entries.get(task_id_snapshot)
                if entry is not None:
                    entry.generation += 1
                    next_due = time.monotonic() + self._interval
                    heapq.heappush(
                        self._heap,
                        (next_due, task_id_snapshot, entry.generation),
                    )

    async def _renew_one(self, task_id: str, on_failure_action: str) -> None:
        """Perform one lease renewal and update the entry's health state."""
        try:
            await self._limiter.extend_lease(task_id, self._limiter.lease_duration)
        except redis.RedisError as e:
            async with self._lock:
                entry = self._entries.get(task_id)
                if entry is not None:
                    entry.is_healthy = False

            if on_failure_action == "kill":
                logger.critical(
                    "[AsyncTaskLifecycle] Heartbeat failed for task %s: %s, terminating worker.",
                    task_id,
                    e,
                )
                os.kill(os.getpid(), signal.SIGTERM)
                return
            logger.critical(
                "[AsyncTaskLifecycle] Heartbeat failed for task %s: %s, flagged as unhealthy.",
                task_id,
                e,
            )
            return

        async with self._lock:
            entry = self._entries.get(task_id)
            if entry is not None and not entry.is_healthy:
                entry.is_healthy = True
                logger.info(
                    "[AsyncTaskLifecycle] Heartbeat connection restored for task %s on limiter %s.",
                    task_id,
                    self._limiter.id,
                )


class AsyncTaskLifecycle:
    """Async context manager responsible for concurrency slot cleanup upon task completion.

    Mirrors ``TaskLifecycle`` from ``limiters.py``; delegates heartbeat
    work to the shared ``AsyncHeartbeatScheduler`` on the limiter.
    """

    def __init__(
        self,
        limiter: AbstractAsyncDistributedRateLimiter,
        task_id: str,
        on_heartbeat_failure: Literal["warn", "kill"] = "warn",
    ):
        """Initialize an async lifecycle context manager.

        Args:
            limiter: The async rate limiter instance.
            task_id: The identifier of the task whose active state is to be cleared.
            on_heartbeat_failure: The strategy for handling heartbeat failures.
        """
        self.limiter = limiter
        self.task_id = task_id
        self.interval = self.limiter.lease_duration / 2
        self.on_failure_action = on_heartbeat_failure.lower()
        self._entry: Optional[HeartbeatEntry] = None

    @property
    def is_healthy(self) -> bool:
        """Report whether the most recent heartbeat for this task succeeded.

        Before ``__aenter__`` and after ``__aexit__`` the task has no live
        scheduler entry, so ``True`` is returned.
        """
        if self._entry is None:
            return True
        healthy: bool = self._entry.is_healthy
        return healthy

    @is_healthy.setter
    def is_healthy(self, value: bool) -> None:
        if self._entry is not None:
            self._entry.is_healthy = value

    async def __aenter__(self) -> AsyncTaskLifecycle:
        """Register the task with the shared async heartbeat scheduler."""
        self._entry = await self.limiter._heartbeat_scheduler.register(
            self.task_id, self.on_failure_action
        )
        logger.debug(
            "[AsyncTaskLifecycle] Task lifecycle entered: limiter=%s, task_id=%s, heartbeat_interval_s=%.3f.",
            self.limiter.id,
            self.task_id,
            self.interval,
        )
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Deregister heartbeat, release concurrency + inflight, publish drain signal, and wake the local drain loop.

        The release script (KEYS[3] = drain channel, ARGV[2] = worker id)
        also publishes the cross-process wake-up, so the only step left
        in Python is the local ``_schedule_drain`` notification of this
        worker's own drain loop.
        """
        await self.limiter._heartbeat_scheduler.deregister(self.task_id)

        try:
            inflight_key = (
                self.limiter.get_inflight_key(self.task_id) if self.task_id else ""
            )
            # fmt: off
            result = cast(  # pragma: no mutate
                list[int],
                await self.limiter._eval_script(
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
                "[AsyncTaskLifecycle] Concurrency slot released and inflight key cleared: limiter=%s, task_id=%s, removed_concurrency=%s, removed_inflight=%s.",
                self.limiter.id,
                self.task_id,
                removed_concurrency == 1,
                inflight_removed == 1,
            )
        finally:
            logger.debug(
                "[AsyncTaskLifecycle] Task lifecycle exited, triggering follow-up consume: limiter=%s, task_id=%s.",
                self.limiter.id,
                self.task_id,
            )
            self.limiter._schedule_drain()


class AsyncDrainLoop:
    """Asyncio-based drain loop that executes ``drain()`` according to a managed schedule.

    Mirrors ``DrainLoop`` from ``limiters.py`` using ``asyncio.Lock``,
    ``asyncio.Condition``, and ``asyncio.create_task()``.
    """

    def __init__(
        self,
        limiter: AbstractAsyncDistributedRateLimiter,
        watchdog_interval: float,
    ) -> None:
        self._limiter = limiter
        self._lock = asyncio.Lock()
        self._condition = asyncio.Condition(self._lock)
        self._next_wake: float | None = None
        self._shutdown = False
        self._watchdog_interval = watchdog_interval
        self._task: asyncio.Task[None] | None = None

    def wake(self, delay: float = 0.0) -> None:
        """Request that a drain be performed ``delay`` seconds from the current time.

        This method is synchronous (safe to call from within the event loop)
        and uses ``asyncio.create_task`` to schedule the condition notification.
        """
        target = time.monotonic() + delay
        asyncio.ensure_future(self._wake_async(target))

    async def _wake_async(self, target: float) -> None:
        """Internal coroutine that acquires the condition and notifies the drain task."""
        async with self._condition:
            if not self._shutdown:
                self._ensure_started()
            if self._next_wake is None or target < self._next_wake:
                self._next_wake = target
                self._condition.notify()

    async def shutdown(self) -> None:
        """Signal the drain task to terminate and wait for it to complete."""
        async with self._condition:
            self._shutdown = True
            self._condition.notify()
        if self._task is not None and not self._task.done():
            try:
                await asyncio.wait_for(self._task, timeout=5.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                self._task.cancel()
                logger.warning(
                    "[AsyncDrainLoop] Shutdown timed out, cancelling task: limiter=%s.",
                    self._limiter.id,
                )

    def _ensure_started(self) -> None:
        """Lazily initialize and start the drain task.

        This method must be called while holding ``self._condition``.
        """
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._run())

    async def _run(self) -> None:
        """Execute the main loop, sleeping until the next scheduled wake or the watchdog timeout elapses."""
        while True:
            async with self._condition:
                if self._shutdown:
                    return
                if self._next_wake is None:
                    try:
                        await asyncio.wait_for(
                            self._condition.wait(),
                            timeout=self._watchdog_interval,
                        )
                    except asyncio.TimeoutError:
                        pass
                    if self._next_wake is not None:
                        continue
                else:
                    remaining = self._next_wake - time.monotonic()
                    if remaining > 0:
                        try:
                            await asyncio.wait_for(
                                self._condition.wait(), timeout=remaining
                            )
                        except asyncio.TimeoutError:
                            pass
                        continue
                self._next_wake = None

            try:
                await self._limiter.drain()
            except Exception:
                logger.exception(
                    "[AsyncDrainLoop] Unhandled exception escaped drain(): limiter=%s.",
                    self._limiter.id,
                )


class AsyncDrainSignalSubscriber:
    """Async subscriber for Redis Pub/Sub drain signals.

    Mirrors ``DrainSignalSubscriber`` from ``limiters.py`` using
    ``redis.asyncio.Redis.pubsub()`` and ``asyncio.create_task()``.
    """

    def __init__(self, limiter: AbstractAsyncDistributedRateLimiter) -> None:
        self._limiter = limiter
        self._channel = f"{limiter.id}:drain_signal"
        self._pubsub = limiter.redis.pubsub()
        self._task: asyncio.Task[None] | None = None
        self._shutdown = False

    async def start(self) -> None:
        """Subscribe to the drain signal channel and start the listener task."""
        await self._pubsub.subscribe(self._channel)
        self._task = asyncio.create_task(self._run())

    async def _run(self) -> None:
        """Listen for drain signals and wake the local drain loop on receipt."""
        while not self._shutdown:
            try:
                message = await self._pubsub.get_message(
                    ignore_subscribe_messages=True, timeout=0.5
                )
                if message is not None and message["type"] == "message":
                    sender_id = message["data"]
                    if isinstance(sender_id, bytes):
                        sender_id = sender_id.decode("utf-8")
                    if sender_id != self._limiter._worker_id:
                        self._limiter._schedule_drain()
            except Exception:
                if self._shutdown:
                    return
                logger.exception(
                    "[AsyncDrainSignalSubscriber] Drain signal subscriber error: limiter=%s.",
                    self._limiter.id,
                )
                await asyncio.sleep(1.0)

    async def shutdown(self) -> None:
        """Stop the subscriber task and release the Pub/Sub connection."""
        self._shutdown = True
        try:
            await self._pubsub.unsubscribe()
            await self._pubsub.aclose()
        except Exception:
            pass
        if self._task is not None and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass


class AsyncBackendHealthMonitor:
    """Periodically checks whether the execution backend is operational (async).

    Mirrors ``BackendHealthMonitor`` from ``limiters.py`` using
    ``asyncio.create_task()`` and ``asyncio.Event``.
    """

    def __init__(
        self,
        limiter: AbstractAsyncDistributedRateLimiter,
        interval: float,
    ) -> None:
        self._limiter = limiter
        self._interval = interval
        self._healthy = True
        self._shutdown_event = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    @property
    def is_healthy(self) -> bool:
        return self._healthy

    def start(self) -> None:
        self._task = asyncio.create_task(self._run())

    async def _run(self) -> None:
        while not self._shutdown_event.is_set():
            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(), timeout=self._interval
                )
            except asyncio.TimeoutError:
                pass
            if not self._shutdown_event.is_set():
                await self._run_once()

    async def _run_once(self) -> None:
        try:
            healthy = await self._limiter._check_backend_health()
        except Exception:
            logger.debug(
                "[AsyncBackendHealthMonitor] Backend health check raised an exception: limiter=%s.",
                self._limiter.id,
                exc_info=True,
            )
            healthy = False

        if self._healthy and not healthy:
            # fmt: off
            logger.warning(
                "[AsyncBackendHealthMonitor] Backend health check failed: limiter=%s. Workers may be unavailable; dispatched tasks will not complete until the backend recovers.",
                self._limiter.id,
            )
            # fmt: on
        elif not self._healthy and healthy:
            # fmt: off
            logger.info(
                "[AsyncBackendHealthMonitor] Backend health check recovered: limiter=%s. Workers are available again.",
                self._limiter.id,
            )
            # fmt: on

        self._healthy = healthy

    async def shutdown(self) -> None:
        """Signal the health check task to terminate and wait for it to complete."""
        self._shutdown_event.set()
        if self._task is not None and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass


# noinspection PyUnnecessaryCast
class AbstractAsyncDistributedRateLimiter(
    DistributedRateLimiterMixin, AbstractAsyncRateLimiter
):
    """Asynchronous implementation of the distributed rate limiter.

    Provides async Redis operations, an asyncio-based drain loop and signal subscriber, async task lifecycle heartbeat, and an async distributed lock. See ``DistributedRateLimiterMixin`` for distributed semantics, Redis requirements, and shared algorithm logic.
    """

    _last_refresh_at: float

    def __init__(
        self,
        redis_client: redis.asyncio.Redis,
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
        """Initialize the async distributed rate limiter.

        Script registration and subscriber startup are deferred to ``start()``
        because ``__init__`` cannot be async.

        Args:
            redis_client: The async Redis client instance.
            limiter_id: The unique identifier of the rate limiter.
            limit: The maximum number of tasks permitted per time window.
            window: The time window in seconds.
            max_concurrency: The maximum number of tasks that may execute concurrently.
                For external broker backends, this value should match the total worker
                capacity of the fleet (see the backend class docstrings for details).
            max_age: The maximum queue residence time in seconds.
            lease_duration: The concurrency slot lease duration in seconds.
            on_heartbeat_failure: The heartbeat failure strategy.
            jitter_enabled: Whether randomized jitter is added to retry delays.
            jitter_min_pct: The minimum jitter percentage.
            jitter_max_pct: The maximum jitter percentage.
            metrics_callback: An optional metrics callback.
            drain_enabled: Whether the drain loop is active.
        """
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

        # Drain loop and signal subscriber are created eagerly but started
        # in start() since their operation requires registered script SHAs.
        if drain_enabled:
            self._drain_loop: AsyncDrainLoop | None = AsyncDrainLoop(
                self,
                watchdog_interval=max(5.0, self.window * 2),
            )
            self._drain_signal_subscriber: AsyncDrainSignalSubscriber | None = (
                AsyncDrainSignalSubscriber(self)
            )
        else:
            self._drain_loop = None
            self._drain_signal_subscriber = None

        # Backend health monitor (created eagerly, started in start()).
        if (
            drain_enabled
            and type(self)._check_backend_health
            is not AbstractAsyncDistributedRateLimiter._check_backend_health
        ):
            self._backend_health_monitor: AsyncBackendHealthMonitor | None = (
                AsyncBackendHealthMonitor(self, interval=float(self.lease_duration))
            )
        else:
            self._backend_health_monitor = None

    async def _check_backend_health(self) -> bool:
        """Check whether the execution backend is operational (async).

        The base implementation returns ``True`` (always healthy), which is
        correct for in-process backends. Distributed backends should override
        this method to verify that remote workers are available.
        """
        return True

    async def start(self) -> None:
        """Perform async initialization that cannot occur in ``__init__``.

        Registers Lua scripts with the Redis server and starts the drain
        signal subscriber and backend health monitor. Must be called after
        construction.
        """
        await super().start()

        await self._register_script("consume.lua")
        await self._register_script("schedule.lua")
        await self._register_script("health.lua")
        await self._register_script("renew.lua")
        await self._register_script("release.lua")

        # Async scheduler is created here (rather than in __init__) because
        # asyncio.create_task requires a running event loop, which is only
        # guaranteed once start() has been entered.
        self._heartbeat_scheduler: AsyncHeartbeatScheduler = AsyncHeartbeatScheduler(
            self
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
            "enabled" if self._backend_health_monitor else "disabled",
        )

        if self._drain_signal_subscriber is not None:
            await self._drain_signal_subscriber.start()

        if self._backend_health_monitor is not None:
            self._backend_health_monitor.start()

    # ---------------------------------------------------------------------------
    # Task scheduling
    # ---------------------------------------------------------------------------

    async def _cleanup_inflight_key(self, inflight_key: str, task_id: str) -> None:
        """Perform a best-effort cleanup of an in-flight key following a scheduling failure."""
        try:
            removed = await self.redis.delete(inflight_key)
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
                "[%s] Failed to cleanup inflight key after schedule failure: limiter=%s, task_id=%s, error=%s.",
                type(self).__name__,
                self.id,
                task_id,
                cleanup_error,
            )

    async def schedule_task(
        self,
        func_path: str,
        payload: dict,
        priority: int = 100,
        max_age: Optional[int] = None,
    ) -> tuple[bool, str]:
        """Schedule a task for execution once the rate limit permits.

        Args:
            func_path: The dotted Python path of the function.
            payload: The task payload dictionary.
            priority: The task priority (default: 100).
            max_age: An optional override for the maximum age.

        Returns:
            A tuple of (``was_scheduled``, ``task_id``).

        Raises:
            ValueError: If ``priority`` is not a finite number.
        """
        if not math.isfinite(priority):
            raise ValueError(f"priority must be a finite number, got {priority}")

        task_signature = self._get_task_signature_str(func_path, payload)
        task_id = hashlib.md5(task_signature.encode()).hexdigest()
        logger.debug(
            "[%s] Scheduling task attempt: limiter=%s, task_id=%s, func_path=%s, priority=%d.",
            type(self).__name__,
            self.id,
            task_id,
            func_path,
            priority,
        )

        inflight_key = self.get_inflight_key(task_id)
        inflight_ttl = self._get_inflight_ttl(max_age_override=max_age)
        if not await self.redis.set(inflight_key, "1", ex=inflight_ttl, nx=True):
            logger.debug(
                "[%s] Task already in-flight: limiter=%s, task_id=%s.",
                type(self).__name__,
                self.id,
                task_id,
            )
            self._emit_metric("schedule", {"scheduled": False, "task_id": task_id})
            return False, task_id

        full_data = self._get_task_data_str(task_id, func_path, payload)

        try:
            await self._eval_script(
                "schedule.lua",
                1,
                self.buffer_key,
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
            await self._cleanup_inflight_key(inflight_key, task_id)
            raise

        await self.trigger_consume()
        self._emit_metric("schedule", {"scheduled": True, "task_id": task_id})
        return True, task_id

    # ---------------------------------------------------------------------------
    # Inline slot acquisition
    # ---------------------------------------------------------------------------

    async def acquire(self, timeout: float, priority: int = 100) -> AsyncTaskLifecycle:
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
            An ``AsyncTaskLifecycle`` whose ``__aenter__`` confirms the slot and
            whose ``__aexit__`` releases it via ``release.lua``.

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
        scheduled, task_id = await self.schedule_task(
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
        # fmt: off
        blpop_result = await cast(  # pragma: no mutate
            Awaitable, self.redis.blpop(signal_key, timeout=timeout)
        )
        # fmt: on
        if blpop_result is None:
            raise AcquireTimeout(
                f"Acquire on limiter '{self.id}' timed out after {timeout}s"
            )
        return self.task_lifecycle(task_id)

    # ---------------------------------------------------------------------------
    # Task consumption and lease management
    # ---------------------------------------------------------------------------

    async def consume(self) -> ConsumeResult:
        """Attempt to consume a task from the queue.

        Returns:
            A result containing the task data if consumption was successful.
        """
        logger.debug(
            "[%s] Consume attempt started: limiter=%s.", type(self).__name__, self.id
        )

        # fmt: off
        result = cast(  # pragma: no mutate
            list[str],
            await self._eval_script(
                "consume.lua",
                4,
                self.id,
                self.buffer_key,
                self.concurrency_key,
                self.dlq_key,
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

    async def extend_lease(self, task_id: str, duration: int) -> None:
        """Extend the lease on a concurrency slot.

        Args:
            task_id: The task identifier.
            duration: The number of seconds by which to extend the lease.
        """
        # fmt: off
        renewed = int(
            cast(  # pragma: no mutate
                str,
                await self._eval_script(
                    "renew.lua",
                    1,
                    self.concurrency_key,
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

    async def get_buffer_count(self) -> int:
        """Return the number of items currently in the buffer."""
        return int(str(await self.redis.zcard(self.buffer_key)))

    async def drain(self) -> None:
        """Attempt to drain an item from the queue.

        Wraps ``_drain_inner`` with exception handling and exponential backoff.
        """
        try:
            now = time.monotonic()
            if hasattr(self, "refresh_config") and now - self._last_refresh_at >= 1.0:
                self._last_refresh_at = now
                await self.refresh_config()

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

            await self._drain_inner()
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

    async def _drain_inner(self) -> None:
        """Execute the core drain logic: consume, dispatch, and schedule a follow-up."""
        logger.debug("[%s] Drain loop start: limiter=%s.", type(self).__name__, self.id)

        if not self._has_local_capacity():
            logger.debug(
                "[%s] Drain deferred: local execution capacity reached for limiter=%s.",
                type(self).__name__,
                self.id,
            )
            self._schedule_drain(delay=self._token_interval)
            return

        result = await self.consume()

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
                await self._dispatch_task(
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

    async def _dispatch_task(self, func_path: str, payload: dict, task_id: str) -> None:
        """Dispatch the task to the concrete async execution backend.

        Args:
            func_path: The dotted Python path to the function.
            payload: The task payload dictionary.
            task_id: The unique task identifier.
        """
        raise NotImplementedError("Subclasses must implement _dispatch_task")

    def _schedule_drain(self, delay: float = 0.0) -> None:
        """Schedule the drain method to execute again after ``delay`` seconds.

        This method is synchronous (safe to call from within coroutines)
        and delegates to the ``AsyncDrainLoop.wake()`` method.
        """
        if self._drain_loop is not None:
            self._drain_loop.wake(delay)

    # ---------------------------------------------------------------------------
    # Cross-process signaling
    # ---------------------------------------------------------------------------

    async def trigger_consume(self) -> None:
        """Trigger consumption from the task queue."""
        logger.debug(
            "[%s] Trigger consume scheduling drain: limiter=%s.",
            type(self).__name__,
            self.id,
        )
        self._schedule_drain()
        await self._publish_drain_signal()

    async def _publish_drain_signal(self) -> None:
        """Publish a drain signal for cross-process notification."""
        try:
            await self.redis.publish(self._drain_signal_channel, self._worker_id)
        except Exception:
            logger.debug(
                "[%s] Failed to publish drain signal: limiter=%s.",
                type(self).__name__,
                self.id,
            )

    # ---------------------------------------------------------------------------
    # Lifecycle
    # ---------------------------------------------------------------------------

    async def shutdown(self) -> None:
        """Stop the drain loop, signal subscriber, and backend health monitor.

        This method must be called when a limiter instance is no longer needed.
        Each limiter created with ``drain_enabled=True`` (the default) runs
        background asyncio tasks for the drain loop, the Redis Pub/Sub signal
        subscriber, and (when the backend overrides ``_check_backend_health``)
        the backend health monitor. Failing to call ``shutdown()`` will leak
        these tasks.
        """
        self._shutdown_called = True
        if self._backend_health_monitor is not None:
            await self._backend_health_monitor.shutdown()
        if self._drain_loop is not None:
            await self._drain_loop.shutdown()
        if self._drain_signal_subscriber is not None:
            await self._drain_signal_subscriber.shutdown()
        if getattr(self, "_heartbeat_scheduler", None) is not None:
            await self._heartbeat_scheduler.shutdown()

    def __del__(self) -> None:
        """Emit a warning if shutdown() was not called.

        Async cleanup cannot be performed in ``__del__``; this method only
        emits a ``ResourceWarning`` to aid debugging.
        """
        if getattr(self, "_drain_loop", None) is not None and not getattr(
            self, "_shutdown_called", False
        ):
            warnings.warn(
                f"limiter={self.id!r} was not shut down; "
                "call await shutdown() to stop background tasks",
                ResourceWarning,
            )

    def task_lifecycle(
        self,
        task_id: str,
        on_heartbeat_failure_override: Optional[Literal["warn", "kill"]] = None,
    ) -> AsyncTaskLifecycle:
        """Create an async context manager that ensures the concurrency slot is released.

        Args:
            task_id: The identifier of the task to be managed.
            on_heartbeat_failure_override: An optional override for the heartbeat failure strategy.

        Returns:
            An ``AsyncTaskLifecycle`` async context manager instance.
        """
        strategy = on_heartbeat_failure_override or self.on_heartbeat_failure

        return AsyncTaskLifecycle(
            limiter=self, task_id=task_id, on_heartbeat_failure=strategy
        )

    # ---------------------------------------------------------------------------
    # Status and monitoring
    # ---------------------------------------------------------------------------

    async def get_status(self) -> dict:
        """Return a snapshot of the current state of the limiter.

        Returns:
            A dictionary containing all status information.
        """
        # fmt: off
        result = cast(  # pragma: no mutate
            list[str],
            await self._eval_script(
                "health.lua",
                3,
                self.id,
                self.buffer_key,
                self.concurrency_key,
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
                "tokens_used": float(result[2]),
                "limit": self.limit,
                "window": self.window,
                "reset_in_ms": result[4],
            },
        }
