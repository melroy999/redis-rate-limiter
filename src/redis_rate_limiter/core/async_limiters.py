"""Async helper classes and abstract base for distributed rate limiting.

This module provides asyncio-based counterparts for the synchronous helpers in
``limiters.py``. Each sync class has a 1:1 async mirror; the Lua scripts and
Redis key structure are identical, and only the I/O and coordination primitives
differ (``asyncio.Task``, ``asyncio.Condition``, ``redis.asyncio.Redis``).
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
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

from redis_rate_limiter.core.base import AbstractAsyncRateLimiter
from redis_rate_limiter.core.limiters import (
    LOCK_ACQUIRE_SCRIPT,
    LOCK_RELEASE_SCRIPT,
    LOCK_SIMPLE_RELEASE_SCRIPT,
    ConsumeResult,
    DistributedRateLimiterMixin,
    TaskData,
)

logger = logging.getLogger(__name__)


# noinspection PyUnnecessaryCast
class AsyncDistributedLock:
    """Async context manager for the distributed dispatch lock.

    Mirrors ``DistributedLock`` from ``limiters.py`` using ``redis.asyncio.Redis``.
    The contention-aware fairness mechanism is identical: a per-worker cooldown
    key prevents lock monopolization under contention, while single-worker
    burst consumption is unaffected.
    """

    def __init__(
        self,
        redis_client: redis.asyncio.Redis,
        lock_key: str,
        timeout_ms: int,
        worker_id: str = "",
        cooldown_ms: int = 0,
        contention_key: str = "",
    ):
        """Initialize the async distributed lock manager.

        Args:
            redis_client: The async Redis client instance.
            lock_key: The Redis key under which the lock is stored.
            timeout_ms: The lock timeout in milliseconds.
            worker_id: A stable identifier for the drainer.
            cooldown_ms: The cooldown duration in milliseconds.
            contention_key: The Redis key for the shared contention counter.
        """
        self.redis = redis_client
        self.lock_key = lock_key
        self.timeout_ms = timeout_ms
        self.token = str(uuid.uuid4())
        self.acquired = False
        self.worker_id = worker_id
        self.cooldown_ms = cooldown_ms
        self.contention_key = contention_key
        self._cooldown_key = f"{lock_key}:cd:{worker_id}" if worker_id else ""
        self._fairness_enabled = bool(worker_id and cooldown_ms > 0 and contention_key)

    async def __aenter__(self) -> bool:
        """Attempt to acquire the dispatch lock."""
        if self._fairness_enabled:
            # fmt: off
            self.acquired = bool(
                await cast(  # pragma: no mutate
                    Awaitable,
                    self.redis.eval(
                        LOCK_ACQUIRE_SCRIPT,
                        3,
                        self.lock_key,
                        self._cooldown_key,
                        self.contention_key,
                        self.token,
                        self.timeout_ms,
                    ),
                )
            )
            # fmt: on
        else:
            self.acquired = bool(
                await self.redis.set(
                    self.lock_key, self.token, px=self.timeout_ms, nx=True
                )
            )

        if self.acquired:
            logger.debug(
                "Dispatch lock acquired (async): key=%s, token=%s, timeout_ms=%d.",
                self.lock_key,
                self.token,
                self.timeout_ms,
            )
        else:
            logger.debug(
                "Dispatch lock contended (async): key=%s.",
                self.lock_key,
            )
        return bool(self.acquired)

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Release the dispatch lock, provided it is still owned by this instance."""
        if self.acquired:
            if self._fairness_enabled:
                # fmt: off
                result = await cast(  # pragma: no mutate
                    Awaitable,
                    self.redis.eval(
                        LOCK_RELEASE_SCRIPT,
                        3,
                        self.lock_key,
                        self._cooldown_key,
                        self.contention_key,
                        self.token,
                        self.cooldown_ms,
                    ),
                )
                # fmt: on
            else:
                # fmt: off
                result = await cast(  # pragma: no mutate
                    Awaitable,
                    self.redis.eval(
                        LOCK_SIMPLE_RELEASE_SCRIPT, 1, self.lock_key, self.token
                    ),
                )
                # fmt: on

            if result:
                logger.debug(
                    "Dispatch lock released (async): key=%s, token=%s.",
                    self.lock_key,
                    self.token,
                )
            else:
                logger.debug(
                    "Dispatch lock already expired before release (async): key=%s, token=%s.",
                    self.lock_key,
                    self.token,
                )


class AsyncTaskLifecycle:
    """Async context manager responsible for concurrency slot cleanup upon task completion.

    Mirrors ``TaskLifecycle`` from ``limiters.py`` using ``asyncio.Task`` for the
    heartbeat loop and ``asyncio.Event`` for stop signaling.
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

        self._stop_event: asyncio.Event = asyncio.Event()
        self._task: Optional[asyncio.Task[None]] = None

        self.on_failure_action = on_heartbeat_failure.lower()
        self.is_healthy = True

    async def _heartbeat_loop(self) -> None:
        """Background coroutine that periodically renews the lease on a concurrency slot."""
        while not self._stop_event.is_set():
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self.interval)

                # Stop event was set.
                return
            except asyncio.TimeoutError:
                # Interval elapsed; renew the lease.
                pass

            try:
                await self.limiter.extend_lease(
                    self.task_id, self.limiter.lease_duration
                )

                if not self.is_healthy:
                    logger.info(
                        "Heartbeat connection restored for task %s on limiter %s.",
                        self.task_id,
                        self.limiter.id,
                    )
                    self.is_healthy = True
            except Exception as e:
                self.is_healthy = False

                if self.on_failure_action == "kill":
                    logger.critical(
                        "Heartbeat failed for task %s: %s, terminating worker.",
                        self.task_id,
                        e,
                    )
                    os.kill(os.getpid(), signal.SIGTERM)
                    return
                else:
                    logger.critical(
                        "Heartbeat failed for task %s: %s, flagged as unhealthy.",
                        self.task_id,
                        e,
                    )

    async def __aenter__(self) -> AsyncTaskLifecycle:
        """Start the heartbeat task."""
        self._task = asyncio.create_task(self._heartbeat_loop())
        logger.debug(
            "Task lifecycle entered (async): limiter=%s, task_id=%s, heartbeat_interval_s=%.3f.",
            self.limiter.id,
            self.task_id,
            self.interval,
        )
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Stop the heartbeat task, release the concurrency slot, and trigger a follow-up drain."""
        self._stop_event.set()
        if self._task is not None and not self._task.done():
            try:
                await asyncio.wait_for(self._task, timeout=1.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                self._task.cancel()

        try:
            removed_concurrency = await self.limiter.redis.zrem(
                self.limiter.concurrency_key, self.task_id
            )

            inflight_removed = 0
            if self.task_id:
                inflight_key = self.limiter.get_inflight_key(self.task_id)
                # fmt: off
                inflight_removed = cast(  # pragma: no mutate
                    int, await self.limiter.redis.delete(inflight_key)
                )
                # fmt: on

            logger.debug(
                "Concurrency slot released and inflight key cleared (async): limiter=%s, task_id=%s, removed_concurrency=%s, removed_inflight=%s.",
                self.limiter.id,
                self.task_id,
                removed_concurrency == 1,
                inflight_removed == 1,
            )
        finally:
            logger.debug(
                "Task lifecycle exited, triggering follow-up consume (async): limiter=%s, task_id=%s.",
                self.limiter.id,
                self.task_id,
            )
            await self.limiter.trigger_consume()


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
                    "Unhandled exception escaped drain() in AsyncDrainLoop: limiter=%s.",
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
                    "Drain signal subscriber error (async): limiter=%s.",
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
            if self._shutdown_event.is_set():
                return
            await self._run_once()

    async def _run_once(self) -> None:
        try:
            healthy = await self._limiter._check_backend_health()
        except Exception:
            logger.debug(
                "Backend health check raised an exception (async): limiter=%s.",
                self._limiter.id,
                exc_info=True,
            )
            healthy = False

        if self._healthy and not healthy:
            logger.warning(
                "Backend health check failed (async): limiter=%s. "
                "Workers may be unavailable; dispatched tasks will not "
                "complete until the backend recovers.",
                self._limiter.id,
            )
        elif not self._healthy and healthy:
            logger.info(
                "Backend health check recovered (async): limiter=%s. "
                "Workers are available again.",
                self._limiter.id,
            )

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

        # Register Lua scripts with the Redis server.
        await self._register_script("consume.lua")
        await self._register_script("schedule.lua")
        await self._register_script("health.lua")
        await self._register_script("renew.lua")

        if self._drain_signal_subscriber is not None:
            await self._drain_signal_subscriber.start()

        if self._backend_health_monitor is not None:
            self._backend_health_monitor.start()

        logger.info(
            "Async rate limiter initialized: id=%s, limit=%d, window_s=%g, max_concurrency=%d, max_age_s=%d, lease_duration_s=%d, heartbeat_failure=%s, jitter_enabled=%s, jitter_min_pct=%.3f, jitter_max_pct=%.3f, metrics_callback=%s, drain_enabled=%s, backend_health_monitor=%s.",
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

    # ---------------------------------------------------------------------------
    # Task scheduling
    # ---------------------------------------------------------------------------

    async def _cleanup_inflight_key(self, inflight_key: str, task_id: str) -> None:
        """Perform a best-effort cleanup of an in-flight key following a scheduling failure."""
        try:
            removed = await self.redis.delete(inflight_key)
            logger.debug(
                "Inflight cleanup attempted (async): limiter=%s, task_id=%s, inflight_key=%s, removed=%s.",
                self.id,
                task_id,
                inflight_key,
                removed,
            )
        except Exception as cleanup_error:
            logger.warning(
                "Failed to cleanup inflight key after schedule failure (async): limiter=%s, task_id=%s, error=%s.",
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
        """
        task_signature = self._get_task_signature_str(func_path, payload)
        task_id = hashlib.md5(task_signature.encode()).hexdigest()
        logger.debug(
            "Scheduling task attempt (async): limiter=%s, task_id=%s, func_path=%s, priority=%d.",
            self.id,
            task_id,
            func_path,
            priority,
        )

        inflight_key = self.get_inflight_key(task_id)
        inflight_ttl = self._get_inflight_ttl(max_age_override=max_age)
        if not await self.redis.set(inflight_key, "1", ex=inflight_ttl, nx=True):
            logger.debug(
                "Task already in-flight (async): limiter=%s, task_id=%s.",
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
                "Task scheduled (async): limiter=%s, task_id=%s, func_path=%s, priority=%d.",
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
    # Task consumption and lease management
    # ---------------------------------------------------------------------------

    async def consume(self) -> ConsumeResult:
        """Attempt to consume a task from the queue.

        Returns:
            A result containing the task data if consumption was successful.
        """
        logger.debug("Consume attempt started (async): limiter=%s.", self.id)

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
            ),
        )
        # fmt: on

        consume_result: ConsumeResult = {
            "success": int(result[0]) == 1,
            "expired": int(result[0]) == -1,
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
            "Consume result (async): limiter=%s, success=%s, task_id=%s, remaining_tokens=%d, active_concurrency=%d.",
            self.id,
            consume_result["success"],
            task_id,
            consume_result["remaining_tokens"],
            consume_result["active_concurrency"],
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
            "Lease extension result (async): limiter=%s, task_id=%s, duration_s=%d, renewed=%s.",
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
            if hasattr(self, "refresh_config"):
                await self.refresh_config()

            if hasattr(self, "_paused_until") and time.time() < self._paused_until:
                remaining = self._paused_until - time.time()
                logger.debug(
                    "Drain deferred (async): limiter=%s is paused for %.3fs.",
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
                "Drain failed (async, attempt #%d), scheduling recovery in %.3fs: limiter=%s.",
                self._consecutive_drain_failures,
                delay,
                self.id,
                exc_info=True,
            )
            try:
                self._schedule_drain(delay=delay)
            except Exception:
                logger.critical(
                    "Recovery scheduling also failed (async): limiter=%s.",
                    self.id,
                    exc_info=True,
                )

    async def _drain_inner(self) -> None:
        """Execute the core drain logic: consume, dispatch, and schedule a follow-up."""
        logger.debug("Drain loop start (async): limiter=%s.", self.id)

        if not self._has_local_capacity():
            logger.debug(
                "Drain deferred: local execution capacity reached (async) for limiter=%s.",
                self.id,
            )
            self._schedule_drain(delay=self._token_interval)
            return

        async with self.execution_lock() as acquired:
            logger.debug(
                "Drain lock result (async): limiter=%s, acquired=%s.",
                self.id,
                acquired,
            )
            if not acquired:
                logger.debug(
                    "Drain skipped: lock held by another drainer (async): limiter=%s.",
                    self.id,
                )
                self._schedule_backup_drain()
                return

            result = await self.consume()

            if result["expired"]:
                logger.warning(
                    "Expired task moved to DLQ during consume (async): limiter=%s.",
                    self.id,
                )

            if result["success"] and result["task"]:
                task = result["task"]
                task_id = task.get("id", "")

                await self._dispatch_task(
                    func_path=task["func_path"],
                    payload=task["payload"],
                    task_id=task_id,
                )
                logger.info(
                    "Task dispatched (async): limiter=%s, task_id=%s, func_path=%s.",
                    self.id,
                    task_id,
                    task["func_path"],
                )

                if result["remaining_tasks"] > 0:
                    logger.debug(
                        "More tasks remain, scheduling follow-up drain (async): limiter=%s, remaining=%d.",
                        self.id,
                        result["remaining_tasks"],
                    )
                    self._schedule_drain()

            elif result["remaining_tasks"] == 0:
                logger.debug(
                    "Drain stopped: buffer empty (async) for limiter=%s.", self.id
                )

            elif result["active_concurrency"] >= self.max_concurrency:
                logger.debug(
                    "Drain stopped: concurrency at capacity (async) for limiter=%s (active=%d, max=%d).",
                    self.id,
                    result["active_concurrency"],
                    self.max_concurrency,
                )

            elif result["remaining_tokens"] <= 0:
                val_previous = result["val_previous"]
                val_current = result["val_current"]
                base_delay = self._calculate_token_recovery_delay(
                    val_previous=val_previous,
                    val_current=val_current,
                    reset_in_ms=result.get("reset_in_ms", 0),
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
                    "Rate limited (async): limiter=%s, delay_s=%.3f, base_delay_s=%.3f, jitter_s=%.3f, remaining_tasks=%d.",
                    self.id,
                    delay_seconds,
                    base_delay,
                    jitter,
                    result["remaining_tasks"],
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
        logger.debug("Trigger consume scheduling drain (async): limiter=%s.", self.id)
        self._schedule_drain()
        await self._publish_drain_signal()

    async def _publish_drain_signal(self) -> None:
        """Publish a drain signal for cross-process notification."""
        try:
            await self.redis.publish(self._drain_signal_channel, self._worker_id)
        except Exception:
            logger.debug(
                "Failed to publish drain signal (async): limiter=%s.",
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
                stacklevel=1,
            )

    def execution_lock(self, timeout_ms: int = 5000) -> AsyncDistributedLock:
        """Create an async distributed lock for drain serialization.

        Args:
            timeout_ms: The lock timeout in milliseconds.

        Returns:
            An ``AsyncDistributedLock`` async context manager.
        """
        cooldown_ms = min(
            int((self.window / self.limit) * 1000) if self.limit > 0 else 0,
            1000,
        )
        return AsyncDistributedLock(
            redis_client=self.redis,
            lock_key=self.lock_key,
            timeout_ms=timeout_ms,
            worker_id=self._worker_id,
            cooldown_ms=cooldown_ms,
            contention_key=self.contention_key,
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
            "dispatcher": {
                "is_locked": await self.redis.exists(f"{self.id}:dispatch_lock")
            },
        }
