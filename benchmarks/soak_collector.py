"""Non-intrusive metric collector for sustained soak tests.

Runs a daemon thread that samples process, limiter, connection pool,
and Redis server metrics at a configurable interval. All Redis reads
use a dedicated sync client that never touches the limiter's pool.
"""

from __future__ import annotations

import gc
import os
import threading
import time
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any

import redis as sync_redis

if TYPE_CHECKING:
    from redis_rate_limiter.core.async_limiters import (
        AbstractAsyncDistributedRateLimiter,
    )
    from redis_rate_limiter.core.limiters import AbstractDistributedRateLimiter


@dataclass(frozen=True, slots=True)
class SoakSnapshot:
    elapsed_s: float

    vm_rss_kb: int
    vm_size_kb: int
    thread_count: int
    thread_names: tuple[str, ...]
    fd_count: int

    gc_gen0_collections: int
    gc_gen1_collections: int
    gc_gen2_collections: int

    heartbeat_entries: int
    heartbeat_heap: int

    pool_created: int
    pool_available: int
    pool_in_use: int

    redis_used_memory: int
    redis_used_memory_rss: int
    redis_mem_fragmentation_ratio: float

    buffer_zcard: int
    buffer_memory_bytes: int
    concurrency_zcard: int
    concurrency_memory_bytes: int
    dlq_llen: int

    cumulative_dispatches: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class _DispatchCounter:
    """Metrics callback that counts consume-success events."""

    def __init__(self) -> None:
        self.count = 0

    def __call__(self, event: str, data: dict) -> None:
        if event == "consume" and data.get("success"):
            self.count += 1


def _read_proc_status() -> tuple[int, int]:
    """Return ``(VmRSS, VmSize)`` in KB from ``/proc/self/status``."""
    rss = 0
    vmsz = 0
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    rss = int(line.split()[1])
                elif line.startswith("VmSize:"):
                    vmsz = int(line.split()[1])
    except OSError:
        pass
    return rss, vmsz


def _count_fds() -> int:
    try:
        return len(os.listdir("/proc/self/fd"))
    except OSError:
        return 0


class SoakCollector:
    """Background sampler that collects time-series metrics during a soak test.

    All reads of limiter internals (``_heartbeat_scheduler._entries``,
    ``._heap``, ``redis.connection_pool``) are GIL-atomic ``len()`` calls
    on built-in containers. All Redis commands use the dedicated
    ``monitor_redis`` client, never the limiter's pool.
    """

    def __init__(
        self,
        limiter: AbstractDistributedRateLimiter | AbstractAsyncDistributedRateLimiter,
        monitor_redis: sync_redis.Redis,
        dispatch_counter: _DispatchCounter,
        interval: float = 1.0,
    ) -> None:
        self._limiter = limiter
        self._monitor_redis = monitor_redis
        self._counter = dispatch_counter
        self._interval = interval

        self._pool = limiter.redis.connection_pool
        self._has_created_connections = hasattr(self._pool, "_created_connections")

        self._snapshots: list[SoakSnapshot] = []
        self._stop_event = threading.Event()
        self._start_mono: float = 0.0
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._start_mono = time.monotonic()
        self._thread = threading.Thread(
            target=self._run, name="SoakCollector", daemon=True
        )
        self._thread.start()

    def stop(self) -> list[SoakSnapshot]:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=5.0)
        return list(self._snapshots)

    def _run(self) -> None:
        while not self._stop_event.wait(self._interval):
            self._snapshots.append(self._sample())

    def _sample(self) -> SoakSnapshot:
        elapsed = time.monotonic() - self._start_mono

        rss, vmsz = _read_proc_status()

        stats = gc.get_stats()
        gc_collections = [s.get("collections", 0) for s in stats]

        scheduler = self._limiter._heartbeat_scheduler  # type: ignore[union-attr]
        hb_entries = len(scheduler._entries)
        hb_heap = len(scheduler._heap)

        if self._has_created_connections:
            pool_created = self._pool._created_connections
        else:
            pool_created = len(self._pool._available_connections) + len(
                self._pool._in_use_connections
            )
        pool_available = len(self._pool._available_connections)
        pool_in_use = len(self._pool._in_use_connections)

        info = self._monitor_redis.info("memory")
        used_mem = int(info.get("used_memory", 0))
        used_mem_rss = int(info.get("used_memory_rss", 0))
        frag = float(info.get("mem_fragmentation_ratio", 1.0))

        limiter = self._limiter
        buf_card = self._monitor_redis.zcard(limiter.buffer_key)
        buf_mem = self._monitor_redis.memory_usage(limiter.buffer_key) or 0
        conc_card = self._monitor_redis.zcard(limiter.concurrency_key)
        conc_mem = self._monitor_redis.memory_usage(limiter.concurrency_key) or 0
        dlq_len = self._monitor_redis.llen(limiter.dlq_key)

        return SoakSnapshot(
            elapsed_s=elapsed,
            vm_rss_kb=rss,
            vm_size_kb=vmsz,
            thread_count=threading.active_count(),
            thread_names=tuple(t.name for t in threading.enumerate()),
            fd_count=_count_fds(),
            gc_gen0_collections=gc_collections[0] if len(gc_collections) > 0 else 0,
            gc_gen1_collections=gc_collections[1] if len(gc_collections) > 1 else 0,
            gc_gen2_collections=gc_collections[2] if len(gc_collections) > 2 else 0,
            heartbeat_entries=hb_entries,
            heartbeat_heap=hb_heap,
            pool_created=pool_created,
            pool_available=pool_available,
            pool_in_use=pool_in_use,
            redis_used_memory=used_mem,
            redis_used_memory_rss=used_mem_rss,
            redis_mem_fragmentation_ratio=frag,
            buffer_zcard=buf_card,
            buffer_memory_bytes=buf_mem,
            concurrency_zcard=conc_card,
            concurrency_memory_bytes=conc_mem,
            dlq_llen=dlq_len,
            cumulative_dispatches=self._counter.count,
        )
