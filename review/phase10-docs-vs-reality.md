# Phase 10: Architecture Documentation vs. Code Reality

## Scope

Compared all ten architecture documentation files against the actual source code to identify inaccuracies, stale references, and gaps.

**Docs reviewed:** `docs/architecture/README.md`, `class-hierarchy.md`, `components.md`, `drain-flow.md`, `error-handling.md`, `redis-keys.md`, `sliding-window.md`, `task-lifecycle.md`, `task-states.md`, `docs/smart-jitter.md`

**Code spot-checked:** `core/limiters.py`, `core/async_limiters.py`, `core/base.py`, `core/managed.py`, `lua/consume.lua`, `__init__.py`, all backend `limiter.py` files.

---

## Summary

The documentation is remarkably well-maintained. Class hierarchies, algorithm descriptions, Redis key patterns, drain flow logic, and error handling paths all match the code precisely. Only minor issues were found.

**Findings: 1 inaccuracy, 1 gap, 0 stale references, 0 missing code features.**

---

## Findings

### F1: INACCURACY -- smart-jitter.md load pressure thresholds are wrong

**File:** `docs/smart-jitter.md`, lines 50-52

**Documented:**
> - **High load** (200+ tasks waiting): uses 70-100% of the jitter range
> - **Medium load** (10-100 tasks): uses 50-70%
> - **Low load** (<10 tasks): uses 30-50%

**Actual code** (`core/limiters.py` lines 699-708):
```python
if remaining_tasks <= 0:
    load_pressure = 0.0
elif remaining_tasks < 10:
    load_pressure = 0.2
elif remaining_tasks < 50:
    load_pressure = 0.5
elif remaining_tasks < 100:
    load_pressure = 0.7
else:  # >= 100, not 200+
    load_pressure = 1.0
```

The "200+" threshold in the doc should be "100+". The five-tier step function in the code (0/10/50/100) does not match the three-tier description (10/100/200) in the doc. The stated jitter range percentages ("70-100%", "50-70%", "30-50%") are also only rough approximations of the actual `jitter_scale = 0.3 + combined_pressure * 0.7` formula -- the real range depends on concurrency pressure weighting, which the doc omits from this summary section (though the algorithm section further down is accurate).

**Severity:** Low. The algorithm details section of the same doc (lines 138-175) shows the correct code. Only the prose summary section is inaccurate.

**Fix:** Update the prose thresholds to match the five-tier code or add a note that the summary is approximate.

---

### F2: GAP -- redis-keys.md omits the Pub/Sub drain signal channel

**File:** `docs/architecture/redis-keys.md`

The `{id}:drain_signal` Pub/Sub channel is used for cross-process drain coordination (referenced in `drain-flow.md` and implemented as `self._drain_signal_channel = f"{self.id}:drain_signal"` at `core/limiters.py` line 558). It is not listed in the Redis Key Map, even though that document is described as a "comprehensive reference of every Redis key used by the system."

While Pub/Sub channels are not stored keys, they are Redis primitives that operators need to know about (e.g., for monitoring, ACLs, or network policy). The key map already covers all other Redis touch-points.

**Severity:** Low. The channel is documented in `drain-flow.md` section "Cross-process Pub/Sub", so the information is not lost -- it is just missing from the centralized key reference.

**Fix:** Add a row to the key reference table for `{id}:drain_signal` with type "PUB/SUB", noting it is a channel rather than a stored key.

---

## Verified as Correct

### Class hierarchy (class-hierarchy.md)

All class names and inheritance chains verified against code:

| Documented | Actual | Status |
|---|---|---|
| `AbstractRateLimiter` | `base.py:28` | Correct |
| `AbstractSyncRateLimiter(AbstractRateLimiter)` | `base.py:89` | Correct |
| `AbstractAsyncRateLimiter(AbstractRateLimiter)` | `base.py:169` | Correct |
| `DistributedRateLimiterMixin(AbstractRateLimiter)` | `limiters.py:500` | Correct |
| `AbstractDistributedRateLimiter(DistributedRateLimiterMixin, AbstractSyncRateLimiter)` | `limiters.py:832` | Correct |
| `AbstractAsyncDistributedRateLimiter(DistributedRateLimiterMixin, AbstractAsyncRateLimiter)` | `async_limiters.py:440` | Correct |
| `ManagedRateLimiterMixin` | `managed.py:31` | Correct |
| `SyncManagedRateLimiter(ManagedRateLimiterMixin)` | `managed.py:176` | Correct |
| `AsyncManagedRateLimiter(ManagedRateLimiterMixin)` | `managed.py:410` | Correct |
| `CeleryRateLimiter(SyncManagedRateLimiter, AbstractDistributedRateLimiter)` | `backends/celery/limiter.py:18` | Correct |
| `ThreadPoolRateLimiter(SyncManagedRateLimiter, AbstractDistributedRateLimiter)` | `backends/threading/limiter.py:19` | Correct |
| `AsyncIOTaskLimiter(AsyncManagedRateLimiter, AbstractAsyncDistributedRateLimiter)` | `backends/asyncio/limiter.py:23` | Correct |
| `ASGIRateLimiter(AsyncManagedRateLimiter, AbstractAsyncRateLimiter)` | `backends/asgi/limiter.py:24` | Correct |

Helper class pairs (sync/async) all present: `DistributedLock`/`AsyncDistributedLock`, `TaskLifecycle`/`AsyncTaskLifecycle`, `DrainLoop`/`AsyncDrainLoop`, `DrainSignalSubscriber`/`AsyncDrainSignalSubscriber`.

### Sliding window algorithm (sliding-window.md, consume.lua)

- Weight formula: `(window_size_ms - time_passed_in_current) / window_size_ms` -- matches `consume.lua` line 48.
- Estimate formula: `current_count + (previous_count * weight)` -- matches `consume.lua` line 49.
- Window counter TTL: `2 * window_size_ms + 10000` applied via `PEXPIRE` when `current_count == 0` -- matches `consume.lua` lines 96-98.
- 2x burst bound explanation and referenced property tests are consistent.

### Redis keys (redis-keys.md)

All key patterns verified against `DistributedRateLimiterMixin.__init__()` at `limiters.py:552-558`:

| Documented Key | Code Assignment | Status |
|---|---|---|
| `{id}:buffer` | `self.buffer_key = f"{self.id}:buffer"` | Correct |
| `{id}:concurrency` | `self.concurrency_key = f"{self.id}:concurrency"` | Correct |
| `{id}:dispatch_lock` | `self.lock_key = f"{self.id}:dispatch_lock"` | Correct |
| `{id}:dispatch_lock:contention` | `self.contention_key = f"{self.id}:dispatch_lock:contention"` | Correct |
| `{id}:dlq` | `self.dlq_key = f"{self.id}:dlq"` | Correct |
| `{id}:inflight:{task_id}` | `get_inflight_key()` at line 597 | Correct |
| `{id}:dispatch_lock:cd:{worker_id}` | `AsyncDistributedLock.__init__()` at `async_limiters.py:81` | Correct |
| `{id}:{window_start_ms}` | `consume.lua` lines 36-38 | Correct |
| `rl:registry:configs` | `managed.py:43` `_REGISTRY_KEY` | Correct |
| `rl:registry:versions` | `managed.py:44` `_VERSION_KEY` | Correct |

### Drain flow (drain-flow.md)

- Three-layer structure (DrainLoop._run / drain / _drain_inner) matches code at lines 361+, 1180+, 1230+.
- Backoff formula `min(window, 0.1 * 2^(n-1))` matches code at line 1209-1211.
- Six feedback entry points all verified in code.
- Watchdog interval `max(5.0, window * 2)` matches constructor at `async_limiters.py:505`.
- Contention-aware cooldown mechanism matches Lua scripts at `limiters.py:70-97`.
- Token recovery delay formula matches code at lines 618-665.

### Error handling (error-handling.md)

- All line number references in the doc match current code positions exactly.
- All 35 failure modes in the traceability table correspond to real code paths.
- All referenced source file paths exist.
- NoScript two-phase retry in `_eval_script()` matches doc at `base.py:128` and `base.py:205`.

### Task lifecycle and states (task-lifecycle.md, task-states.md)

- Seven task states accurately described.
- `TaskLifecycle.__exit__()` try/finally pattern matches code at lines 320-358.
- Self-healing `ZREMRANGEBYSCORE` in `consume.lua` line 29 matches doc.
- Inflight TTL formula `max(1, max_age) + max(1, lease_duration) + max(1, window)` matches `_get_inflight_ttl()` at line 599.

### Components diagram (components.md)

- All four architectural layers accurately depicted.
- ASGI request flow accurately describes `acquire.lua` path and fail_open/fail_closed middleware behavior.
- Backend names and dispatch methods match code.

### Public API (__init__.py)

All exports in `__all__` match documented classes. `PrometheusMetricsExporter` and utility functions (`import_string`, `resolve_import_path`) are exported but not covered in architecture docs -- this is acceptable as they are integration/utility concerns rather than architectural components.

---

## Conclusion

The architecture documentation is highly accurate and well-synchronized with the codebase. The two findings are both low severity: one is an inaccurate summary paragraph in `smart-jitter.md` where the detailed algorithm section is already correct, and the other is a missing Pub/Sub channel entry in the Redis key map that is already documented elsewhere.
