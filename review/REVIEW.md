# Code Review: redis-rate-limiter

Full-context review covering the entire codebase (~37,000 lines across ~160 files).
Each finding is self-contained so it can be tackled in an independent session.

Findings are observations, not prescriptions. Some may have existing justifications.

---

## Scope

| Phase | Area | Verdict |
|-------|------|---------|
| Lua scripts (5 files) | Correctness, atomicity, cross-script consistency | 1 concern |
| Core source (base, scripts, decorators, importing) | Design, type safety, errors | Clean |
| Sync/async mirror (limiters.py vs async_limiters.py) | Structural divergences | 1 concern |
| Managed mixin (managed.py) | Lifecycle, config persistence, refresh | 2 concerns |
| Backends (celery, asyncio, ASGI, threading) | Contract compliance, dispatch, cleanup | 1 concern, 1 improvement |
| Prometheus integration | Metrics, labels, cardinality | 1 concern |
| Test suite (~18,000 lines, 646 tests) | Coverage gaps, contract quality | Thorough |
| Config & packaging | pyproject.toml, CI, Docker, linting | 2 medium findings |
| Docs vs reality (10 architecture docs) | Accuracy | Remarkably accurate |
| Examples & demo (15 files) | API compatibility | All pass |
| Cross-cutting | Error handling, naming, public API | 1 concern |

---

## A. Potential Correctness Issues

### A1. `_persist_config` is not atomic

**Files:** `core/managed.py` lines 360-369 (sync), lines 611-630 (async)

Three separate Redis calls (`HSET`, `HINCRBY`, `HGET`) persist config and bump the
version. Between `HSET` and `HINCRBY`, another process running `refresh_config` could
see the new config with the old version number and skip it. A pipeline or Lua script
would make this atomic.

**Impact:** Low probability (microsecond crash window), but violates the documented
"single source of truth" guarantee.

---

### A2. `health.lua` doesn't clean expired leases before counting

**File:** `lua/health.lua`

`consume.lua` runs `ZREMRANGEBYSCORE` to remove expired concurrency leases before
counting (line 29). `health.lua` does not. `get_status()` may report stale concurrency
counts that include expired leases from crashed workers.

This may be intentional (health checks shouldn't mutate state), but the status endpoint
can show a limiter as "at capacity" when it actually has free slots.

---

### A3. `get_status()` returns mixed types

**Files:** `core/limiters.py` lines 1527-1546, `core/async_limiters.py` lines 1031-1052

Some values in the returned dict are cast to Python types (`int`, `float`), others are
left as raw Lua result values (strings). Compare with `consume()` which casts every
field consistently. Consumers may get unexpected string values for `val_previous`,
`val_current`, `current` (concurrency), and `count` (buffer).

---

### A4. Sync `DrainSignalSubscriber` doesn't decode bytes

**File:** `core/limiters.py` line 476

The sync `_run()` reads `message["data"]` and compares it directly to
`self._limiter._worker_id` (a str). Redis-py's sync Pub/Sub returns bytes by default.
So `b"uuid-string" != "uuid-string"` is always `True`, meaning the sync subscriber
**never filters out self-notifications** — every drain signal triggers a local drain,
including the worker's own signals.

The async version (`async_limiters.py` lines 410-411) correctly handles this:
```python
if isinstance(sender_id, bytes):
    sender_id = sender_id.decode("utf-8")
```

**Impact:** Minor performance — redundant self-drains. The drain loop coalesces
wake-ups, so correctness is preserved. But the intent (filtering self-signals) is
silently broken in sync mode.

---

### A5. `__all__` lists names that may not be imported

**File:** `__init__.py` lines 13-27

`CeleryRateLimiter` and `PrometheusMetricsExporter` are imported inside `try/except
ImportError` blocks, but they're listed in `__all__`. If `celery` or `prometheus_client`
isn't installed, `from redis_rate_limiter import *` raises `AttributeError`.

---

### A6. Expired tasks drain one-per-cycle, throttling throughput

**File:** `lua/consume.lua` lines 63-88

The script pops the head of the buffer, then checks its age. If expired, the task
goes to the DLQ and the function returns `-1` without consuming a rate limit token.
This means one `consume()` call is "wasted" on the expired task. If many tasks expire
simultaneously (e.g., a burst that sat in the buffer too long), each drain cycle
removes only one expired task, creating latency before a valid task is reached.

**Improvement:** Consider a loop inside the Lua script that pops expired tasks until
a valid task is found or the buffer is empty, all within the same EVAL.

---

### A7. Prometheus: two exporters on the same registry will crash

**File:** `integrations/prometheus.py` lines 85-122

Each `PrometheusMetricsExporter.__init__` creates **new** `Counter` and `Gauge` objects.
If two exporters are created for different `limiter_id` values on the same registry
(including the default global), the second raises `ValueError: Duplicated timeseries
in CollectorRegistry`.

**Impact:** Multi-limiter setups with Prometheus crash at startup.

---

## B. Code Observations

### B1. `hasattr(self, "_paused_until")` is redundant

**Files:** `core/limiters.py` line 1195, `core/async_limiters.py` line 757

`_paused_until` is initialized to `0.0` in `AbstractRateLimiter.__init__` (line 52 of
`base.py`). The `hasattr` guard in `drain()` is unnecessary.

---

### B2. Private attribute access: `executor._max_workers`

**File:** `backends/threading/limiter.py` line 109

`ThreadPoolExecutor._max_workers` is a CPython implementation detail. The async
counterpart avoids this by accepting an explicit `max_tasks` constructor parameter.

---

### B3. `asyncio.ensure_future` is deprecated

**File:** `core/async_limiters.py` line 315

Soft-deprecated since Python 3.10. Preferred replacement is `asyncio.create_task`.

---

### B4. Threading backend silently swallows task exceptions

**File:** `backends/threading/limiter.py` lines 121-127

The `_run_task` closure uses a bare `try/finally`. User function exceptions propagate
silently into the `ThreadPoolExecutor` (which swallows them). The asyncio backend
explicitly catches and logs exceptions. The threading backend should do the same.

---

### B5. `@rate_limited` decorator has undocumented required kwarg

**File:** `core/decorators.py` line 57

`kwargs.pop("_rate_limit_task_id")` raises a raw `KeyError` if the kwarg is missing.
This is an internal protocol (the dispatcher injects it), but calling a decorated
function directly produces a confusing error with no helpful message.

---

### B6. No shutdown of replaced instances on `create(override=True)`

**File:** `core/managed.py` lines 243-256, 482-496

When an existing instance is overwritten, the old instance's background threads/tasks
(drain loop, pubsub subscriber) continue running as orphans. Neither documented nor
enforced.

---

### B7. `refresh_config` TOCTOU between version check and config read

**File:** `core/managed.py` lines 371-406 (sync), 632-667 (async)

`refresh_config()` reads the version, then reads the config. Between the two reads,
another `update()` could change the config. The worker applies a config that doesn't
match the version it recorded.

**Impact:** Transient inconsistency. Eventually converges on the next refresh cycle.
Acceptable for a distributed system.

---

## C. Configuration & Packaging

### C1. mypy global `ignore_missing_imports` hides real import errors

**File:** `mypy.ini`

The global `ignore_missing_imports = True` makes per-library overrides (`[mypy-redis.*]`,
`[mypy-celery.*]`) redundant and silences all import errors, including typos and
forgotten installs. Move to per-library sections only.

---

### C2. `|| true` in CI pytest command masks test failures

**File:** `.github/workflows/ci.yml` line ~75

If pytest crashes before writing the coverage XML, the job silently continues. The
coverage check step would fail with a misleading error ("Error parsing coverage" instead
of "tests failed"). Consider capturing the exit code explicitly.

---

### C3. Minor packaging items

- `pyproject.toml`: `description` field is empty.
- `pyproject.toml`: `asgi` extra has no upper-bound pins on `fastapi`/`uvicorn`.
- `ruff.toml`: Rule set is minimal; `B` (bugbear), `UP` (pyupgrade), `SIM` (simplify) would catch more.
- `ci.yml`: `ruff format --check` not run; formatting drift undetected.
- `ci.yml`: No Python version matrix (only 3.12 tested).
- `Dockerfile`: Production image only includes `celery` extra; `prometheus` and `asgi` extras excluded.

---

## D. Documentation

### D1. `smart-jitter.md` load pressure thresholds are inaccurate

**File:** `docs/smart-jitter.md` lines 50-52

The prose summary says "200+ tasks" for high load. The code uses a five-tier step
function with thresholds at 0/10/50/100 (not 10/100/200). The detailed algorithm
section further down in the same file is correct.

---

### D2. `redis-keys.md` omits Pub/Sub drain signal channel

**File:** `docs/architecture/redis-keys.md`

The `{id}:drain_signal` Pub/Sub channel is documented in `drain-flow.md` but missing
from the centralized key reference. Add a row for completeness.

---

## E. Sync/Async Mirror

`limiters.py` (~1,550 lines) and `async_limiters.py` (~1,050 lines) are manually
maintained mirrors. The shared `DistributedRateLimiterMixin` eliminates most
duplication, but the helper classes (lock, lifecycle, drain loop, signal subscriber)
are duplicated. Finding A4 (bytes decode) demonstrates how the mirrors can silently
diverge.

**Suggested CLAUDE.md rule:**
```
limiters.py and async_limiters.py are manually-maintained mirrors.
Any change to one MUST be reflected in the other.
```

---

## F. Test Suite Assessment

646 tests across 7 categories. Every source module has corresponding tests. Contract
tests verify genuine interface guarantees (not implementation details) and are
inherited by all backends. Property tests (Hypothesis) cover mathematical invariants.
Mutation score: 99.7% (1,906/1,912 killed).

### Gaps

1. **No input validation** for `window=0`, negative `limit`/`window`/`max_concurrency`,
   `lease_duration=0`. Documented as deferred in `MIGRATION_STATUS.md`.
2. **No ASGI contract test class.** Would matter if a second ASGI implementation is added.
3. **DrainSignalSubscriber** error handling and worker_id filtering paths lack direct tests.
4. **Prometheus**: No test for duplicate-registration crash (A6) or missing data keys.

---

## Checklist

### Correctness
- [ ] A1: Evaluate atomicity of `_persist_config`
- [ ] A2: Decide if `health.lua` should clean expired leases
- [ ] A3: Cast all `get_status()` values to proper Python types
- [ ] A4: Add bytes decode to sync `DrainSignalSubscriber._run()`
- [ ] A5: Fix `__all__` for optional dependencies
- [ ] A6: Consider looping over expired tasks in `consume.lua` instead of one-per-cycle
- [ ] A7: Fix Prometheus duplicate registration on same registry

### Code quality
- [ ] B1: Remove redundant `hasattr` for `_paused_until`
- [ ] B2: Evaluate `executor._max_workers` access
- [ ] B3: Replace `asyncio.ensure_future` with `create_task`
- [ ] B4: Add exception logging to threading backend `_run_task`
- [ ] B5: Add descriptive error for missing `_rate_limit_task_id`
- [ ] B6: Document or enforce old-instance shutdown on `create(override=True)`
- [ ] B7: Document TOCTOU in `refresh_config` as known limitation

### Config & packaging
- [ ] C1: Move `ignore_missing_imports` to per-library sections
- [ ] C2: Fix `|| true` masking test failures in CI

### Docs
- [ ] D1: Update `smart-jitter.md` load pressure thresholds
- [ ] D2: Add drain signal channel to `redis-keys.md`

### Mirror
- [ ] E: Add sync/async mirror rule to CLAUDE.md
