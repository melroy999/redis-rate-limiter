# Code Review: redis-rate-limiter

External review performed with full codebase context. Findings are grouped by
category. Each item is self-contained so that it can be tackled in an
independent Claude session without needing to re-read unrelated files.

---

## A. Bugs / Correctness

### A1. ASGI import is unguarded but fastapi is optional

**Files:** `src/redis_rate_limiter/__init__.py`, `pyproject.toml`

Celery and Prometheus imports are wrapped in `try/except ImportError`.
The ASGI import on line 38 is not, yet `fastapi` is declared as an optional
dependency under `[project.optional-dependencies] asgi`. A bare
`pip install redis-rate-limiter` will crash on import.

**Fix:** wrap the ASGI import in the same `try/except ImportError` pattern
used for Celery (lines 30-33).

---

### A2. `_persist_config` is not atomic

**Files:** `src/redis_rate_limiter/core/managed.py` lines 360-369 (sync),
lines 611-630 (async)

Three separate Redis calls (`HSET`, `HINCRBY`, `HGET`) are used to persist
config and bump the version. Between `HSET` and `HINCRBY`, another process
running `refresh_config` could see the new config with the old version number
and skip it. This contradicts the project's own design principle of atomic
Redis operations via Lua scripts.

**Fix:** use a Redis pipeline for the write path, or a small Lua script that
performs all three operations atomically.

---

### A3. `_shutdown_called` is never initialized in `__init__`

**Files:** `src/redis_rate_limiter/core/limiters.py` line 1421,
`src/redis_rate_limiter/core/async_limiters.py` line 943

`_shutdown_called` is first set inside `shutdown()`. The `__del__` method
uses `getattr(self, "_shutdown_called", False)` as a safety net, but this
means the attribute doesn't exist on live instances until shutdown is called.
Initializing it to `False` in `__init__` would be cleaner and consistent
with how `_consecutive_drain_failures` is handled.

**Fix:** add `self._shutdown_called: bool = False` in both
`AbstractDistributedRateLimiter.__init__` and
`AbstractAsyncDistributedRateLimiter.__init__`.

---

## B. Code Smells / Design

### B1. `hasattr` checks in `drain()` for own-class attributes

**Files:** `src/redis_rate_limiter/core/limiters.py` lines 1190-1195,
`src/redis_rate_limiter/core/async_limiters.py` lines 754-757

`drain()` does `hasattr(self, "refresh_config")` and
`hasattr(self, "_paused_until")` to guard against being used without the
managed mixin. However:

- `_paused_until` is already initialized in `AbstractRateLimiter.__init__`
  (line 52 of `base.py`), so the `hasattr` check is unnecessary today.
- `refresh_config` could be a no-op on the base class instead of a runtime
  `hasattr` check.

**Fix:** remove the `hasattr` for `_paused_until` (it's always present).
For `refresh_config`, add a no-op method to `DistributedRateLimiterMixin`
or `AbstractRateLimiter`.

---

### B2. Private attribute access: `executor._max_workers`

**File:** `src/redis_rate_limiter/backends/threading/limiter.py` line 109

`ThreadPoolExecutor._max_workers` is a CPython implementation detail, not
part of the public API. The async counterpart (`AsyncIOTaskLimiter`) avoids
this by accepting an explicit `max_tasks` parameter.

**Fix:** accept `max_workers` as a constructor parameter (mirroring the
async backend's `max_tasks` approach) or read it once during `__init__`
and store it.

---

### B3. `asyncio.ensure_future` is deprecated

**File:** `src/redis_rate_limiter/core/async_limiters.py` line 315

`asyncio.ensure_future` has been soft-deprecated since Python 3.10 in favor
of `asyncio.create_task`.

**Fix:** replace with `asyncio.create_task(self._wake_async(target))`.

---

### B4. `get_status()` returns raw Lua result types inconsistently

**Files:** `src/redis_rate_limiter/core/limiters.py` lines 1527-1546,
`src/redis_rate_limiter/core/async_limiters.py` lines 1031-1052

Some values in the returned dict are cast to Python types (`int`, `float`),
others are left as raw Lua result strings (e.g., `result[0]`, `result[3]`,
`result[5]`). Compare with `consume()` which casts every field.

**Fix:** cast all values to their proper Python types (`int` / `float`)
for a consistent API.

---

### B5. `health.lua` doesn't clean expired leases before counting

**File:** `src/redis_rate_limiter/lua/health.lua`

`consume.lua` runs `ZREMRANGEBYSCORE` to remove expired leases before
counting active concurrency (line 29). `health.lua` does not. This means
`get_status()` can report stale concurrency counts that include expired
leases from crashed workers.

**Fix:** add the same `ZREMRANGEBYSCORE` cleanup to `health.lua` before
the `ZCARD` call, matching the pattern in `consume.lua`.

---

## C. Sync/Async Drift Risk

The sync (`limiters.py`, ~1550 lines) and async (`async_limiters.py`,
~1050 lines) implementations are manually maintained copies.

### Current drift points observed: none (they match today).

### Structural risk

Any fix to drain logic, consume parsing, lifecycle handling, or status
reporting must be applied to both files. There is no automated check for
this. Suggested mitigations:

1. Add a comment at the top of both files:
   `# MIRROR: changes here must be reflected in {other_file}.`
2. Add to `CLAUDE.md`:
   `Any change to limiters.py drain/consume/lifecycle logic MUST be mirrored
   in async_limiters.py. Always read both files before modifying either.`
3. Long-term: consider generating one from the other, or extracting shared
   logic into a template that produces both variants.

---

## D. Suggested CLAUDE.md Additions

Add a **Cross-Cutting Invariants** section to help future sessions maintain
consistency:

```markdown
## Cross-Cutting Invariants
- Optional backend imports in `__init__.py` MUST be wrapped in try/except ImportError.
- All Redis multi-step write mutations MUST be atomic (pipeline or Lua).
- Changes to drain/consume/lifecycle in `limiters.py` MUST be mirrored in `async_limiters.py`.
- Do not access private attributes of stdlib classes (e.g., `executor._max_workers`).
- `_paused_until` is always initialized by the base class; do not use hasattr for it.
```

---

## E. Session Hygiene for Multi-Session Development

The main risk with context-limited sessions is cross-file consistency.
The most token-efficient mitigation is encoding rules in `CLAUDE.md` so
every session starts with the right knowledge automatically.

Don't waste tokens loading files "just in case." Instead, put specific
rules in `CLAUDE.md` (section D above) and let Claude apply them when
it naturally encounters the relevant files. For example, adding
`limiters.py and async_limiters.py are mirrors; change one, change both`
means Claude will know to check the async file when it opens the sync
file — without being told upfront every session.

---

## Checklist

For tracking completion. Each item maps to a section above.

- [ ] A1: Guard ASGI import with try/except ImportError
- [ ] A2: Make `_persist_config` atomic (pipeline or Lua)
- [ ] A3: Initialize `_shutdown_called = False` in `__init__`
- [ ] B1: Remove unnecessary `hasattr` checks in `drain()`
- [ ] B2: Stop accessing `executor._max_workers`
- [ ] B3: Replace `asyncio.ensure_future` with `create_task`
- [ ] B4: Cast all `get_status()` values to proper Python types
- [ ] B5: Add lease cleanup to `health.lua`
- [ ] C: Add mirror comments and CLAUDE.md invariant rules
- [ ] D: Add cross-cutting invariants to CLAUDE.md
