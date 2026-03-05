# Code Review: redis-rate-limiter

Full-context external review. Each finding is self-contained so it can be
tackled in an independent Claude session without re-reading unrelated files.

Findings are observations, not prescriptions. Some may have existing
justifications we're not aware of — the student should evaluate each one.

---

## A. Findings: Potential Correctness Issues

### A1. `_persist_config` is not atomic

**Files:** `src/redis_rate_limiter/core/managed.py` lines 360-369 (sync),
lines 611-630 (async)

Three separate Redis calls (`HSET`, `HINCRBY`, `HGET`) persist config and
bump the version. Between `HSET` and `HINCRBY`, another process running
`refresh_config` could see the new config with the old version number and
skip it. The rest of the project uses Lua scripts for atomic Redis
operations — this is the only multi-step write that doesn't.

**Question:** is there a reason this isn't pipelined or in a Lua script?
If config updates are rare enough that the race is acceptable, that's a
valid answer — but it should be documented.

---

### A2. `health.lua` doesn't clean expired leases before counting

**File:** `src/redis_rate_limiter/lua/health.lua`

`consume.lua` runs `ZREMRANGEBYSCORE` to remove expired concurrency leases
before counting (line 29 of `consume.lua`). `health.lua` does not. This
means `get_status()` may report stale concurrency counts that include
expired leases from crashed workers.

This could be intentional (health checks shouldn't mutate state), but it
means the status endpoint can show a limiter as "at capacity" when it
actually has free slots.

---

### A3. `get_status()` returns mixed types

**Files:** `src/redis_rate_limiter/core/limiters.py` lines 1527-1546,
`src/redis_rate_limiter/core/async_limiters.py` lines 1031-1052

Some values in the returned dict are cast to Python types (`int`, `float`),
others are left as raw Lua result values (e.g., `result[0]`, `result[3]`,
`result[5]`). Compare with `consume()` which casts every field consistently.

---

## B. Findings: Code Observations

### B1. `hasattr(self, "_paused_until")` is redundant

**Files:** `src/redis_rate_limiter/core/limiters.py` line 1195,
`src/redis_rate_limiter/core/async_limiters.py` line 757

`_paused_until` is initialized to `0.0` in `AbstractRateLimiter.__init__`
(line 52 of `base.py`). It is always present on every instance. The
`hasattr` guard in `drain()` is unnecessary.

The `hasattr(self, "refresh_config")` on the line above it is a different
story — that one depends on whether the managed mixin is in the MRO,
so the `hasattr` may be justified there.

---

### B2. Private attribute access: `executor._max_workers`

**File:** `src/redis_rate_limiter/backends/threading/limiter.py` line 109

`ThreadPoolExecutor._max_workers` is a CPython implementation detail.
The async counterpart (`AsyncIOTaskLimiter`) avoids this by accepting an
explicit `max_tasks` constructor parameter.

---

### B3. `asyncio.ensure_future` is deprecated

**File:** `src/redis_rate_limiter/core/async_limiters.py` line 315

`asyncio.ensure_future` has been soft-deprecated since Python 3.10. The
preferred replacement is `asyncio.create_task`.

---

## C. Sync/Async Mirror Risk

`limiters.py` (~1550 lines) and `async_limiters.py` (~1050 lines) are
manually-maintained mirrors. They match today, but any future change to
drain logic, consume parsing, lifecycle handling, or status reporting
must be applied to both files. There is no automated check for this.

**Suggested CLAUDE.md rule:**
```
limiters.py and async_limiters.py are manually-maintained mirrors.
Any change to one MUST be reflected in the other.
```

This lets Claude catch it automatically when it opens either file,
without wasting tokens loading both files preemptively.

---

## Checklist

- [ ] A1: Evaluate atomicity of `_persist_config`
- [ ] A2: Decide if `health.lua` should clean expired leases
- [ ] A3: Cast all `get_status()` values to proper Python types
- [ ] B1: Remove redundant `hasattr` for `_paused_until`
- [ ] B2: Evaluate `executor._max_workers` access
- [ ] B3: Replace `asyncio.ensure_future` with `create_task`
- [ ] C: Add mirror rule to CLAUDE.md
