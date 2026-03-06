# Phase 3: Core Source Review

Files reviewed: base.py (254), scripts.py (53), decorators.py (85), importing.py (93)

## base.py — Abstract Base Classes

### NOTE: Clean three-layer hierarchy
- `AbstractRateLimiter`: config-only (id, limit, window), keyword-only __init__
- `AbstractSyncRateLimiter`: adds sync Redis client, script SHA caching, NOSCRIPT recovery
- `AbstractAsyncRateLimiter`: async mirror with `start()` lifecycle hook

The cooperative `super().__init__(**kwargs)` chain with `object.__init__()` as terminator
correctly catches unconsumed kwargs. Well-designed for diamond inheritance.

### NOTE: NOSCRIPT recovery is robust
Both sync and async `_eval_script` handle `NoScriptError` by re-registering and retrying
once, then raising `RuntimeError` on second failure. Good resilience for Redis restarts.

### NOTE: `_paused_until` initialized to 0.0 (base.py:52)
Confirms that the `hasattr(self, "_paused_until")` check in limiters.py is redundant
(as noted in existing REVIEW.md finding B1).

### NOTE: `_config_version` initialized to 0 (base.py:51)
This resolves the question from Phase 5 — the version is initialized in the base class,
so `refresh_config()` will always find it.

## scripts.py — Lua Script Loader

### NOTE: Simple and correct
`load_lua_script()` tries two package paths (installed wheel, source tree) and raises
`ImportError` if neither works. Uses `importlib.resources.files()` (Python 3.9+ API).

No issues found.

## decorators.py — @rate_limited

### CONCERN: Undocumented required kwarg `_rate_limit_task_id` (decorators.py:57)
`kwargs.pop("_rate_limit_task_id")` will raise `KeyError` if not present. This is an
internal protocol (the dispatcher injects it), but:
1. The decorator docstring doesn't mention it
2. A user calling a decorated function directly gets a confusing KeyError
3. Could be improved with a descriptive error message

### NOTE: Sync-only decorator
The decorator only wraps sync functions (uses `with limiter.task_lifecycle()`). There's
no async equivalent. Users with async backends must manage the lifecycle manually via
`async with limiter.task_lifecycle()`. This is a feature gap but may be intentional
given the different dispatch patterns.

### NOTE: Default limiter resolver uses ThreadPool
`_get_default_limiter` returns `ThreadPoolRateLimiter.get(limiter_id)`. This means the
decorator works out of the box without Celery, but it implicitly depends on
ThreadPoolRateLimiter being configured. If it's not configured, the user gets a
RuntimeError from the managed class API.

## importing.py — Dynamic Import Utility

### NOTE: Round-trip verification is a nice safety net
`resolve_import_path()` (importing.py:79-85) verifies that `import_string(derived_path)`
resolves back to the same object. This catches edge cases like re-exported names.

### NOTE: Comprehensive rejection of non-importable callables
Lambdas, closures, nested functions, and class-bound methods are all explicitly rejected
with descriptive error messages. Good defensive coding.

No bugs found in importing.py.

## Summary
These files are well-written and correct. The main actionable item is the undocumented
`_rate_limit_task_id` kwarg in the decorator. No bugs found.
