# Phase 12: Cross-Cutting Concerns

## Public API Surface

### CONCERN: `__all__` lists names that may not be imported (__init__.py:13-27)
The package-level `__all__` includes `CeleryRateLimiter` and `PrometheusMetricsExporter`,
but these are imported inside `try/except ImportError` blocks. If celery or
prometheus_client isn't installed, the names are in `__all__` but not actually bound
in the module namespace. This means:
- `from redis_rate_limiter import *` would raise `AttributeError` (Python iterates `__all__`)
- `from redis_rate_limiter import CeleryRateLimiter` would raise `ImportError`

The fix is either: remove optional items from `__all__`, or dynamically build `__all__`,
or add the names to the except block (e.g., `CeleryRateLimiter = None`).

### NOTE: core/__init__.py exports internal classes
The core `__init__.py` exports `AsyncDrainLoop`, `AsyncDrainSignalSubscriber`,
`DistributedRateLimiterMixin`, `ManagedRateLimiterMixin`, `load_lua_script`, etc.
These are implementation details, not public API. They're not re-exported at the
package level, so the exposure is limited to `from redis_rate_limiter.core import ...`.
Acceptable but could be tightened.

## Error Handling Patterns

### Consistent: Metrics callback protection
`_emit_metric()` (limiters.py:747-768) wraps user callbacks in try/except to prevent
user code from crashing the limiter. Good defensive pattern.

### Consistent: Drain failure recovery
Both sync and async `drain()` methods have exponential backoff with cap at `window`.
Recovery scheduling itself is wrapped in try/except. Robust.

### Consistent: Pub/Sub signal failures are swallowed
`_publish_drain_signal()` catches all exceptions and logs at DEBUG level. This is
appropriate — signal loss is non-fatal due to the watchdog timer.

### CONCERN: `_rate_limit_task_id` is a required kwarg with no validation (decorators.py:57)
The `@rate_limited` decorator does `kwargs.pop("_rate_limit_task_id")` without a
default or try/except. If the caller doesn't pass `_rate_limit_task_id`, this raises
a raw `KeyError` with no helpful message. The decorator docstring doesn't mention this
required kwarg. This is an internal contract (the dispatch system injects it), but the
error message would be confusing if someone calls the decorated function directly.

## Naming Conventions

### Consistent: Redis key construction
All keys follow `{limiter_id}:{purpose}` pattern: `:buffer`, `:concurrency`,
`:dispatch_lock`, `:dispatch_lock:contention`, `:dlq`, `:inflight:{task_id}`,
`:drain_signal`, `:cd:{worker_id}`. Consistent and well-namespaced.

### Consistent: Logging format
All log messages follow `"Action description: key1=%s, key2=%s."` pattern with
trailing period. Module-level `logger = logging.getLogger(__name__)` everywhere.

### NOTE: Mixed use of `id` as attribute name
`self.id` shadows the built-in `id()`. Not a bug (the built-in is rarely needed
on instances), but it's a common Python anti-pattern. Used consistently throughout,
so changing it would be a large refactor with no functional benefit.

## Type Safety

### NOTE: Heavy use of `cast()` with `# pragma: no mutate`
The codebase uses `cast(Awaitable, ...)` extensively in the async code to satisfy
mypy's type checker for `redis.asyncio` calls. These are all annotated with
`# pragma: no mutate` and `# fmt: off` to protect from mutation testing and formatter
changes. This is a workaround for incomplete type stubs in `redis.asyncio`. Correct
but verbose — worth noting as tech debt that resolves itself when redis-py improves stubs.

### NOTE: `Any` in cooperative __init__ signatures
The `*args: Any, **kwargs: Any` pattern in mixin `__init__` methods is necessary for
cooperative MRO but means mypy can't catch mistyped constructor arguments. Acceptable
trade-off for the diamond inheritance pattern used here.

## MD5 for Task IDs

### NOTE: MD5 used for task deduplication (limiters.py:589, 985)
`hashlib.md5(task_signature.encode()).hexdigest()` generates task IDs. MD5 is not
collision-resistant for security purposes, but it's used here purely for deduplication
of `{func_path, payload}` pairs. The collision probability for legitimate inputs is
negligible. Not a security concern (no cryptographic use), but could be SHA-256 for
"correctness by default" without meaningful performance impact.
