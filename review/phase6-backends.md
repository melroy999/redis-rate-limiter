# Phase 6: Backend Implementations Review

Reviewed all 4 backend implementations: Celery, AsyncIO, Threading, and ASGI.

---

## 1. Celery Backend

**Files:** `backends/celery/limiter.py`, `backends/celery/tasks/worker.py`, `backends/celery/tasks/__init__.py`, `backends/celery/__init__.py`

### Inheritance

Extends `SyncManagedRateLimiter, AbstractDistributedRateLimiter`. Correct.

### `_dispatch_task`

Delegates to `self.app.send_task()` with two code paths: generic worker (via `use_executor` flag) or custom user-defined Celery task. The enhanced payload wrapping (`_get_enhanced_payload` / `schedule_task` override) is clean.

### Lifecycle

No `shutdown()` override -- inherits from `AbstractDistributedRateLimiter`, which handles drain loop and signal subscriber. No custom resources to clean up. Correct.

### `_has_local_capacity`

Not overridden. Falls back to the base mixin's `return True`. This is correct for Celery because task execution happens in a remote worker process, not locally.

### `task_lifecycle`

Not used in `_dispatch_task`. This is correct for Celery: the `@rate_limited` decorator on `generic_rate_limited_worker` handles the lifecycle context (concurrency slot release, heartbeat) on the worker side.

### Findings

- **(NOTE)** The `schedule_task` override adds a `use_executor` meta field to the payload. The `cast()` on line 133 is annotated with `# pragma: no mutate` to avoid mutation testing noise. Acceptable pattern.

---

## 2. AsyncIO Backend

**Files:** `backends/asyncio/limiter.py`, `backends/asyncio/__init__.py`

### Inheritance

Extends `AsyncManagedRateLimiter, AbstractAsyncDistributedRateLimiter`. Correct.

### `_dispatch_task`

Creates an `asyncio.Task` via `asyncio.create_task()`. The task calls the target function inside `self.task_lifecycle(task_id)`, which handles concurrency slot release and heartbeat.

### `_has_local_capacity`

Overridden. Returns `self._active_count < self.max_tasks`. Correct -- prevents acquiring Redis concurrency slots when the local event loop is saturated.

### `shutdown()`

Calls `await super().shutdown()` (which stops drain loop and signal subscriber), then cancels all active tasks via `task.cancel()` + `asyncio.gather(..., return_exceptions=True)` + `clear()` + reset count. Complete cleanup.

### Findings

- **(CONCERN) `_active_count` is not thread-safe, but that is acceptable** because the asyncio backend runs entirely within a single-threaded event loop. The increment/decrement in `_dispatch_task` and the `_run_task` `finally` block are never concurrent within the same coroutine step.

- **(NOTE) Type check happens inside the lifecycle context.** At line 124, the `asyncio.iscoroutinefunction(target_func)` check is performed after entering `self.task_lifecycle(task_id)`, which starts a heartbeat. If the check raises `TypeError`, the `except Exception` block on line 131 logs it and the `finally` block correctly decrements `_active_count` and discards the task. The lifecycle context manager's `__aexit__` will release the concurrency slot. No bug, but the check could be hoisted before `task_lifecycle` entry to avoid starting a heartbeat for a task that will immediately fail.

- **(NOTE) Error swallowing.** On line 131, all exceptions from user task functions are caught and logged but not re-raised. This is intentional for fire-and-forget dispatch -- the task lifecycle still cleans up. Consistent with the threading backend behavior.

---

## 3. Threading Backend

**Files:** `backends/threading/limiter.py`, `backends/threading/__init__.py`

### Inheritance

Extends `SyncManagedRateLimiter, AbstractDistributedRateLimiter`. Correct.

### `_dispatch_task`

Submits a `_run_task` closure to `self.executor.submit()`. The closure uses `self.task_lifecycle(task_id)` for concurrency slot management and heartbeat. Correct.

### `_has_local_capacity`

Overridden. Uses `self._local_dispatch_lock` (a `threading.Lock`) to safely check `self._local_dispatched < self.executor._max_workers`. Correct thread-safe implementation.

### Lifecycle

No `shutdown()` override. Inherits from `AbstractDistributedRateLimiter`. The `ThreadPoolExecutor` is passed in from outside and its lifecycle is the caller's responsibility. No resource leak.

### Findings

- **(CONCERN) Access to private `executor._max_workers`.** Line 109 accesses `self.executor._max_workers`, which is a CPython implementation detail of `concurrent.futures.ThreadPoolExecutor`. This attribute has existed since Python 3.2 and is stable in practice, but it is not part of the public API. If Python ever changes the internal naming, this will break silently (returning `AttributeError` in `_has_local_capacity`, causing the drain loop to crash).

  Mitigation: store the max workers value at construction time, e.g., `self._max_workers = executor._max_workers`.

- **(IMPROVEMENT) No error logging in `_run_task`.** Unlike the AsyncIO backend which catches and logs exceptions from user functions, the threading backend's `_run_task` (lines 121-127) uses a bare `try/finally` that lets exceptions propagate silently into the `ThreadPoolExecutor` (which swallows them). If a user function raises, the exception is lost unless the caller holds a reference to the `Future`.

  The asyncio backend explicitly logs exceptions at line 131-137. The threading backend should do the same for consistency:

  ```python
  def _run_task() -> None:
      try:
          with self.task_lifecycle(task_id):
              target_func(**payload)
      except Exception:
          logger.exception(
              "Task raised an exception: limiter=%s, task_id=%s, func_path=%s.",
              self.id, task_id, func_path,
          )
      finally:
          with self._local_dispatch_lock:
              self._local_dispatched -= 1
  ```

---

## 4. ASGI Backend

**Files:** `backends/asgi/limiter.py`, `backends/asgi/middleware.py`, `backends/asgi/keys.py`, `backends/asgi/types.py`, `backends/asgi/__init__.py`

### Inheritance

`ASGIRateLimiter` extends `AsyncManagedRateLimiter, AbstractAsyncRateLimiter`. This is the **request-oriented** base (not the distributed task limiter base). Correct -- ASGI uses a simpler sliding window counter without buffer/concurrency/drain machinery.

### `start()`

Registers `acquire.lua` eagerly during `start()`. Correct.

### `acquire()`

Performs a throttled `refresh_config()` (at most every 5 seconds) and then evaluates the `acquire.lua` script with the dynamic key. Returns a structured `AcquireResult` TypedDict. Clean.

### Middleware (`RateLimitMiddleware`)

- Non-HTTP scopes pass through. Correct.
- `key_func` returning `None` bypasses rate limiting. Correct.
- `on_error` supports `"fail_open"` (pass through) and `"fail_closed"` (503). Correct.
- `on_blocked` custom callback replaces the default 429 response. Correct.
- Rate limit headers are injected via `_wrap_send`. Correct.
- The `retry-after` header uses ceiling division: `max(1, (reset_ms + 999) // 1000)`. Correct.

### Key Functions (`keys.py`)

- `by_client_ip`: extracts `scope["client"][0]`. Returns `None` when absent. Correct.
- `by_header(name)`: factory that returns a key function matching a case-insensitive header. Uses `latin-1` encoding per HTTP/1.1 spec. Correct.

### Lifecycle

No `shutdown()` needed -- `ASGIRateLimiter` extends `AbstractAsyncRateLimiter`, which has no background tasks (no drain loop, no signal subscriber). There is nothing to clean up.

### Findings

- **(NOTE) `on_error` string comparison.** Line 68: `self.on_error = on_error.lower()`. Line 92: `if self.on_error == "fail_closed"`. Any unrecognized value silently falls through to `fail_open` behavior. This is acceptable as documented, but a stricter validation during `__init__` could catch typos like `"fail_close"`.

- **(NOTE) `AcquireResult` is a TypedDict.** The middleware accesses it with string keys (`result["allowed"]`, `result["remaining"]`), which is correct TypedDict usage. Consistent.

---

## Cross-Backend Comparison

### Error Handling Consistency

| Backend   | Exceptions in user functions | Behavior |
|-----------|------------------------------|----------|
| Celery    | Handled by Celery worker framework | N/A (remote execution) |
| AsyncIO   | Caught, logged, swallowed | Correct |
| Threading | Not caught -- silently swallowed by `ThreadPoolExecutor` | **Gap** |
| ASGI      | N/A (no user function dispatch) | N/A |

**Finding (IMPROVEMENT):** The threading backend should add an `except Exception` block with logging, matching the asyncio backend pattern. See threading findings above.

### `_has_local_capacity` Implementation

| Backend   | Override? | Mechanism |
|-----------|-----------|-----------|
| Celery    | No        | Base returns `True` (remote dispatch, no local capacity concern) |
| AsyncIO   | Yes       | `_active_count < max_tasks` |
| Threading | Yes       | `_local_dispatched < executor._max_workers` (under lock) |
| ASGI      | N/A       | Not a distributed task limiter |

All correct. The celery backend correctly does not override since tasks execute remotely.

### `task_lifecycle` Usage

| Backend   | Used in `_dispatch_task`? | Mechanism |
|-----------|--------------------------|-----------|
| Celery    | No  | `@rate_limited` decorator on the worker handles it |
| AsyncIO   | Yes | `async with self.task_lifecycle(task_id)` inside the asyncio task |
| Threading | Yes | `with self.task_lifecycle(task_id)` inside the thread closure |
| ASGI      | N/A | No task lifecycle concept |

All correct. The celery backend's separation of lifecycle management to the worker side (via the `@rate_limited` decorator) is architecturally sound.

### Managed Backend Hooks

All four backends implement the five required abstract methods:
- `_configure_backend(**backend_context)`
- `_has_backend_context()`
- `_get_instance_context()`
- `_reset_backend_context()`
- `_configure_hint()`

| Backend   | Required context |
|-----------|-----------------|
| Celery    | `celery_app` |
| AsyncIO   | `max_tasks` |
| Threading | `executor` |
| ASGI      | None (no-op) |

All implementations are consistent and complete.

### Shutdown / Resource Cleanup

| Backend   | Custom `shutdown()`? | Resources managed |
|-----------|---------------------|-------------------|
| Celery    | No (inherits base)  | Drain loop, signal subscriber (via base) |
| AsyncIO   | Yes                 | Base cleanup + cancels active asyncio tasks |
| Threading | No (inherits base)  | Drain loop, signal subscriber (via base); executor is external |
| ASGI      | No (not needed)     | No background tasks |

All correct. No resource leaks identified.

---

## Summary of Findings

### Bug

None found.

### Concern

1. **Threading: private attribute access `executor._max_workers`** (threading/limiter.py:109). Stable in CPython but not part of the public API. Could break in alternative Python implementations or future versions. Consider caching the value at construction time.

### Improvement

2. **Threading: missing exception logging in `_run_task`** (threading/limiter.py:121-127). User function exceptions are silently swallowed by the `ThreadPoolExecutor`. The asyncio backend explicitly catches and logs them. The threading backend should do the same for operational visibility and consistency.

3. **AsyncIO: type check could be hoisted** (asyncio/limiter.py:124). The `asyncio.iscoroutinefunction` check is inside the `task_lifecycle` context, meaning a heartbeat is started before the check. Moving the check before `async with self.task_lifecycle(task_id)` would avoid unnecessary heartbeat startup for invalid functions. Minor, since the lifecycle cleanup handles it correctly.

### Note

4. **ASGI middleware `on_error` validation** (asgi/middleware.py:68). Unrecognized values silently default to `fail_open`. Acceptable but could benefit from a `ValueError` guard in `__init__`.

5. **All backends correctly implement the managed API hooks.** The five abstract methods from `ManagedRateLimiterMixin` are consistently implemented across all four backends.

6. **The ASGI backend is architecturally distinct.** It extends `AbstractAsyncRateLimiter` (not the distributed task limiter), using a simpler sliding-window-counter model without buffer, concurrency set, or drain loop. This is intentional and correct for request-rate-limiting.
