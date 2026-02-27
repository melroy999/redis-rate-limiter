# Error Handling and Propagation

This document serves as a reference for understanding how exceptions propagate through the system layers, where they are caught or retried, and which failure modes are covered by the test suite. It complements the [Task State Diagram](task-states.md), which models nominal and recovery state transitions, by focusing specifically on the exception flow between architectural layers. The [Drain Loop Flow](drain-flow.md) documents the control flow under normal conditions; this document covers what happens when those operations fail. The content is organised as four diagrams: an overview that shows how exceptions propagate across layers, followed by one detailed flowchart per layer. Each detail diagram is accompanied by a test coverage table that maps every decision branch to the test(s) that exercise it.

## Overview

```mermaid
%%{init: {"theme": "default", "themeVariables": {"lineColor": "#6e7781"}}}%%
flowchart TD
    subgraph SL ["Scheduling Layer"]
        S_BLOCK["NoScript retry,<br>inflight cleanup,<br>re-raise to caller"]
    end

    subgraph CL ["Consumption and Dispatch Layer"]
        C_BLOCK["NoScript retry,<br>propagate to drain"]
    end

    subgraph DL ["Drain Control Layer"]
        D_BLOCK["Catch all exceptions,<br>backoff, schedule recovery"]
    end

    subgraph EL ["Execution Layer"]
        E_BLOCK["Decorator guards,<br>lifecycle cleanup,<br>heartbeat strategies"]
    end

    CALLER["Caller<br>(user code)"]

    S_BLOCK -. "non-recoverable<br>exception" .-> CALLER
    C_BLOCK -. "exception" .-> D_BLOCK
    D_BLOCK -. "double failure:<br>rely on watchdog" .-> D_BLOCK
    E_BLOCK -. "trigger_consume()<br>wakes DrainLoop" .-> D_BLOCK
    E_BLOCK -. "user exception<br>propagates" .-> CALLER

    style SL fill:#e8f5e9,stroke:#388E3C
    style CL fill:#e3f2fd,stroke:#1976D2
    style DL fill:#fff3e0,stroke:#F57C00
    style EL fill:#f3e5f5,stroke:#7B1FA2
```

**Legend:**

- *Green subgraph* (Scheduling Layer): exception handling within `schedule_task()`, including the NoScriptError retry pattern and inflight key cleanup on any failure.
- *Blue subgraph* (Consumption and Dispatch Layer): exception handling within `consume()` and `_dispatch_task()`, both of which propagate non-recoverable errors to the drain control layer.
- *Orange subgraph* (Drain Control Layer): the `drain()` method catches all exceptions from `_drain_inner()`, applies exponential backoff, and schedules recovery drains.
- *Purple subgraph* (Execution Layer): the `@rate_limited` decorator, `TaskLifecycle` context manager, and heartbeat loop, which handle task execution errors and concurrency slot cleanup.
- *Dashed arrows* indicate cross-layer exception propagation or asynchronous triggers (e.g., `trigger_consume()` waking the `DrainLoop`).

## Scheduling Layer

The scheduling layer handles exceptions raised during `schedule_task()`. The two primary concerns are recovering from `NoScriptError` (Lua script cache flush) and cleaning up the inflight deduplication key when any exception prevents successful buffering.

```mermaid
%%{init: {"theme": "default", "themeVariables": {"lineColor": "#6e7781"}}}%%
flowchart TD
    S_EVAL["schedule_task() calls<br>EVALSHA schedule.lua"]
    S_NOSCRIPT{"NoScriptError?"}
    S_RETRY["Reload SHA,<br>cleanup inflight key,<br>retry with retry=False"]
    S_RETRY_FAIL{"2nd attempt<br>also fails?"}
    S_RUNTIME["RuntimeError:<br>script not retained"]
    S_OTHER["Non-NoScript exception<br>(e.g., ConnectionError)"]
    S_CLEANUP["Cleanup inflight key<br>via _cleanup_inflight_key()"]
    S_RERAISE["Re-raise original<br>exception to caller"]
    S_INFLIGHT_FAIL["Cleanup itself fails:<br>catch, log warning,<br>continue with re-raise"]

    S_EVAL --> S_NOSCRIPT
    S_NOSCRIPT -- "Yes" --> S_RETRY --> S_RETRY_FAIL
    S_RETRY_FAIL -- "Yes" --> S_RUNTIME
    S_RETRY_FAIL -- "No" --> S_EVAL
    S_NOSCRIPT -- "No" --> S_OTHER --> S_CLEANUP --> S_RERAISE
    S_CLEANUP -. "on failure" .-> S_INFLIGHT_FAIL --> S_RERAISE

    style S_RUNTIME fill:#ffcdd2,stroke:#D32F2F
    style S_RERAISE fill:#ffcdd2,stroke:#D32F2F
```

**Test coverage:**

| Path | Description | Tested by |
|------|-------------|-----------|
| NoScript → reload → retry succeeds | SHA reload recovers from cache flush | `implementations/test_rate_limiter::test_lua_script_recovery_on_noscript_error` |
| NoScript → reload → retry fails | Permanent failure raises RuntimeError | `implementations/test_rate_limiter::test_lua_script_permanent_failure_raises_error` |
| Non-NoScript → cleanup → re-raise | Inflight key cleaned up before re-raising | `implementations/test_rate_limiter::test_schedule_non_noscript_failure_cleans_inflight_and_reraises` |
| Cleanup itself fails | Suppressed, log warning, continue with re-raise | `implementations/test_internal_helpers::test_cleanup_inflight_key_suppresses_redis_failure` |
| Lua script not found on disk | ImportError at initialization | `implementations/test_internal_helpers::test_load_lua_script_raises_import_error_on_failure` |
| Lua script fallback via second package | First package fails, second succeeds | `implementations/test_internal_helpers::test_load_lua_script_falls_back_to_second_package` |
| Metrics callback exception during schedule | Does not disrupt scheduling | `implementations/test_metrics_callback::test_callback_exception_does_not_break_schedule` |

### NoScriptError Two-Phase Retry

All Lua script operations (`schedule_task`, `consume`, `extend_lease`, `get_status`, and the ASGI `acquire`) share a centralized two-phase retry implemented in `_eval_script()`. The first attempt uses `EVALSHA`; if Redis returns a `NoScriptError` (indicating that the script cache was flushed, e.g., after a `SCRIPT FLUSH` or Redis restart), the method reloads the SHA via `script_load` and retries with `retry=False`. If the second attempt also fails with `NoScriptError`, a `RuntimeError` is raised. This pattern tolerates transient script cache losses while preventing infinite retry loops.

- [base.py:128-165](../../src/celery_rate_limiter/core/base.py): `AbstractSyncRateLimiter._eval_script()` (sync two-phase retry).
- [base.py:205-242](../../src/celery_rate_limiter/core/base.py): `AbstractAsyncRateLimiter._eval_script()` (async two-phase retry).

### Inflight Key Cleanup on Any Schedule Failure

`schedule_task()` acquires the inflight key via `SET NX` before calling the Lua script. If the Lua call fails for any reason (NoScriptError, ConnectionError, or any other exception), the inflight key is cleaned up via `_cleanup_inflight_key()` to prevent orphaned deduplication locks that would permanently block resubmission. The cleanup method itself suppresses all exceptions and logs a warning, such that a secondary Redis failure during cleanup does not mask the original error.

- [limiters.py:939-957](../../src/celery_rate_limiter/core/limiters.py): `_cleanup_inflight_key()`.
- [limiters.py:1030-1034](../../src/celery_rate_limiter/core/limiters.py): cleanup calls on generic exceptions.

## Consumption and Dispatch Layer

The consumption and dispatch layer handles exceptions raised during `consume()` and `_dispatch_task()`. Non-recoverable exceptions from either operation propagate to the `drain()` method, which catches them and applies exponential backoff (see [Drain Control Layer](#drain-control-layer)).

```mermaid
%%{init: {"theme": "default", "themeVariables": {"lineColor": "#6e7781"}}}%%
flowchart TD
    C_EVAL["consume() calls<br>EVALSHA consume.lua"]
    C_NOSCRIPT{"NoScriptError?"}
    C_RETRY["Reload SHA,<br>retry with retry=False"]
    C_RETRY_FAIL{"2nd attempt<br>also fails?"}
    C_RUNTIME["RuntimeError:<br>script not retained"]
    C_PROPAGATE["Non-NoScript exception<br>propagates to drain()"]
    C_DISPATCH["_dispatch_task() calls<br>backend (send_task / executor)"]
    C_DISPATCH_FAIL["Backend exception<br>propagates to drain()"]

    C_EVAL --> C_NOSCRIPT
    C_NOSCRIPT -- "Yes" --> C_RETRY --> C_RETRY_FAIL
    C_RETRY_FAIL -- "Yes" --> C_RUNTIME
    C_RETRY_FAIL -- "No" --> C_EVAL
    C_NOSCRIPT -- "No" --> C_PROPAGATE
    C_DISPATCH --> C_DISPATCH_FAIL

    style C_RUNTIME fill:#ffcdd2,stroke:#D32F2F
    style C_PROPAGATE fill:#fff3e0,stroke:#F57C00
    style C_DISPATCH_FAIL fill:#fff3e0,stroke:#F57C00
```

**Test coverage:**

| Path | Description | Tested by |
|------|-------------|-----------|
| Consume NoScript → reload → retry succeeds | SHA reload during consume | `implementations/test_rate_limiter::test_consume_lua_script_recovery_on_noscript_error` |
| Consume NoScript → retry fails | Permanent failure raises RuntimeError | `implementations/test_rate_limiter::test_consume_lua_script_permanent_failure_raises_error` |
| Consume non-NoScript → propagate | ConnectionError propagates to drain() | `implementations/test_rate_limiter::test_consume_connection_error_propagates` |
| Dispatch Celery failure → propagate | send_task() exception propagates to drain() | `implementations/celery/test_celery_limiter::test_dispatch_task_send_task_failure_propagates` |
| Dispatch ThreadPool failure → propagate | import_string() exception propagates to drain() | `implementations/threadpool/test_threadpool_limiter::test_dispatch_task_import_failure_propagates` |
| Dispatch AsyncIO sync function → TypeError | Sync function passed to async dispatch raises TypeError | `implementations/asyncio/test_asyncio_limiter::test_dispatch_sync_function_raises_type_error` |
| Metrics callback exception during consume | Does not disrupt consumption | `implementations/test_metrics_callback::test_callback_exception_does_not_break_consume` |

### Metrics Callback Isolation

`_emit_metric()` wraps the user-provided callback in a `try/except` that catches and logs all exceptions, thereby preventing a buggy callback from disrupting the limiter. This isolation boundary ensures that observability integrations cannot introduce cascading failures into the rate limiting logic.

- [limiters.py:745-766](../../src/celery_rate_limiter/core/limiters.py): `_emit_metric()` with exception suppression.

## Drain Control Layer

The drain control layer wraps `_drain_inner()` in a `try/except` that catches all exceptions from the consumption and dispatch operations. It applies exponential backoff and schedules recovery drains. The [Drain Loop Flow](drain-flow.md) documents the control flow in detail; this section focuses specifically on the error handling aspects.

```mermaid
%%{init: {"theme": "default", "themeVariables": {"lineColor": "#6e7781"}}}%%
flowchart TD
    D_INNER["_drain_inner() orchestrates<br>lock, consume, dispatch"]
    D_CATCH["drain() catches<br>all exceptions"]
    D_BACKOFF["Increment failure count<br>delay = min(window, 0.1 × 2^(n−1))"]
    D_SCHEDULE["Schedule recovery drain<br>via _schedule_drain(delay)"]
    D_DOUBLE_FAIL{"Recovery scheduling<br>also fails?"}
    D_CRITICAL["Log critical:<br>rely on watchdog,<br>trigger_consume(),<br>or task completion"]
    D_RESET["On success:<br>reset failure count to 0"]

    D_INNER -- "exception" --> D_CATCH --> D_BACKOFF --> D_SCHEDULE
    D_SCHEDULE -- "exception" --> D_DOUBLE_FAIL
    D_DOUBLE_FAIL -- "Yes" --> D_CRITICAL
    D_INNER -- "success" --> D_RESET

    style D_CATCH fill:#fff3e0,stroke:#FF9800
    style D_CRITICAL fill:#ffcdd2,stroke:#D32F2F
```

**Test coverage:**

| Path | Description | Tested by |
|------|-------------|-----------|
| Exception → backoff → schedule recovery | Consume or dispatch exception caught | `implementations/test_drain::test_drain_handles_consume_exception`, `implementations/test_drain::test_drain_handles_dispatch_exception` |
| Escalating backoff | Delay doubles on consecutive failures | `implementations/test_drain::test_drain_backoff_increases_with_consecutive_failures` |
| Double failure → critical log | Recovery scheduling itself fails | `implementations/test_drain::test_drain_handles_double_failure_when_schedule_drain_also_fails` |
| Success → reset counter | Counter resets to 0 | `implementations/test_drain::test_drain_resets_failure_counter_on_success` |

### Exponential Backoff on Failure

`drain()` wraps `_drain_inner()` in a `try/except` that catches all exceptions, increments `_consecutive_drain_failures`, and schedules a recovery drain with `delay = min(window, 0.1 * 2^(n-1))`. The backoff starts at 100ms for the first failure and doubles on each consecutive failure, capped at the window duration. On the first successful drain, the error counter resets to 0. If the recovery scheduling itself also fails, the system logs a critical error and relies on the next external trigger (a `trigger_consume()` call from `schedule_task()` or `TaskLifecycle.__exit__()`, or a watchdog timeout) to resume the drain loop.

- [limiters.py:1169-1218](../../src/celery_rate_limiter/core/limiters.py): drain exception handling and backoff calculation.

## Execution Layer

The execution layer encompasses the `@rate_limited` decorator, the `TaskLifecycle` context manager, and the heartbeat loop. These components handle task execution errors, concurrency slot cleanup, and lease renewal failures.

```mermaid
%%{init: {"theme": "default", "themeVariables": {"lineColor": "#6e7781"}}}%%
flowchart TD
    E_DEC_LID["@rate_limited: resolve<br>limiter_id and limiter"]
    E_DEC_TID["Pop _rate_limit_task_id<br>from kwargs"]
    E_MISSING_LID["ValueError:<br>missing limiter_id"]
    E_MISSING_TID["KeyError:<br>missing _rate_limit_task_id"]
    E_LIFECYCLE["TaskLifecycle context:<br>execute user function"]
    E_USER_EXC["User function raises:<br>exception propagates"]
    E_CLEANUP["__exit__: ZREM concurrency,<br>DEL inflight key"]
    E_CLEANUP_FAIL["Redis error during cleanup:<br>propagates, but finally<br>still runs trigger_consume()"]
    E_TRIGGER["finally: trigger_consume()<br>always called"]
    E_HB_LOOP["Heartbeat loop:<br>extend_lease() every<br>lease_duration / 2"]
    E_HB_FAIL{"on_heartbeat_failure<br>strategy?"}
    E_HB_WARN["warn: set is_healthy=False,<br>log critical, continue"]
    E_HB_KILL["kill: os.kill(SIGTERM),<br>break heartbeat loop"]

    E_DEC_LID -- "missing" --> E_MISSING_LID
    E_DEC_TID -- "missing" --> E_MISSING_TID
    E_LIFECYCLE -- "user exception" --> E_USER_EXC --> E_CLEANUP
    E_LIFECYCLE -- "success" --> E_CLEANUP
    E_CLEANUP -- "Redis error" --> E_CLEANUP_FAIL --> E_TRIGGER
    E_CLEANUP -- "success" --> E_TRIGGER
    E_HB_LOOP -- "exception" --> E_HB_FAIL
    E_HB_FAIL -- "warn" --> E_HB_WARN
    E_HB_FAIL -- "kill" --> E_HB_KILL

    style E_MISSING_LID fill:#ffcdd2,stroke:#D32F2F
    style E_MISSING_TID fill:#ffcdd2,stroke:#D32F2F
    style E_TRIGGER fill:#e8f5e9,stroke:#388E3C
```

**Test coverage:**

| Path | Description | Tested by |
|------|-------------|-----------|
| Missing limiter_id → ValueError | Decorator rejects missing limiter_id | `implementations/test_decorator::test_decorator_raises_value_error_when_limiter_id_missing` |
| Missing _rate_limit_task_id → KeyError | Decorator rejects missing task_id | `implementations/test_decorator::test_decorator_raises_when_task_id_missing` |
| User exception → cleanup → trigger | Exception propagates; cleanup and trigger still fire | `implementations/test_decorator::test_decorator_propagates_wrapped_function_exception` |
| Normal → cleanup → trigger | Lifecycle cleans up and triggers next drain | `contracts/test_task_lifecycle::test_lifecycle_cleans_up_on_exception` |
| Redis error during cleanup → trigger still fires | trigger_consume() fires in finally block | `implementations/test_task_lifecycle::test_lifecycle_handles_redis_failure_during_cleanup` |
| Heartbeat warn mode | is_healthy = False, log critical, continue | `implementations/test_task_lifecycle::test_heartbeat_loop_flags_unhealthy_on_failure_warn_mode` |
| Heartbeat kill mode | os.kill(SIGTERM), break loop | `implementations/test_task_lifecycle::test_heartbeat_loop_terminates_worker_on_failure_kill_mode` |
| extend_lease NoScript recovery | SHA reload during heartbeat | `implementations/test_task_lifecycle::test_extend_lease_recovery_on_noscript_error` |
| extend_lease permanent failure | RuntimeError propagates to heartbeat | `implementations/test_task_lifecycle::test_extend_lease_permanent_failure_raises_error` |
| extend_lease unknown task | KeyError for unknown task_id | `implementations/test_task_lifecycle::test_extend_lease_raises_key_error_for_unknown_task` |

### TaskLifecycle Finally Block

`TaskLifecycle.__exit__()` performs cleanup (ZREM on the concurrency set, DEL on the inflight key) in a `try` block, with `trigger_consume()` in the `finally` block. This guarantees that the feedback loop continues even if the Redis cleanup operations fail, such that a freed concurrency slot is always followed by a consumption attempt.

- [limiters.py:320-356](../../src/celery_rate_limiter/core/limiters.py): `TaskLifecycle.__exit__()` with try/finally.

### Heartbeat Failure Strategies

The heartbeat loop catches all exceptions from `extend_lease()`. In `"warn"` mode, it sets `is_healthy = False` and logs a critical message, allowing the task to continue running at the risk of the concurrency slot lease expiring. In `"kill"` mode, it sends `SIGTERM` to the worker process, ensuring that the task is terminated and the concurrency slot self-heals via lease expiry. The choice between strategies is configured per `TaskLifecycle` instance.

- [limiters.py:272-306](../../src/celery_rate_limiter/core/limiters.py): `_heartbeat_loop()` exception handling.

## ASGI Layer

The ASGI backend uses a request-oriented architecture that differs from the task-oriented backends. The `ASGIRateLimiter.acquire()` method wraps the `acquire.lua` Lua script call in a `try/except` that logs all exceptions and re-raises. The `RateLimitMiddleware` catches exceptions from `acquire()` and applies a configurable error strategy: `"fail_open"` allows the request to proceed without rate limit headers, while `"fail_closed"` returns a 503 Service Unavailable response.

```mermaid
%%{init: {"theme": "default", "themeVariables": {"lineColor": "#6e7781"}}}%%
flowchart TD
    A_REQUEST["Incoming HTTP request"]
    A_KEY{"key_func returns<br>a key?"}
    A_PASSTHROUGH["Pass through to<br>inner application"]
    A_ACQUIRE["ASGIRateLimiter.acquire(key)<br>EVALSHA acquire.lua"]
    A_NOSCRIPT{"NoScriptError?"}
    A_RETRY["Reload SHA,<br>retry via _eval_script()"]
    A_OTHER["Non-NoScript exception<br>logged by acquire()"]
    A_STRATEGY{"on_error<br>strategy?"}
    A_OPEN["fail_open: proceed<br>without headers"]
    A_CLOSED["fail_closed: return<br>503 Service Unavailable"]
    A_ALLOWED{"Request<br>allowed?"}
    A_HEADERS["Inject X-RateLimit-*<br>headers, pass through"]
    A_BLOCKED["Return 429 Too Many<br>Requests (or custom callback)"]

    A_REQUEST --> A_KEY
    A_KEY -- "None" --> A_PASSTHROUGH
    A_KEY -- "key" --> A_ACQUIRE
    A_ACQUIRE --> A_NOSCRIPT
    A_NOSCRIPT -- "Yes" --> A_RETRY --> A_ACQUIRE
    A_NOSCRIPT -- "No (other exception)" --> A_OTHER --> A_STRATEGY
    A_STRATEGY -- "fail_open" --> A_OPEN
    A_STRATEGY -- "fail_closed" --> A_CLOSED
    A_ACQUIRE -- "success" --> A_ALLOWED
    A_ALLOWED -- "Yes" --> A_HEADERS
    A_ALLOWED -- "No" --> A_BLOCKED

    style A_CLOSED fill:#ffcdd2,stroke:#D32F2F
    style A_BLOCKED fill:#fff3e0,stroke:#FF9800
    style A_OPEN fill:#e8f5e9,stroke:#388E3C
```

**Test coverage:**

| Path | Description | Tested by |
|------|-------------|-----------|
| key_func returns None | Request passes through without rate limiting | `implementations/asgi/test_middleware::test_none_key_bypasses_rate_limiting` |
| Non-HTTP scope | Request passes through unconditionally | `implementations/asgi/test_middleware::test_non_http_scope_passes_through` |
| acquire() raises, fail_open | Request proceeds without headers | `implementations/asgi/test_middleware::test_fail_open_allows_on_error` |
| acquire() raises, fail_closed | Returns 503 Service Unavailable | `implementations/asgi/test_middleware::test_fail_closed_returns_503_on_error` |
| acquire() script exception logged | Exception is logged with limiter ID | `implementations/asgi/test_asgi_limiter::test_acquire_logs_exception_on_script_failure` |
| Request allowed | X-RateLimit-* headers injected | `implementations/asgi/test_middleware::test_allowed_response_includes_rate_limit_headers` |
| Request blocked | 429 response with Retry-After header | `implementations/asgi/test_middleware::test_blocked_response_returns_429` |

## Failure Mode Traceability

The following table enumerates every identified failure mode, its handling strategy, and the test that validates it. Each failure mode is also visualised in the layer diagrams above; this table serves as a flat, searchable reference. Rows marked **Yes** in the Gap column represent failure paths that lack dedicated test coverage.

| # | Failure Mode | Source | Exception Type | Handling Strategy | Test Coverage | Gap? |
|---|---|---|---|---|---|---|
| 1 | Lua script cache flushed during `schedule_task()` | `schedule_task()` | `NoScriptError` | Reload SHA, retry once; `RuntimeError` on 2nd failure | `implementations/test_rate_limiter::test_lua_script_recovery_on_noscript_error`, `test_lua_script_permanent_failure_raises_error` | No |
| 2 | Lua script cache flushed during `consume()` | `consume()` | `NoScriptError` | Reload SHA, retry once; `RuntimeError` on 2nd failure | `implementations/test_rate_limiter::test_consume_lua_script_recovery_on_noscript_error`, `test_consume_lua_script_permanent_failure_raises_error` | No |
| 3 | Lua script cache flushed during `extend_lease()` | `extend_lease()` | `NoScriptError` | Reload SHA, retry once; `RuntimeError` on 2nd failure | `implementations/test_task_lifecycle::test_extend_lease_recovery_on_noscript_error`, `test_extend_lease_permanent_failure_raises_error` | No |
| 4 | Lua script cache flushed during `get_status()` | `get_status()` | `NoScriptError` | Reload SHA, retry once; `RuntimeError` on 2nd failure | `implementations/test_get_status::test_get_status_recovery_on_noscript_error`, `test_get_status_permanent_failure_raises_error` | No |
| 5 | Redis unreachable during `schedule_task()` | `schedule_task()` | `ConnectionError` | Cleanup inflight key, re-raise | `implementations/test_rate_limiter::test_schedule_non_noscript_failure_cleans_inflight_and_reraises` | No |
| 6 | Redis unreachable during `consume()` | `consume()` | `ConnectionError` | Propagates to `drain()` backoff | `implementations/test_rate_limiter::test_consume_connection_error_propagates` | No |
| 7 | Redis unreachable during `get_status()` | `get_status()` | `ConnectionError` | Propagates to caller (not caught) | `implementations/test_get_status::test_get_status_connection_error_propagates` | No |
| 8 | Redis unreachable during `extend_lease()` | `extend_lease()` | `ConnectionError` | Propagates to heartbeat loop; handled by warn/kill strategy | Indirectly via heartbeat tests with generic `Exception` mock | Partial |
| 9 | Redis unreachable during `TaskLifecycle.__exit__()` cleanup | `TaskLifecycle.__exit__()` | `Exception` | Propagates, but `trigger_consume()` still fires in `finally` | `contracts/test_task_lifecycle::test_lifecycle_cleans_up_on_exception` | No |
| 10 | Lease renewal for unknown task | `extend_lease()` | `KeyError` | Propagates to heartbeat loop | `implementations/test_task_lifecycle::test_extend_lease_raises_key_error_for_unknown_task` | No |
| 11 | Heartbeat failure in "warn" mode | `_heartbeat_loop()` | Any `Exception` | `is_healthy = False`, log critical | `implementations/test_task_lifecycle::test_heartbeat_loop_flags_unhealthy_on_failure_warn_mode` | No |
| 12 | Heartbeat failure in "kill" mode | `_heartbeat_loop()` | Any `Exception` | `os.kill(SIGTERM)`, break loop | `implementations/test_task_lifecycle::test_heartbeat_loop_terminates_worker_on_failure_kill_mode` | No |
| 13 | Lock acquire Lua script fails (fairness mode) | `DistributedLock.__enter__()` | `RedisError` | Propagates to `drain()` backoff (lock treated as not acquired) | Covered implicitly by #14 (drain catches all `_drain_inner()` exceptions) | No |
| 14 | Lock release Lua script fails (fairness mode) | `DistributedLock.__exit__()` | `RedisError` | Propagates to `drain()` backoff; lock expires via TTL (self-healing) | Covered implicitly by #15 (drain catches all `_drain_inner()` exceptions) | No |
| 15 | `drain()` inner failure (any exception) | `drain()` | Any `Exception` | Increment failure counter, schedule recovery with backoff | `implementations/test_drain::test_drain_handles_consume_exception`, `test_drain_handles_dispatch_exception` | No |
| 16 | Recovery scheduling also fails | `drain()` | Any `Exception` | Log critical; rely on watchdog or external trigger | `implementations/test_drain::test_drain_handles_double_failure_when_schedule_drain_also_fails` | No |
| 17 | Missing `limiter_id` in decorator | `@rate_limited` | `ValueError` | Propagates to caller | `implementations/test_decorator::test_decorator_raises_value_error_when_limiter_id_missing` | No |
| 18 | Missing `_rate_limit_task_id` in decorator | `@rate_limited` | `KeyError` | Propagates to caller | `implementations/test_decorator::test_decorator_raises_when_task_id_missing` | No |
| 19 | User function raises exception inside `@rate_limited` | `@rate_limited` wrapper | Any `Exception` | Propagates; `TaskLifecycle.__exit__()` cleanup still runs | `implementations/test_decorator::test_decorator_propagates_wrapped_function_exception` | No |
| 20 | `configure()` not called before `create()`/`get()` | Class API | `RuntimeError` | Propagates to caller | `implementations/test_rate_limiter_class_api::test_create_without_configure_raises`, `implementations/asyncio/test_rate_limiter_class_api::test_create_without_configure_raises` | No |
| 21 | Limiter not found in cache or Redis | `get()` | `ValueError` | Propagates to caller | `implementations/test_rate_limiter_class_api::test_get_nonexistent_limiter_raises_value_error`, `implementations/asyncio/test_rate_limiter_class_api::test_get_nonexistent_limiter_raises_value_error` | No |
| 22 | Duplicate limiter creation without override | `create()` | `ValueError` | Propagates to caller | `implementations/test_rate_limiter_class_api::test_create_duplicate_without_override_raises`, `implementations/asyncio/test_rate_limiter_class_api::test_create_duplicate_without_override_raises` | No |
| 23 | Direct constructor invocation (bypass class API) | `__init__()` | `RuntimeError` | Propagates to caller | `implementations/test_rate_limiter_class_api::test_direct_construction_raises_runtime_error`, `implementations/asyncio/test_rate_limiter_class_api::test_direct_construction_raises_runtime_error` | No |
| 24 | Corrupted JSON in persisted config | `refresh_config()` | `JSONDecodeError` | Catch, log warning, return `False` | `implementations/test_rate_limiter_class_api::test_refresh_config_handles_corrupted_redis_data`, `implementations/asyncio/test_rate_limiter_class_api::test_refresh_config_handles_corrupted_redis_data` | No |
| 25 | Lua script not found on disk | `load_lua_script()` | `ImportError` | Propagates (fatal at initialization) | `implementations/test_internal_helpers::test_load_lua_script_raises_import_error_on_failure` | No |
| 26 | Inflight key cleanup fails during scheduling error | `_cleanup_inflight_key()` | Any `Exception` | Suppressed, log warning | `implementations/test_internal_helpers::test_cleanup_inflight_key_suppresses_redis_failure` | No |
| 27 | Celery `send_task()` fails during dispatch | `_dispatch_task()` (Celery) | `Exception` | Propagates to `drain()` backoff | `implementations/celery/test_celery_limiter::test_dispatch_task_send_task_failure_propagates` | No |
| 28 | `import_string()` fails during dispatch | `_dispatch_task()` (ThreadPool) | `ModuleNotFoundError` | Propagates to `drain()` backoff | `implementations/threadpool/test_threadpool_limiter::test_dispatch_task_import_failure_propagates` | No |
| 29 | Metrics callback raises exception | `_emit_metric()` | Any `Exception` | Caught, logged, does not disrupt limiter | `implementations/test_metrics_callback::test_callback_exception_does_not_break_consume` | No |
| 30 | Missing backend context during `configure()` | `_configure_backend()` | `RuntimeError` | Propagates to caller | `test_rate_limiter_class_api` (per backend) | No |
| 31 | Sync function dispatched to async backend | `_dispatch_task()` (AsyncIO) | `TypeError` | Caught by `_run_task()` exception handler; task set cleaned up | `implementations/asyncio/test_asyncio_limiter::test_dispatch_sync_function_raises_type_error` | No |
| 32 | Lua script first package unavailable | `load_lua_script()` | `ModuleNotFoundError` | Falls back to second package in `resource_packages` | `implementations/test_internal_helpers::test_load_lua_script_falls_back_to_second_package` | No |
| 33 | ASGI `acquire()` script failure | `ASGIRateLimiter.acquire()` | Any `Exception` | Logged, re-raised to middleware | `implementations/asgi/test_asgi_limiter::test_acquire_logs_exception_on_script_failure` | No |
| 34 | ASGI middleware error, fail_open | `RateLimitMiddleware.__call__()` | Any `Exception` | Request proceeds without rate limit headers | `implementations/asgi/test_middleware::test_fail_open_allows_on_error` | No |
| 35 | ASGI middleware error, fail_closed | `RateLimitMiddleware.__call__()` | Any `Exception` | Returns 503 Service Unavailable | `implementations/asgi/test_middleware::test_fail_closed_returns_503_on_error` | No |

## References

- [base.py](../../src/celery_rate_limiter/core/base.py): centralized `_eval_script()` two-phase NoScriptError retry (sync and async).
- [scripts.py](../../src/celery_rate_limiter/core/scripts.py): `load_lua_script()` resource loading with fallback.
- [limiters.py](../../src/celery_rate_limiter/core/limiters.py): sync core implementation (scheduling, consumption, lifecycle, drain error handling).
- [async_limiters.py](../../src/celery_rate_limiter/core/async_limiters.py): async core implementation (async drain, dispatch, lifecycle).
- [decorators.py](../../src/celery_rate_limiter/core/decorators.py): `@rate_limited` decorator error paths.
- [celery/limiter.py](../../src/celery_rate_limiter/backends/celery/limiter.py): Celery backend dispatch (`send_task`) error propagation.
- [threading/limiter.py](../../src/celery_rate_limiter/backends/threading/limiter.py): ThreadPool backend dispatch (`import_string`) error propagation.
- [asyncio/limiter.py](../../src/celery_rate_limiter/backends/asyncio/limiter.py): AsyncIO backend dispatch (`create_task`) error propagation and sync function guard.
- [asgi/limiter.py](../../src/celery_rate_limiter/backends/asgi/limiter.py): ASGI `acquire()` error logging and re-raise.
- [asgi/middleware.py](../../src/celery_rate_limiter/backends/asgi/middleware.py): ASGI middleware fail_open/fail_closed error strategy.
- [Drain Loop Flow](drain-flow.md): the three-layer drain control loop and feedback entry points.
- [Task State Diagram](task-states.md): all possible task states, including crash recovery mechanisms.
