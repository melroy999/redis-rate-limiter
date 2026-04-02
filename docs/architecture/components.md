# Component Diagram

The component diagram provides a high-level overview of the system's architecture. It depicts the major runtime components and the interactions between them, and as such, it is intended to be the first point of reference when reading the codebase. The content is organised as four diagrams: an overview that shows the four architectural layers as abstracted blocks, followed by one detail diagram per phase (scheduling, drain/consume/dispatch, execution/completion). Each detail diagram is accompanied by a test coverage table that maps every interaction arrow to the test(s) that exercise it.

There are several aspects of the architecture that are worth highlighting:

- **Redis is the single source of truth** for all rate limiting state, i.e., all state mutations are performed through atomic Lua scripts that are executed on the Redis server.
- **The feedback loop**: upon task completion, the `TaskLifecycle` component signals the `DrainLoop` to wake up and fill the freed concurrency slot with the next buffered task. Said feedback loop is the mechanism that keeps the throughput at the configured rate.
- **Backends are interchangeable**, which means that the core rate limiter is agnostic to the dispatch mechanism used, i.e., it does not need to know whether tasks are executed in Celery workers or local threads.

## Overview

```mermaid
%%{init: {"theme": "default", "themeVariables": {"lineColor": "#6e7781"}}}%%
graph LR
    User["User Code"]

    subgraph Limiter ["Rate Limiter Core"]
        LimiterBlock["Scheduler, DrainLoop,<br>DistributedLock, Consumer"]
    end

    subgraph RedisLayer ["Redis"]
        RedisBlock["Lua Scripts, Buffer,<br>Window Counters, Concurrency Set,<br>Inflight Keys, Dead Letter Queue"]
    end

    subgraph Backends ["Backend"]
        BackendBlock["CeleryRateLimiter,<br>DramatiqRateLimiter,<br>HueyRateLimiter,<br>RQRateLimiter,<br>ThreadPoolRateLimiter,<br>AsyncIOTaskLimiter,<br>ASGIRateLimiter"]
    end

    subgraph Execution ["Task Execution"]
        ExecBlock["Worker / Thread,<br>TaskLifecycle"]
    end

    User -->|"schedule_task()"| LimiterBlock
    LimiterBlock -->|"Lua scripts"| RedisBlock
    LimiterBlock -->|"_dispatch_task()"| BackendBlock
    BackendBlock -->|"send_task() / actor.send() / submit()"| ExecBlock
    ExecBlock -->|"cleanup + renew"| RedisBlock
    ExecBlock -.->|"trigger_consume()<br>feedback loop"| LimiterBlock

    style Limiter fill:#e8f4f8,stroke:#2196F3
    style RedisLayer fill:#fff3e0,stroke:#FF9800
    style Backends fill:#e8f5e9,stroke:#4CAF50
    style Execution fill:#f3e5f5,stroke:#9C27B0
```

**Legend:**

- *Blue subgraph* (Rate Limiter Core): the scheduler, drain loop, distributed lock and consumer components that orchestrate task flow.
- *Orange subgraph* (Redis): all Lua scripts and data structures that constitute the single source of truth.
- *Green subgraph* (Backend): the interchangeable dispatch backends (Celery, Dramatiq, Huey, RQ, ThreadPool, AsyncIO and ASGI).
- *Purple subgraph* (Task Execution): the worker or thread that runs the user function and the `TaskLifecycle` context manager that manages the concurrency lease.
- *Solid arrows* represent synchronous calls or Redis commands.
- *Dashed arrow* represents the feedback loop, i.e., the path through which task completion triggers the next drain cycle.

## Scheduling

The scheduling phase covers the path from user code to the Redis buffer. When `schedule_task()` is called, the scheduler acquires the inflight deduplication key, buffers the task via the `schedule.lua` Lua script, and wakes the drain loop.

```mermaid
%%{init: {"theme": "default", "themeVariables": {"lineColor": "#6e7781"}}}%%
graph TD
    User["User Code<br>schedule_task(func, payload)"]

    subgraph Limiter ["Rate Limiter Core"]
        Scheduler["Scheduler<br>Dedup check + buffer task"]
        DrainLoop["DrainLoop<br>Background thread<br>Coalesces wake signals"]
    end

    subgraph Redis ["Redis"]
        LuaScripts["Lua Scripts<br>schedule.lua"]
        Buffer["Buffer<br>ZSET: priority queue"]
        Inflight["Inflight Keys<br>Dedup markers via SET NX"]
    end

    User -->|"schedule_task()"| Scheduler
    Scheduler -->|"EVALSHA schedule.lua"| LuaScripts
    LuaScripts -->|"ZADD"| Buffer
    Scheduler -->|"SET NX"| Inflight
    Scheduler -->|"wake(delay=0)"| DrainLoop

    style Limiter fill:#e8f4f8,stroke:#2196F3
    style Redis fill:#fff3e0,stroke:#FF9800
```

**Test coverage:**

| Arrow | Interaction | Tested by |
|-------|-------------|-----------|
| User → Scheduler | `schedule_task()` called | `contracts/test_rate_limiter::test_schedule_task_returns_success_and_task_id` |
| Scheduler → LuaScripts | EVALSHA schedule.lua | `implementations/test_rate_limiter::test_schedule_single_task_stores_correctly`, `implementations/test_lua_script_infrastructure::test_eval_script_recovers_from_transient_noscript` |
| Scheduler → Inflight | SET NX dedup marker | `contracts/test_rate_limiter::test_schedule_task_marks_task_as_inflight`, `implementations/test_rate_limiter::test_schedule_duplicate_task_skips_second` |
| Scheduler → DrainLoop | wake(delay=0) | `implementations/test_drain::test_trigger_consume_schedules_drain` |

## Drain, Consume, and Dispatch

The drain, consume and dispatch phase covers the path from the drain loop through consumption to backend dispatch. The drain loop acquires the distributed lock, the consumer invokes `consume.lua` to atomically check the rate window, verify concurrency capacity, pop a task from the buffer and register the concurrency lease, and then the consumer dispatches the task to the configured backend.

```mermaid
%%{init: {"theme": "default", "themeVariables": {"lineColor": "#6e7781"}}}%%
graph TD
    subgraph Limiter ["Rate Limiter Core"]
        DrainLoop["DrainLoop<br>Background thread<br>Coalesces wake signals"]
        Lock["DistributedLock<br>Contention-aware fairness<br>Prevents concurrent drains"]
        Consumer["Consumer<br>Check window + concurrency<br>Pop task from buffer"]
    end

    subgraph Redis ["Redis"]
        LuaScripts["Lua Scripts<br>consume.lua"]
        Buffer["Buffer<br>ZSET: priority queue"]
        WindowCounters["Window Counters<br>Sliding window tokens"]
        ConcurrencySet["Concurrency Set<br>ZSET: lease-based slots"]
        DLQ["Dead Letter Queue<br>Expired tasks"]
    end

    subgraph Backends ["Backend"]
        Celery["CeleryRateLimiter<br>app.send_task()"]
        Dramatiq["DramatiqRateLimiter<br>actor.send()"]
        Huey["HueyRateLimiter<br>huey_task()"]
        RQ["RQRateLimiter<br>queue.enqueue()"]
        ThreadPool["ThreadPoolRateLimiter<br>executor.submit()"]
        AsyncIO["AsyncIOTaskLimiter<br>asyncio.create_task()"]
    end

    Worker["Worker / Thread / Coroutine<br>Runs user function"]

    DrainLoop -->|"drain()"| Lock
    Lock -->|"if acquired"| Consumer
    Consumer -->|"EVALSHA consume.lua"| LuaScripts
    LuaScripts -->|"GET/INCR"| WindowCounters
    LuaScripts -->|"ZPOPMIN"| Buffer
    LuaScripts -->|"ZADD lease"| ConcurrencySet
    LuaScripts -->|"RPUSH expired"| DLQ
    Consumer -->|"_dispatch_task()"| Celery
    Consumer -->|"_dispatch_task()"| Dramatiq
    Consumer -->|"_dispatch_task()"| Huey
    Consumer -->|"_dispatch_task()"| RQ
    Consumer -->|"_dispatch_task()"| ThreadPool
    Consumer -->|"_dispatch_task()"| AsyncIO
    Celery -->|"send_task()"| Worker
    Dramatiq -->|"actor.send()"| Worker
    Huey -->|"huey_task()"| Worker
    RQ -->|"queue.enqueue()"| Worker
    ThreadPool -->|"submit()"| Worker
    AsyncIO -->|"create_task()"| Worker

    style Limiter fill:#e8f4f8,stroke:#2196F3
    style Redis fill:#fff3e0,stroke:#FF9800
    style Backends fill:#e8f5e9,stroke:#4CAF50
```

**Test coverage:**

| Arrow | Interaction | Tested by |
|-------|-------------|-----------|
| DrainLoop → Lock | drain() acquires dispatch_lock | `implementations/test_drain_loop::test_wake_default_delay_is_zero`, `implementations/test_drain::test_drain_schedules_backup_when_lock_contended` |
| Lock → Consumer | Lock acquired, proceed | `implementations/test_concurrent_access::test_distributed_lock_serializes_drains` |
| Consumer → LuaScripts | EVALSHA consume.lua | `contracts/test_rate_limiter::test_consume_returns_expected_structure`, `implementations/test_lua_script_infrastructure::test_eval_script_recovers_from_transient_noscript` |
| LuaScripts → WindowCounters | GET/INCR rate check | `integration/test_rate_limiting::test_basic_rate_limit_enforcement` |
| LuaScripts → ConcurrencySet | ZADD lease slot | `integration/test_rate_limiting::test_concurrency_limit_enforcement` |
| LuaScripts → DLQ | RPUSH expired task | `integration/test_rate_limiting::test_expired_task_moved_to_dlq` |
| Consumer → Backends | _dispatch_task() | `implementations/celery/test_celery_limiter::test_dispatch_task_use_executor_true_sends_generic_worker`, `implementations/dramatiq/test_dramatiq_limiter::test_dispatch_task_use_executor_true_sends_generic_worker`, `implementations/huey/test_huey_limiter::test_dispatch_task_use_executor_true_calls_generic_worker`, `implementations/threadpool/test_threadpool_limiter::test_dispatch_task_submits_to_executor`, `implementations/asyncio/test_asyncio_limiter::test_dispatch_task_creates_asyncio_task` |
| Backends → Worker | send_task() / actor.send() / huey_task() / queue.enqueue() / submit() / create_task() | `implementations/celery/test_celery_limiter::test_dispatch_task_use_executor_false_sends_custom_task`, `implementations/dramatiq/test_dramatiq_limiter::test_dispatch_task_use_executor_false_sends_custom_actor`, `implementations/huey/test_huey_limiter::test_dispatch_task_use_executor_false_calls_custom_task`, `implementations/rq/test_rq_limiter::test_dispatch_task_use_executor_true_enqueues_generic_worker`, `implementations/threadpool/test_threadpool_limiter::test_dispatch_task_resolves_function_path`, `implementations/asyncio/test_asyncio_limiter::test_dispatch_sync_function_raises_type_error` |

## Execution and Completion

The execution and completion phase covers the path from the worker through the `TaskLifecycle` context manager back to the drain loop. The lifecycle manages the heartbeat thread that renews the concurrency lease, and upon completion (or exception), it releases the concurrency slot, deletes the inflight deduplication key, and triggers the drain loop to consume the next buffered task.

```mermaid
%%{init: {"theme": "default", "themeVariables": {"lineColor": "#6e7781"}}}%%
graph TD
    subgraph Execution ["Task Execution"]
        Worker["Worker / Thread / Coroutine<br>Runs user function"]
        Lifecycle["TaskLifecycle<br>Heartbeat thread<br>Lease renewal"]
    end

    subgraph Redis ["Redis"]
        LuaScripts["Lua Scripts<br>renew.lua"]
        ConcurrencySet["Concurrency Set<br>ZSET: lease-based slots"]
        Inflight["Inflight Keys<br>Dedup markers via SET NX"]
    end

    DrainLoop["DrainLoop<br>Background thread<br>Coalesces wake signals"]

    Worker -->|"wraps execution"| Lifecycle
    Lifecycle -->|"EVALSHA renew.lua"| LuaScripts
    LuaScripts -->|"ZADD new expiry"| ConcurrencySet
    Lifecycle -->|"ZREM slot"| ConcurrencySet
    Lifecycle -->|"DEL marker"| Inflight
    Lifecycle -.->|"trigger_consume()<br>feedback loop"| DrainLoop

    style Execution fill:#f3e5f5,stroke:#9C27B0
    style Redis fill:#fff3e0,stroke:#FF9800
```

**Test coverage:**

| Arrow | Interaction | Tested by |
|-------|-------------|-----------|
| Worker → Lifecycle | Wraps execution in TaskLifecycle | `implementations/test_decorator::test_decorator_wraps_function_in_task_lifecycle`, `implementations/threadpool/test_threadpool_limiter::test_dispatch_task_wraps_in_lifecycle` |
| Lifecycle → LuaScripts | EVALSHA renew.lua heartbeat | `implementations/test_task_lifecycle::test_heartbeat_loop_calls_extend_lease_with_correct_parameters`, `implementations/test_task_lifecycle::test_extend_lease_succeeds_for_existing_task` |
| Lifecycle → DrainLoop | trigger_consume() feedback loop | `contracts/test_task_lifecycle::test_lifecycle_triggers_consume` |

## ASGI Request Flow

The ASGI backend uses a fundamentally different architecture from the task-oriented backends. There is no buffer, concurrency set, drain loop, or task lifecycle. Instead, each HTTP request performs a lightweight "try acquire" check against the sliding window counter via `acquire.lua`. The `RateLimitMiddleware` extracts the client identity using a configurable `key_func`, invokes `ASGIRateLimiter.acquire()`, and either passes the request through with rate limit headers or returns a 429 response.

```mermaid
%%{init: {"theme": "default", "themeVariables": {"lineColor": "#6e7781"}}}%%
graph TD
    Request["HTTP Request"]

    subgraph Middleware ["RateLimitMiddleware"]
        KeyFunc["key_func(scope)<br>Extract client identity"]
        Acquire["ASGIRateLimiter.acquire(key)"]
        Decision{"Allowed?"}
        Headers["Inject X-RateLimit-*<br>headers, pass through"]
        Blocked["429 Too Many Requests<br>(or custom on_blocked callback)"]
        ErrorStrategy{"on_error?"}
        FailOpen["fail_open: proceed<br>without headers"]
        FailClosed["fail_closed: 503<br>Service Unavailable"]
    end

    subgraph Redis ["Redis"]
        AcquireLua["Lua Scripts<br>acquire.lua"]
        WindowCounters["Per-Identity<br>Window Counters"]
    end

    InnerApp["Inner ASGI Application"]

    Request --> KeyFunc
    KeyFunc --> Acquire
    Acquire -->|"EVALSHA acquire.lua"| AcquireLua
    AcquireLua -->|"GET/INCR"| WindowCounters
    Acquire --> Decision
    Decision -- "Yes" --> Headers --> InnerApp
    Decision -- "No" --> Blocked
    Acquire -. "exception" .-> ErrorStrategy
    ErrorStrategy -- "fail_open" --> FailOpen --> InnerApp
    ErrorStrategy -- "fail_closed" --> FailClosed

    style Middleware fill:#e8f4f8,stroke:#2196F3
    style Redis fill:#fff3e0,stroke:#FF9800
```

**Test coverage:**

| Arrow | Interaction | Tested by |
|-------|-------------|-----------|
| Request → KeyFunc | key_func extracts identity from scope | `implementations/asgi/test_keys::test_extracts_ip_from_client_tuple`, `implementations/asgi/test_keys::test_extracts_header_value` |
| KeyFunc → Acquire | acquire() called with extracted key | `implementations/asgi/test_asgi_limiter::test_acquire_allowed_under_limit` |
| Acquire → AcquireLua | EVALSHA acquire.lua | `implementations/asgi/test_asgi_limiter::test_acquire_denied_at_limit` |
| Allowed → Headers → InnerApp | Rate limit headers injected | `implementations/asgi/test_middleware::test_allowed_response_includes_rate_limit_headers` |
| Not allowed → Blocked | 429 response returned | `implementations/asgi/test_middleware::test_blocked_request_returns_429` |
| Exception → fail_open | Request proceeds | `implementations/asgi/test_middleware::test_fail_open_allows_on_error` |
| Exception → fail_closed | 503 response returned | `implementations/asgi/test_middleware::test_fail_closed_returns_503_on_error` |
