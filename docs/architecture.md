# Architecture Diagrams

This document presents a collection of diagrams that provide visual overviews of the system's architecture. The diagrams have been created using [Mermaid](https://mermaid.js.org/), such that they can be rendered natively on GitHub and produce meaningful diffs during code review.

The structure of this document is as follows. First, the *Component Diagram* in Section 1 depicts the major runtime components and their interactions. Subsequently, the *Task Lifecycle Sequence* in Section 2 traces the flow of a single task through the system. Finally, the *Class Hierarchy* in Section 3 presents the inheritance and composition relationships between the classes.

## 1. Component Diagram

The component diagram provides a high-level overview of the system's architecture. It depicts the major runtime components and the interactions between them, and as such, it is intended to be the first point of reference when reading the codebase.

There are several aspects of the architecture that are worth highlighting:

- **Redis is the single source of truth** for all rate limiting state, i.e., all state mutations are performed through atomic Lua scripts that are executed on the Redis server.
- **The feedback loop**: upon task completion, the `TaskLifecycle` component signals the `DrainLoop` to wake up and fill the freed concurrency slot with the next buffered task. Said feedback loop is the mechanism that keeps the throughput at the configured rate.
- **Backends are interchangeable**, which means that the core rate limiter is agnostic to the dispatch mechanism used, i.e., it does not need to know whether tasks are executed in Celery workers or local threads.

```mermaid
graph TD
    User["User Code\nschedule_task(func, payload)"]

    subgraph Limiter ["Rate Limiter Core"]
        Scheduler["Scheduler\nDedup check + buffer task"]
        DrainLoop["DrainLoop\nBackground thread\nCoalesces wake signals"]
        Lock["DistributedLock\nRedis SET NX\nPrevents concurrent drains"]
        Consumer["Consumer\nCheck window + concurrency\nPop task from buffer"]
    end

    subgraph Redis ["Redis"]
        LuaScripts["Lua Scripts\nschedule, consume\nhealth, renew"]
        Buffer["Buffer\nZSET: priority queue"]
        WindowCounters["Window Counters\nSliding window tokens"]
        ConcurrencySet["Concurrency Set\nZSET: lease-based slots"]
        Inflight["Inflight Keys\nDedup markers via SET NX"]
        DLQ["Dead Letter Queue\nExpired tasks"]
    end

    subgraph Backends ["Backend"]
        Celery["CeleryRateLimiter\napp.send_task()"]
        ThreadPool["ThreadPoolRateLimiter\nexecutor.submit()"]
    end

    subgraph Execution ["Task Execution"]
        Worker["Worker / Thread\nRuns user function"]
        Lifecycle["TaskLifecycle\nHeartbeat thread\nLease renewal"]
    end

    User -->|"schedule_task()"| Scheduler
    Scheduler -->|"EVALSHA schedule.lua"| LuaScripts
    LuaScripts -->|"ZADD"| Buffer
    Scheduler -->|"SET NX"| Inflight
    Scheduler -->|"wake(delay=0)"| DrainLoop

    DrainLoop -->|"drain()"| Lock
    Lock -->|"if acquired"| Consumer
    Consumer -->|"EVALSHA consume.lua"| LuaScripts
    LuaScripts -->|"GET/INCR"| WindowCounters
    LuaScripts -->|"ZPOPMIN"| Buffer
    LuaScripts -->|"ZADD lease"| ConcurrencySet
    LuaScripts -->|"RPUSH expired"| DLQ

    Consumer -->|"_dispatch_task()"| Celery
    Consumer -->|"_dispatch_task()"| ThreadPool

    Celery -->|"send_task()"| Worker
    ThreadPool -->|"submit()"| Worker

    Worker -->|"wraps execution"| Lifecycle
    Lifecycle -->|"EVALSHA renew.lua"| LuaScripts
    LuaScripts -->|"ZADD new expiry"| ConcurrencySet

    Lifecycle -->|"ZREM slot"| ConcurrencySet
    Lifecycle -->|"DEL marker"| Inflight
    Lifecycle -.->|"trigger_consume()\nfeedback loop"| DrainLoop

    style Limiter fill:#e8f4f8,stroke:#2196F3
    style Redis fill:#fff3e0,stroke:#FF9800
    style Backends fill:#e8f5e9,stroke:#4CAF50
    style Execution fill:#f3e5f5,stroke:#9C27B0
```

**Legend:**

- *Solid arrows* represent direct method calls or Redis commands.
- *Dashed arrow* represents the feedback loop, i.e., the path through which task completion triggers the next drain cycle.
- *Subgraph colors*: blue for the limiter core, orange for Redis, green for the backends and purple for the task execution layer.

## 2. Task Lifecycle Sequence

The sequence diagram traces the flow of a single task from the moment it is scheduled to the moment its execution completes. The diagram is divided into four phases, namely the *scheduling* phase, the *drain and consume* phase, the *dispatch and execution* phase and the *completion* phase. It should be noted that the diagram depicts the nominal flow exclusively. The handling of error conditions, such as script cache misses, lock contention and task expiration to the dead letter queue, is documented in the source code at [limiters.py](../src/celery_rate_limiter/core/limiters.py).

Several observations can be made about the lifecycle:

- **Scheduling is synchronous**, i.e., the caller receives the result `(True, task_id)` immediately upon scheduling.
- **Consumption is asynchronous**: the `DrainLoop` runs in a background thread and may fire milliseconds or seconds after the task has been scheduled.
- **Lua scripts are atomic**: `consume.lua` checks the rate window, verifies the concurrency capacity, pops the task from the buffer, increments the window counter and registers the concurrency lease, all within a single indivisible Redis operation. As such, race conditions between concurrent consumers are eliminated.
- **The heartbeat keeps the lease alive** during task execution. If the worker crashes, the lease expires and the concurrency slot is reclaimed automatically on the next consume invocation.
- **The cycle repeats**: the completion of a task triggers the `DrainLoop` to wake up and consume the next buffered task, which in turn closes the feedback loop.

```mermaid
sequenceDiagram
    participant U as User Code
    participant L as Limiter
    participant R as Redis
    participant D as DrainLoop
    participant B as Backend
    participant W as Worker

    note over U,W: Phase 1: Schedule

    U->>+L: schedule_task(func, payload, priority)
    L->>R: SET NX inflight:{task_id} (dedup)
    R-->>L: OK (first time) / nil (duplicate)

    L->>R: EVALSHA schedule.lua
    note right of R: Injects __meta_arrived_at,<br/>ZADD buffer with priority

    L->>D: wake(delay=0)
    L-->>-U: return (True, task_id)

    note over U,W: Phase 2: Drain and Consume

    D->>+L: drain()
    L->>R: SET NX dispatch_lock (acquire)
    R-->>L: OK (lock acquired)

    L->>R: EVALSHA consume.lua
    note right of R: Atomic: check window rate,<br/>check concurrency cap,<br/>pop task from buffer,<br/>increment window counter,<br/>register concurrency lease

    R-->>L: task_data + telemetry

    note over U,W: Phase 3: Dispatch and Execute

    L->>B: _dispatch_task(func, payload, task_id)
    L->>R: DEL dispatch_lock (release)
    deactivate L

    B->>+W: send_task() / submit()
    activate W
    note over W: TaskLifecycle.__enter__()

    loop Every lease_duration / 2
        W->>R: EVALSHA renew.lua
        note right of R: ZADD concurrency<br/>with new expiry
    end

    note over W: Execute user function

    note over U,W: Phase 4: Completion

    note over W: TaskLifecycle.__exit__()
    W->>R: ZREM concurrency (release slot)
    W->>R: DEL inflight:{task_id} (clear dedup)
    W->>D: trigger_consume()
    deactivate W

    note over D: DrainLoop wakes, cycle repeats
```

## 3. Class Hierarchy

The class diagram presents the inheritance and composition relationships between the classes that constitute the rate limiter. It is intended to serve as a reference for developers that wish to extend the system with a new backend or understand the existing extension points.

The class hierarchy consists of two levels of abstraction:

- **`AbstractDistributedRateLimiter`** contains all of the core rate limiting logic, including the Lua script execution, drain orchestration and sliding window calculations.
- **`AbstractRedisManagedRateLimiter`** extends the base class with a registry pattern that provides a class-level API for the creation, retrieval and updating of limiter instances, with the configuration being persisted to Redis.

Concrete backends, such as `CeleryRateLimiter` and `ThreadPoolRateLimiter`, inherit from the managed class and are required to implement the following:

- **`_dispatch_task()`**, which defines how tasks are sent to the execution environment.
- **The four backend context methods** (`_configure_backend`, `_has_backend_context`, `_get_instance_context`, `_reset_backend_context`), which define how the backend is configured.

All other functionality is inherited from the abstract classes.

Furthermore, the supporting classes `DrainLoop`, `DistributedLock` and `TaskLifecycle` are *composed* rather than inherited. The `DrainLoop` is owned by the limiter and created during construction, whereas the `DistributedLock` and `TaskLifecycle` instances are created on demand through the factory methods `execution_lock()` and `task_lifecycle()` respectively.

```mermaid
classDiagram
    class AbstractDistributedRateLimiter {
        <<abstract>>
        +Redis redis
        +str id
        +int limit
        +float window
        +int max_concurrency
        +schedule_task(func_path, payload, priority, max_age) tuple
        +consume() ConsumeResult
        +drain()
        +trigger_consume()
        +extend_lease(task_id, duration)
        +get_status() dict
        +task_lifecycle(task_id) TaskLifecycle
        +execution_lock(timeout_ms) DistributedLock
        +shutdown()
        #_dispatch_task(func_path, payload, task_id)* void
    }

    class AbstractRedisManagedRateLimiter {
        <<abstract>>
        +configure(redis_client, **backend_context)$ void
        +create(limiter_id, limit, window, max_concurrency)$ instance
        +get(limiter_id)$ instance
        +update(limiter_id, **kwargs)$ instance
        +refresh_config() bool
        #_configure_backend(**context)* void
        #_has_backend_context()* bool
        #_get_instance_context()* dict
        #_reset_backend_context()* void
    }

    class CeleryRateLimiter {
        +Celery app
        #_dispatch_task(func_path, payload, task_id) void
        #_configure_backend(**context) void
        #_has_backend_context() bool
        #_get_instance_context() dict
    }

    class ThreadPoolRateLimiter {
        +ThreadPoolExecutor executor
        #_dispatch_task(func_path, payload, task_id) void
        #_configure_backend(**context) void
        #_has_backend_context() bool
        #_get_instance_context() dict
    }

    class DrainLoop {
        +wake(delay) void
        +shutdown() void
        -_run() void
    }

    class DistributedLock {
        +__enter__() bool
        +__exit__() void
    }

    class TaskLifecycle {
        +bool is_healthy
        +__enter__() TaskLifecycle
        +__exit__() void
        -_heartbeat_loop() void
    }

    AbstractRedisManagedRateLimiter --|> AbstractDistributedRateLimiter : inherits
    CeleryRateLimiter --|> AbstractRedisManagedRateLimiter : inherits
    ThreadPoolRateLimiter --|> AbstractRedisManagedRateLimiter : inherits

    AbstractDistributedRateLimiter *-- DrainLoop : owns (1 instance)
    AbstractDistributedRateLimiter ..> DistributedLock : creates via execution_lock()
    AbstractDistributedRateLimiter ..> TaskLifecycle : creates via task_lifecycle()
```

**UML notation guide:**

| Symbol | Meaning |
|--------|---------|
| `+` | Public method or attribute |
| `#` | Protected method (intended for subclass use) |
| `-` | Private method (internal implementation) |
| `$` | Class method (called on the class, not an instance) |
| `*` | Abstract (must be implemented by subclasses) |
| `--\|>` | Inheritance (*is a*) |
| `*--` | Composition (*owns a*, created and destroyed together) |
| `..>` | Dependency (*uses*, creates on demand, does not own) |
