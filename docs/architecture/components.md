# Component Diagram

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
