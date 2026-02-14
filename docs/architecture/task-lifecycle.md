# Task Lifecycle Sequence

The sequence diagram traces the flow of a single task from the moment it is scheduled to the moment its execution completes. The diagram is divided into four phases, namely the *scheduling* phase, the *drain and consume* phase, the *dispatch and execution* phase and the *completion* phase. It should be noted that the diagram depicts the nominal flow exclusively. The handling of error conditions, such as script cache misses, lock contention and task expiration to the dead letter queue, is documented in the source code at [limiters.py](../../src/celery_rate_limiter/core/limiters.py).

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
