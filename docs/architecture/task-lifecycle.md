# Task Lifecycle Sequence

The sequence diagram traces the flow of a single task from the moment it is scheduled to the moment its execution completes. The diagram is divided into four phases, namely the *scheduling* phase, the *drain and consume* phase, the *dispatch and execution* phase and the *completion* phase. It should be noted that the diagram depicts the nominal flow exclusively. The handling of error conditions, such as script cache misses, lock contention and task expiration to the dead letter queue, is documented in the [error handling](error-handling.md) reference.

Several observations can be made about the lifecycle:

- **Scheduling is synchronous**, i.e., the caller receives the result `(True, task_id)` immediately upon scheduling.
- **Consumption is asynchronous**: the `DrainLoop` runs in a background thread and may fire milliseconds or seconds after the task has been scheduled.
- **Lua scripts are atomic**: `consume.lua` checks the rate window, verifies the concurrency capacity, pops the task from the buffer, increments the window counter and registers the concurrency lease, all within a single indivisible Redis operation. As such, race conditions between concurrent consumers are eliminated.
- **The heartbeat keeps the lease alive** during task execution. If the worker crashes, the lease expires and the concurrency slot is reclaimed automatically on the next consume invocation.
- **The cycle repeats**: the completion of a task triggers the `DrainLoop` to wake up and consume the next buffered task, which in turn closes the feedback loop.

```mermaid
%%{init: {"theme": "default", "themeVariables": {"lineColor": "#6e7781"}}}%%
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
    note right of R: Injects __meta_arrived_at,<br>ZADD buffer with priority

    L->>D: wake(delay=0)
    L-->>-U: return (True, task_id)

    note over U,W: Phase 2: Drain and Consume

    D->>+L: drain()
    L->>R: Acquire dispatch_lock (check cooldown, SET NX, record contention)
    R-->>L: OK (lock acquired)

    L->>R: EVALSHA consume.lua
    note right of R: Atomic: check window rate,<br>check concurrency cap,<br>pop task from buffer,<br>increment window counter,<br>register concurrency lease

    R-->>L: task_data + telemetry

    note over U,W: Phase 3: Dispatch and Execute

    L->>B: _dispatch_task(func, payload, task_id)
    L->>R: Release dispatch_lock (verify token, check contention, set cooldown)
    deactivate L

    B->>+W: send_task() / submit()
    activate W
    note over W: TaskLifecycle.__enter__()

    loop Every lease_duration / 2
        W->>R: EVALSHA renew.lua
        note right of R: ZADD concurrency<br>with new expiry
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

**Test coverage:**

| Phase | Message | Description | Tested by |
|-------|---------|-------------|-----------|
| **Schedule** | U → L: schedule_task() | Schedule invocation returns (bool, task_id) | `contracts/test_rate_limiter::test_schedule_task_returns_success_and_task_id` |
| **Schedule** | L → R: SET NX inflight | Deduplication via inflight key | `contracts/test_rate_limiter::test_schedule_task_marks_task_as_inflight`, `integration/test_rate_limiting::test_bulk_deduplication_only_buffers_one_task` |
| **Schedule** | L → R: EVALSHA schedule.lua | Buffer insertion | `contracts/test_rate_limiter::test_schedule_task_adds_to_buffer`, `implementations/test_rate_limiter::test_schedule_single_task_stores_correctly` |
| **Schedule** | L → D: wake(delay=0) | DrainLoop triggered after scheduling | `implementations/test_drain::test_trigger_consume_schedules_drain` |
| **Drain** | D → L: drain() | DrainLoop calls drain | `implementations/test_drain_loop::test_wake_fires_drain_immediately` |
| **Drain** | L → R: Acquire dispatch_lock | Distributed lock acquisition (contention-aware) | `implementations/test_drain::test_drain_schedules_backup_when_lock_contended`, `implementations/test_concurrent_access::test_distributed_lock_serializes_drains`, `implementations/test_concurrent_access::test_contention_aware_cooldown_distributes_drains` |
| **Drain** | L → R: EVALSHA consume.lua | Atomic consumption | `contracts/test_rate_limiter::test_consume_returns_expected_structure`, `integration/test_rate_limiting::test_basic_rate_limit_enforcement` |
| **Execute** | L → B: _dispatch_task() | Backend dispatch | `implementations/test_drain::test_drain_dispatches_task_and_schedules_follow_up` |
| **Execute** | W: TaskLifecycle.__enter__() | Lifecycle context entered | `implementations/test_decorator::test_decorator_wraps_function_in_task_lifecycle` |
| **Execute** | W → R: EVALSHA renew.lua | Heartbeat lease renewal | `implementations/test_task_lifecycle::test_heartbeat_loop_extends_lease_periodically`, `implementations/test_task_lifecycle::test_extend_lease_succeeds_for_existing_task` |
| **Completion** | W: TaskLifecycle.__exit__() | Lifecycle context exit | `contracts/test_task_lifecycle::test_lifecycle_removes_task_from_concurrency_set` |
| **Completion** | W → R: ZREM + DEL | Slot released, dedup cleared | `contracts/test_task_lifecycle::test_lifecycle_removes_active_marker`, `integration/test_rate_limiting::test_task_lifecycle_releases_slot_on_error` |
| **Completion** | W → D: trigger_consume() | Feedback loop: completion triggers next drain | `contracts/test_task_lifecycle::test_lifecycle_triggers_consume`, `contracts/test_task_lifecycle::test_lifecycle_triggers_consume_even_on_exception` |
