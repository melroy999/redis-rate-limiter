# Drain Loop Flow

The drain loop is a three-layer control loop that orchestrates the consumption and dispatch of buffered tasks. The outermost layer (`DrainLoop._run()`) is a background thread that manages wake signals and watchdog timing. The middle layer (`drain()`) handles configuration refresh, window-change pauses, error recovery, and exponential backoff. The innermost layer (`_drain_inner()`) performs the actual lock acquisition, task consumption, dispatch, and rescheduling. This document presents the control loop as four diagrams: an overview that shows how the layers connect, followed by one detailed flowchart per layer. Each detail diagram is accompanied by a test coverage table that maps every decision branch to the test(s) that exercise it.

## Overview

```mermaid
flowchart TD
    subgraph Triggers ["Feedback Entry Points"]
        T_SCHEDULE["schedule_task()\ncalls trigger_consume()"]
        T_LIFECYCLE["TaskLifecycle.__exit__()\ncalls trigger_consume()"]
        T_FOLLOWUP["Remaining tasks > 0:\nimmediate follow-up"]
        T_RATE["Rate limited:\ndelayed retry"]
        T_WATCHDOG["Watchdog timer:\nevery window × 2"]
    end

    subgraph L1 ["Layer 1: DrainLoop._run()"]
        L1_BLOCK["Background thread:\nmanages wake signals,\nwatchdog timeout,\nwake coalescing"]
    end

    subgraph L2 ["Layer 2: drain()"]
        L2_BLOCK["Config refresh,\nwindow-change pause,\nerror recovery"]
    end

    subgraph L3 ["Layer 3: _drain_inner()"]
        L3_BLOCK["Lock acquisition,\ntask consumption,\ndispatch,\nrescheduling"]
    end

    T_SCHEDULE -- "wake(0)" --> L1_BLOCK
    T_LIFECYCLE -- "wake(0)" --> L1_BLOCK
    T_FOLLOWUP -- "wake(0)" --> L1_BLOCK
    T_RATE -- "wake(delay)" --> L1_BLOCK
    T_WATCHDOG -- "timeout elapsed" --> L1_BLOCK

    L1_BLOCK -- "calls drain()" --> L2_BLOCK
    L2_BLOCK -- "calls _drain_inner()" --> L3_BLOCK

    L3_BLOCK -. "remaining tasks" .-> T_FOLLOWUP
    L3_BLOCK -. "rate limited" .-> T_RATE
    L2_BLOCK -. "exception → backoff\n→ _schedule_drain(delay)" .-> L1_BLOCK

    style Triggers fill:#e8f5e9,stroke:#388E3C
    style L1 fill:#e8f4f8,stroke:#2196F3
    style L2 fill:#fff3e0,stroke:#FF9800
    style L3 fill:#f3e5f5,stroke:#9C27B0
```

**Legend:**

- *Green subgraph* (Feedback Entry Points): the five triggers that drive the drain loop; see [Layer 3](#layer-3-_drain_inner) for detailed descriptions.
- *Blue subgraph* (Layer 1): the `DrainLoop._run()` background thread.
- *Orange subgraph* (Layer 2): the `drain()` method.
- *Purple subgraph* (Layer 3): the `_drain_inner()` method.
- *Solid arrows* indicate synchronous call chains (Layer 1 → Layer 2 → Layer 3).
- *Dashed arrows* indicate asynchronous feedback loops (rescheduling, error recovery).

## Layer 1: DrainLoop._run()

The `DrainLoop._run()` background thread manages wake signals and watchdog timing. It sleeps on a condition variable until either an explicit `wake()` call sets `_next_wake` or the watchdog timeout elapses, then calls `drain()` and loops back to the shutdown check.

```mermaid
flowchart TD
    L1_START["Thread start"]
    L1_SHUTDOWN{"Shutdown\nflag set?"}
    L1_EXIT["Return (thread exits)"]
    L1_HAS_WAKE{"_next_wake\nis set?"}
    L1_WAIT_WATCHDOG["Wait on condition variable\ntimeout = watchdog_interval"]
    L1_WOKE_REAL{"_next_wake set\nduring wait?"}
    L1_CALC_REMAINING["remaining = _next_wake − now"]
    L1_REMAINING_POS{"remaining\n> 0?"}
    L1_WAIT_REMAINING["Wait on condition variable\ntimeout = remaining"]
    L1_CLEAR_WAKE["Clear _next_wake = None"]
    L1_CALL_DRAIN["Call limiter.drain()"]

    L1_START --> L1_SHUTDOWN
    L1_SHUTDOWN -- "Yes" --> L1_EXIT
    L1_SHUTDOWN -- "No" --> L1_HAS_WAKE
    L1_HAS_WAKE -- "No" --> L1_WAIT_WATCHDOG
    L1_WAIT_WATCHDOG --> L1_WOKE_REAL
    L1_WOKE_REAL -- "Yes" --> L1_SHUTDOWN
    L1_WOKE_REAL -- "No (watchdog fired)" --> L1_CLEAR_WAKE
    L1_HAS_WAKE -- "Yes" --> L1_CALC_REMAINING
    L1_CALC_REMAINING --> L1_REMAINING_POS
    L1_REMAINING_POS -- "Yes" --> L1_WAIT_REMAINING
    L1_WAIT_REMAINING --> L1_SHUTDOWN
    L1_REMAINING_POS -- "No (time elapsed)" --> L1_CLEAR_WAKE
    L1_CLEAR_WAKE --> L1_CALL_DRAIN
    L1_CALL_DRAIN --> L1_SHUTDOWN

    style L1_CALL_DRAIN fill:#e8f4f8,stroke:#2196F3
```

**Test coverage:**

| Path | Description | Tested by |
|------|-------------|-----------|
| Wake(0) fires immediately | `wake(0)` causes prompt `drain()` call | `implementations/test_drain_loop::test_wake_fires_drain_immediately` |
| Wake(delay) fires after delay | `wake(delay)` waits before calling `drain()` | `implementations/test_drain_loop::test_wake_with_delay_fires_after_delay` |
| Coalesce to sooner time | `wake(0)` overrides pending `wake(10)` | `implementations/test_drain_loop::test_wake_coalesces_to_sooner_time` |
| Ignore later time | `wake(10)` does not override pending `wake(0)` | `implementations/test_drain_loop::test_wake_ignores_later_time` |
| Watchdog fires on idle | Watchdog timeout calls `drain()` without explicit `wake()` | `implementations/test_drain_loop::test_watchdog_fires_drain_when_idle` |
| Shutdown stops thread | `shutdown()` terminates the drain thread | `implementations/test_drain_loop::test_shutdown_stops_thread` |
| Lazy start | Thread not created until first `wake()` | `implementations/test_drain_loop::test_lazy_start` |

### Wake Coalescing

The `DrainLoop` implements a "keep earliest" coalescing strategy for wake signals. When `wake(delay)` is called, the method computes a target wake time as `time.monotonic() + delay`. If no wake is currently pending (i.e., `_next_wake is None`), the target is stored directly. If a wake is already pending, the new target replaces the existing one *only if it is earlier*; otherwise, the call is a no-op. This prevents the within-process equivalent of a thundering herd, where multiple `trigger_consume()` calls could each schedule redundant drains. The coalescing is implemented via the `_next_wake` timestamp, which is protected by the condition variable's underlying lock. After setting a new `_next_wake`, the condition variable is notified so that the sleeping `_run()` thread re-evaluates its wait timeout.

### The Watchdog Timer

The watchdog fires every `window * 2` seconds. Its purpose is to recover from scenarios where the normal feedback loop is broken: a worker crash that prevents `TaskLifecycle.__exit__()` from firing, a lost network packet that drops a `trigger_consume()` call, or an exception in `drain()` that prevents rescheduling. When the watchdog timeout elapses and no explicit `_next_wake` was set during the wait, the `DrainLoop` clears `_next_wake` and calls `drain()` unconditionally. Given that the watchdog interval is set to twice the window duration, the system self-recovers within a bounded time that is proportional to the configured rate limit window. The watchdog does not interfere with normal operation, as any explicit `wake()` call that arrives during the watchdog wait causes the thread to re-enter the main loop and process the scheduled wake instead.

## Layer 2: drain()

The `drain()` method handles configuration refresh, window-change pauses, and error recovery. It wraps `_drain_inner()` in a `try/except` that catches all exceptions and schedules recovery drains with exponential backoff.

```mermaid
flowchart TD
    L2_REFRESH["Call refresh_config()"]
    L2_PAUSED{"time.time()\n< _paused_until?"}
    L2_SCHEDULE_RESUME["Schedule drain at\nremaining pause time"]
    L2_RETURN_PAUSED["Return (skip drain)"]
    L2_TRY["Call _drain_inner()"]
    L2_SUCCESS["Reset _consecutive_drain_failures = 0"]
    L2_EXCEPT["Increment _consecutive_drain_failures"]
    L2_BACKOFF["delay = min(window, 0.1 × 2^(n−1))"]
    L2_SCHEDULE_RETRY["Schedule recovery drain\nwith backoff delay"]
    L2_DOUBLE_FAIL{"Recovery scheduling\nalso fails?"}
    L2_CRITICAL["Log critical:\nrely on watchdog or\nexternal trigger"]

    L2_REFRESH --> L2_PAUSED
    L2_PAUSED -- "Yes" --> L2_SCHEDULE_RESUME --> L2_RETURN_PAUSED
    L2_PAUSED -- "No" --> L2_TRY
    L2_TRY -- "Success" --> L2_SUCCESS
    L2_TRY -- "Exception" --> L2_EXCEPT --> L2_BACKOFF --> L2_SCHEDULE_RETRY
    L2_SCHEDULE_RETRY -- "Exception" --> L2_DOUBLE_FAIL
    L2_DOUBLE_FAIL -- "Yes" --> L2_CRITICAL

    style L2_EXCEPT fill:#fff3e0,stroke:#FF9800
    style L2_CRITICAL fill:#ffcdd2,stroke:#D32F2F
```

**Test coverage:**

| Path | Description | Tested by |
|------|-------------|-----------|
| Defers when paused | Skips drain, schedules resume | `implementations/test_drain::test_drain_defers_when_paused` |
| Calls refresh_config | Config refresh before draining | `implementations/test_drain::test_drain_calls_refresh_config_if_available` |
| Success resets counter | `_consecutive_drain_failures = 0` on success | `implementations/test_drain::test_drain_resets_failure_counter_on_success` |
| Consume exception → backoff | Catches exception, schedules recovery | `implementations/test_drain::test_drain_handles_consume_exception` |
| Dispatch exception → backoff | Catches exception, schedules recovery | `implementations/test_drain::test_drain_handles_dispatch_exception` |
| Escalating backoff | Delay doubles on consecutive failures | `implementations/test_drain::test_drain_backoff_increases_with_consecutive_failures` |
| Double failure | Recovery scheduling also fails; log critical | `implementations/test_drain::test_drain_handles_double_failure_when_schedule_drain_also_fails` |

### Exponential Backoff on Failure

When `_drain_inner()` raises an exception, the `drain()` method increments the `_consecutive_drain_failures` counter and calculates a recovery delay using the formula:

```
delay = min(window, 0.1 * 2^(n - 1))
```

where `n` is the consecutive error count. The backoff starts at 100ms for the first failure and doubles on each consecutive failure (200ms, 400ms, 800ms, and so on), capped at the window duration. This cap prevents the backoff from growing unboundedly while still providing sufficient spacing between retries. On the first successful drain after a series of failures, the error counter resets to 0, restoring normal drain scheduling. If the recovery scheduling itself fails (i.e., `_schedule_drain()` also raises an exception), the system logs a critical error and relies on the next external trigger (a `trigger_consume()` call from `schedule_task()` or `TaskLifecycle.__exit__()`, or a watchdog timeout) to resume the drain loop.

## Layer 3: _drain_inner()

The `_drain_inner()` method performs the actual lock acquisition, task consumption, dispatch, and rescheduling. It is the most complex layer, as it must handle all possible outcomes of the consumption attempt and determine the appropriate follow-up action.

```mermaid
flowchart TD
    L3_LOCK{"Acquire dispatch_lock\nSET NX with UUID token"}
    L3_NOT_ACQUIRED["Schedule backup drain\ndelay = window / limit"]
    L3_RETURN_LOCK["Return"]
    L3_CONSUME["Call consume()\nEVALSHA consume.lua"]
    L3_EXPIRED{"Task\nexpired?"}
    L3_LOG_EXPIRED["Log warning:\ntask moved to DLQ"]
    L3_SUCCESS{"Success and\ntask present?"}
    L3_DISPATCH["Dispatch via _dispatch_task()"]
    L3_RELEASE_OK["Release lock"]
    L3_REMAINING{"remaining_tasks\n> 0?"}
    L3_FOLLOWUP["Schedule immediate\nfollow-up drain (delay=0)"]
    L3_EMPTY{"remaining_tasks\n== 0?"}
    L3_STOP_EMPTY["Stop: wait for\nschedule_task() trigger"]
    L3_CONCURRENCY{"active_concurrency\n>= max_concurrency?"}
    L3_STOP_FULL["Stop: wait for\nTaskLifecycle.__exit__() trigger"]
    L3_RATE_LIMITED{"remaining_tokens\n<= 0?"}
    L3_CALC_DELAY["Calculate token recovery delay\nvia _calculate_token_recovery_delay()"]
    L3_IS_FALLBACK{"val_previous <= 0\nor val_current >= limit?"}
    L3_ADD_JITTER["Add smart jitter via\n_calculate_smart_jitter()"]
    L3_NO_JITTER["jitter = 0"]
    L3_SCHEDULE_RATE["Schedule retry with\nmax(0.001, base_delay + jitter)"]

    L3_LOCK -- "Not acquired" --> L3_NOT_ACQUIRED --> L3_RETURN_LOCK
    L3_LOCK -- "Acquired" --> L3_CONSUME
    L3_CONSUME --> L3_EXPIRED
    L3_EXPIRED -- "Yes" --> L3_LOG_EXPIRED
    L3_EXPIRED -- "No" --> L3_SUCCESS
    L3_LOG_EXPIRED --> L3_SUCCESS
    L3_SUCCESS -- "Yes" --> L3_DISPATCH --> L3_RELEASE_OK --> L3_REMAINING
    L3_REMAINING -- "Yes" --> L3_FOLLOWUP
    L3_REMAINING -- "No" --> L3_EMPTY
    L3_SUCCESS -- "No" --> L3_EMPTY
    L3_EMPTY -- "Yes" --> L3_STOP_EMPTY
    L3_EMPTY -- "No" --> L3_CONCURRENCY
    L3_CONCURRENCY -- "Yes" --> L3_STOP_FULL
    L3_CONCURRENCY -- "No" --> L3_RATE_LIMITED
    L3_RATE_LIMITED -- "Yes" --> L3_CALC_DELAY --> L3_IS_FALLBACK
    L3_IS_FALLBACK -- "Yes (fallback path)" --> L3_ADD_JITTER --> L3_SCHEDULE_RATE
    L3_IS_FALLBACK -- "No (primary path)" --> L3_NO_JITTER --> L3_SCHEDULE_RATE

    style L3_STOP_EMPTY fill:#e8f5e9,stroke:#388E3C
    style L3_STOP_FULL fill:#e8f5e9,stroke:#388E3C
    style L3_FOLLOWUP fill:#e8f4f8,stroke:#2196F3
    style L3_SCHEDULE_RATE fill:#fff3e0,stroke:#FF9800
```

**Test coverage:**

| Path | Description | Tested by |
|------|-------------|-----------|
| Lock not acquired → backup drain | Schedule backup drain at `window / limit` | `implementations/test_drain::test_drain_schedules_backup_when_lock_contended` |
| Successful consume → dispatch → follow-up | Dispatch task, schedule immediate follow-up | `implementations/test_drain::test_drain_dispatches_task_and_schedules_follow_up` |
| Expired task → DLQ | Expired consume result is not dispatched | `implementations/test_drain::test_drain_handles_expired_task_without_dispatch` |
| Buffer empty → stop | No follow-up scheduled | `implementations/test_drain::test_drain_stops_when_buffer_empty` |
| Concurrency full → stop | Wait for `TaskLifecycle.__exit__()` trigger | `implementations/test_drain::test_drain_stops_when_concurrency_at_capacity` |
| Rate limited → fallback (jitter) | Window reset delay + smart jitter | `implementations/test_drain::test_drain_schedules_delayed_retry_when_rate_limited` |
| Rate limited → token recovery (no jitter) | Sliding-window decay calculation, jitter skipped | `implementations/test_drain::test_drain_skips_jitter_on_token_recovery_path` |
| Concurrent lock serialization | Distributed lock serializes drains across workers | `implementations/test_concurrent_access::test_distributed_lock_serializes_drains` |
| All tasks eventually consumed | Buffer fully drained under contention | `implementations/test_concurrent_access::test_all_tasks_eventually_consumed_under_contention` |

### Feedback Entry Points

The drain loop does not run on a fixed schedule. Instead, it is driven by five distinct feedback entry points, each of which corresponds to a specific event in the task lifecycle. As such, the system remains responsive without resorting to polling.

1. **`schedule_task()`**: after a new task is buffered via the `schedule.lua` Lua script, `trigger_consume()` is called, which invokes `_schedule_drain(delay=0)`. This in turn calls `wake(0)` on the `DrainLoop`, ensuring that newly scheduled tasks are consumed promptly.

2. **`TaskLifecycle.__exit__()`**: upon task completion, the `TaskLifecycle` context manager releases the concurrency slot (via `ZREM` on the concurrency set), clears the in-flight deduplication marker (via `DEL`), and then calls `trigger_consume()`, which invokes `wake(0)`. This fills the freed concurrency slot with the next buffered task.

3. **Remaining tasks follow-up**: after a successful consumption, if the `ConsumeResult` indicates that `remaining_tasks > 0`, `_schedule_drain()` is called with the default `delay=0` to immediately consume the next task. This ensures that the system drains the buffer as quickly as the rate limit and concurrency constraints permit.

4. **Rate-limited retry**: when a consumption attempt is rate-limited (i.e., `remaining_tokens <= 0`), a retry delay is calculated via `_calculate_token_recovery_delay()` (primary path) or via the window reset time plus smart jitter (fallback path). The calculated delay is then passed to `_schedule_drain(delay)`, which schedules the next attempt at the optimal time.

5. **Watchdog timer**: the `DrainLoop` wakes periodically (every `window * 2` seconds) even without explicit trigger signals. This recovers from crashed workers, lost trigger chains, or failed drain attempts. The watchdog interval is set during `DrainLoop` construction and is passed as the `timeout` to the condition variable wait when no scheduled wake is pending.

### Delay Calculation

#### Token recovery delay (primary path)

When a rate limit is hit, the system calculates exactly when the next token will become available by solving the sliding window decay equation. The sliding window counter algorithm estimates the current usage as:

```
estimated = val_previous * weight + val_current
```

where `weight = (window_ms - time_passed_ms) / window_ms` decreases linearly from 1.0 to 0.0 as the current window progresses. The `_calculate_token_recovery_delay()` calculates the earliest point in time at which `estimated < limit`, i.e., the moment at which the decay of the previous window's counter frees one token. Specifically, it computes:

```
t_needed_ms = window_ms * (1.0 - (limit - val_current) / val_previous)
wait_ms = t_needed_ms - time_passed_ms
```

If the result is negative or zero, a token has already been freed and the retry is scheduled immediately (with a 1ms floor). If the previous window's counter is zero or the current window's counter alone meets or exceeds the limit, no amount of decay will free a token within the current window; in this case, the method falls back to the window reset time (`reset_in_ms / 1000 + 0.001`). The token recovery path does not add jitter, as the calculation already produces a precise delay that is naturally staggered across workers by their individual sliding window positions.

#### Fallback delay (smart jitter)

When the token recovery calculation cannot determine an exact delay via previous-window decay (i.e., when `val_previous <= 0` or `val_current >= limit`), the system falls back to scheduling a retry at the next window boundary. In this scenario, multiple workers are likely to be waiting for the same window reset, which creates the conditions for a thundering herd. To mitigate this, adaptive smart jitter is added to the base delay via `_calculate_smart_jitter()`. The jitter is proportional to the window size and adaptive to the current system load (queue depth and concurrency pressure). See [Smart Jitter](../smart-jitter.md) for the full adaptive thundering herd prevention strategy.

## References

- [limiters.py](../../src/celery_rate_limiter/core/limiters.py): core implementation (DrainLoop, drain, _drain_inner, delay calculations).
- [Smart Jitter](../smart-jitter.md): adaptive thundering herd prevention strategy for retry delays.
- [Task State Diagram](task-states.md): all possible task states and their transitions.
- [Component Diagram](components.md): high-level component overview showing the DrainLoop's position in the architecture.
- [Error Handling](error-handling.md): exception propagation paths and failure mode traceability.
