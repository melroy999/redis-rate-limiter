# Phase 2: Lua Scripts Review

All 5 scripts reviewed together for correctness, atomicity, key consistency, and cross-script agreement.

## consume.lua (110 lines) — Task consumption with sliding window

### Correctness: Sound
- Sliding window counter formula is correct: `estimated = current + previous * weight`
- Weight decreases linearly from 1.0 to 0.0 as time passes in current window
- Self-healing `ZREMRANGEBYSCORE` removes expired leases before counting (line 29)
- Expired tasks moved to DLQ with inflight key cleanup (lines 79-87)
- Window key TTL set only on first increment (`current_count == 0`), avoiding repeated PEXPIRE calls

### Atomicity: Correct
Single EVAL = single Redis command. All reads and writes are within one atomic execution.
No TOCTOU issues — the check (estimated_count < rate_limit) and the INCR happen in the
same script invocation.

### NOTE: `false` in return arrays becomes `nil` in Redis protocol
Lines 87 and 110 return `false` as the second element. In Redis Lua, `false` becomes
a nil bulk string response. The Python side checks `if result[1]` which is falsy for
empty string, so this works. But it's a subtle Redis-Lua interop detail worth knowing.

### NOTE: expired task returns buffer_count - 1 but doesn't decrement remaining tokens
Line 87: expired task return includes `remaining` (not `remaining - 1`) because no
token was consumed. Correct behavior.

## health.lua (42 lines) — Status check

### CONCERN: Doesn't clean expired leases before counting (already in REVIEW.md as A2)
`health.lua` does `ZCARD` on the concurrency set without first running `ZREMRANGEBYSCORE`
to remove expired leases. This means `get_status()` may overcount active concurrency.
`consume.lua` cleans up on line 29 before counting.

This is arguably intentional (health checks shouldn't mutate state), but the documented
behavior should note this discrepancy.

### NOTE: Sliding window formula is identical to consume.lua
Same weight calculation, same key construction. Consistent.

## acquire.lua (50 lines) — ASGI request-rate-limit check

### Correctness: Sound
Mirrors the sliding window logic from consume.lua exactly, but without buffer/concurrency
management. Clean, minimal script for the request-oriented ASGI use case.

### NOTE: No concurrency tracking
This is by design — ASGI rate limiting only cares about request rate, not concurrent slots.

### Key consistency: Correct
Uses same `base_key .. ':' .. window_start` pattern as consume.lua and health.lua.

## renew.lua (21 lines) — Lease renewal

### Correctness: Sound
- Checks task exists in concurrency set via `ZSCORE` before updating
- Uses Redis `TIME` for server-side timestamp (avoids clock skew)
- Updates lease by setting new ZADD score = `timestamp + lease_duration`

### NOTE: No TTL set on the concurrency sorted set itself
The concurrency set (`ZCARD` key) has no `EXPIRE` or `PEXPIRE`. This is fine because
`consume.lua` does `ZREMRANGEBYSCORE` to self-heal expired entries. But if all consumers
crash permanently and no one ever calls consume again, the sorted set stays forever
(with expired entries that no one cleans up). In practice, the watchdog timer prevents
this scenario.

## schedule.lua (25 lines) — Buffer insertion

### Correctness: Sound
- Injects `__meta_arrived_at` (millisecond timestamp) and optional `__meta_max_age`
  into the JSON payload via string manipulation
- Uses `string.gsub` to insert before the closing brace

### CONCERN: gsub regex pattern assumes well-formed JSON
Line 23: `string.gsub(task_json, '}$', metadata_fields .. '}', 1)` replaces the last `}`
in the JSON string. This works correctly for the flat JSON objects produced by
`_get_task_data_str()` (which uses `json.dumps(sort_keys=True)`), but would break for:
- Nested objects in the payload (e.g., `{"payload": {"nested": {}}}`) — gsub with `$`
  anchor matches only the very last `}`, which is correct even for nested objects.

Actually, looking again: `}$` matches `}` at end-of-string. With the count limit of 1,
this correctly replaces only the outermost closing brace. Even with nested payloads,
the `$` anchor ensures only the final `}` is replaced. The comment in the code
(lines 15-16) explains the avoidance of cjson decode/encode to preserve empty arrays.

**Revised: This is correct.** The `$` anchor handles nesting correctly.

## Cross-Script Consistency

### Shared key naming: Consistent
All scripts use the same key construction:
- Window counter: `base_key .. ':' .. window_start_ms`
- Buffer: `buffer_key` (sorted set with priority scores)
- Concurrency: `concurrency_key` (sorted set with timestamp scores)

### Shared timestamp logic: Consistent
All scripts use `redis.call('TIME')` for server-side timestamps. Millisecond construction
is identical: `(seconds * 1000) + floor(microseconds / 1000)`.

### Sliding window formula: Consistent
consume.lua, health.lua, and acquire.lua all use the identical formula.
- Weight = `(window_size_ms - time_passed_in_current) / window_size_ms`
- Estimated = `current_count + previous_count * weight`

### Data format agreement: Consistent
- schedule.lua writes JSON with `__meta_arrived_at` and optional `__meta_max_age`
- consume.lua reads both fields: `task_data['__meta_max_age']` and `task_data['__meta_arrived_at']`
- renew.lua uses task_id as member in sorted set; consume.lua writes task_id as member

## Summary

No bugs found. The scripts are well-written, atomic, and consistent with each other.
The only concern (health.lua not cleaning expired leases) was already identified in the
existing REVIEW.md.
