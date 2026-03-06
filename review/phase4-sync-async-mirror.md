# Phase 4: Sync/Async Mirror Audit

Compared limiters.py (1546 lines) vs async_limiters.py (1052 lines).
The async module imports shared types/scripts from sync: TaskData, ConsumeResult,
DistributedRateLimiterMixin, LOCK_*_SCRIPT constants. Good — no duplication of domain logic.

## Structural Mirrors (correct)
- DistributedLock <-> AsyncDistributedLock (same Lua scripts, same keys)
- TaskLifecycle <-> AsyncTaskLifecycle (thread vs asyncio.Task)
- DrainLoop <-> AsyncDrainLoop (Thread+Condition vs asyncio.Task+Condition)
- DrainSignalSubscriber <-> AsyncDrainSignalSubscriber
- AbstractDistributedRateLimiter <-> AbstractAsyncDistributedRateLimiter

## Findings

### CONCERN: Sync DrainSignalSubscriber doesn't decode bytes (limiters.py:476)
The sync `_run()` method reads `message["data"]` and compares it directly to
`self._limiter._worker_id` (a str). Redis-py's sync Pub/Sub returns bytes by default.
So `b"uuid-string" != "uuid-string"` is always True, meaning the sync subscriber
**never filters out self-notifications** — every drain signal triggers a local drain,
including the worker's own signals.

The async version (async_limiters.py:410-411) explicitly handles this:
```python
if isinstance(sender_id, bytes):
    sender_id = sender_id.decode("utf-8")
```

**Impact**: Minor performance — redundant self-drains. The drain loop coalesces
wake-ups, so functional correctness is preserved. But the intent (filtering self-signals)
is silently broken in sync mode.

**Fix**: Add the same bytes decode logic to the sync subscriber.

### NOTE: Sync subscriber doesn't use ignore_subscribe_messages
- Sync (limiters.py:474): `self._pubsub.get_message(timeout=0.5)` — processes subscribe
  confirmation messages and checks `message["type"] == "message"` manually.
- Async (async_limiters.py:405-406): `get_message(ignore_subscribe_messages=True, timeout=0.5)`
  — filters them out automatically.

Both work correctly (the type check catches it either way), but it's an inconsistency.

### NOTE: execution_lock return type annotation differs
- Sync (limiters.py:1446): `-> ContextManager[bool]` (abstract)
- Async (async_limiters.py:965): `-> AsyncDistributedLock` (concrete)

The sync version is more abstract, which is arguably better API design. Cosmetic.

### NOTE: Async _drain_inner has less detailed comments
The sync version has explanatory comments for each branch in `_drain_inner()`. The async
version has the same logic but fewer inline comments. Not a bug, but reduces maintainability
if someone only reads the async file.

### NOTE: Sync get_status returns raw strings for some fields
Both sync and async `get_status()` return `result[0]`, `result[1]`, `result[3]`, `result[5]`
as raw strings from the Lua script (not cast to int). The `tokens_used` field is cast to
float. The `reset_in_ms` field is also raw. This means consumers get string values for
`val_previous`, `val_current`, `current` (concurrency), and `count` (buffer). Inconsistent
with the int-typed `ConsumeResult`. Both sync and async have this — it's a shared issue,
not a mirror divergence.

## Verdict
The mirror is structurally sound. The shared mixin eliminates most duplication risk.
The bytes-decode bug in the sync subscriber is the only actionable finding.
