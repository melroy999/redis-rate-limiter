# Test Migration Status

Tracks compliance of each file against `TESTING_GUIDELINES.md`. Files are checked off as they are reviewed and brought into compliance.

## Legend

- [ ] Pending review
- [x] Reviewed and compliant

## Batch 1: Infrastructure

- [x] `tests/conftest.py`
- [x] `tests/helpers/adapters.py`
- [x] `tests/helpers/strategies.py`
- [x] `tests/helpers/tasks.py`
- [x] `tests/helpers/utils.py`
- [x] `tests/fixtures/celery_backend.py`
- [x] `tests/fixtures/threadpool_backend.py`

## Batch 2: algorithms/

- [x] `tests/algorithms/sliding_window_counter.py`
- [x] `tests/algorithms/test_sliding_window_counter.py`

## Batch 3: contracts/

- [x] `tests/contracts/test_rate_limiter.py`
- [x] `tests/contracts/test_task_lifecycle.py`
- [x] `tests/contracts/test_distributed_lock.py`

## Batch 4: implementations/ core mixins

- [x] `tests/implementations/conftest.py`
- [x] `tests/implementations/test_rate_limiter.py`
- [x] `tests/implementations/test_drain.py`
- [x] `tests/implementations/test_metrics_callback.py`

## Batch 5: implementations/ remaining mixins

- [x] `tests/implementations/test_smart_jitter.py`
- [x] `tests/implementations/test_get_status.py`
- [x] `tests/implementations/test_decorator.py`
- [x] `tests/implementations/test_importing.py`

## Batch 6: implementations/ lifecycle, lock, internal, concurrent

- [x] `tests/implementations/test_task_lifecycle.py`
- [x] `tests/implementations/test_async_task_lifecycle.py`
- [x] `tests/implementations/test_distributed_lock.py`
- [x] `tests/implementations/test_async_distributed_lock.py`
- [x] `tests/implementations/test_lua_script_infrastructure.py` (split from `test_internal_helpers.py`)
- [x] `tests/implementations/test_task_data_helpers.py` (split from `test_internal_helpers.py`)
- [x] `tests/implementations/test_token_recovery_delay.py` (split from `test_internal_helpers.py`)
- [x] `tests/implementations/test_limiter_config.py` (split from `test_internal_helpers.py`)
- [x] `tests/implementations/test_rate_limiter_class_api.py`
- [x] `tests/implementations/test_concurrent_access.py`

## Batch 7: implementations/ drain loops + backend subdirectories

- [x] `tests/implementations/test_drain_loop.py`
- [x] `tests/implementations/test_async_drain_loop.py`
- [x] `tests/implementations/celery/conftest.py`
- [x] `tests/implementations/celery/test_contracts.py` (no changes needed)
- [x] `tests/implementations/celery/test_celery_limiter.py`
- [x] `tests/implementations/celery/test_rate_limiter_class_api.py`
- [x] `tests/implementations/celery/test_tasks.py`
- [x] `tests/implementations/threadpool/conftest.py`
- [x] `tests/implementations/threadpool/test_contracts.py`
- [x] `tests/implementations/threadpool/test_threadpool_limiter.py`
- [x] `tests/implementations/threadpool/test_rate_limiter_class_api.py`
- [x] `tests/implementations/asyncio/conftest.py`
- [x] `tests/implementations/asyncio/test_contracts.py`
- [x] `tests/implementations/asyncio/test_asyncio_limiter.py`
- [x] `tests/implementations/asyncio/test_rate_limiter_class_api.py`
- [x] `tests/implementations/asgi/conftest.py`
- [x] `tests/implementations/asgi/test_asgi_limiter.py`
- [x] `tests/implementations/asgi/test_keys.py` (no changes needed)
- [x] `tests/implementations/asgi/test_middleware.py`

## Batch 8: properties/

- [ ] `tests/properties/conftest.py`
- [ ] `tests/properties/test_sliding_window_counter.py`
- [ ] `tests/properties/test_token_recovery.py`
- [ ] `tests/properties/test_smart_jitter.py`
- [ ] `tests/properties/test_concurrency_invariants.py`
- [ ] `tests/properties/test_serialization.py`
- [ ] `tests/properties/test_inflight_ttl.py`
- [ ] `tests/properties/test_config_round_trip.py`
- [ ] `tests/properties/test_is_subset.py`
- [ ] `tests/properties/asgi/test_keys.py`

## Batch 9: integration/ and integrations/

- [ ] `tests/integration/conftest.py`
- [ ] `tests/integration/test_rate_limiting.py`
- [ ] `tests/integration/test_distributed_rate_limiting.py`
- [ ] `tests/integrations/test_prometheus.py`

## Deferred Testing Decisions

Decisions made during migration review where a coverage gap was identified but deferred to a more appropriate future test category. Each entry references the relevant section of `TESTING_GUIDELINES.md`.

### Deferred to Lua script tests (Section 10)

| Gap | Rationale | Future location |
|---|---|---|
| `limit=0` denies all consumption requests | At the algorithm level this tests `0.0 < 0`, which is a trivial property of Python's `<` operator. The meaningful test is at the Lua level, where it verifies ARGV parsing, the boundary check in `consume.lua`, and the return value encoding with real Redis state. | `tests/lua/test_consume.py` (Section 10.4: "Boundary decisions") |
| `consume.lua` expired task path: DLQ insertion, buffer count decrement, inflight cleanup | Drain tests mock `consume()` entirely; the Lua-level expired-task handling (return value `-1`, `RPUSH` to DLQ, `ZREM` from buffer, inflight key deletion) is never exercised by Python tests. | `tests/lua/test_consume.py` |
| `consume.lua` concurrency enforcement: rejection when `ZCARD >= max_concurrency` | Drain tests set `active_concurrency=max_concurrency` in mock results but the actual Lua-level `ZCARD` check and early return are not exercised. | `tests/lua/test_consume.py` |
| `consume.lua` token counter expiry: `PEXPIRE` only on first increment | The `PEXPIRE` on the current-window counter key is set only when `current_count == 0` (i.e., on the first increment). No test verifies that subsequent consumes within the same window do not reset the TTL. A mutation changing `== 0` to `== 1` would go undetected. | `tests/lua/test_consume.py` |
| `schedule.lua` with `max_age=None` producing ARGV[3]="" | When `max_age` is not provided, the Python layer passes `""` to the Lua script. `tonumber("")` returns `nil`, so no `__meta_max_age` field is injected. No test verifies this interaction. | `tests/lua/test_schedule.py` |
| `schedule.lua` metadata injection via `string.gsub(task_json, '}$', ...)` | The `gsub` approach is correct for `json.dumps(sort_keys=True)` output but fragile in principle. A Lua-level test with a payload containing a trailing `}` inside a nested string value would verify the pattern's correctness. | `tests/lua/test_schedule.py` |
| `health.lua` return value format and edge cases | The Python-side `get_status()` tests mock `_eval_script`. No test exercises `health.lua` directly against real Redis to verify: return value ordering, behavior when rate limit keys do not yet exist, and behavior during a window boundary crossing. | `tests/lua/test_health.py` |

### Deferred to configuration edge cases (Section 11.1)

| Gap | Rationale | Future location |
|---|---|---|
| `window_ms=0` causes `ZeroDivisionError` in the reference implementation | Should be rejected at configuration time, not handled in the algorithm. Testing this belongs in a configuration validation test. | `tests/implementations/` (Section 11.1: "`window=0` or very small windows") |
| Negative values for `limit`, `window`, `max_concurrency` | Same as above; should be rejected at configuration time. | `tests/implementations/` (Section 11.1: "Negative values") |
| `elapsed_ms > window_ms` or `elapsed_ms < 0` | Impossible inputs; the window counter resets before `elapsed_ms` exceeds `window_ms`, and Redis TIME is monotonically non-decreasing. No value at the algorithm level or the Lua level. | N/A (not testable; document as a non-issue) |
| `max_concurrency=0` | Would make the `active_concurrency >= max_concurrency` check always true, effectively disabling task dispatch. Should be rejected at configuration time. | `tests/implementations/` (Section 11.1) |
| `lease_duration=0` | `_get_inflight_ttl` floors at `max(1.0, ...)`, but the Lua consume script receives the raw value as ARGV. Should be rejected or documented as a minimum. | `tests/implementations/` (Section 11.1) |
| Priority boundary values (`priority=0`, negative, extreme float64) | Priority is stored as a Redis ZSET score (double-precision float). Extreme values may lose precision; zero and negative priorities could violate ordering assumptions. Should be validated at configuration time. | `tests/implementations/` (Section 11.1) |

### Deferred to Lua script tests (Section 10) and Lua-level contracts

| Gap | Rationale | Future location |
|---|---|---|
| `extend_lease()` contract test | Currently tested indirectly via heartbeat in lifecycle tests. A direct contract test would verify the score update in the concurrency set, but the Lua script (`renew.lua`) handles the actual semantics. A Lua-level unit test for `renew.lua` (verifying return value, score update, and "task not found" behavior) would be more targeted than a Python contract test. | `tests/lua/test_renew.py` (Section 10) |

### Deferred to error handling tests

| Gap | Rationale | Future location |
|---|---|---|
| `rate_limited` decorator when `get_limiter` resolver returns `None` | The decorator assumes the resolver returns a valid limiter and calls `limiter.task_lifecycle()`. If the resolver returns `None`, an `AttributeError` propagates. No test validates this path or whether it should raise a more descriptive error. | `tests/implementations/test_decorator.py` |
| `rate_limited` decorator when `task_lifecycle.__enter__` raises | The context manager entry is not wrapped in a try/except. No test verifies exception propagation when the lifecycle context manager fails on entry. | `tests/implementations/test_decorator.py` |

### Deferred to concurrency tests

*All items in this section have been resolved.*

| Gap | Resolution |
|---|---|
| ~~Concurrent `schedule_task` for the same payload under race conditions~~ | Pre-existing coverage identified during batch 6 review: `TestConcurrentScheduling.test_concurrent_duplicate_scheduling_produces_single_entry` in `tests/implementations/test_concurrent_access.py`. The original deferral was added without cross-referencing pending test files. |

### Deferred to Lua script tests (batch 6 additions)

| Gap | Rationale | Future location |
|---|---|---|
| `acquire.lua` edge cases: atomic token comparison on release, lock key exists with different token, renewal semantics | `test_distributed_lock.py` verifies the Python wrapper (UUID token generation, SET NX). Lua-level atomicity and token comparison are not exercised by Python tests. | `tests/lua/test_acquire.py` |

### Deferred to async class API parity (batch 7 verification)

*All items in this section have been resolved.*

| Gap | Resolution |
|---|---|
| ~~`AsyncManagedRateLimiter` class API observability parity~~ | Resolved during batch 7 review: added `TestConfigureObservability`, `TestCreateObservability`, `TestGetObservability`, and `TestUpdateObservability` to `tests/implementations/asyncio/test_rate_limiter_class_api.py`, covering all 7 async managed log emissions. |
| ~~`AsyncManagedRateLimiter.create` default value signature tests~~ | Resolved during batch 7 review: added `TestCreateDefaults` with `test_create_persist_defaults_to_true` and `test_create_override_defaults_to_false` to `tests/implementations/asyncio/test_rate_limiter_class_api.py`. |

### Deferred to idempotency tests (Section 11.2)

| Gap | Rationale | Future location |
|---|---|---|
| `shutdown()` idempotency contract | Calling `shutdown()` multiple times should not raise exceptions or corrupt state. Behavior varies between backends (thread cleanup vs task cancellation), so a single contract test is difficult to write universally. Better addressed as a per-backend idempotency test. | `tests/implementations/<backend>/` (Section 11.2: "Calling `shutdown()` multiple times") |
