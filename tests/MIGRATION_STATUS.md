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
- [x] `tests/fixtures/processpool_backend.py`
- [x] `tests/fixtures/rq_backend.py`
- [x] `tests/fixtures/threadpool_backend.py`

## Batch 2: algorithms/

- [x] `tests/algorithms/sliding_window_counter.py`
- [x] `tests/algorithms/test_sliding_window_counter.py`

## Batch 3: contracts/

- [x] `tests/contracts/test_rate_limiter.py`
- [x] `tests/contracts/test_task_lifecycle.py`
- [x] `tests/contracts/test_distributed_lock.py`
- [x] `tests/contracts/test_managed_mixin.py`

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

- [x] `tests/properties/conftest.py` (no changes needed)
- [x] `tests/properties/test_sliding_window_counter.py`
- [x] `tests/properties/test_token_recovery.py`
- [x] `tests/properties/test_smart_jitter.py`
- [x] `tests/properties/test_serialization.py`
- [x] `tests/properties/test_concurrency_invariants.py`
- [x] `tests/properties/test_inflight_ttl.py`
- [x] `tests/properties/test_config_round_trip.py`
- [x] `tests/properties/test_is_subset.py` (no changes needed)
- [x] `tests/properties/asgi/test_keys.py` (no changes needed)

## Batch 9: integration/ and integrations/

- [x] `tests/integration/conftest.py`
- [x] `tests/integration/test_rate_limiting.py`
- [x] `tests/integration/test_distributed_rate_limiting.py`
- [x] `tests/integrations/test_prometheus.py`

## Batch 10: Post-migration additions

- [x] `tests/implementations/test_backend_health_monitor.py`
- [x] `tests/implementations/test_destructor_warning.py`
- [x] `tests/implementations/test_optional_imports.py`
- [x] `tests/lua/conftest.py`
- [x] `tests/lua/test_acquire.py`
- [x] `tests/lua/test_consume.py`
- [x] `tests/lua/test_health.py`
- [x] `tests/lua/test_lock_scripts.py`
- [x] `tests/lua/test_renew.py`
- [x] `tests/lua/test_schedule.py`
- [x] `tests/plugins/mutmut_defaults_patch.py`
- [x] `tests/plugins/verify_defaults_patch.py`

## Deferred Testing Decisions

Decisions made during migration review where a coverage gap was identified but deferred to a more appropriate future test category. Each entry references the relevant section of `TESTING_GUIDELINES.md`.

### Deferred to Lua script tests (Section 10)

*All items in this section have been resolved.*

| Gap | Resolution |
|---|---|
| ~~`limit=0` denies all consumption requests~~ | Resolved: `TestConsumeBoundaryDecisions.test_limit_zero_denies_all_requests` in `tests/lua/test_consume.py`. |
| ~~`consume.lua` expired task path: DLQ insertion, buffer count decrement, inflight cleanup~~ | Resolved: `TestConsumeBoundaryDecisions.test_expired_task_routed_to_dlq` and `test_expired_task_does_not_consume_token` in `tests/lua/test_consume.py`. |
| ~~`consume.lua` concurrency enforcement: rejection when `ZCARD >= max_concurrency`~~ | Resolved: `TestConsumeBoundaryDecisions.test_concurrency_at_max_denies_request` and `test_concurrency_below_max_allows_request` in `tests/lua/test_consume.py`. |
| ~~`consume.lua` token counter expiry: `PEXPIRE` only on first increment~~ | Resolved: `TestConsumeBoundaryDecisions.test_pexpire_set_on_first_increment_only` in `tests/lua/test_consume.py`. |
| ~~`schedule.lua` with `max_age=None` producing ARGV[3]=""~~ | Resolved: `TestSchedule.test_meta_max_age_absent_when_argv3_is_empty_string` in `tests/lua/test_schedule.py`. |
| ~~`schedule.lua` metadata injection via `string.gsub(task_json, '}$', ...)`~~ | Resolved: `TestSchedule.test_gsub_handles_nested_closing_brace` in `tests/lua/test_schedule.py`. |
| ~~`health.lua` return value format and edge cases~~ | Resolved: `TestHealthReturnValues` (6 tests) and `TestHealthReadOnly.test_does_not_modify_redis_state` in `tests/lua/test_health.py`. |

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

*All items in this section have been resolved.*

| Gap | Resolution |
|---|---|
| ~~`extend_lease()` contract test~~ | Resolved: `TestRenew` (5 tests: return values, score update, missing task, member preservation) in `tests/lua/test_renew.py`. |

### Deferred to error handling tests

*All items in this section have been resolved.*

| Gap | Resolution |
|---|---|
| ~~`rate_limited` decorator when `get_limiter` resolver returns `None`~~ | Resolved: `TestRateLimitedDecorator.test_raises_attribute_error_when_resolver_returns_none` in `tests/implementations/test_decorator.py`. Verifies that a resolver returning `None` propagates an `AttributeError` when `limiter.task_lifecycle()` is called. |
| ~~`rate_limited` decorator when `task_lifecycle.__enter__` raises~~ | Resolved: `TestRateLimitedDecorator.test_propagates_exception_from_lifecycle_enter` in `tests/implementations/test_decorator.py`. Verifies that a `RuntimeError` from `__enter__` propagates unchanged to the caller. |

### Deferred to concurrency tests

*All items in this section have been resolved.*

| Gap | Resolution |
|---|---|
| ~~Concurrent `schedule_task` for the same payload under race conditions~~ | Pre-existing coverage identified during batch 6 review: `TestConcurrentScheduling.test_concurrent_duplicate_scheduling_produces_single_entry` in `tests/implementations/test_concurrent_access.py`. The original deferral was added without cross-referencing pending test files. |

### Deferred to Lua script tests (batch 6 additions)

*All items in this section have been resolved.*

| Gap | Resolution |
|---|---|
| ~~Inline lock script edge cases: atomic token comparison on release, lock key with different token, contention-aware fairness~~ | Resolved: `TestLockAcquireScript` (5 tests), `TestLockReleaseScript` (5 tests), and `TestLockSimpleReleaseScript` (3 tests) in `tests/lua/test_lock_scripts.py`. The original entry incorrectly referenced `acquire.lua`; the distributed lock uses inline Lua scripts (`LOCK_ACQUIRE_SCRIPT`, `LOCK_RELEASE_SCRIPT`, `LOCK_SIMPLE_RELEASE_SCRIPT`) in `limiters.py`, not `acquire.lua` (which is the ASGI sliding window counter). |

### Deferred to async class API parity (batch 7 verification)

*All items in this section have been resolved.*

| Gap | Resolution |
|---|---|
| ~~`AsyncManagedRateLimiter` class API observability parity~~ | Resolved during batch 7 review: added `TestConfigureObservability`, `TestCreateObservability`, `TestGetObservability`, and `TestUpdateObservability` to `tests/implementations/asyncio/test_rate_limiter_class_api.py`, covering all 7 async managed log emissions. |
| ~~`AsyncManagedRateLimiter.create` default value signature tests~~ | Resolved during batch 7 review: added `TestCreateDefaults` with `test_create_persist_defaults_to_true` and `test_create_override_defaults_to_false` to `tests/implementations/asyncio/test_rate_limiter_class_api.py`. |

### Deferred to idempotency tests (Section 11.2)

*All items in this section have been resolved.*

| Gap | Resolution |
|---|---|
| ~~`shutdown()` idempotency contract~~ | Resolved across multiple test files: `TestDrainLoop.test_shutdown_is_idempotent` in `test_drain_loop.py` (sync drain loop), `TestAsyncDrainLoop.test_shutdown_is_idempotent` in `test_async_drain_loop.py` (async drain loop), `DrainBehaviorTests.test_shutdown_twice_does_not_raise` in `test_drain.py` (limiter-level, covers both sync and async via mixin), and `DrainDisabledTests.test_shutdown_twice_does_not_raise_when_drain_disabled` in `test_drain.py` (drain-disabled variant, covers both sync and async via mixin). |
