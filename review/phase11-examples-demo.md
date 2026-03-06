# Phase 11: Examples and Demo API Compatibility Review

## Scope

Verified that all example files (`examples/`) and demo files (`demo/`) use
import paths, constructor arguments, method names, and signatures that match
the current public API exposed by `src/redis_rate_limiter/`.

---

## Summary

All examples and the demo are **fully compatible** with the current API.
No broken imports, no stale constructor arguments, no removed/renamed methods.

---

## Detailed Findings by File

### examples/threadpool/demo.py -- PASS

| Check | Result |
|-------|--------|
| `from redis_rate_limiter import ThreadPoolRateLimiter` | Exported in `__init__.py` |
| `ThreadPoolRateLimiter.configure(redis_client, executor=executor)` | Matches `SyncManagedRateLimiter.configure(redis_client, **backend_context)` and `_configure_backend` requires `executor` |
| `.create(limiter_id=..., limit=..., window=..., max_concurrency=..., drain_enabled=False, override=True)` | All params valid: `limiter_id` + `**config` forwarded to `AbstractDistributedRateLimiter.__init__` which accepts `drain_enabled` |
| `.create(..., override=True, persist=False)` (consumer) | `persist` and `override` are explicit params on `SyncManagedRateLimiter.create()` |
| `scheduler.shutdown()`, `executor.shutdown(wait=True)` | `shutdown()` exists on `AbstractDistributedRateLimiter` |
| Uses `run_demo()` from `examples.runner` | Compatible (see runner analysis below) |

### examples/celery/demo.py -- PASS

| Check | Result |
|-------|--------|
| `from redis_rate_limiter import CeleryRateLimiter` | Exported via try/except in `__init__.py` |
| `CeleryRateLimiter.configure(redis_client, celery_app=celery_app)` | Matches overridden `configure()` which requires `celery_app` kwarg |
| `.create(limiter_id=..., limit=..., window=..., max_concurrency=..., override=True)` | Valid |
| `.create(..., drain_enabled=False, override=True)` | Valid |
| `.create(..., override=True, persist=False)` | Valid |
| `celery_app.conf.imports = ["redis_rate_limiter.backends.celery.tasks.worker"]` | Module exists at `src/redis_rate_limiter/backends/celery/tasks/worker.py` |
| `@worker_init.connect` handler calling `configure()` + `create()` | Correct pattern for Celery worker subprocess init |
| `scheduler.shutdown()`, `worker_proc.terminate()` | Valid |

### examples/asyncio/demo.py -- PASS

| Check | Result |
|-------|--------|
| `from redis_rate_limiter import AsyncIOTaskLimiter` | Exported in `__init__.py` |
| `AsyncIOTaskLimiter.configure(redis_client, max_tasks=ASYNCIO_MAX_TASKS)` | Matches `_configure_backend` which requires `max_tasks` |
| `await AsyncIOTaskLimiter.create(limiter_id=..., drain_enabled=False, override=True)` | Valid; `AsyncManagedRateLimiter.create()` is async and accepts `**config` |
| `await AsyncIOTaskLimiter.create(..., override=True, persist=False)` | Valid |
| `await scheduler.schedule_task(ASYNC_FUNC_PATH, {"user_id": 1})` returns `(bool, str)` | Matches `async_limiters.py schedule_task() -> tuple[bool, str]` |
| `await consumer.trigger_consume()` | Async method exists on `AbstractAsyncDistributedRateLimiter` |
| `await consumer.get_status()` | Async method exists |
| `await consumer.shutdown()`, `await scheduler.shutdown()` | Valid |
| `await redis_client.aclose()` | Standard redis.asyncio cleanup |
| Uses `redis.asyncio.Redis` | Correct async Redis client |

### examples/asgi/demo.py -- PASS

| Check | Result |
|-------|--------|
| `from redis_rate_limiter.backends.asgi import ASGIRateLimiter, by_client_ip` | Both exported in `backends/asgi/__init__.py` |
| `ASGIRateLimiter.configure(redis_client)` | Valid; `_configure_backend` is a no-op for ASGI |
| `await ASGIRateLimiter.create(limiter_id=..., limit=..., window=..., override=True)` | Valid; no `max_concurrency` needed for ASGI limiter |
| `await limiter.acquire(key)` returns `AcquireResult` | Method exists; returns TypedDict with `allowed`, `remaining`, `reset_in_ms` |
| `result["allowed"]`, `result["remaining"]`, `result["reset_in_ms"]` | All keys present in `AcquireResult` TypedDict |
| `ASGIRateLimiter._reset()` | Exists on `SyncManagedRateLimiter` (inherited via `AsyncManagedRateLimiter`) at line 120 of `managed.py` |
| `_key_func(scope)` returning `None` for bypass | Matches `KeyFunc` type: `Callable[[Scope], Optional[str]]` |

### examples/runner.py -- PASS

| Check | Result |
|-------|--------|
| `scheduler.schedule_task(FUNC_PATH, {"user_id": 1})` returns `(bool, str)`, indexed `[0]` | Correct; `schedule_task() -> tuple[bool, str]` |
| `scheduler.schedule_task(func_path, payload, priority=priority)` | `priority` is a valid keyword arg |
| `consumer.trigger_consume()` | Sync method on `AbstractDistributedRateLimiter` |
| `consumer.get_status()` | Returns dict with `buffer.count`, `concurrency.current`/`max`, `rate_limit.*`, `dispatcher.is_locked` |
| `consumer.shutdown()` | Valid |

### examples/dashboard.py -- PASS

| Check | Result |
|-------|--------|
| Reads `status["rate_limit"]["val_current"]` | Present in `get_status()` return dict |
| Reads `status["concurrency"]["current"]`, `["max"]` | Present |
| Reads `status["buffer"]["count"]` | Present |
| Reads `status["rate_limit"]["limit"]`, `["reset_in_ms"]`, `["val_previous"]`, `["tokens_used"]` | Present |
| Reads `status["dispatcher"]["is_locked"]` | Present |

### examples/tasks.py -- PASS

| Check | Result |
|-------|--------|
| Defines `FUNC_PATH = "examples.tasks.mock_api_call"` | Resolvable via `import_string` |
| Function signatures `(user_id: int, priority: int = 100)` | Compatible with `**payload` dispatch pattern |
| Async variants use `asyncio.sleep` | Correct for AsyncIO backend |

### examples/config.py -- PASS

Pure configuration; no API calls. All parameter names match what the examples consume.

### examples/README.md -- PASS

| Check | Result |
|-------|--------|
| `poetry install --extras asgi` | Assumes an `asgi` extras group exists |
| `poetry run python -m examples.threadpool.demo` | Valid module path |
| `poetry run python -m examples.celery.demo` | Valid module path |
| `poetry run python -m examples.asyncio.demo` | Valid module path |
| `poetry run python -m examples.asgi.demo` | Valid module path |
| `poetry run uvicorn examples.asgi.demo:app` | `app` is a module-level `FastAPI()` instance |

---

### demo/__main__.py -- PASS

| Check | Result |
|-------|--------|
| `from redis_rate_limiter import PrometheusMetricsExporter, ThreadPoolRateLimiter` | Both exported in `__init__.py` (Prometheus via try/except) |
| `ThreadPoolRateLimiter.configure(redis_client, executor=executor)` | Valid |
| `ThreadPoolRateLimiter.create(..., metrics_callback=exporter, drain_enabled=...)` | `metrics_callback` and `drain_enabled` are valid `AbstractDistributedRateLimiter.__init__` params |
| `PrometheusMetricsExporter(limiter_id=config.LIMITER_ID)` | Callable instance matching `Callable[[str, dict], None]` |
| `limiter.schedule_task(FUNC_PATH, {"seq": seq})` | Valid |
| `limiter.trigger_consume()` | Valid sync method |
| `limiter.shutdown()` | Valid |

### demo/tasks.py -- PASS

| Check | Result |
|-------|--------|
| `FUNC_PATH = "demo.tasks.mock_work"` | Resolvable via `import_string` at runtime |
| `mock_work(_rate_limit_task_id: str = "", **kwargs)` | Matches the dispatch pattern where `_rate_limit_task_id` is injected by the generic worker |

### demo/config.py -- PASS

Pure configuration; no API calls. Handles K8s `REDIS_PORT` URL injection correctly.

### demo/Dockerfile -- PASS

| Check | Result |
|-------|--------|
| `poetry install --only main -E prometheus` | Installs prometheus extras |
| `ENTRYPOINT ["python", "-m", "demo"]` | Runs `demo/__main__.py` |
| Copies `src/` and `demo/` | Both needed for the library and demo code |

### demo/docker-compose.yaml -- PASS

Standard compose file; no API surface concerns. Environment variables match `demo/config.py` expectations.

### demo/README.md -- PASS

Documentation only; references match actual environment variable names and defaults in `demo/config.py`.

---

## Issues Found

**None.** All 15 files use the current API correctly.

---

## Checklist Summary

| # | Check | All Pass? |
|---|-------|-----------|
| 1 | Import paths match current package structure | Yes |
| 2 | Constructor arguments match current API signatures | Yes |
| 3 | Method calls use current method names and signatures | Yes |
| 4 | No references to removed/renamed features | Yes |
| 5 | Examples would run without import or API errors | Yes |
