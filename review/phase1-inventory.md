# Phase 1: Inventory

Total: ~37,300 lines across ~160 files (excluding caches, .git, poetry.lock).

## Source Code (src/redis_rate_limiter/)
| File | Lines | Role |
|------|-------|------|
| core/limiters.py | 1546 | Sync distributed rate limiter, lock, lifecycle, drain loop, mixin |
| core/async_limiters.py | 1052 | Async mirror of limiters.py |
| core/managed.py | 667 | Managed mixin (sync+async) for dynamic config via Redis |
| core/base.py | 254 | Abstract base classes (sync + async) |
| core/importing.py | 93 | Dynamic function import utility |
| core/decorators.py | 85 | @rate_limited decorator |
| core/scripts.py | 53 | Lua script loader/evaluator |
| core/__init__.py | 50 | Core package exports |
| integrations/prometheus.py | 178 | Prometheus metrics integration |
| backends/celery/limiter.py | 171 | Celery backend |
| backends/asyncio/limiter.py | 171 | Asyncio backend |
| backends/asgi/middleware.py | 168 | ASGI middleware |
| backends/asgi/limiter.py | 155 | ASGI rate limiter |
| backends/threading/limiter.py | 136 | ThreadPool backend |
| backends/asgi/keys.py | 47 | ASGI key extraction |
| backends/asgi/types.py | 26 | ASGI type definitions |
| backends/celery/tasks/worker.py | 34 | Celery worker task |
| __init__.py | 48 | Package-level exports |

## Lua Scripts
| File | Lines | Role |
|------|-------|------|
| lua/consume.lua | 110 | Sliding window consume + concurrency check |
| lua/acquire.lua | 50 | Concurrency slot acquisition (standalone) |
| lua/health.lua | 42 | Health/status check |
| lua/schedule.lua | 25 | Buffer insertion with priority |
| lua/renew.lua | 21 | Lease renewal |

## Tests (~18,000 lines)
- tests/lua/ - Direct Lua script tests (6 files)
- tests/contracts/ - Contract/interface tests (4 files)
- tests/implementations/ - Implementation tests (~30 files, largest area)
- tests/properties/ - Property-based tests (10 files)
- tests/integration/ - Integration tests (2 files)
- tests/integrations/ - Prometheus tests (1 file)
- tests/algorithms/ - Sliding window algorithm tests (2 files)

## Docs
- docs/architecture/ - 9 files covering class hierarchy, components, drain flow, error handling, redis keys, sliding window, task lifecycle, task states
- docs/smart-jitter.md - Smart jitter algorithm documentation

## Config
- pyproject.toml, ruff.toml, mypy.ini, poetry.toml
- .github/workflows/ci.yml, mutation.yml
- docker-compose.yml, Dockerfile

## Examples & Demo
- examples/ - 4 backend demos + shared config/tasks/runner/dashboard
- demo/ - Docker-based demo with Prometheus + Grafana + K8s manifests
