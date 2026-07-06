# CLAUDE.md

This file provides context for Claude Code when working on this project.

## Project Overview

A distributed rate limiter library for Python task queues, backed by Redis. The core algorithm is a **sliding window counter** (not a generic "sliding window" or a "sliding window log"). This is a fixed-window-pair weighted-average approximation with O(1) memory per rate limit key.

Pluggable task execution backends: Celery, RQ, Dramatiq, Huey, ProcessPool, ThreadPool, AsyncIO, ASGI middleware. All rate limiting state lives in a single Redis node; Redis Cluster is not supported because the Lua scripts require atomic multi-key operations on a single shard.

## Algorithm: Sliding Window Counter

The algorithm is implemented in `src/redis_rate_limiter/lua/consume.lua` and mirrored in a Python reference implementation at `tests/algorithms/sliding_window_counter.py` for deterministic testing.

**Key formula:**

```
weight = (window_size_ms - elapsed_ms) / window_size_ms
estimated_count = current_count + (previous_count × weight)
```

A request is permitted when `estimated_count < limit`.

**Important properties:**
- **2x burst bound**: a mathematical property of the algorithm, not a bug. An empty previous window followed by a boundary crossing can allow up to 2× limit in a sliding measurement window.
- **Steady-state**: under sustained greedy load, allows at most `limit + 1` per fixed window (due to timing jitter and discrete increments).
- **Long-term convergence**: average rate over multiple windows converges to the configured limit.

These properties are formally verified via Hypothesis in `tests/properties/test_sliding_window_counter.py`.

## Development Commands

```bash
poetry run ruff check --select I --fix .                     # fix import sorting
poetry run ruff format .                                     # format code
poetry run ruff check .                                      # lint
poetry run mypy src                                          # type check
docker compose --profile test up                             # fast tests (excludes @slow)
docker compose --profile test-all up                         # all tests including slow
docker compose --profile mutate up --build --abort-on-container-exit --exit-code-from mutate  # mutation testing
```

Tests require Redis and must be run through Docker; `pytest` invocations on the host will not work. Mutation testing requires `--build` to pick up source changes (no volume mount). Results summary is printed at the end of the run and written to `./mutmut-results/report.txt`.

## Code Conventions

- **Python**: 3.12+ required
- **Type checking**: strict mypy with `disallow_untyped_defs = True`
- **Formatting**: Ruff formatter, double quotes, 88-char line length
- **Linting**: Ruff with Pyflakes (F), Pycodestyle (E, W), Isort (I); E501 ignored
- **Prose tone**: formal, no contractions, no em dashes; prefer commas, semicolons, colons, "i.e.", "e.g."
- **Markdown**: no line wrapping (one paragraph = one long line)
- **Logging**: `logger.debug/info/warning/error` with structured parameters (e.g., `limiter=%s, task_id=%s`)
- **`# fmt: off` / `# fmt: on`**: used in three specific cases to prevent Ruff from splitting single expressions across multiple lines, which would create independent mutation targets that escape the test harness's built-in detection mechanisms (label check, capitalization check, `%S` crash, `^` anchor). Approved uses: (1) `cast()` calls with multi-line arguments, where splitting exposes the cast's arguments to mutations that `# pragma: no mutate` on the `cast(` line cannot suppress; (2) multi-part log format strings consolidated into a single string literal, so that mutmut treats the entire message as one atomic mutation target; (3) multi-part error message strings in `raise` statements, so that `pytest.raises(match=r"^...")` with a `^` anchor catches xx_wrap mutations on the single consolidated literal.

## Project Structure

```
src/redis_rate_limiter/
    core/                    # Core logic, decorators, drain loop
        base.py              # Abstract base classes (AbstractRateLimiter, AbstractSyncRateLimiter, AbstractAsyncRateLimiter)
        limiters.py          # AbstractDistributedRateLimiter (sync core)
        async_limiters.py    # AbstractAsyncDistributedRateLimiter (async core)
        managed.py           # SyncManagedRateLimiter, AsyncManagedRateLimiter
        decorators.py        # @rate_limited decorator
        importing.py         # import_string() utility
        scripts.py           # load_lua_script() utility
    backends/
        celery/              # CeleryRateLimiter, generic worker task
        dramatiq/            # DramatiqRateLimiter, generic worker actor
        huey/                # HueyRateLimiter, generic worker task
        rq/                  # RQRateLimiter, generic worker task
        processpool/         # ProcessPoolRateLimiter
        threading/           # ThreadPoolRateLimiter
        asyncio/             # AsyncIOTaskLimiter
        asgi/                # ASGIRateLimiter, RateLimitMiddleware
    integrations/
        prometheus.py        # PrometheusMetricsExporter
    lua/                     # Redis Lua scripts (consume, schedule, health, renew, acquire, release)

tests/
    contracts/               # Interface contracts all backends must satisfy
    implementations/         # Backend-agnostic + backend-specific tests
    algorithms/              # Pure algorithm unit tests and Python reference implementation
    properties/              # Hypothesis property-based tests
    integration/             # End-to-end timing tests (marked @slow)
    lua/                     # Redis Lua script unit tests
    integrations/            # Third-party integration tests (Prometheus)
    fixtures/                # Shared backend fixtures
    helpers/                 # Test utilities and Hypothesis strategies

docs/architecture/           # Mermaid diagrams, Redis key map, algorithm docs
```

## Things to Avoid

- **Algorithm naming**: always refer to it as the "sliding window counter" algorithm, never just "sliding window"
- **Em dashes**: do not use `—` or ` -- ` (spaced) in prose or documentation
- **Redis Cluster**: do not add Cluster support; the Lua scripts require single-node atomicity
- **`time.sleep()` in tests**: for precise timing, use `precise_sleep()` from `tests/integration/conftest.py`, which uses active polling to work around Windows timer resolution issues (~15ms granularity)
- **Sub-second timing on Windows**: integration tests with tight timing are unreliable on Windows; run them in Docker via `docker compose --profile test up`

## Test Formatting

- **AAA pattern**: structure tests with `# Arrange`, `# Act`, `# Assert` section comments on their own lines; follow-up context comments go on new lines below each section header
- **Assertion messages**: every `assert` must include a custom failure message; lowercase, no trailing punctuation (e.g., `assert count == 5, "consumed count should equal the configured limit"`)

## Testing Strategy

- **Fast tests** (`docker compose --profile test up`): contracts, algorithms, properties, and implementation tests; no real timing
- **All tests** (`docker compose --profile test-all up`): includes integration tests that use `precise_sleep` and real Redis
- **Host pytest**: does not work; tests require Redis and must be run through Docker
- **Coverage**: 90% threshold on master, 70% on dev
- **Guidelines**: see [tests/TESTING_GUIDELINES.md](tests/TESTING_GUIDELINES.md) for detailed test authoring rules, assertion patterns, separation of concerns, and mutation testing conventions
