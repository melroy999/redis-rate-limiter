# Test Suite Documentation

This directory contains the test suite for the `redis-rate-limiter` project, organized using **contract-based**, **property-based**, **algorithm/spec** and **integration** testing patterns.

All test categories assume that a real Redis instance is available, given that the core limiter logic is implemented in Redis Lua scripts.

## Backend Structuring (Important)

The suite is structured around both test type and backend scope:

- `tests/<category>/...` contains backend-agnostic core behavior.
- `tests/<category>/<backend>/...` contains backend-specific behavior.

The following backends are currently supported:

- `tests/implementations/celery/...` for Celery-specific assertions.
- `tests/implementations/rq/...` for RQ-specific assertions.
- `tests/implementations/processpool/...` for ProcessPool-specific assertions.
- `tests/implementations/threadpool/...` for ThreadPool-specific assertions.
- `tests/implementations/asyncio/...` for AsyncIO-specific assertions.
- `tests/implementations/asgi/...` for ASGI-specific assertions.

All backends inherit the shared contract suite via `RateLimiterContractTest` (unified async). Sync backends participate by wrapping their implementations with `SyncToAsyncLimiterAdapter`, while async backends run natively. Each backend only adds tests for behavior that is unique to that backend.

## Directory Structure

```text
tests/
├── contracts/                          # Abstract interface contracts
│   ├── test_rate_limiter.py            # Tests any limiter must satisfy (unified async)
│   ├── test_distributed_lock.py        # Tests any lock must satisfy (unified async)
│   └── test_task_lifecycle.py          # Tests any lifecycle manager must satisfy (unified async)
│
├── implementations/                    # Core implementation tests (backend-agnostic)
│   ├── conftest.py                     # Shared core test fixtures/limiters
│   ├── test_rate_limiter.py            # Generic limiter implementation behavior
│   ├── test_rate_limiter_class_api.py  # Managed class API behavior (generic backend)
│   ├── test_distributed_lock.py        # Redis-backed lock implementation tests
│   ├── test_task_lifecycle.py          # Lifecycle manager implementation tests
│   ├── test_drain.py                   # Drain and trigger_consume branch tests
│   ├── test_drain_loop.py             # DrainLoop scheduling and coalescing tests
│   ├── test_get_status.py              # Status reporting tests
│   ├── test_lua_script_infrastructure.py  # Script loading, registration, and _eval_script recovery tests
│   ├── test_task_data_helpers.py          # Task signature building, inflight TTL computation, and cleanup tests
│   ├── test_token_recovery_delay.py       # Token recovery delay calculation boundary and exact-value tests
│   ├── test_limiter_config.py             # Initial configuration defaults and window change tests
│   ├── test_async_drain_loop.py           # Async drain loop scheduling, coalescing, and shutdown tests
│   ├── test_smart_jitter.py            # Adaptive jitter calculation tests
│   ├── test_metrics_callback.py        # Metrics callback observability tests
│   ├── test_concurrent_access.py       # Multi-worker contention and atomicity tests
│   ├── test_async_distributed_lock.py   # Async lock implementation tests
│   ├── test_async_task_lifecycle.py     # Async lifecycle implementation tests
│   ├── test_decorator.py               # Decorator behavior (core)
│   ├── test_importing.py               # Dynamic import helper behavior
│   ├── celery/                         # Celery-specific implementation tests
│   │   ├── conftest.py                 # Imports Celery backend fixtures
│   │   ├── test_contracts.py           # Contract suite against real CeleryRateLimiter
│   │   ├── test_celery_limiter.py      # Celery payload/dispatch behavior
│   │   ├── test_rate_limiter_class_api.py  # Celery-only class API tests
│   │   └── test_tasks.py              # Celery task helper tests
│   ├── rq/                             # RQ-specific implementation tests
│   │   ├── conftest.py                 # Imports RQ backend fixtures
│   │   ├── test_contracts.py           # Contract suite against real RQRateLimiter
│   │   ├── test_rq_limiter.py          # RQ payload/dispatch behavior
│   │   ├── test_rate_limiter_class_api.py  # RQ-only class API tests
│   │   └── test_tasks.py              # RQ task helper tests
│   ├── processpool/                   # ProcessPool-specific implementation tests
│   │   ├── conftest.py                 # Imports ProcessPool backend fixtures
│   │   ├── test_contracts.py           # Contract suite against real ProcessPoolRateLimiter
│   │   ├── test_processpool_limiter.py # ProcessPool dispatch behavior
│   │   └── test_rate_limiter_class_api.py  # ProcessPool-only class API tests
│   ├── threadpool/                     # ThreadPool-specific implementation tests
│   │   ├── conftest.py                 # Imports ThreadPool backend fixtures
│   │   ├── test_contracts.py           # Contract suite against real ThreadPoolRateLimiter
│   │   ├── test_threadpool_limiter.py  # ThreadPool dispatch behavior
│   │   └── test_rate_limiter_class_api.py  # ThreadPool-only class API tests
│   ├── asyncio/                        # AsyncIO-specific implementation tests
│   │   ├── conftest.py                 # Imports AsyncIO backend fixtures
│   │   ├── test_contracts.py           # Contract suite against real AsyncIOTaskLimiter
│   │   ├── test_asyncio_limiter.py     # AsyncIO dispatch and lifecycle behavior
│   │   └── test_rate_limiter_class_api.py  # AsyncIO-only class API tests
│   └── asgi/                           # ASGI-specific implementation tests
│       ├── conftest.py                 # Imports ASGI backend fixtures
│       ├── test_asgi_limiter.py        # ASGI limiter acquire behavior
│       ├── test_middleware.py          # ASGI middleware integration tests
│       └── test_keys.py               # Key extraction function tests
│
├── algorithms/                         # Pure algorithm/spec tests (no backend)
│   ├── sliding_window_counter.py       # Shared pure algorithm used by tests
│   └── test_sliding_window_counter.py  # Deterministic algorithm/spec tests
│
├── properties/                         # Property-based tests (Hypothesis)
│   ├── test_concurrency_invariants.py  # Concurrency bound invariants
│   ├── test_serialization.py           # Payload serialization properties
│   ├── test_is_subset.py               # Mathematical subset properties
│   ├── test_sliding_window_counter.py  # Sliding window invariants (Hypothesis)
│   ├── test_smart_jitter.py            # Smart jitter invariants (Hypothesis)
│   ├── test_token_recovery.py          # Token recovery delay invariants (Hypothesis)
│   ├── test_inflight_ttl.py            # In-flight TTL calculation invariants (Hypothesis)
│   ├── test_config_round_trip.py       # Config persist/hydrate round-trip invariants (Hypothesis)
│   └── asgi/                           # ASGI-specific property tests
│       └── test_keys.py                # Key extraction invariants (Hypothesis)
│
├── integration/                        # End-to-end integration tests
│   ├── test_rate_limiting.py           # Single-consumer rate limiting behavior
│   ├── test_distributed_rate_limiting.py  # Multi-consumer temporal rate limiting
│   ├── README.md                       # Platform requirements and timing notes
│   └── celery/                         # Reserved for Celery-specific integration tests
│
├── integrations/                       # Third-party integration tests
│   └── test_prometheus.py              # Prometheus metrics exporter tests
│
├── fixtures/                           # Shared backend fixture modules
│   ├── celery_backend.py               # Celery backend fixture definitions
│   ├── processpool_backend.py          # ProcessPool backend fixture definitions
│   └── threadpool_backend.py           # ThreadPool backend fixture definitions
│
├── helpers/                            # Shared test utilities and strategies
│   ├── utils.py                        # Subset checker and approximate equality
│   ├── strategies.py                   # Shared Hypothesis strategies
│   ├── adapters.py                     # Sync-to-async adapters for unified contract tests
│   └── tasks.py                        # Shared task functions for backend tests
│
├── conftest.py                         # Global pytest fixtures and configuration
└── README.md                           # This file
```

## Quick Placement Rules

1. Contract tests define interface guarantees and are placed in `contracts/`.
2. Implementation tests that are backend-agnostic are placed in `implementations/`.
3. Implementation tests that assert backend-specific behavior are placed in `implementations/<backend>/`.
4. Algorithm/spec tests validate pure logic with no Redis/Celery dependency in the test code and are placed in `algorithms/`.
5. Property-based tests use Hypothesis to check invariants and are placed in `properties/`. Backend-scoped property tests are placed in `properties/<backend>/` (e.g., `properties/asgi/`).
6. Integration tests verify end-to-end behavior with real Redis/Lua wiring and are placed in `integration/`.
7. If an integration scenario depends on backend dispatch/wiring details, it should be placed under `integration/<backend>/`.

## Within-File Organization

Tests that cover the same feature, function, or component are grouped within a single class. Each class acts as a logical unit of related assertions, and when a file tests multiple distinct features, each feature gets its own class. Methods that do not use `self` are decorated with `@staticmethod`. The only exception is Hypothesis `@given`-decorated methods, which require `self` due to a framework limitation.

## Testing Philosophy

### 1. Contract-Based Testing

Contract tests define the expected behavior for interfaces, ensuring that all implementations satisfy the same requirements.

**Benefits:**

- New implementations automatically inherit all contract tests.
- Consistency across different limiter implementations is ensured.
- The required interface behavior is documented through the tests themselves.
- The Liskov Substitution Principle can be verified straightforwardly.

**Example:**
```python
# contracts/test_rate_limiter.py
class RateLimiterContractTest:
    """Abstract test suite that any rate limiter implementation must pass."""

    @staticmethod
    def test_schedule_task_returns_success_and_task_id(limiter, redis_client):
        """Contract: ``schedule_task()`` must return ``(bool, str)`` tuple."""
        success, task_id = limiter.schedule_task("path", {})
        assert isinstance(success, bool)
        assert isinstance(task_id, str)

# implementations/test_rate_limiter.py: contract compliance.
class TestRateLimiterContracts(RateLimiterContractTest):
    """Verify the generic backend satisfies all rate limiter contracts."""
    pass

# implementations/threadpool/test_contracts.py: real backend.
class TestThreadPoolContracts(RateLimiterContractTest):
    """Verify ``ThreadPoolRateLimiter`` satisfies all rate limiter contracts."""
    pass
```

### 2. Property-Based Testing

Property-based tests use [Hypothesis](https://hypothesis.readthedocs.io/) to automatically generate hundreds of test cases, verifying that invariants hold for any input.

**Benefits:**

- Edge cases that would not be considered in manual testing are discovered automatically.
- Mathematical properties (reflexivity, transitivity, etc.) can be verified.
- Stronger guarantees are provided compared to example-based tests.
- Failing cases are automatically shrunk to minimal examples.

**Example:**

```python
from hypothesis import given
from tests.helpers.strategies import json_value

# Generates arbitrary JSON structures.
@given(payload=json_value)
def test_json_payload_survives_redis_round_trip(self, limiter, redis_client, payload):
    """Property: any JSON payload survives Redis round-trip."""
    success, task_id = limiter.schedule_task("path", payload)
    # Verify payload was preserved.
```

### 3. Algorithm/Spec Testing

Algorithm/spec tests validate pure logic extracted from the Redis/Lua behavior, without any backend dependencies.

**Benefits:**

- Core mathematical logic and edge cases are verified deterministically.
- Complex logic remains testable without the need for Redis time control.
- The tests serve as a specification for the Lua implementation.

**Example:**

```python
from tests.algorithms.sliding_window_counter import sliding_window_estimate


def test_weight_is_half_at_midpoint():
    # Assert
    assert sliding_window_estimate(10, 0, 1000, 500) == 5.0, (
        "estimate should halve previous-window count at midpoint"
    )
```

### 4. Integration Testing

Integration tests verify the end-to-end behavior of the rate limiter with real Redis and Lua scripts. The suite is split into single-consumer tests (`test_rate_limiting.py`) that verify core rate and concurrency enforcement, and multi-consumer tests (`test_distributed_rate_limiting.py`) that verify temporal rate-limiting correctness when multiple independent limiter instances share the same Redis backend over sustained time periods.

**Benefits:**

- The full system is tested, including Redis interactions and Lua script execution.
- Rate limiting behavior is verified in realistic scenarios.
- Burst handling, concurrency limits and telemetry tracking are tested.
- Multi-consumer tests validate per-window consumption bounds, backpressure dynamics and sine-wave traffic patterns.
- Explicit fixture configuration is used to maintain test independence.

**Example:**
```python
# integration/test_rate_limiting.py
@pytest.fixture
def integration_limiter(redis_client, limiter_id):
    """Create a backend-agnostic limiter for integration tests."""
    return StubRateLimiter(
        redis_client=redis_client,
        limiter_id=f"{limiter_id}_integration_default",
        limit=5,
        window=60,
        max_concurrency=2,
        max_age=3600,
        lease_duration=30,
    )


@staticmethod
def test_basic_rate_limit_enforcement(integration_limiter, func_path):
    """Verify rate limiter enforces the configured limit."""
    # Arrange
    for i in range(10):
        integration_limiter.schedule_task(func_path, {"index": i})

    # Act
    consumed = sum(1 for _ in range(10) if integration_limiter.consume()["success"])

    # Assert
    assert consumed == 5, "consumed count should equal the configured limit"
```

### 5. Arrange-Act-Assert Pattern

All tests follow the AAA pattern for clarity:

```python
@staticmethod
def test_example(limiter, redis_client):
    """Clear description of what this test verifies."""
    # Arrange
    payload = {"user_id": 123}

    # Act
    success, task_id = limiter.schedule_task("path", payload)

    # Assert
    assert success is True, "task should be scheduled successfully"
```

## Running Tests

All test runs expect a real Redis instance to be available. The Redis connection is configurable via environment variables:

- `REDIS_HOST` (default: `localhost`)
- `REDIS_PORT` (default: `6379`)

### Run All Tests (Excluding Slow Tests)
```bash
pytest tests/
```

### Run Slow Tests
Slow tests use real `time.sleep()` calls and run parameterized configurations. They are excluded by default for faster local development.
```bash
# Run only slow tests.
pytest -m slow tests/

# Run all tests including slow tests (as in CI).
pytest --override-ini='addopts=' tests/
```

### Run via Docker (Recommended for Integration Tests)
Sliding window timing tests are skipped on Windows. Docker should be used for reliable results:
```bash
# Fast tests only.
docker compose --profile test up

# Includes @pytest.mark.slow.
docker compose --profile test-all up
```

### Run Specific Test Categories
```bash
# Contract tests only.
pytest tests/contracts/

# Implementation tests only.
pytest tests/implementations/

# Core implementation tests only (no backend-specific).
pytest tests/implementations/ --ignore=tests/implementations/celery --ignore=tests/implementations/processpool --ignore=tests/implementations/threadpool --ignore=tests/implementations/asyncio --ignore=tests/implementations/asgi

# Celery-specific implementation tests only.
pytest tests/implementations/celery/

# ProcessPool-specific implementation tests only.
pytest tests/implementations/processpool/

# ThreadPool-specific implementation tests only.
pytest tests/implementations/threadpool/

# AsyncIO-specific implementation tests only.
pytest tests/implementations/asyncio/

# ASGI-specific implementation tests only.
pytest tests/implementations/asgi/

# Property-based tests only.
pytest tests/properties/

# Algorithm/spec tests only.
pytest tests/algorithms/

# Integration tests only.
pytest tests/integration/
```

### Run with Coverage
```bash
pytest tests/ --cov=redis_rate_limiter --cov-report=html
```

### Run Property Tests with More Examples
```bash
# Default: 50 examples per property.
pytest tests/properties/

# More thorough: 200 examples.
pytest tests/properties/ --hypothesis-max-examples=200
```

## Mutation Testing

[Mutation testing](https://en.wikipedia.org/wiki/Mutation_testing) evaluates the quality of the test suite by systematically introducing small code changes (mutations) and checking whether the tests detect them. A mutation that causes at least one test to fail is "killed"; a mutation where all tests still pass is a "survivor", indicating a potential gap in assertions or coverage. The project uses [mutmut](https://mutmut.readthedocs.io/) for mutation testing, configured in `pyproject.toml` under `[tool.mutmut]`.

### Running

mutmut requires `fork()` support and must be run inside Docker. The `--build` flag is required because the source code is copied into the image rather than volume-mounted:

```bash
docker compose --profile mutate up --build --abort-on-container-exit --exit-code-from mutate
```

### Interpreting Results

The output ends with a progress line and a list of unresolved mutants:

```
1912/1912  🎉 1906 🫥 0  ⏰ 5  🤔 0  🙁 1  🔇 0
    redis_rate_limiter.core.limiters.xǁSomeClassǁsome_method__mutmut_6: survived
```

mutmut processes one mutant at a time. For each mutant, it applies the mutation, runs the mapped tests, classifies the outcome, and advances the progress counter. The progress line reads as `processed/total` followed by six cumulative outcome counts (i.e., the running totals across all mutants processed so far, summing to `processed`):

- 🎉 **Killed** (1906): a mapped test failed, confirming the mutation was detected.
- 🫥 **No tests** (0): mutmut could not map the mutant to any test via coverage data.
- ⏰ **Timeout** (5): the mapped tests timed out, typically because the mutation caused an infinite loop (e.g., mutating a shutdown flag); effectively killed.
- 🤔 **Suspicious** (0): the test suite exited with an unexpected status (e.g., a segfault or internal error rather than a clean pass or fail); warrants manual investigation.
- 🙁 **Survived** (1): all mapped tests passed despite the mutation; investigate whether a stronger assertion is needed.
- 🔇 **Skipped** (0): the mutant was not tested, typically due to mutmut configuration or filters.

mutmut uses coverage data to select only the tests that exercise the mutated code path, rather than running the full suite for each mutant.

Not all survivors are actionable. Logger format string mutations, type cast changes (e.g., `cast(int, x)` to `cast(float, x)`), and mutations in abstract methods that are always overridden are common false positives and can be suppressed with `# pragma: no mutate`.

### Known Trampoline Limitations

mutmut v3 rewrites each function with a trampoline dispatcher that uses `object.__getattribute__(self, ...)` to resolve the original and mutant variants. This approach has three known limitations:

1. **`__init_subclass__`**: the trampoline generates `self` as the first parameter, but `__init_subclass__` receives `cls`. This causes a `NameError` that poisons test collection. The fix is to add an explicit `@classmethod` decorator, which is redundant at runtime (Python implicitly wraps `__init_subclass__`) but tells mutmut to use `cls` in the trampoline. See `ManagedRateLimiterMixin.__init_subclass__` in `core/managed.py` and [mutmut#366](https://github.com/boxed/mutmut/issues/366).
2. **`async def` methods**: in released versions (up to 3.4.0), the trampoline dispatcher is a synchronous function wrapping `async def` methods, causing `TypeError: object dict can't be used in 'await' expression`. This was fixed on the mutmut main branch in commit `810d761` ("Preserve original signature, including async keyword"), which is why the dependency points at the git main branch rather than a PyPI release.
3. **Default parameter values**: Python stores default parameter values in the function object's `__defaults__` tuple when the `def` statement executes at import time. mutmut's AST mutations only modify the code object inside forked children, but the `__defaults__` tuple inherited from the parent process is unchanged. This means any mutation to a default value (e.g., `delay: float = 0.0` to `delay: float = 1.0`) is invisible to the test suite, regardless of what tests exist. Use `inspect.signature` tests to verify default values independently: these catch real regressions in normal development, even though they cannot catch mutmut mutations. The classifier reports these as "fork-immune" false survivors.

### Why Mutation Testing Runs Locally

Mutation testing runs on a local machine rather than on CI. The CI environment (GitHub Actions private repository runners: 2 shared vCPUs, 8 GB RAM) produces a significantly lower mutation score than local runs, despite using the same test suite. The additional CI survivors are covered by existing tests that kill them locally; on CI, those tests fail to detect the mutations.

Attempts to replicate the CI environment through local experimentation (resource-constrained Docker containers with CPU pinning, memory limits, and co-located Redis) led to discrepancies that could not be fully explained. One notable observation is that a considerable number of mutations end up as timeouts locally, while the same mutations survive on CI. Because the local environment consistently produces reliable and reproducible results, mutation testing is run locally instead of in CI.

The mutation score badge is updated automatically when `mutmut-results/mutation-score.txt` is pushed to master (see `.github/workflows/mutation.yml`). The workflow retains a manual dispatch trigger for running mutation testing on CI directly, which may become viable when the repository is made public (upgrading the runner to 4 cores and 16 GB RAM).

## Adding a New Limiter Implementation

When adding a new backend limiter implementation, the following steps should be taken:

### Step 1: Create the Backend Test Directory
```bash
mkdir -p tests/implementations/mybackend
touch tests/implementations/mybackend/__init__.py
```

### Step 2: Add Backend Fixtures (If Needed)

If the backend requires shared fixtures, a fixture module should be added and imported from the backend conftests:

```text
tests/fixtures/mybackend_backend.py
tests/implementations/mybackend/conftest.py
```

This keeps the backend fixtures centralized and reusable across categories.

### Step 3: Add Fixtures and Contract Compliance Tests
```python
# tests/implementations/mybackend/conftest.py
import pytest
from your_module import MyBackendRateLimiter


@pytest.fixture
def limiter(limiter_id):
    return MyBackendRateLimiter(
        limiter_id=f"{limiter_id}_mybackend_default",
        limit=100,
        window=60,
        max_concurrency=10,
    )


# tests/implementations/mybackend/test_contracts.py
from tests.contracts.test_rate_limiter import RateLimiterContractTest


class TestMyBackendContracts(RateLimiterContractTest):
    """Verify ``MyBackendRateLimiter`` satisfies all rate limiter contracts."""
    pass
```

### Step 4: Add Backend-Specific Integration/Property Tests Only Where Needed

If the behavior depends on backend internals, those tests should be placed under:

- `tests/integration/mybackend/`
- `tests/properties/mybackend/` (only if backend internals alter invariants)

### Step 5: Run the Backend Tests
```bash
pytest tests/implementations/mybackend/
```

## Shared Resources

### helpers/utils.py

Contains shared utility functions used across multiple tests:

- `is_subset(target, superset)`: a recursive dictionary subset checker.
- `dict_equals_approx(left, right)`: approximate equality for nested structures, with configurable tolerance for float comparisons.

**Usage:**

```python
from tests.helpers.utils import is_subset, dict_equals_approx

assert is_subset({"a": 1}, {"a": 1, "b": 2})
assert dict_equals_approx({"x": 1.0000001}, {"x": 1.0})
```

### helpers/strategies.py

Contains shared Hypothesis strategies for generating test data:

- `json_value`: generates arbitrary JSON-serializable values (recursive structure of primitives, lists and dicts).
- `nested_dict`: generates arbitrary nested dictionaries (filtered from `json_value`).

**Usage:**

```python
from hypothesis import given
from tests.helpers.strategies import json_value, nested_dict


@given(payload=json_value)
def test_something(self, payload):
    pass
```

### helpers/adapters.py

Contains sync-to-async adapter classes that wrap synchronous rate limiters, distributed locks, and task lifecycle managers so that the unified async contract test suite can exercise sync backends via `await`. The underlying sync Redis calls block the event loop briefly, which is acceptable in a test context.

- `SyncToAsyncLimiterAdapter`: wraps a sync `AbstractDistributedRateLimiter` as an async-compatible limiter.
- `SyncToAsyncLockAdapter`: wraps a sync `DistributedLock` as an async context manager (`async with`).
- `SyncToAsyncLifecycleAdapter`: wraps a sync `TaskLifecycle` as an async context manager.

**Usage:**

```python
from tests.helpers.adapters import SyncToAsyncLimiterAdapter

# Wrap a sync limiter for use in async contract tests.
async_compatible = SyncToAsyncLimiterAdapter(sync_limiter)
result = await async_compatible.schedule_task("path", {})
```

### helpers/tasks.py

Contains shared task functions used by backend tests. These are referenced via their dotted-path strings (e.g., `"tests.helpers.tasks.noop_task"`) in tests that exercise task dispatch:

- `noop_task(**kwargs)`: a synchronous no-op task that accepts any keyword arguments and returns immediately.
- `noop_task_2(**kwargs)`: a second synchronous no-op task with a distinct function path for deduplication tests.
- `async_noop_task(**kwargs)`: an async no-op task that accepts any keyword arguments and returns immediately. Used by the AsyncIO backend tests, which require coroutine functions.
- `async_noop_task_2(**kwargs)`: a second async no-op task with a distinct function path for deduplication tests.
- `slow_task(**kwargs)`: an async task that sleeps for a long duration, used for cancellation tests.

**Usage:**

```python
# Sync backend: pass a sync function path.
limiter.schedule_task("tests.helpers.tasks.noop_task", {"key": "value"})

# Async backend: pass an async function path.
await limiter.schedule_task("tests.helpers.tasks.async_noop_task", {"key": "value"})
```

## Test Naming Conventions

### Test Class Names

- **Contract classes**: `{Component}ContractTest` (e.g., `RateLimiterContractTest`).
- **Implementation classes**: `Test{Implementation}{Component}` (e.g., `TestCeleryRateLimiter`).

### Test Method Names

Test method names should be descriptive and read like sentences. They should start with `test_` and include both what is being tested and the expected outcome.

**Recommended:**
- `test_schedule_task_returns_success_and_task_id`
- `test_lock_releases_on_exception`
- `test_payload_survives_redis_round_trip`

**Avoid:**
- `test_schedule`
- `test_1`
- `test_lock`

### Docstrings

- *Contract tests*: start with `Contract: ` to clarify the requirement.
- *Property tests*: start with `Property: ` to clarify the invariant.
- *Implementation tests*: describe the specific behavior being tested.

**Examples:**
```python
@staticmethod
def test_schedule_duplicate_task_returns_false(limiter, func_path, payload):
    """Contract: scheduling identical tasks must return False on duplicate."""

@given(payload=json_value)
def test_payload_survives_round_trip(self, payload):
    """Property: any JSON-serializable payload survives Redis round-trip."""

@staticmethod
def test_lua_script_recovery_on_noscript_error(limiter, func_path):
    """Verify limiter recovers from ``NoScriptError`` by reloading Lua script."""
```

## Assertion Style

### Use Lowercase for Messages
```python
# Recommended.
assert success is True, "task should be scheduled successfully"

# Avoid.
assert success is True, "Task should be scheduled successfully"
```

### Provide Context in Failure Messages
```python
# Recommended.
assert len(results) > 0, f"task with ID {task_id} not found in buffer"

# Avoid.
assert len(results) > 0
```

### Comments Before Lines, Not After
```python
# Recommended.
# Clean state for each example.
redis_client.flushdb()

# Avoid.
redis_client.flushdb()  # Clean state
```

## Fixture Architecture

### Naming Convention

Fixtures use natural, descriptive names without prefixes. A fixture's name describes what it provides, not its role in a derivation chain.

For example, `limiter_id` is preferred over `default_limiter_id`: callers already know it is a base from context. Backend limiter fixtures are all named `limiter`, since directory-scoped conftests prevent collisions.

### Definition Location

Fixtures are placed at the **narrowest scope** that serves all their consumers:

| Level | Location | What belongs here |
|---|---|---|
| **Root** | `tests/conftest.py` | Global infrastructure (`redis_client`, `async_redis_client`) and universal identifiers/values (`limiter_id`, `module_limiter_id`, `lock_key`, `func_path`, `payload`) |
| **Category** | `tests/{category}/conftest.py` | Shared fixtures for a test category (e.g., `implementations/conftest.py` has `stub_limiter`, `tracking_limiter`, while `properties/conftest.py` has `property_redis_client`) |
| **Backend** | `tests/implementations/{backend}/conftest.py` | Backend-specific `limiter` fixture and autouse reset fixtures |
| **External module** | `tests/fixtures/{backend}_backend.py` | Sync backend fixture definitions re-exported by conftest (Celery, ThreadPool: needed for cross-directory import) |
| **Test file** | The test file itself | Fixtures used exclusively by that file (`mock_limiter`, `inflight_key`, `create_lifecycle`, local factories) |

**Rule: never duplicate a fixture across files.** If two files need the same fixture, promote it to the nearest shared conftest.

### No Redefinition Without Transformation

Each backend conftest provides its limiter under the name `limiter`. Contract test binding follows naturally:

- **Async backends** (AsyncIO): conftest provides `limiter`: the contract test class inherits directly (no override needed).
- **Sync backends** (Celery, ThreadPool): conftest provides `limiter`: the contract test class overrides with `SyncToAsyncLimiterAdapter` wrapping (genuine transformation, justified).
- **Generic implementations**: `implementations/conftest.py` provides `stub_limiter` (distinct name because it coexists with backend `limiter` fixtures in the same directory tree): test classes map to `limiter` at class level.

**Rule: a fixture override at the class or file level is only justified when it transforms the value.** A pass-through that returns the input unchanged must be removed, and the source fixture should be renamed to match the expected name instead.

### Scoping Conventions

| Scope | When to use | Examples |
|---|---|---|
| `session` | Expensive creation (connections, apps, executors), immutable constants | `_redis_connection`, `celery_app`, `executor`, `func_path`, `payload` |
| `module` | Property-based test fixtures needing persistence across Hypothesis examples | `module_limiter_id`, `property_redis_client`, `property_limiter` |
| `function` | Everything else: ensures test isolation (this is the default) | `redis_client`, `limiter_id`, `lock_key`, `limiter`, all test-specific fixtures |

### Fixture Reference

#### Root fixtures (`tests/conftest.py`)

- `_redis_connection` (session): a single Redis connection for the entire test suite (configurable via `REDIS_HOST`/`REDIS_PORT`).
- `redis_client` (function): wraps `_redis_connection` with `flushdb()` before and after each test.
- `async_redis_client` (function): a per-test async Redis client with `flushdb()` before and after each test.
- `limiter_id` (function): a unique limiter ID per test (UUID-backed).
- `module_limiter_id` (module): a unique limiter ID per module.
- `lock_key` (function): a unique lock key per test.
- `func_path` (session): a fictional function path for test task scheduling.
- `payload` (session): the default payload `{"user_id": 123}` for tests.

#### Implementation fixtures (`tests/implementations/conftest.py`)

- `stub_limiter`: a `StubRateLimiter` instance (no-op dispatch/schedule) for testing `AbstractDistributedRateLimiter` behavior.
- `tracking_limiter`: a `TrackingRateLimiter` instance that records `_dispatch_task()` and `_schedule_drain()` calls.
- `make_limiter_pool`: a factory fixture that creates N limiter instances sharing the same Redis-backed limiter ID.
- `task_id`: a unique task ID string for testing.

#### Backend fixtures

Sync backend fixtures are centralized in `tests/fixtures/` and imported where needed:

- `tests/fixtures/celery_backend.py` defines Celery fixtures (`celery_app`, `celery_config`, `limiter`, class-state reset fixture).
- `tests/fixtures/processpool_backend.py` defines ProcessPool fixtures (`executor`, `limiter`, class-state reset fixture).
- `tests/fixtures/threadpool_backend.py` defines ThreadPool fixtures (`executor`, `limiter`, class-state reset fixture).

Async backend fixtures are defined directly in their backend-local conftests:

- `tests/implementations/asyncio/conftest.py` defines `limiter` (function-scoped, creates an `AsyncIOTaskLimiter`) and an autouse class-state reset fixture.
- `tests/implementations/asgi/conftest.py` defines `limiter` (function-scoped, creates an `ASGIRateLimiter`) and an autouse class-state reset fixture.

#### Property test fixtures

- `property_redis_client` (module): a shared Redis client for a module's Hypothesis runs (defined in `tests/properties/conftest.py`).
- `property_limiter` (module): a shared limiter for a module's Hypothesis runs (defined locally per property test module, as each module uses different limiter configurations).

## Best Practices

### 1. Keep Tests Focused

Each test should verify one specific behavior. If multiple assertions are needed, they should all relate to the same behavior.

### 2. Use Parametrize for Variations
```python
@pytest.mark.parametrize(
    "payload",
    [{"a": 1}, {}, {"nested": {"b": 2}}],
    ids=["simple", "empty", "nested"],
)
@staticmethod
def test_various_payloads(limiter, payload):
    pass
```

### 3. Mock External Dependencies

`unittest.mock` should be used for backend-specific behavior where full worker execution is not required.

### 4. Clean Up Resources

Fixtures with proper teardown or context managers should be used to ensure that resources are cleaned up even if tests fail.

### 5. Use Unique IDs in Fixtures

Fixture-provided unique IDs should be preferred over hardcoded IDs for limiter names, lock keys and task IDs.

```python
def test_example(limiter_id, lock_key):
    derived_id = f"{limiter_id}_example"
```

Explicit hardcoded IDs should only be used when the test is specifically about ID identity or the readability of a known failure case.

## Troubleshooting

### Tests Fail Due to Redis Connection

Ensure that Redis is running:
```bash
# Should return "PONG".
redis-cli ping
```

### Hypothesis Tests Are Slow

The number of examples can be reduced for faster iteration:
```bash
pytest tests/properties/ --hypothesis-max-examples=10
```

### Import Errors

Ensure that pytest is run from the project root:
```bash
cd /path/to/redis-rate-limiter
pytest tests/
```

## Contributing

When adding new tests, the following should be kept in mind:

1. Follow the existing structure (contracts, implementations, algorithms, properties, integration).
2. Respect the backend scoping rules (`<category>/` for core, `<category>/<backend>/` for backend-specific).
3. Use the AAA pattern (Arrange-Act-Assert).
4. Add descriptive docstrings.
5. Follow the naming conventions.
6. Update this README if adding new patterns or conventions.

## Resources

- [Pytest Documentation](https://docs.pytest.org/)
- [Hypothesis Documentation](https://hypothesis.readthedocs.io/)
- [Contract Testing Explained](https://martinfowler.com/bliki/ContractTest.html)
- [Property-Based Testing](https://increment.com/testing/in-praise-of-property-based-testing/)
