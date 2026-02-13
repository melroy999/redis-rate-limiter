# Test Suite Documentation

This directory contains the test suite for the `celery-rate-limiter` project, organized using **contract-based**, **property-based**, **algorithm/spec** and **integration** testing patterns.

All test categories assume that a real Redis instance is available, given that the core limiter logic is implemented in Redis Lua scripts.

## Backend Structuring (Important)

The suite is structured around both test type and backend scope:

- `tests/<category>/...` contains backend-agnostic core behavior.
- `tests/<category>/<backend>/...` contains backend-specific behavior.

The following backends are currently supported:

- `tests/implementations/celery/...` for Celery-specific assertions.
- `tests/implementations/threadpool/...` for ThreadPool-specific assertions.

Each backend inherits the shared contract suite via `RateLimiterContractTest` and only adds tests for behavior that is unique to that backend.

## Directory Structure

```text
tests/
├── contracts/                          # Abstract interface contracts
│   ├── test_rate_limiter.py            # Tests any limiter must satisfy
│   ├── test_distributed_lock.py        # Tests any lock must satisfy
│   └── test_task_lifecycle.py          # Tests any lifecycle manager must satisfy
│
├── implementations/                    # Core implementation tests (backend-agnostic)
│   ├── conftest.py                     # Shared core test fixtures/limiters
│   ├── test_rate_limiter_impl.py       # Generic limiter implementation behavior
│   ├── test_rate_limiter_class_api.py  # Managed class API behavior (generic backend)
│   ├── test_distributed_lock.py        # Redis-backed lock implementation tests
│   ├── test_task_lifecycle.py          # Lifecycle manager implementation tests
│   ├── test_drain.py                   # Drain and trigger_consume branch tests
│   ├── test_drain_loop.py             # DrainLoop scheduling and coalescing tests
│   ├── test_get_status.py              # Status reporting tests
│   ├── test_internal_helpers.py        # Lua script loading and helpers
│   ├── test_smart_jitter.py            # Adaptive jitter calculation tests
│   ├── test_metrics_callback.py        # Metrics callback observability tests
│   ├── test_concurrent_access.py       # Multi-worker contention and atomicity tests
│   ├── test_decorator.py               # Decorator behavior (core)
│   ├── test_importing.py               # Dynamic import helper behavior
│   ├── celery/                         # Celery-specific implementation tests
│   │   ├── conftest.py                 # Imports Celery backend fixtures
│   │   ├── test_contracts.py           # Contract suite against real CeleryRateLimiter
│   │   ├── test_celery_limiter.py      # Celery payload/dispatch behavior
│   │   ├── test_rate_limiter_class_api.py  # Celery-only class API tests
│   │   └── test_tasks.py              # Celery task helper tests
│   └── threadpool/                     # ThreadPool-specific implementation tests
│       ├── conftest.py                 # Imports ThreadPool backend fixtures
│       ├── test_contracts.py           # Contract suite against real ThreadPoolRateLimiter
│       ├── test_threadpool_limiter.py  # ThreadPool dispatch behavior
│       └── test_rate_limiter_class_api.py  # ThreadPool-only class API tests
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
│   └── test_smart_jitter.py            # Smart jitter invariants (Hypothesis)
│
├── integration/                        # End-to-end integration tests
│   ├── test_rate_limiting.py           # Core Redis/Lua integration behavior
│   ├── README.md                       # Platform requirements and timing notes
│   └── celery/                         # Reserved for Celery-specific integration tests
│
├── fixtures/                           # Shared backend fixture modules
│   ├── celery_backend.py               # Celery backend fixture definitions
│   └── threadpool_backend.py           # ThreadPool backend fixture definitions
│
├── helpers/                            # Shared test utilities and strategies
│   ├── utils.py                        # Subset checker and approximate equality
│   └── strategies.py                   # Shared Hypothesis strategies
│
├── conftest.py                         # Global pytest fixtures and configuration
└── README.md                           # This file
```

## Quick Placement Rules

1. Contract tests define interface guarantees and are placed in `contracts/`.
2. Implementation tests that are backend-agnostic are placed in `implementations/`.
3. Implementation tests that assert backend-specific behavior are placed in `implementations/<backend>/`.
4. Algorithm/spec tests validate pure logic with no Redis/Celery dependency in the test code and are placed in `algorithms/`.
5. Property-based tests use Hypothesis to check invariants and are placed in `properties/`.
6. Integration tests verify end-to-end behavior with real Redis/Lua wiring and are placed in `integration/`.
7. If an integration scenario depends on backend dispatch/wiring details, it should be placed under `integration/<backend>/`.

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

# implementations/test_rate_limiter_impl.py -- generic backend
class TestRateLimiterImplementation(RateLimiterContractTest):
    """Inherits all contract tests + adds generic implementation tests."""
    pass

# implementations/threadpool/test_contracts.py -- real backend
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

Integration tests verify the end-to-end behavior of the rate limiter with real Redis and Lua scripts.

**Benefits:**

- The full system is tested, including Redis interactions and Lua script execution.
- Rate limiting behavior is verified in realistic scenarios.
- Burst handling, concurrency limits and telemetry tracking are tested.
- Explicit fixture configuration is used to maintain test independence.

**Example:**
```python
# integration/test_rate_limiting.py
@pytest.fixture
def integration_limiter(redis_client, default_limiter_id):
    """Create a backend-agnostic limiter for integration tests."""
    return MinimalRateLimiter(
        redis_client=redis_client,
        limiter_id=f"{default_limiter_id}_integration_default",
        limit=5,
        window=60,
        max_concurrency=2,
        max_age=3600,
        lease_duration=30,
    )


def test_basic_rate_limit_enforcement(self, integration_limiter):
    """Verify rate limiter enforces the configured limit."""
    # Arrange
    for i in range(10):
        integration_limiter.schedule_task("path", {"index": i})

    # Act
    consumed = sum(1 for _ in range(10) if integration_limiter.consume()["success"])

    # Assert
    assert consumed == 5, "consumed count should equal the configured limit"
```

### 5. Arrange-Act-Assert Pattern

All tests follow the AAA pattern for clarity:

```python
def test_example(self, limiter, redis_client):
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
pytest tests/implementations/ --ignore=tests/implementations/celery --ignore=tests/implementations/threadpool

# Celery-specific implementation tests only.
pytest tests/implementations/celery/

# ThreadPool-specific implementation tests only.
pytest tests/implementations/threadpool/

# Property-based tests only.
pytest tests/properties/

# Algorithm/spec tests only.
pytest tests/algorithms/

# Integration tests only.
pytest tests/integration/
```

### Run with Coverage
```bash
pytest tests/ --cov=celery_rate_limiter --cov-report=html
```

### Run Property Tests with More Examples
```bash
# Default: 50 examples per property.
pytest tests/properties/

# More thorough: 200 examples.
pytest tests/properties/ --hypothesis-max-examples=200
```

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

### Step 3: Add Contract-Inheriting Tests
```python
# tests/implementations/mybackend/test_mybackend_limiter.py
import pytest
from your_module import MyBackendRateLimiter
from tests.contracts.test_rate_limiter import RateLimiterContractTest


class TestMyBackendRateLimiter(RateLimiterContractTest):
    """Test my backend limiter implementation.

    Inherits all contract tests automatically.
    """

    @pytest.fixture
    def limiter(self, default_limiter_id):
        return MyBackendRateLimiter(
            limiter_id=f"{default_limiter_id}_mybackend_default",
            limit=100,
            window=60,
            max_concurrency=10,
        )
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
def test_schedule_duplicate_task_returns_false(self, limiter):
    """Contract: scheduling identical tasks must return False on duplicate."""

@given(payload=json_value)
def test_payload_survives_round_trip(self, payload):
    """Property: any JSON-serializable payload survives Redis round-trip."""

def test_lua_script_recovery_on_noscript_error(self, limiter):
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

## Fixtures

### Session-Scoped Fixtures

Used for expensive, one-time setup:

- `_redis_connection`: a single Redis connection for the entire test suite (configurable via `REDIS_HOST`/`REDIS_PORT` environment variables).
- `func_path`: a fictional function path for test task scheduling.
- `default_payload`: the default payload `{"user_id": 123}` for tests.

### Function-Scoped Fixtures (Default)

Used for most tests to ensure a clean state between tests:

- `redis_client`: wraps `_redis_connection` with `flushall()` before and after each test.
- `default_limiter_id`: a unique limiter ID per test (UUID-backed).
- `default_lock_key`: a unique lock key per test for distributed lock tests.

### Core Implementation Fixtures

Defined in `implementations/conftest.py`:

- `generic_limiter`: a `MinimalRateLimiter` instance (no-op dispatch/schedule) for testing `AbstractDistributedRateLimiter` behavior.
- `tracking_limiter`: a `TrackingRateLimiter` instance that records `_dispatch_task()` and `_schedule_drain()` calls.
- `make_limiter_pool`: a factory fixture that creates N limiter instances sharing the same Redis-backed limiter ID.
- `task_id`: a unique task ID string for testing.

### Backend Fixture Modules

Backend fixtures are centralized in `tests/fixtures/` and imported where needed:

- `tests/fixtures/celery_backend.py` defines Celery fixtures (`celery_app`, `celery_config`, `limiter`, class-state reset fixture).
- `tests/fixtures/threadpool_backend.py` defines ThreadPool fixtures (`executor`, `limiter`, class-state reset fixture).
- Backend-local conftests (e.g., `tests/implementations/celery/conftest.py`) import from the corresponding fixture module.

This avoids leaking backend fixtures into unrelated test categories.

### Module-Scoped Fixtures

Used in property tests to improve performance:

- `default_module_limiter_id`: a unique limiter ID per module.
- `property_redis_client`: a shared Redis client for a module's Hypothesis runs.
- `property_limiter`: a shared limiter for a module's Hypothesis runs.

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
def test_various_payloads(self, limiter, payload):
    pass
```

### 3. Mock External Dependencies

`unittest.mock` should be used for backend-specific behavior where full worker execution is not required.

### 4. Clean Up Resources

Fixtures with proper teardown or context managers should be used to ensure that resources are cleaned up even if tests fail.

### 5. Use Unique IDs in Fixtures

Fixture-provided unique IDs should be preferred over hardcoded IDs for limiter names, lock keys and task IDs.

```python
def test_example(default_limiter_id, default_lock_key):
    limiter_id = f"{default_limiter_id}_example"
    lock_key = default_lock_key
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
cd /path/to/celery-rate-limiter
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
