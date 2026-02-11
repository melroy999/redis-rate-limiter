# Test Suite Documentation

This directory contains a comprehensive test suite for the celery-rate-limiter project, organized using **contract-based**, **property-based**, **algorithm/spec**, and **integration** testing patterns.

All test categories assume a real Redis instance is available, because the core limiter logic is implemented in Redis Lua scripts.

## Directory Structure

```
tests/
├── contracts/                          # Abstract interface contracts
│   ├── test_rate_limiter.py            # Tests any limiter must satisfy
│   ├── test_distributed_lock.py        # Tests any lock must satisfy
│   └── test_task_lifecycle.py          # Tests any lifecycle manager must satisfy
│
├── implementations/                    # Implementation-specific tests
│   ├── conftest.py                     # Shared fixtures for implementation tests
│   ├── test_distributed_lock.py        # Redis-backed lock implementation tests
│   ├── test_drain.py                   # Drain and trigger_consume branch tests
│   ├── test_get_status.py             # Status reporting tests
│   ├── test_internal_helpers.py        # Lua script loading and helpers
│   ├── test_task_lifecycle.py          # Lifecycle manager implementation tests
│   ├── test_smart_jitter.py            # Adaptive jitter calculation tests
│   ├── test_metrics_callback.py        # Metrics callback observability tests
│   ├── test_rate_limiter_class_api.py  # Class-level API (configure/create/get/update)
│   ├── test_concurrent_access.py       # Multi-worker contention and atomicity tests
│   └── celery/                         # Celery implementation (uses Redis + Lua)
│       ├── test_celery_limiter.py      # Inherits contract + adds Celery tests
│       ├── test_decorator.py           # Rate-limited decorator tests
│       └── test_tasks.py              # Celery task helper tests
│
├── algorithms/                         # Pure algorithm/spec tests (no backend)
│   ├── sliding_window_counter.py       # Shared pure algorithm used by tests
│   └── test_sliding_window_counter.py  # Deterministic algorithm/spec tests
│
├── properties/                         # Property-based tests (Hypothesis)
│   ├── conftest.py                     # Shared property-test fixtures
│   ├── test_concurrency_invariants.py  # Concurrency bound invariants
│   ├── test_serialization.py           # Payload serialization properties
│   ├── test_is_subset.py               # Mathematical subset properties
│   ├── test_sliding_window_counter.py  # Sliding window invariants (Hypothesis)
│   └── test_smart_jitter.py            # Smart jitter invariants (Hypothesis)
│
├── integration/                        # End-to-end integration tests
│   ├── test_rate_limiting.py           # Rate limiting behavior verification
│   └── README.md                       # Platform requirements and timing notes
│
├── helpers/                            # Shared test utilities and strategies
│   ├── utils.py                        # Subset checker and approximate equality
│   └── strategies.py                   # Shared Hypothesis strategies
│
├── conftest.py                         # Pytest fixtures and configuration
└── README.md                           # This file
```

## Quick Placement Rules

1. Contract tests define interface guarantees and live in `contracts/`.
1. Implementation tests verify backend-specific behavior and live in `implementations/<backend>/` (or directly in `implementations/` when shared).
1. Algorithm/spec tests validate pure logic with no Redis/Celery dependency in the test code and live in `algorithms/`.
1. Property-based tests use Hypothesis to check invariants and live in `properties/`.
1. Integration tests verify end-to-end behavior with real Redis/Lua/Celery wiring and live in `integration/`.

## Testing Philosophy

### 1. Contract-Based Testing

Contract tests define the **expected behavior** for interfaces, ensuring all implementations satisfy the same requirements.

**Benefits:**
- New implementations automatically inherit all contract tests
- Ensures consistency across different limiter implementations
- Documents the required interface behavior
- Makes it easy to verify Liskov Substitution Principle

**Example:**
```python
# contracts/test_rate_limiter.py
class RateLimiterContractTest:
    """Abstract test suite that any RateLimiter must pass."""

    @staticmethod
    def test_schedule_task_returns_success_and_task_id(limiter, redis_client):
        """Contract: schedule_task must return (bool, str) tuple."""
        success, task_id = limiter.schedule_task("path", {})
        assert isinstance(success, bool)
        assert isinstance(task_id, str)

# implementations/celery/test_celery_limiter.py
class TestCeleryRateLimiter(RateLimiterContractTest):
    """Inherits all contract tests + adds Celery-specific tests."""
    pass  # Automatically runs all contract tests!
```

### 2. Property-Based Testing

Property-based tests use [Hypothesis](https://hypothesis.readthedocs.io/) to automatically generate hundreds of test cases, verifying invariants hold for **any** input.

**Benefits:**
- Discovers edge cases you wouldn't think to test manually
- Tests mathematical properties (reflexivity, transitivity, etc.)
- Provides stronger guarantees than example-based tests
- Automatically shrinks failing cases to minimal examples

**Example:**

```python
from hypothesis import given
from tests.helpers.strategies import json_value


@given(payload=json_value)  # Generates arbitrary JSON structures
def test_json_payload_survives_redis_round_trip(self, limiter, redis_client, payload):
    """Property: ANY JSON payload survives Redis round-trip."""
    success, task_id = limiter.schedule_task("path", payload)
    # Verify payload was preserved...
```

### 3. Algorithm/Spec Testing

Algorithm/spec tests validate pure logic extracted from Redis/Lua behavior, without any backend dependencies.

**Benefits:**
- Verifies core math and edge cases deterministically
- Keeps tricky logic testable without Redis time control
- Acts as a spec for the Lua implementation

**Example:**

```python
from tests.algorithms.sliding_window_counter import sliding_window_estimate


def test_weight_is_half_at_midpoint():
    assert sliding_window_estimate(10, 0, 1000, 500) == 5.0
```

### 4. Integration Testing

Integration tests verify end-to-end behavior of the rate limiter with real Redis and Lua scripts.

**Benefits:**
- Tests the full system including Redis interactions and Lua script execution
- Verifies rate limiting behavior in realistic scenarios
- Tests burst handling, concurrency limits, and telemetry tracking
- Uses explicit fixture configuration to maintain test independence

**Example:**
```python
# integration/celery/test_rate_limiting.py
@pytest.fixture
def integration_limiter(redis_client, celery_app, default_limiter_id):
    """Create a limiter with explicit configuration for integration tests."""
    return CeleryRateLimiter.create(
        limiter_id=f"{default_limiter_id}_integration_default",
        limit=5,
        window=60,
        max_concurrency=2,
        max_age=3600,
        lease_duration=30,
        override=True,
    )

def test_basic_rate_limit_enforcement(self, integration_limiter, redis_client):
    """Verify rate limiter enforces the configured limit."""
    # Schedule 10 tasks, consume up to limit (5), verify remaining queued
    for i in range(10):
        integration_limiter.schedule_task("path", {"index": i})

    consumed = sum(1 for _ in range(10) if integration_limiter.consume()["success"])
    assert consumed == 5  # Only 5 consumed due to rate limit
```

### 5. Arrange-Act-Assert Pattern

All tests follow the AAA pattern for clarity:

```python
def test_example(self, limiter, redis_client):
    """Clear description of what this test verifies."""
    # Arrange
    # Setup test data and preconditions.
    payload = {"user_id": 123}

    # Act
    # Execute the operation being tested.
    success, task_id = limiter.schedule_task("path", payload)

    # Assert
    # Verify the expected outcome.
    assert success is True, "task should be scheduled successfully"
```

## Running Tests

All test runs expect a real Redis instance to be available. The Redis connection is configurable via environment variables:
- `REDIS_HOST` (default: `localhost`)
- `REDIS_PORT` (default: `6379`)

### Run All Tests (excluding slow tests)
```bash
pytest tests/
```

### Run Slow Tests
Slow tests use real `time.sleep()` calls and run parameterized configurations.
They are excluded by default for faster local development.
```bash
# Run only slow tests
pytest -m slow tests/

# Run ALL tests including slow tests (as in CI)
pytest --override-ini='addopts=' tests/
```

### Run via Docker (recommended for integration tests)
Sliding window timing tests are skipped on Windows. Use Docker for reliable results:
```bash
docker compose --profile test up       # fast tests only
docker compose --profile test-all up   # includes @pytest.mark.slow
```

### Run Specific Test Categories
```bash
# Contract tests only
pytest tests/contracts/

# Implementation tests only
pytest tests/implementations/

# Property-based tests only
pytest tests/properties/

# Algorithm/spec tests only
pytest tests/algorithms/

# Integration tests only
pytest tests/integration/

# Specific implementation
pytest tests/implementations/celery/
```

### Run with Coverage
```bash
pytest tests/ --cov=celery_rate_limiter --cov-report=html
```

### Run Property Tests with More Examples
```bash
# Default: 50 examples per property
pytest tests/properties/

# More thorough: 200 examples
pytest tests/properties/ --hypothesis-max-examples=200
```

## Adding a New Limiter Implementation

When adding a new rate limiter implementation (e.g., in-memory, different backend), follow these steps:

### Step 1: Create Implementation Directory
```bash
mkdir -p tests/implementations/inmemory
touch tests/implementations/inmemory/__init__.py
```

### Step 2: Create Test File Inheriting from Contracts
```python
# tests/implementations/inmemory/test_inmemory_limiter.py
import pytest
from your_module import InMemoryRateLimiter
from tests.contracts.test_rate_limiter import RateLimiterContractTest

class TestInMemoryRateLimiter(RateLimiterContractTest):
    """Test in-memory limiter implementation.

    Inherits all contract tests automatically.
    """

    @pytest.fixture
    def limiter(self, default_limiter_id):
        """Provide the in-memory limiter instance."""
        return InMemoryRateLimiter(
            limiter_id=f"{default_limiter_id}_inmemory_default",
            limit=100,
            window=60,
            max_concurrency=10,
        )

    # Add implementation-specific tests
    def test_inmemory_specific_behavior(self, limiter):
        """Test something specific to the in-memory implementation."""
        # ...
```

### Step 3: Run Tests
```bash
pytest tests/implementations/inmemory/
```

All contract tests will run automatically against your new implementation!

## Shared Resources

### helpers/utils.py
Contains shared utility functions used across multiple tests:
- `is_subset(target, superset)`: Recursive dictionary subset checker
- `dict_equals_approx(left, right)`: Approximate equality for nested structures, with configurable tolerance for float comparisons

**Usage:**

```python
from tests.helpers.utils import is_subset, dict_equals_approx

assert is_subset({"a": 1}, {"a": 1, "b": 2})  # True
assert dict_equals_approx({"x": 1.0000001}, {"x": 1.0})  # True
```

### helpers/strategies.py
Contains shared Hypothesis strategies for generating test data:
- `json_value`: Generates arbitrary JSON-serializable values (recursive structure of primitives, lists, and dicts)
- `nested_dict`: Generates arbitrary nested dictionaries (filtered from `json_value`)

**Usage:**

```python
from hypothesis import given
from tests.helpers.strategies import json_value, nested_dict


@given(payload=json_value)
def test_something(self, payload):
    # Test with arbitrary JSON value
    pass
```

## Test Naming Conventions

### Test Class Names
- **Contract classes**: `{Component}ContractTest` (e.g., `RateLimiterContractTest`)
- **Implementation classes**: `Test{Implementation}{Component}` (e.g., `TestCeleryRateLimiter`)

### Test Method Names
- Use descriptive names that read like sentences
- Start with `test_`
- Include what is being tested and expected outcome

**Good examples:**
- `test_schedule_task_returns_success_and_task_id`
- `test_lock_releases_on_exception`
- `test_payload_survives_redis_round_trip`

**Poor examples:**
- `test_schedule` (too vague)
- `test_1` (meaningless)
- `test_lock` (what about the lock?)

### Docstrings
- Contract tests: Start with "Contract: " to clarify the requirement
- Property tests: Start with "Property: " to clarify the invariant
- Implementation tests: Describe the specific behavior being tested

**Examples:**
```python
def test_schedule_duplicate_task_returns_false(self, limiter):
    """Contract: scheduling identical tasks must return False on duplicate."""

@given(payload=json_value)
def test_payload_survives_round_trip(self, payload):
    """Property: any JSON-serializable payload survives Redis round-trip."""

def test_lua_script_recovery_on_noscript_error(self, limiter):
    """Verify limiter recovers from NoScriptError by reloading Lua script."""
```

## Assertion Style

### Use Lowercase for Messages
```python
# Good
assert success is True, "task should be scheduled successfully"

# Bad
assert success is True, "Task should be scheduled successfully"
```

### Provide Context in Failure Messages
```python
# Good
assert len(results) > 0, f"task with ID {task_id} not found in buffer"

# Bad
assert len(results) > 0
```

### Comments Before Lines, Not After
```python
# Good
# Clean state for each example.
redis_client.flushdb()

# Bad
redis_client.flushdb()  # Clean state
```

## Fixtures

### Session-Scoped Fixtures
Used for expensive, one-time setup:
- `_redis_connection`: Single Redis connection for the entire test suite (configurable via `REDIS_HOST`/`REDIS_PORT` environment variables)
- `celery_app`: Celery application instance (provided by `celery.contrib.pytest`)
- `celery_config`: Celery configuration with Redis broker
- `func_path`: Fictional function path for test task scheduling
- `default_payload`: Default payload `{"user_id": 123}` for tests

### Function-Scoped Fixtures (Default)
Used for most tests. Clean state between tests:
- `redis_client`: Wraps `_redis_connection` with `flushall()` before and after each test
- `limiter`: Fresh `CeleryRateLimiter` instance created via the class API (`CeleryRateLimiter.create(...)`)
- `default_limiter_id`: Unique limiter ID per test (UUID-backed) for fixtures/tests that need a limiter name
- `default_lock_key`: Unique lock key per test for distributed lock tests
- `_reset_limiter_class_state` (autouse): Resets class-level singleton cache and calls `configure()` with test fixtures before each test, preventing state pollution between tests

### Implementation-Scoped Fixtures
Defined in `implementations/conftest.py`. Provide non-Celery concrete implementations for testing abstract behavior:
- `generic_limiter`: `MinimalRateLimiter` instance (no-op dispatch/schedule) for testing `AbstractDistributedRateLimiter` behavior
- `tracking_limiter`: `TrackingRateLimiter` instance that records `_dispatch_task()` and `_schedule_drain()` calls, used by drain branch-coverage tests
- `make_limiter_pool`: Factory fixture that creates N limiter instances sharing the same Redis-backed limiter ID, used by concurrent-access contention tests
- `task_id`: Unique task ID string for testing

### Module-Scoped Fixtures
Used for property-based tests to improve performance:
- `default_module_limiter_id`: Unique limiter ID per module for module-scoped limiter fixtures
- `property_redis_client`: Shared Redis client for Hypothesis tests
- `property_limiter`: Shared limiter for Hypothesis tests

## Best Practices

### 1. Keep Tests Focused
Each test should verify **one specific behavior**. If you need multiple assertions, they should all relate to the same behavior.

### 2. Use Parametrize for Variations
```python
@pytest.mark.parametrize(
    "payload",
    [{"a": 1}, {}, {"nested": {"b": 2}}],
    ids=["simple", "empty", "nested"],
)
def test_various_payloads(self, limiter, payload):
    # Test runs 3 times with different payloads
    pass
```

### 3. Mock External Dependencies
Use `unittest.mock` for Celery-specific behavior to avoid needing full Celery workers in tests.

### 4. Clean Up Resources
Use fixtures with proper teardown or context managers to ensure resources are cleaned up even if tests fail.

### 5. Use Unique IDs in Fixtures
Prefer fixture-provided unique IDs over hardcoded IDs for limiter names, lock keys, and task IDs.
This reduces accidental coupling and keeps tests robust if fixture scope or cleanup behavior changes.

```python
def test_example(default_limiter_id, default_lock_key):
    limiter_id = f"{default_limiter_id}_example"
    lock_key = default_lock_key
```

Use explicit hardcoded IDs only when the test is specifically about ID identity or readability of a known failure case.

## Troubleshooting

### Tests Fail Due to Redis Connection
Ensure Redis is running:
```bash
redis-cli ping  # Should return "PONG"
```

### Hypothesis Tests Are Slow
Reduce the number of examples for faster iteration:
```bash
pytest tests/properties/ --hypothesis-max-examples=10
```

### Import Errors
Ensure you're running pytest from the project root:
```bash
cd /path/to/celery-rate-limiter
pytest tests/
```

## Contributing

When adding new tests:
1. Follow the existing structure (contracts, implementations, algorithms, properties, integration)
2. Use the AAA pattern (Arrange-Act-Assert)
3. Add descriptive docstrings
4. Follow naming conventions
5. Update this README if adding new patterns or conventions

## Resources

- [Pytest Documentation](https://docs.pytest.org/)
- [Hypothesis Documentation](https://hypothesis.readthedocs.io/)
- [Contract Testing Explained](https://martinfowler.com/bliki/ContractTest.html)
- [Property-Based Testing](https://increment.com/testing/in-praise-of-property-based-testing/)
