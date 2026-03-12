# Testing Guidelines

This document defines the rules that all tests in this project must follow. It serves as the single source of truth for test authoring conventions, assertion patterns, documentation requirements, and mutation testing practices. Any test that does not comply with these guidelines should be updated at the next opportunity.

The conventions described here are prescriptive, not aspirational. Existing tests that predate this document may not yet comply; a separate migration effort will bring them into alignment. New tests must follow these guidelines from the outset.

## 1. Test Organization

### 1.1 File Placement

Test files belong in the directory that matches their scope and subject.

| Scope | Directory | Example |
|---|---|---|
| Abstract contracts (any backend must pass) | `tests/contracts/` | `test_rate_limiter.py`, `test_distributed_lock.py` |
| Backend-agnostic implementation | `tests/implementations/` | `test_rate_limiter.py`, `test_drain.py` |
| Backend-specific implementation | `tests/implementations/<backend>/` | `celery/test_celery_limiter.py` |
| Pure algorithm verification | `tests/algorithms/` | `test_sliding_window_counter.py` |
| Property-based (Hypothesis) | `tests/properties/` | `test_sliding_window_counter.py` |
| End-to-end timing | `tests/integration/` | `test_rate_limiting.py` |
| Lua script unit tests | `tests/lua/` | `test_consume.py`, `test_acquire.py` |
| Third-party integrations | `tests/integrations/` | `test_prometheus.py` |
| Shared test utilities | `tests/helpers/` | `adapters.py`, `strategies.py`, `utils.py` |
| Backend fixture modules | `tests/fixtures/` | `celery_backend.py`, `threadpool_backend.py` |

When adding a new backend, create the corresponding subdirectory under `tests/implementations/` and a fixture module under `tests/fixtures/`.

### 1.2 Class Naming Conventions

| Pattern | Purpose | Example |
|---|---|---|
| `<Feature>ContractTest` | Abstract contract base; pytest does not collect it because the name lacks a `Test` prefix | `RateLimiterContractTest`, `DistributedLockContractTest` |
| `<Feature>Tests` | Unified mixin base for sync/async deduplication; pytest does not collect it | `DrainBehaviorTests`, `GetStatusTests`, `MetricsCallbackTests` |
| `Test<Subject>` | Concrete test class that pytest collects and executes | `TestSyncDrain`, `TestAsyncDrainLoop`, `TestCeleryRateLimiter` |

The absence of the `Test` prefix on abstract bases and mixins is deliberate. It prevents pytest from attempting to instantiate classes that require subclass-provided fixtures.

### 1.3 Method Naming

All test methods follow the pattern `test_<subject>_<expected_behavior>`. The name must be descriptive enough to convey the assertion without reading the method body.

- **Correct**: `test_schedule_single_task_stores_correctly`, `test_lock_expires_after_timeout`, `test_drain_defers_when_paused`
- **Incorrect**: `test_schedule_1`, `test_lock_works`, `test_bug_fix_123`

### 1.4 The `@staticmethod` Rule

All test methods must be decorated with `@staticmethod` unless the method requires access to `self` for class-level customization points. This convention makes it explicit that the test does not depend on instance state, which is important for mixin-based test classes where the `self` parameter could be misleading.

The primary exception is mixin bases that define class-level helpers; for instance, `DrainBehaviorTests` uses `self.lock_result(acquired)` to produce a sync or async context manager depending on the subclass. In such cases, omitting `@staticmethod` is correct.

**Hypothesis `@given` tests**: the `@staticmethod` rule applies equally to `@given`-decorated tests. Hypothesis correctly handles `@staticmethod @given(...)` with both pure parameters (all supplied by `@given`) and mixed parameters (some from `@given`, some from pytest fixtures). The decorator order must be `@staticmethod` outermost, then `@given(...)`, then `@settings(...)` if present.

### 1.5 Sync/Async Deduplication: Mixins vs Separate Files

The project maintains both sync and async implementations of core components. The following rules determine how tests for these implementations are organized.

**Use the mixin pattern** for business logic tests where the only difference between sync and async is the Redis call layer. Tests are written once in async form; the sync implementation participates via `SyncToAsyncLimiterAdapter` from `tests/helpers/adapters.py`. Two concrete subclasses are created: one wrapping the sync limiter in the adapter, one running the async limiter natively.

Examples of mixin-appropriate tests: `schedule_task`, `consume`, `get_status`, token recovery delay, metrics callbacks, smart jitter, distributed lock semantics.

**Use separate files** for concurrency infrastructure tests where the test setup itself requires fundamentally different primitives. The sync drain loop uses `threading.Event`, `threading.Condition`, and `time.sleep()`; the async drain loop uses `asyncio.Event`, `asyncio.Condition`, and `asyncio.sleep()`. These differences are not bridgeable by the adapter; the test infrastructure is structurally different.

Examples of separate-file tests: `test_drain_loop.py` / `test_async_drain_loop.py`, `test_task_lifecycle.py` / `test_async_task_lifecycle.py`.

**Rule for the boundary**: if a test uses `threading.*`, `time.sleep()`, or `asyncio.Task`/`asyncio.Event` as part of its Arrange or Act sections (not just Assert), it belongs in a separate file rather than a mixin.

### 1.6 Section Separators

Use a three-line block to separate major sections within a test file:

```python
# ---------------------------------------------------------------------------
# Section title
# ---------------------------------------------------------------------------
```

The separator line is exactly 75 dashes. Do not use separators between individual test methods within a single class.

**When to use separators**: a separator is required at each boundary between groups of classes or functions that serve structurally different roles. The following transitions warrant a separator:

| Transition | Example title |
|---|---|
| Module-level helpers before test classes | `Helpers` |
| Mixin base classes before concrete subclasses | `Unified implementation tests` / `Concrete test cases` |
| Behavioral mixin before observability mixin | `Unified observability tests` |
| Behavioral concrete class before observability concrete class (non-mixin files) | `Observability tests` |
| Distinct categories of test classes in the same file | `Drain-disabled tests`, `Cross-process drain signal tests` |
| Sync-specific tests before async-specific tests (when in one file) | `Sync-specific tests` / `Async-specific tests` |
| Signature or class-variable tests at end of file | `Signature tests` or `Class variable tests` |

A file with only one class and no helpers does not need any separators.

## 2. Documentation

### 2.1 Module Docstrings

Every test file must have a module-level docstring that states:

1. **What is tested**: the subject under test and the testing scope.
2. **How deduplication works** *(if applicable)*: whether the file uses the mixin pattern, adapter pattern, or is a standalone sync/async file.
3. **Fixture dependencies**: any non-obvious fixtures that must be provided by conftest files.

### 2.2 Class Docstrings

Every test class must have a docstring. Mixin bases must document the fixtures and customization points that subclasses are required to provide. Concrete classes may use a one-liner if the class name and module docstring already convey the context.

### 2.3 Method Docstrings

Every test method must have a docstring. The docstring is a single sentence that begins with a verb and describes what the test verifies, not how it works.

- **Contract tests** use the prefix `Contract:` to distinguish interface guarantees from implementation tests.
- **Algorithm property tests** use the prefix `Property:` to distinguish mathematical invariants verified by the test from behavioral assertions.
- **Mutation-targeted tests** include a `Mutation target:` annotation (see Section 6.1). This annotation is reserved for tests that exist primarily to document API contracts (e.g., signature defaults, class-variable constants) rather than to verify behavioral correctness.

Examples:
- `"""Verify that ``shutdown()`` sets the ``_shutdown`` flag to exactly ``True``."""`
- `"""Contract: ``schedule_task()`` must return a ``(bool, str)`` tuple."""`
- `"""Property: the 2x burst bound holds for various limit and window configurations."""`
- `"""Verify that the ``delay`` parameter of ``wake()`` defaults to ``0.0``.\n\nMutation target: default value of ``delay`` in ``DrainLoop.wake()``."""`

### 2.4 AAA Section Comments

Tests must use `# Arrange`, `# Act`, and `# Assert` as section comments on their own lines. A blank line must precede each section comment.

**Rules for combining sections**:
- `# Act & Assert` is permitted only when the action and assertion are inseparable, such as assertion inside an `async with` block or an iterative loop that both performs and verifies. It is not permitted for simple sequential flows.
- `# Arrange & Act` may be combined when a parametrized arrangement naturally flows into the action with no meaningful boundary.
- `# Arrange` may be omitted when the test has no setup (e.g., calling a pure function with literal arguments).
- Never combine all three into one section.

**Follow-up context**: additional explanatory comments go on new lines below the section header, never on the same line as the section header.

### 2.5 When to Comment and When Not To

Comments explain *why*, not *what*. Do not write inline comments that restate what the code does.

- **Good**: `# weight = (1000 - 0) / 1000 = 1.0` (explains the expected calculation for a reader verifying correctness)
- **Good**: `# Identity check catches mutations to None and False.` (explains why `is True` is used instead of a truthiness check)
- **Bad**: `# Call schedule_task with func_path and payload` (restates the next line of code)

Formula comments in algorithm tests are encouraged because they document the expected arithmetic and allow a reviewer to verify correctness without running the test.

### 2.6 Prose Conventions

All prose in the test suite, including docstrings, assertion messages, and inline comments, must follow the project writing conventions:

- **No contractions**: use "do not" instead of "don't", "it is" instead of "it's", "cannot" instead of "can't".
- **No em dashes**: no `—` or ` -- ` (spaced). Use commas, semicolons, colons, "i.e.", "e.g.", or subordinate clauses instead.
- **Formal connectives** where appropriate: "as such", "hence", "moreover", "furthermore", "given that", "such that".
- **Complete sentences with articles**: every docstring and comment is a grammatically complete sentence.

## 3. Assertion Priorities and Targets

### 3.1 Assertion Hierarchy

Assertions within a test method must be ordered according to the following priority. This is an ordering rule, not an importance rule; all five categories are mandatory when applicable.

| Priority | Category | What it proves | Example |
|---|---|---|---|
| 1 | Observable state | The system produced the correct external state | `assert await redis.exists(key) == 1` |
| 2 | Return value / exception | The API communicated the correct result | `assert result["success"] is True` |
| 3 | Side-effect verification | The correct collaborators were invoked | `mock.assert_called_once_with(...)` |
| 4 | Observability | The system reported what it did via logs | `assert any(record... for record in caplog.records)` |
| 5 | Implementation detail | An internal formula or constant is correct | `assert delay == pytest.approx(0.1)` |

Higher-priority assertions appear first in the Assert section because their failure messages are more diagnostic. Observability assertions (priority 4) are as important as state assertions; they simply appear later because a behavioral failure provides a more immediate signal of what went wrong.

**Important**: observability assertions (priority 4) must exist in their own test methods, not in the same method as priority 1-3 assertions. See Section 4 for the separation of concerns rule.

### 3.2 Assertion Messages

Every `assert` statement must include a custom failure message. The message must be lowercase, must not end with punctuation, and must describe what was expected.

- **Good**: `"buffer should contain exactly one task"`
- **Good**: `f"remaining_tokens should equal limit, got {result['remaining_tokens']}"`
- **Acceptable**: `"remaining_tokens should equal the configured limit"`
- **Bad**: `"wrong value"`, `"test failed"`, or no message at all

Including the actual value in the failure message is recommended for assertions where the expected vs actual comparison is not immediately obvious from context.

### 3.3 When Assertions Are Superfluous

Do not assert on values that are already guaranteed by the type system, framework, or a preceding assertion:

- Do not assert that a fixture return value is not `None` when the fixture always returns a value.
- Do not assert `isinstance(x, str)` when a subsequent assertion on `x` (e.g., `assert x.startswith(...)`) already implies the type.
- Do not assert `mock.call_count == 1` when `mock.assert_called_once_with(...)` already enforces that the call count is exactly one.

### 3.4 Blind Spots to Watch For

- **Index swap mutations**: when parsing a list or tuple result (e.g., Lua return values), use distinct sentinel values per index to verify that each index maps to the correct result field. The existing `test_consume_result_index_mapping_is_correct` in `tests/implementations/test_rate_limiter.py` is the reference pattern.
- **Boundary conditions**: always test the boundary, not just the happy path. If a value should be `>= 0`, also test at exactly `0`. If a formula has a floor (e.g., `max(delay, 0.001)`), test the input that produces exactly the floor value.
- **Timing races**: in concurrency tests, assert state after synchronization (`Event.wait`, `asyncio.wait_for`), not after a blind `time.sleep()`. Synchronization primitives provide deterministic proof that the background work completed.
- **Floating-point comparisons**: always use `pytest.approx()` for floating-point assertions; never use `==` directly.

## 4. Separation of Concerns

### 4.1 The Rule

Behavioral assertions (priority 1, 2, 3) and observability assertions (priority 4) must be in **separate test methods**. A single test method must not mix behavioral verification with log assertion verification.

**Rationale**: a log format change should break an observability test, not a behavioral test. Mixed tests produce ambiguous failure signals: did the behavior break, or did the log message format change? Separating them ensures that each test failure points to a specific concern.

### 4.2 Naming Convention for Observability Tests

Observability tests (those asserting on log messages) use one of the following naming patterns:

- `test_<action>_emits_<level>_log`: when verifying a specific log level for a specific action.
- `test_<action>_emits_expected_logs`: when verifying multiple log emissions for a single action.

### 4.3 When Combining Is Acceptable

Combined behavioral and observability assertions are permitted only when the log emission IS the behavior being tested. This applies when:

1. The method under test is designed to suppress exceptions and log them instead. The log is the only observable proof that the exception was caught and handled, not silently swallowed.
2. The method under test has no other observable side effect; the log is the entire behavioral contract of that code path.

Examples:
- `test_cleanup_inflight_key_suppresses_redis_failure`: the method must not propagate the Redis exception, and the warning log is the only proof of suppression.
- `test_drain_loop_survives_drain_exception`: the loop must continue running after an exception, and the error log is the only proof that the exception was handled.

### 4.4 Log Assertions Are Part of the Observability Contract

Log messages exist for debugging and operational monitoring. If a value is supposed to appear in a log message, a test must verify that it is present. This is not overhead introduced by mutation testing; it is a deliberate correctness requirement.

Removing a parameter from a log call is a real degradation of observability. Operators rely on structured log fields (`limiter=%s`, `task_id=%s`, `removed=%s`) for querying and alerting. As such, every structured parameter in a log message must be verified by an observability test.

The separation rule still applies: log assertions belong in their own test methods. But they must exist, and they must be thorough.

### 4.5 Mutmut Impact

Separating log assertions into their own test methods does not reduce mutation coverage. The observability tests still kill the same mutations that the tacked-on assertions killed. What changes is that behavioral tests no longer carry log-assertion baggage, and each test has a single, clear responsibility.

## 5. Tools and Patterns

### 5.1 Mocking Rules

| Tool | When to use | When not to use |
|---|---|---|
| **Real Redis** | Always for state verification in contract and implementation tests | Never mock Redis for these test categories |
| **`MagicMock`** | For component interfaces consumed by the system under test (e.g., the mock limiter in lifecycle tests) | Not for the system under test itself |
| **`AsyncMock`** | For methods that the system under test awaits | Not for sync methods, even in an async test context |
| **`patch.object`** | For intercepting specific method calls (`evalsha`, `send_task`, `script_load`) | Not for replacing entire subsystems |
| **`wraps=`** | When the test needs to spy on a real method while still executing the original behavior | When the real behavior has side effects that the test must prevent |

**Determining mock class in unified tests**: when a mixin test needs to mock a method that may be sync or async depending on the concrete subclass, use the following pattern from `tests/implementations/test_rate_limiter.py`:

```python
actual_limiter = getattr(limiter, "_inner", limiter)
mock_cls = (
    AsyncMock
    if inspect.iscoroutinefunction(actual_limiter._eval_script)
    else MagicMock
)
```

**Data-flow verification via mocks**: tests that mock internal method calls and assert on the arguments forwarded to those methods are a valid and important category. They verify that the correct data flows through the system: if the arguments passed to an internal method are swapped (e.g., `task_id` and `func_path`), these tests catch it. Data-flow verification tests are normal behavioral tests; they do not require a `Mutation target:` annotation because they guard against real bugs, not merely theoretical mutations.

```python
def test_execution_lock_forwards_all_attributes(limiter, ...):
    """Verify that ``execution_lock()`` forwards the correct lock key, worker ID, and contention key."""
```

Do not confuse data-flow verification with call-count verification. Asserting that a method was called with specific arguments is valuable; asserting only that a method was called a specific number of times (without verifying the arguments) is weaker and should be avoided unless the call count itself is the behavioral contract.

### 5.2 Fixture Conventions

| Fixture | Scope | Provider | Purpose |
|---|---|---|---|
| `redis_client` | function | `tests/conftest.py` | Sync Redis client with per-test `flushdb` |
| `async_redis_client` | function | `tests/conftest.py` | Async Redis client with per-test `flushdb` |
| `limiter_id` | function | `tests/conftest.py` | Unique `limiter_{test}_{uuid}` identifier |
| `module_limiter_id` | module | `tests/conftest.py` | Shared identifier within a module |
| `func_path` | session | `tests/conftest.py` | Static function path string |
| `payload` | session | `tests/conftest.py` | Static payload dictionary |
| `task_id` | function | `tests/implementations/conftest.py` | Unique `task_{uuid}` identifier |
| `stub_limiter` | function | `tests/implementations/conftest.py` | Sync `StubRateLimiter` with teardown |
| `async_stub_limiter` | function | `tests/implementations/conftest.py` | Async `AsyncStubRateLimiter` with teardown |
| `tracking_limiter` | function | `tests/implementations/conftest.py` | Sync limiter that records dispatch and schedule calls |

**Fixture rules**:
- **Teardown**: all limiter fixtures must call `shutdown()` in teardown to stop subscriber threads and tasks. For **function-scoped** fixtures that depend on `redis_client` or `async_redis_client`, explicit key cleanup is not required because those root fixtures call `flushdb()` before and after every test (see Section 7.5). **Module-scoped or session-scoped** fixtures (e.g., property test fixtures) must handle their own key cleanup, because the per-test `flushdb()` cycle does not apply at broader scopes.
- **Factory fixtures** (e.g., `make_limiter_pool`): must track all created objects in a list and clean up every object in teardown.
- **Async fixtures**: must call `await limiter.start()` during setup to initialize the drain signal subscriber.
- **Isolation**: never hardcode limiter IDs; always derive them from the `limiter_id` fixture with a disambiguation suffix (e.g., `f"{limiter_id}_generic"`).
- **Backend-specific fixtures**: should be placed in `tests/fixtures/<backend>_backend.py` and imported into `tests/implementations/<backend>/conftest.py`.

**Singleton class-state reset**: backend classes that use the managed limiter pattern (`CeleryRateLimiter`, `ThreadPoolRateLimiter`, `AsyncIOTaskLimiter`, `ASGIRateLimiter`) maintain class-level state: `_instances` (a dict caching limiter objects), `_redis_client`, and backend-specific context (e.g., `_celery_app`, `_executor`). This state persists across tests unless explicitly cleared. Every backend's fixture module must include an `autouse=True` fixture that resets class state before and after each test:

```python
@pytest.fixture(autouse=True)
def _reset_limiter_class_state(redis_client, celery_app):
    """Reset class-level state to prevent singleton cache pollution between tests."""
    CeleryRateLimiter._reset()
    CeleryRateLimiter.configure(redis_client, celery_app=celery_app)
    yield
    CeleryRateLimiter._reset()
```

This fixture must: (1) call `_reset()` before the test to clear any state left by a previous test, (2) call `configure()` with the test fixtures to establish a clean starting state, and (3) call `_reset()` after the test to prevent leakage into the next test. Omitting this fixture causes subtle, hard-to-diagnose test pollution: a later test may retrieve a stale limiter instance from the class cache that was configured with a different Redis client or limiter ID.

Current fixture module locations:
- [celery_backend.py](tests/fixtures/celery_backend.py): `_reset_limiter_class_state` for `CeleryRateLimiter`
- [threadpool_backend.py](tests/fixtures/threadpool_backend.py): `_reset_limiter_class_state` for `ThreadPoolRateLimiter`
- [asyncio/conftest.py](tests/implementations/asyncio/conftest.py): `_reset_managed_limiter_class_state` for `AsyncIOTaskLimiter`
- [asgi/conftest.py](tests/implementations/asgi/conftest.py): `_reset_asgi_limiter_class_state` for `ASGIRateLimiter`

### 5.3 Log Assertion Pattern

The project uses the following pattern for log assertions. All observability tests should follow this shape consistently:

```python
assert any(
    record.levelname == "<LEVEL>"
    and f"field_a={expected_a}" in record.message
    and f"field_b={expected_b}" in record.message
    for record in caplog.records
), "should emit a <level> log containing field_a and field_b"
```

**Structure**: the level check comes first, followed by all fragment checks joined with `and`. Each fragment verifies a specific structured parameter. Fragments must use the `key=value` format (e.g., `f"limiter={limiter.id}"`, `f"task_id={task_id}"`) rather than bare values (e.g., `limiter.id`). Bare values may match spuriously in unrelated parts of the log message; the key-value format ensures that the correct structured field is present.

**Standard helper** (implemented in ``tests/helpers/utils.py``):

```python
from tests.helpers.utils import assert_log_emitted

assert_log_emitted(
    caplog.records,
    level="INFO",
    required_fragments=[f"limiter={limiter.id}", f"task_id={task_id}"],
    message="should emit an info log for the dispatched task",
)
```

All new observability tests must use this helper. Existing tests may be migrated opportunistically.

**Logger-specific capture**: when testing logs from the rate limiter, use `caplog.at_level(logging.LEVEL, logger="redis_rate_limiter")` to filter out noise from third-party libraries. Always specify the logger name to ensure the test captures only relevant records.

**Negative log assertions**: to verify that a log is NOT emitted (e.g., verifying that a code path does not produce a spurious warning), assert the absence explicitly:

```python
assert not any(
    record.levelname == "WARNING" and "unexpected" in record.message
    for record in caplog.records
), "should not emit a warning log for this code path"
```

**Multiple log events in one test**: if a test needs to verify multiple sequential log emissions from the same action, use a single `caplog.at_level()` context manager and assert on each expected record. Do not call `caplog.clear()` between assertions unless the test performs multiple distinct actions.

### 5.4 Timeout Patterns

| Context | Pattern | Example |
|---|---|---|
| Async operations that should complete promptly | `asyncio.wait_for(coro, timeout=N)` | `await asyncio.wait_for(loop.shutdown(), timeout=1.0)` |
| Sync thread synchronization | `Event.wait(timeout=N)` | `fired = drain_called.wait(timeout=2.0)` |
| Safety net for mutation-induced infinite loops | `threading.Timer` | `Timer(0.5, lambda: setattr(subscriber, "_shutdown", True))` |

**Timer safety-net requirement**: every usage of the `threading.Timer` safety-net pattern must include a docstring or inline comment explaining which mutation it defends against. Without this documentation, the timer appears to be unnecessary complexity.

### 5.5 `time.sleep()` Rules

| Context | Acceptable? | Reason |
|---|---|---|
| Integration tests requiring timing precision | No; use `precise_sleep()` from `tests/integration/conftest.py` | Windows has ~15ms timer resolution; `time.sleep()` is unreliable for sub-second sleeps |
| Thread synchronization where no event hook exists | Yes | Heartbeat interval tests, subscriber processing delays |
| Mock side effects simulating blocking I/O | Yes | Pubsub mock `get_message` that sleeps to simulate socket blocking |
| Waiting for a condition to become true | Prefer `Event.wait(timeout=)` | Event-based synchronization is deterministic and faster than blind sleeping |

### 5.6 Parametrize

When using `@pytest.mark.parametrize`, always provide the `ids=` parameter for readable test output. Prefer class-level parametrize when all methods in a class need the same parameter set.

```python
@pytest.mark.parametrize(
    ("invalid_path", "expected_exception"),
    [
        ("", ValueError),
        ("no_dot_path", ValueError),
    ],
    ids=["empty_string", "no_dot"],
)
```

### 5.7 Exception Testing Patterns

Tests that verify error handling must use `pytest.raises` with the `match=` parameter to assert on both the exception type and the diagnostic content of the error message:

```python
with pytest.raises(RuntimeError, match="celery_app"):
    CeleryRateLimiter.configure(redis_client)
```

**Rules**:
- Always specify the most specific exception type. Do not catch `Exception` when `ValueError` is expected.
- Always include the `match=` parameter with a substring or regex pattern that identifies the error. This prevents the test from passing on an unrelated exception of the same type.
- Prefer substring matching over full message matching to avoid brittleness. Match on the key diagnostic term (e.g., `"celery_app"`, `"not found"`, `"already exists"`), not the entire sentence.
- When testing that an exception is NOT raised, do not use a try/except block; simply call the method and let pytest's default failure reporting handle unexpected exceptions.

### 5.8 Concurrency Testing Infrastructure

Tests that verify behavior under concurrent access use a synchronized start pattern to maximize contention. The `run_concurrently()` helper in `tests/implementations/test_concurrent_access.py` implements this pattern:

```python
def run_concurrently(fn, args_list):
    barrier = threading.Barrier(len(args_list))

    def _wrapped(args):
        barrier.wait()
        return fn(*args)

    with ThreadPoolExecutor(max_workers=len(args_list)) as pool:
        futures = [pool.submit(_wrapped, args) for args in args_list]
        for future in as_completed(futures):
            ...
```

**Key patterns**:
- **`threading.Barrier`**: ensures all threads begin their work at the same instant, maximizing contention probability on the Redis side.
- **`ThreadPoolExecutor` + `as_completed()`**: collects results and exceptions from all threads.
- **`ExceptionGroup`**: aggregates exceptions from concurrent workers so that no failure is silently lost.
- **`SlowDispatchTrackingRateLimiter`**: extends the critical section with `time.sleep(0.2)` to ensure contenders encounter lock contention deterministically.

Concurrency tests are NOT marked `@pytest.mark.slow` because they are deterministic; they rely on contention, not wall-clock timing. They should be part of the fast test suite.

If more test files require concurrent execution, consider extracting `run_concurrently()` to `tests/helpers/utils.py`.

### 5.9 Helper Function Decision Rules

Utility code belongs in `tests/helpers/` when it meets any of the following criteria:

- **Used in 3 or more test files**: extract to avoid duplication and ensure consistent behavior.
- **Implements a non-trivial algorithm**: e.g., `dict_equals_approx` (recursive approximate comparison), `wait_for_key_expiry` (polling with wall-clock deadline).
- **Bridges sync/async**: adapters such as `SyncToAsyncLimiterAdapter` belong in `tests/helpers/adapters.py`.
- **Provides Hypothesis strategies**: custom strategies belong in `tests/helpers/strategies.py`.

Helper module reference:

| Module | Contents | Purpose |
|---|---|---|
| `tests/helpers/adapters.py` | `SyncToAsyncLimiterAdapter`, `SyncToAsyncLockAdapter`, `SyncToAsyncLifecycleAdapter` | Bridge sync implementations to async test interfaces for mixin-based deduplication |
| `tests/helpers/strategies.py` | `json_value`, `nested_dict` | Hypothesis strategies for generating test data |
| `tests/helpers/tasks.py` | `noop_task`, `async_noop_task`, `slow_task` | Reusable task functions for dispatch tests |
| `tests/helpers/utils.py` | `wait_for_key_expiry`, `dict_equals_approx`, `is_subset` | General-purpose test utilities |

Do not create helpers preemptively. Inline the code until the third use, then extract.

## 6. Mutation Testing

### 6.1 Mutation-Targeted Test Annotation

The `Mutation target:` annotation is reserved for tests that exist primarily to document API contracts or implementation constants that a developer would not naturally write without mutation analysis. Tests that verify genuinely important behavior (formula correctness, boundary conditions, data integrity, concurrency safety, HTTP response structure, argument forwarding) should be written as normal tests with a single-sentence docstring per Section 2.3, even if mutation testing originally surfaced the gap.

**Format**: the annotation begins with `Mutation target:` on a new paragraph after the first-line summary. It identifies the mutated code element and its location concisely, without repeating the test logic. Use back-ticked code references for operators, constants, method names, and index expressions.

```python
def test_wake_default_delay_is_zero():
    """Verify that the ``delay`` parameter of ``wake()`` defaults to ``0.0``.

    Mutation target: default value of ``delay`` in ``DrainLoop.wake()``.
    """
```

This annotation serves two purposes: (a) it prevents future developers from removing the test as "over-specified," and (b) it documents the coupling so that refactors know to update the test alongside the implementation.

Tests that need this annotation are limited to:
- Tests asserting on `inspect.signature` defaults (e.g., `test_wake_default_delay_is_zero`).
- Tests asserting on class-variable constants that document configuration defaults (e.g., `test_refresh_interval_defaults_to_5`).

Tests that do **not** need this annotation (write them as normal behavioral tests instead):
- Tests asserting exact numeric outputs of internal formula methods (these verify algorithm correctness).
- Tests asserting on result index mapping with sentinel values (these verify data integrity).
- Tests asserting that shutdown completes within a timing deadline (these verify concurrency safety).
- Tests asserting on HTTP response structure, status codes, or headers (these verify protocol compliance).
- Tests asserting on argument forwarding via mock call verification (these verify data-flow correctness).
- Tests asserting on Prometheus metric names, descriptions, or label names (these verify observability contracts).

**Known limitation for default-parameter tests**: Python stores default parameter values in the function's `__defaults__` tuple when the `def` statement executes during import. Mutmut's fork model imports the module in the parent process (setting `__defaults__`), then forks children for mutation testing. The mutation modifies the code object in the child, but `__defaults__` is an attribute of the function object and remains unchanged. As a result, `inspect.signature` tests for default values are undetectable under mutmut. These tests are still valuable for documenting the expected contract; the annotation should reference the default parameter without repeating this limitation note.

### 6.2 `# pragma: no mutate` Rules

`# pragma: no mutate` is a last resort. It should only be used when writing a test is genuinely impossible or the mutation is provably equivalent.

**Acceptable**:
- **Equivalent mutants** where the mutation produces identical runtime behavior: `cast()` is a no-op at runtime, so any mutation to the cast call produces equivalent code.
- **Encoding equivalence**: `"latin-1"` vs single-character encoding mutations on data that is within the ASCII subset; the behavior is identical.
- **Mutmut trampoline bugs** where mutmut itself cannot generate a valid mutant (note: the `__init_subclass__` trampoline bug in `managed.py` was resolved by adding an explicit `@classmethod` decorator; see [mutmut#366](https://github.com/boxed/mutmut/issues/366)).

**Prohibited**:
- Behavioral code where writing a test is merely difficult or tedious.
- Any line that contains decision logic (conditionals, loops, returns).
- Logger lines; log messages are part of the observability contract and must be tested (see Section 4.4).
- Heuristic constants or formula values; write an exact-value test instead.

### 6.3 Equivalent Mutant Reference

The following categories of equivalent mutants have been identified in the codebase. This table should be maintained alongside mutation testing results.

| Category | Description | Locations | Pragma justification |
|---|---|---|---|
| `cast()` calls | `cast()` is a no-op at runtime; any mutation produces equivalent behavior | `limiters.py`, `async_limiters.py`, `base.py`, `decorators.py`, `importing.py`, `celery/limiter.py` | The mutation does not change observable behavior |
| `"latin-1"` encoding | Encoding mutations on ASCII data produce identical bytes | `backends/asgi/keys.py` | ASCII subset is identical across common encodings |
| `"utf-8"` encoding case | `"utf-8"` and `"UTF-8"` resolve to the same codec via `codecs.lookup()` | `core/managed.py:_parse_raw_config` | Python normalizes encoding names case-insensitively |
| `__init_subclass__` body | Previously required `# pragma: no mutate` due to a mutmut trampoline bug; resolved by adding an explicit `@classmethod` decorator ([mutmut#366](https://github.com/boxed/mutmut/issues/366)). Mutmut may still skip mutating this method entirely. | `managed.py` | No longer pragmaed; kept for reference |

## 7. Pitfalls and Checklist

### 7.1 Async Fixture Lifecycle

Async fixtures that create limiter instances must call `await limiter.start()` during setup and `await limiter.shutdown()` during teardown. Omitting `start()` leaves the drain signal subscriber un-started, which may mask bugs in signal-driven drain scheduling.

### 7.2 Test Isolation via `limiter_id`

Every test must use a unique `limiter_id` derived from the fixture in `tests/conftest.py`. Tests that manually construct limiter IDs must append a disambiguation suffix (e.g., `f"{limiter_id}_generic"`). Never use a hardcoded string such as `"test_limiter"` that could collide across parallel test runs.

### 7.3 Synchronization Preferences

When a test needs to wait for a background thread or task to complete work, prefer event-based synchronization over blind sleeping:

- **Sync threads**: use `threading.Event.wait(timeout=N)`, which returns as soon as the event is set, making tests faster and more deterministic.
- **Async tasks**: use `asyncio.wait_for(event.wait(), timeout=N)` or `asyncio.wait_for(coro, timeout=N)`.
- **Fallback**: `time.sleep()` is acceptable only when there is no event to hook into (e.g., heartbeat interval tests where the loop runs on a fixed timer).

### 7.4 Contract Test Inheritance Checklist

When adding a new backend, create the following:

1. `tests/fixtures/<backend>_backend.py`: fixture module with limiter construction and teardown.
2. `tests/implementations/<backend>/conftest.py`: imports the fixtures from the fixture module.
3. `tests/implementations/<backend>/test_contracts.py`: concrete subclass of `RateLimiterContractTest` (and `DistributedLockContractTest`, `TaskLifecycleContractTest` if applicable) with a `limiter` fixture providing the backend-specific limiter instance.
4. `tests/implementations/<backend>/test_<backend>_limiter.py`: backend-specific tests for dispatch logic, payload handling, and other behavior unique to the backend.
5. Add the backend class to the `TestConfigureHintCompliance` parametrize list in `tests/contracts/test_managed_mixin.py`.

### 7.5 Redis Cleanup Guarantees

The `redis_client` and `async_redis_client` fixtures call `flushdb()` both before and after the test. However, if a test raises an exception before the fixture's `yield`, the post-test `flushdb()` may not execute. The pre-test `flushdb()` in the next test mitigates this, but tests should not rely on a clean database at startup without the fixture's guarantee. Always use the fixture rather than manual Redis setup.

### 7.6 Warning Suppression

Async tests that create tasks or event loops may produce `RuntimeWarning` about unawaited coroutines during teardown. These warnings occur because tasks that were scheduled but not yet awaited are garbage-collected when the event loop closes. Use `@pytest.mark.filterwarnings("ignore::RuntimeWarning")` on the test class or method to suppress expected warnings. Apply the filter at the narrowest possible scope (method level preferred, class level acceptable); never suppress globally. Only suppress warnings that are expected consequences of the test's teardown, not warnings from the code under test. Where possible, prefer explicitly cancelling or awaiting outstanding tasks instead of suppressing the warning.

### 7.7 Singleton Class-State Pollution

Backend fixture modules must include an `autouse=True` fixture that calls `_reset()` before and after each test. See Section 5.2 for the pattern and the list of current fixture module locations. Forgetting this fixture is the most common source of test pollution in new backend test modules: a later test may silently retrieve a stale limiter instance from the class cache, causing intermittent failures that are difficult to reproduce.

### 7.8 Async Event Loop Scope

The project uses `asyncio_mode = "auto"` (see Section 8.1). Each test function gets its own event loop by default. Async fixtures that are function-scoped work correctly with this setting. Session-scoped async fixtures would require explicit event loop scope configuration and should be avoided unless strictly necessary. If a session-scoped async fixture is genuinely needed, document the event loop scope requirement in the fixture's docstring.

## 8. Pytest Configuration and Markers

### 8.1 pytest-asyncio Configuration

The project configures `asyncio_mode = "auto"` in `pyproject.toml`. This setting instructs pytest-asyncio to automatically detect `async def test_*` functions and wrap them in an event loop. No explicit `@pytest.mark.asyncio` decorator is needed on individual tests.

The `async_redis_client` fixture in `tests/conftest.py` is function-scoped (not session-scoped) to avoid event loop conflicts: each test function receives its own event loop under `auto` mode, so a session-scoped async connection created on one loop would be invalid in another test's loop.

### 8.2 Marker Reference

| Marker | Purpose | When to apply |
|---|---|---|
| `@pytest.mark.slow` | Marks tests that require real timing, i.e., `precise_sleep()` or actual Redis key expiration. Excluded from fast test runs via the default `-m 'not slow'` addopts. | Any test that relies on wall-clock time for its assertions. |
| `@pytest.mark.parametrize` | Parametrize test cases with multiple inputs. Must always include `ids=` for readable output (see Section 5.6). | When testing the same logic with multiple input combinations. Prefer class-level parametrize when all methods in a class share the parameter. |
| `@pytest.mark.filterwarnings("ignore::RuntimeWarning")` | Suppress expected RuntimeWarnings from async teardown (unawaited coroutines, unclosed event loops). | Apply at the test class or method level, never globally. Only for warnings that are expected consequences of the test's teardown, not warnings from the code under test. See Section 7.6. |
| `@pytest.mark.skipif` | Skip tests based on platform, environment, or dependency availability. | Platform-specific tests (e.g., Windows timer resolution), optional dependency availability. |

### 8.3 Category Marker Policy

Every concrete test class (i.e., classes whose name starts with `Test`) must carry exactly one category marker: `@pytest.mark.behavior`, `@pytest.mark.observability`, `@pytest.mark.signature`, `@pytest.mark.contract`, or `@pytest.mark.concurrency`. This ensures that all tests are reachable via marker-based selection (e.g., `pytest -m behavior`).

Mixin base classes (names that do **not** start with `Test`, e.g., `DrainBehaviorTests`, `DistributedLockBoundaryTests`) must **not** carry category markers. The concrete subclass that inherits the mixin is responsible for applying the appropriate marker. Placing a marker on a mixin is redundant because pytest does not collect classes whose names do not start with `Test`.

### 8.4 Custom Marker Registration

All custom markers must be registered in `pyproject.toml` under `[tool.pytest.ini_options].markers` to prevent `PytestUnknownMarkWarning`.

## 9. Test Data and Constants

### 9.1 Session Fixtures vs Module Constants

Use session-scoped fixtures (`func_path`, `payload`) for values that are shared across the entire test suite and do not vary between tests. Use module-level constants (uppercase with underscores, e.g., `WORKERS = 8`, `SHORT_TIMEOUT_MS = 500`) for values that are specific to a single test file but used by multiple tests within that file.

### 9.2 Magic Numbers

Extract numeric literals to named constants when the value has a non-obvious meaning or is used in more than one test method within the same file. The constant name should convey the intent, not the value (e.g., `SHORT_TIMEOUT_MS` rather than `TIMEOUT_500`).

### 9.3 Documenting Value Rationale

When a test value was chosen for a specific technical reason, include a comment explaining why. This is particularly important for timeout values, concurrency counts, and timing thresholds that may appear arbitrary without context.

```python
# Increased from 200ms to 500ms to accommodate mutmut trampoline overhead
# during mutation testing runs.
SHORT_TIMEOUT_MS = 500
```

Without such documentation, a future contributor may "optimize" the value back to a lower threshold and reintroduce flakiness.

## 10. Lua Script Testing

### 10.1 Rationale

The core rate limiting algorithm is implemented in Lua scripts: `consume.lua`, `acquire.lua`, `schedule.lua`, `health.lua`, and `renew.lua`. While the algorithm is verified via a Python reference implementation in `tests/algorithms/` and tested through the Python integration layer, the Lua scripts themselves have no isolated unit tests. A Lua-specific bug (e.g., off-by-one in return value indexing, incorrect `ARGV` parsing, a rounding difference vs the Python reference) would only be caught indirectly.

### 10.2 Test Location and Structure

Lua script tests should be placed in `tests/lua/` with one test file per Lua script (e.g., `test_consume.py`, `test_acquire.py`, `test_schedule.py`).

### 10.3 Test Pattern

Lua tests call `redis.eval()` or `redis.evalsha()` directly with controlled Redis state. The test sets up Redis keys manually (e.g., pre-populating window counters, buffer entries, concurrency set members), invokes the Lua script via `eval()`, and asserts on the returned values and the resulting Redis state.

```python
@staticmethod
async def test_consume_denies_when_estimate_meets_limit(async_redis_client):
    """Verify that the Lua script denies consumption when the estimated count equals the limit."""
    # Arrange
    # Pre-populate the current window counter to exactly the limit.
    ...

    # Act
    result = await async_redis_client.eval(CONSUME_LUA_SOURCE, num_keys, ...)

    # Assert
    assert result[0] == 0, "consume should deny when estimate meets the limit"
```

### 10.4 What to Test at the Lua Level

- **Return value structure**: verify each index in the returned array maps to the correct field. This mirrors the existing `test_consume_result_index_mapping_is_correct` pattern but tests the Lua script directly rather than through Python's `_eval_script`.
- **Boundary decisions**: `estimated_count == limit` should deny (the check is strictly less than); `estimated_count == limit - 1` should allow.
- **Concurrency enforcement**: an active task count at exactly `max_concurrency` should deny the request.
- **DLQ routing**: expired tasks should be moved to the dead letter queue, not dispatched.
- **Window key TTL**: the `PEXPIRE` should be set on first increment only (when `current_count == 0`).
- **Self-healing**: expired leases in the concurrency set should be removed before the rate limit evaluation.
- **Telemetry accuracy**: `remaining`, `reset_in_ms`, and `buffer_count` should reflect the post-operation state.

### 10.5 What NOT to Test at the Lua Level

- The sliding window counter formula itself; it is already formally verified via Hypothesis in `tests/properties/` and unit-tested via the Python reference implementation in `tests/algorithms/`.
- Redis command semantics (`ZADD`, `ZREM`, `GET`, `INCR`); trust that Redis works.
- Error handling for impossible states (e.g., negative counts); the Lua scripts assume Redis data integrity.

## 11. Missing Test Categories (Roadmap)

This section identifies test categories that should exist but are currently absent or incomplete. These are not formatting or convention issues; they are coverage gaps that should be addressed as separate tasks.

### 11.1 Configuration Edge Cases

Tests should verify the system behavior for boundary and degenerate configurations:

- **`limit=0`**: should deny all consumption requests. The current test (`test_execution_lock_cooldown_is_zero_when_limit_is_zero`) only verifies the cooldown calculation, not `consume()` behavior.
- **`window=0` or very small windows**: should be handled gracefully, either rejected at configuration time or treated as a valid edge case with defined behavior.
- **Negative values** for `limit`, `window`, `max_concurrency`: should be rejected with clear error messages.
- **Very large values** (`limit=10**9`, `window=86400`): should not cause integer overflow or excessive memory allocation in Lua.

### 11.2 Idempotency

Tests should verify that repeated operations produce the same result without side effects. Duplicate `schedule_task()` calls are already thoroughly tested at the contract, implementation, integration, and metrics levels. The following idempotency scenarios are not yet covered:

- Calling `shutdown()` multiple times should not raise exceptions or corrupt state.
- Calling `extend_lease()` on an already-expired lease (where the self-healing `ZREMRANGEBYSCORE` in `consume.lua` has removed the task from the concurrency set) should not re-register the task; `renew.lua` should return 0. The existing `test_extend_lease_raises_key_error_for_unknown_task` covers the "unknown task" case, but not the "previously known, now expired" case.

### 11.3 Redis Connection Resilience

The current test suite mocks `ConnectionError` for specific methods but does not test real connection loss and recovery scenarios:

- What happens if Redis goes down mid-drain? The drain loop should log the error and continue retrying.
- What happens if Redis returns after a brief outage? The limiter should resume normal operation without manual intervention.
- What happens if the Redis connection pool is exhausted? This may be out of scope for unit tests; document as an integration-level concern.

### 11.4 Performance and Load Testing

No performance tests currently exist. If added, they should:

- Verify that `consume()` latency stays below a threshold under sustained load.
- Verify that the drain loop throughput matches the configured rate.
- Be marked `@pytest.mark.slow` and excluded from fast test runs.
- Use real Redis, not mocks.

Performance tests are inherently environment-sensitive. They should use relative thresholds (e.g., "latency under load should not exceed 2x single-request latency") rather than absolute millisecond targets.
