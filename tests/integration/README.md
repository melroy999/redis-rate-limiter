# Integration Tests

This directory contains end-to-end integration tests that verify rate limiter behaviour against a real Redis instance and Lua scripts. A number of these tests are timing-sensitive and therefore require Linux-level timer precision.

## Platform Requirements

### Windows: Sliding Window Tests Are Skipped

The sliding window behaviour tests (`TestSlidingWindowBehavior`) are **automatically skipped on Windows** owing to OS-level timing limitations:

1. **Timer resolution:** Windows exhibits approximately 15ms timer resolution, compared to approximately 1 microsecond on Linux. For a 1-second window, this translates to 1.5% jitter per measurement -- sufficient to push results outside the expected bounds.
2. **`time.sleep()` unreliability:** On Windows, `time.sleep(0.3)` may return after as little as approximately 35ms (i.e., 10x too early). Even active polling at 1ms intervals cannot fully compensate for this deficiency.

**The algorithm itself is correct** -- it is formally verified in `tests/properties/test_sliding_window_counter.py` using the pure algorithm extracted from the Lua script, with no timing dependency. The integration tests verify that Redis, Lua, and real timing operate as expected in concert, which requires Linux-level timer precision.

**To run these tests, use Docker:**
```bash
docker compose --profile test up
```

## Test Structure

### `TestRateLimitingIntegration`

These are the core integration tests that verify fundamental rate limiter behaviour: rate limit enforcement, concurrency limits, telemetry tracking, deduplication, task lifecycle management, expiry and DLQ handling, and status reporting. These tests employ a 60-second window and do not depend on sub-second timing.

### `TestSlidingWindowBehavior`

Two tests validate the sliding window counter algorithm against real Redis:

1. **`test_long_term_rate_converges_to_limit`** -- Verifies that the average consumption rate over multiple windows converges to the configured limit, and that no window-sized period exceeds 2x the limit.

2. **`test_burst_at_window_boundary_after_empty_window`** -- Verifies that consumption across a window boundary (following an empty previous window) produces a burst that exceeds the limit but remains within the 2x algorithmic bound.

Both tests use client-side `time.time()` timestamps to measure consumption patterns. The assertions are deliberately conservative (i.e., convergence tolerance bands, 2x burst bound) to account for the inherent imprecision of measuring Redis-enforced invariants from the client side.

## What Is NOT Tested Here (and Why)

### Per-fixed-window counts

The sliding window algorithm guarantees that each internal fixed-window counter never exceeds `limit` (the `INCR` is only executed after `estimated_count < limit`, and the estimate includes a non-negative previous-window contribution). This invariant is **provably correct** and is verified in the property tests.

However, **this invariant cannot be reliably verified from the client side**. The test records timestamps using `time.time()` (the local clock), whereas Redis enforces windows using `redis.call('TIME')` (the server clock). Mapping between the two requires a clock offset calibration that is subject to per-request jitter from network round-trips. Under high-throughput consumption near window boundaries, this jitter is sufficient to misattribute requests across windows -- making it appear that a window exceeded `limit` when Redis itself enforced the limit correctly.

This was empirically confirmed: even on native Linux, a `limit=20` configuration produced a measured count of 21 in a fixed window (1 failure in 80 test runs), despite Redis having correctly enforced the limit.

### Parameterised configurations

The sliding window algorithm contains no special-casing for different limit/window values -- it applies the same arithmetic with different inputs. Running the same integration test across many (limit, window) combinations exercises identical code paths. The mathematical properties (the 2x bound and the per-fixed-window invariant) are verified more rigorously by the property tests in `tests/properties/test_sliding_window_counter.py`, which are capable of testing thousands of random combinations via Hypothesis.

## Running Tests

```bash
# Standard run:
pytest tests/integration/test_rate_limiting.py -v

# In Docker (recommended):
docker compose --profile test up
```

## Algorithm Properties

The sliding window counter algorithm (in `consume.lua`) exhibits three key properties:

- **2x burst bound:** Up to 2x the limit may be permitted in edge cases (an empty previous window combined with boundary crossing). This is verified in the integration tests (client-side) and the property tests (pure algorithm).
- **Per-fixed-window bound:** Each internal counter never exceeds `limit`. This is verified in the property tests only (it cannot be reliably measured from the client side; see above).
- **Long-term convergence:** The average rate over multiple windows converges to the configured limit. This is verified in the integration tests.
