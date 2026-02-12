# Integration Tests

This directory contains end-to-end integration tests that verify rate limiter behavior with real Redis and Lua scripts. Some tests are timing-sensitive and require Linux-level timer precision.

## Platform Requirements

### Windows: Sliding Window Tests Skipped

The sliding window behavior tests (`TestSlidingWindowBehavior`) are **automatically skipped on Windows** due to OS-level timing limitations:

1. **Timer resolution:** Windows has approximately 15ms timer resolution vs approximately 1 microsecond on Linux. For a 1-second window, this means 1.5% jitter per measurement -- enough to push results outside expected bounds.
2. **`time.sleep()` unreliability:** On Windows, `time.sleep(0.3)` can return after as little as approximately 35ms (10x too early). Even active polling with 1ms intervals can't fully compensate.

**The algorithm itself is correct** -- it is formally verified in `tests/properties/test_sliding_window_counter.py` using the pure algorithm extracted from the Lua script, with no timing dependency. The integration tests verify that Redis + Lua + real timing works as expected, which requires Linux-level timer precision.

**To run these tests, use Docker:**
```bash
docker compose --profile test up
```

## Test Structure

### `TestRateLimitingIntegration`

Core integration tests that verify fundamental rate limiter behavior: rate limit enforcement, concurrency limits, telemetry tracking, deduplication, task lifecycle, expiry/DLQ handling, and status reporting. These tests use a 60-second window and do not depend on sub-second timing.

### `TestSlidingWindowBehavior`

Two tests that validate the sliding window counter algorithm against real Redis:

1. **`test_long_term_rate_converges_to_limit`** -- Verifies that the average consumption rate over multiple windows converges to the configured limit, and that no window-sized period exceeds 2x the limit.

2. **`test_burst_at_window_boundary_after_empty_window`** -- Verifies that consumption across a window boundary (after an empty previous window) produces a burst that exceeds the limit but stays within the 2x algorithmic bound.

Both tests use client-side `time.time()` timestamps to measure consumption patterns. The assertions are deliberately conservative (convergence tolerance bands, 2x burst bound) to account for the inherent imprecision of measuring Redis-enforced invariants from the client side.

## What Is NOT Tested Here (and Why)

### Per-fixed-window counts

The sliding window algorithm guarantees that each internal fixed-window counter never exceeds `limit` (the `INCR` only fires after `estimated_count < limit`, and the estimate includes a non-negative previous-window contribution). This invariant is **provably correct** and is verified in the property tests.

However, **this invariant cannot be reliably verified from the client side**. The test records timestamps with `time.time()` (local clock), while Redis enforces windows using `redis.call('TIME')` (server clock). Mapping between the two requires a clock offset calibration that has per-request jitter from network round-trips. Under high-throughput consumption near window boundaries, this jitter is sufficient to misattribute requests across windows -- making it appear that a window exceeded `limit` when Redis itself enforced the limit correctly.

This was empirically confirmed: even on native Linux, a `limit=20` config produced a measured count of 21 in a fixed window (1 failure in 80 test runs), despite Redis correctly enforcing the limit.

### Parameterized configs

The sliding window algorithm has no special-casing for different limit/window values -- it's the same arithmetic with different inputs. Running the same integration test across many (limit, window) combinations exercises identical code paths. The mathematical properties (2x bound, per-fixed-window invariant) are verified more rigorously by the property tests in `tests/properties/test_sliding_window_counter.py`, which can test thousands of random combinations via Hypothesis.

## Running Tests

```bash
# Standard run:
pytest tests/integration/test_rate_limiting.py -v

# In Docker (recommended):
docker compose --profile test up
```

## Algorithm Properties

The sliding window counter algorithm (in `consume.lua`) has three key properties:

- **2x burst bound:** Can allow up to 2x the limit in edge cases (empty previous window + boundary crossing). Verified in integration tests (client-side) and property tests (pure algorithm).
- **Per-fixed-window bound:** Each internal counter never exceeds `limit`. Verified in property tests only (cannot be reliably measured from client side; see above).
- **Long-term convergence:** Average rate over multiple windows converges to configured limit. Verified in integration tests.
