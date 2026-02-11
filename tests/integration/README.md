# Integration Tests

This directory contains end-to-end integration tests that verify rate limiter behavior with real Redis and Lua scripts. These tests are inherently timing-sensitive and require special handling.

## Platform Requirements

### Windows: Sliding Window Tests Skipped

The sliding window behavior tests (`TestSlidingWindowBehavior`, `TestSlidingWindowBehaviorParametrized`) are **automatically skipped on Windows** due to fundamental OS-level timing limitations:

1. **Timer resolution:** Windows has approximately 15ms timer resolution vs approximately 1 microsecond on Linux. For a 1-second window, this means 1.5% jitter per measurement -- enough to push results outside expected bounds.
2. **`time.sleep()` unreliability:** On Windows, `time.sleep(0.3)` can return after as little as approximately 35ms (10x too early). Even active polling with 1ms intervals can't fully compensate.
3. **Retry rate Goldilocks problem:** When rate-limited, the test needs to retry at a specific pace. On Windows, no single retry interval works reliably: 10ms is too aggressive (over-consumes), 100ms is too slow (under-consumes).

**The algorithm itself is correct** -- it is formally verified in `tests/properties/test_sliding_window_counter.py` using the pure algorithm extracted from the Lua script, with no timing dependency. The integration tests verify that Redis + Lua + real timing works as expected, which requires Linux-level timer precision.

**To run these tests, use Docker:**
```bash
docker compose --profile test up       # fast tests only
docker compose --profile test-all up   # includes @pytest.mark.slow
```

## Timing Considerations

### Sliding Window Tests

The tests in `TestSlidingWindowBehavior` validate real-world behavior of the sliding window counter algorithm against Redis.

### Design Decisions

**Problem:**
- Tests were flaky and failing inconsistently on both platforms
- Burst test would achieve varying results (11-48 instead of consistently near 50)
- Convergence test steady-state max would vary unpredictably

**Root Causes:**
1. Original test settings too sensitive: `window=0.5s, limit=10` made 10-20ms jitter significant (2-4% of window)
2. Original code calculated wait time based on stale `reset_in_ms`, resulting in positioning at 0.8% through window instead of target 80%
3. Windows timer resolution makes sub-second timing inherently unreliable

**Solutions Implemented:**
1. **Production-realistic settings:** Changed to `window=1.0s, limit=25` (actual production use case), making timing jitter only 1-2% of window instead of 2-4%
2. **Direct window positioning:** Use `redis_client.time()` to compute exact Redis window boundaries, then sleep directly to the target position with no intermediate consumes
3. **Active polling instead of sleep():** Use `while time.time() < target_time: sleep(0.001)` for sub-second precision
4. **Skip on Windows:** Tests are skipped on Windows with `@pytest.mark.skipif(sys.platform == "win32")` since no amount of workaround can compensate for the OS timer resolution

**Expected behavior (Docker/Linux):**
- Burst test: achieves close to the theoretical 2x limit, with positioning near the target 80% mark
- Convergence test: total consumed tracks `num_windows * limit`, per-fixed-window counts stay within `limit`

## Running Tests

**Standard run (excludes @pytest.mark.slow):**
```bash
pytest tests/integration/test_rate_limiting.py -v
```

**All tests including slow parameterized matrix:**
```bash
pytest --override-ini='addopts=' tests/integration/test_rate_limiting.py -v
```

**In Docker (recommended):**
```bash
docker compose --profile test up       # fast tests
docker compose --profile test-all up   # all tests
```

## Algorithm Properties Verified

The sliding window counter algorithm (in `consume.lua`):
- **2x burst bound:** Can allow up to 2x the limit in edge cases (empty previous window + boundary crossing)
- **Per-fixed-window bound:** Each internal counter never exceeds `limit` (provable, see below)
- **Long-term convergence:** Average rate over multiple windows converges to configured limit

These properties are formally verified in `tests/properties/test_sliding_window_counter.py` (pure algorithm) and `tests/integration/test_rate_limiting.py` (Redis + Lua + real timing).

### Estimate vs. true sliding window count

The sliding window counter *approximates* the true sliding window count using:

```
estimated = current_count + previous_count * weight
```

where `weight` linearly decays from 1.0 to 0.0 over the window. A request is allowed when `estimated < limit`. This approximation implicitly assumes requests are uniformly distributed within each fixed window.

**Per-fixed-window invariant (provable).** The `INCR` on the current window counter only fires after `estimated < limit` passes. Because the estimate includes a non-negative contribution from the previous window (`previous_count * weight >= 0`), `current_count` can never exceed `limit`. This holds for any traffic pattern, not just greedy consumption.

**Measurement window count (not asserted).** A *measurement* window (an arbitrary window-sized interval anchored at any timestamp) can straddle two fixed windows. The count it observes depends on how requests are actually distributed within each fixed window, not on the uniform distribution the estimate assumes.

Under greedy consumption with a sleep-when-denied consumer, the distribution is non-uniform: requests are back-loaded within each fixed window (the algorithm blocks at the start while `weight * previous_count` is high, then admits a burst as `weight` decays). When a window is under-saturated (fewer than `limit` requests due to timing imprecision), the next window receives a compensating burst at its start — the `weight * previous_count` term leaves headroom, so the algorithm immediately admits multiple requests.

A Python-side measurement window positioned to capture the tail of an under-saturated window plus the compensating burst at the start of the next can observe well above `limit`. The degree of excess depends on:

1. **How much under-saturation occurred** — a function of sleep precision, Docker scheduling, Redis round-trip variance
2. **Measurement window alignment** — which portion of each fixed window the measurement window captures

Since the per-fixed-window invariant guarantees each counter `<= limit`, the measurement window count is bounded above by `2 * limit` (straddling two full windows). However, no tighter bound between `limit` and `2 * limit` is provable without assumptions about timing precision that vary across environments. In testing, observed values ranged from `limit + 1` to `1.9 * limit` depending on configuration and system load.

The steady-state sliding window max is reported in test output for diagnostics but is not asserted. The per-fixed-window invariant is the provable guarantee from the algorithm.

### Clock calibration

The algorithm uses `redis.call('TIME')` for window alignment while the test records `time.time()` on the Python side. To map Python timestamps into Redis-aligned fixed windows for the per-fixed-window check, we calibrate the clock offset once before the consumption loop using `redis_client.time()` (O(1)). This avoids calling `KEYS` or `SCAN`, which block Redis and could affect test timing.
