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
2. **Fixed timing calculation:** Implemented two-phase positioning:
   - Coarse: Wait 2 windows for clean state
   - Fine-tune: Use current `reset_in_ms` to calculate precise wait to target position
   - Verify: Check actual position after wait
3. **Active polling instead of sleep():** Use `while time.time() < target_time: sleep(0.001)` for sub-second precision
4. **Skip on Windows:** Tests are skipped on Windows with `@pytest.mark.skipif(sys.platform == "win32")` since no amount of workaround can compensate for the OS timer resolution

**Test Results (Docker/Linux):**
- Burst: 45-47 (out of max 50), positioning 80-83%
- Convergence: total consumed within expected bounds, steady-state max <= limit+1

## Running Tests

**Standard run (excludes @pytest.mark.slow):**
```bash
pytest tests/integration/celery/test_rate_limiting.py -v
```

**All tests including slow parameterized matrix:**
```bash
pytest --override-ini='addopts=' tests/integration/celery/test_rate_limiting.py -v
```

**In Docker (recommended):**
```bash
docker compose --profile test up       # fast tests
docker compose --profile test-all up   # all tests
```

## Algorithm Properties Verified

The sliding window counter algorithm (in `consume.lua`):
- **2x burst bound:** Can allow up to 2x the limit in edge cases (empty previous window + boundary crossing)
- **Steady-state approximation:** Under sustained greedy load, allows up to limit+1 in any window
- **Long-term convergence:** Average rate over multiple windows converges to configured limit

These properties are formally verified in `tests/properties/test_sliding_window_counter.py`.
