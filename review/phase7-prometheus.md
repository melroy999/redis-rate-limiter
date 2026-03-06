# Phase 7: Prometheus Integration Review

**Files reviewed:**
- `src/redis_rate_limiter/integrations/prometheus.py` (178 lines)
- `src/redis_rate_limiter/integrations/__init__.py` (empty)
- `tests/integrations/test_prometheus.py` (467 lines)

---

## Summary

The Prometheus integration is clean, well-documented, and correctly structured. Metric types are appropriate, labels are bounded, and the callback pattern integrates naturally. There are no bugs. The findings below are concerns and improvements only.

---

## Findings

### CONCERN-1: Two exporters on the same registry will crash

**Severity:** Concern

**Location:** `prometheus.py` lines 85-122 (`__init__`)

Each `PrometheusMetricsExporter.__init__` call creates **new** `Counter` and `Gauge` objects and registers them with the given registry. If two exporters are created for different `limiter_id` values on the **same** registry (including the default global registry), the second instantiation will raise `ValueError: Duplicated timeseries in CollectorRegistry`.

This is because `prometheus_client` metric constructors register by metric **name**, not by label values. Two `Counter("redis_rate_limiter_consume_total", ...)` calls on the same registry collide.

**Impact:** In production, creating two limiter instances with Prometheus metrics (a reasonable multi-tenant scenario) will crash at startup.

**Suggested fix:** Use a module-level dict or the `prometheus_client` pattern of checking the registry before creating metrics. Alternatively, accept pre-created metric objects, or use a class-level cache keyed by `(metric_name, registry_id)`:

```python
# Option A: create metrics at module scope, share across instances
_consume_total = Counter(
    "redis_rate_limiter_consume_total", ..., ["limiter_id", "outcome"]
)

class PrometheusMetricsExporter:
    def __init__(self, limiter_id, registry=None):
        self._limiter_id = limiter_id
        self._consume_total = _consume_total
        ...
```

```python
# Option B: cache per registry to support custom registries in tests
_METRIC_CACHE: dict[int, dict[str, Counter | Gauge]] = {}

def _get_or_create(registry):
    key = id(registry)
    if key not in _METRIC_CACHE:
        _METRIC_CACHE[key] = {
            "consume": Counter(..., registry=registry),
            ...
        }
    return _METRIC_CACHE[key]
```

---

### CONCERN-2: `_handle_consume` crashes on missing dict keys

**Severity:** Concern

**Location:** `prometheus.py` lines 168-170

The gauge-update lines use hard `data["remaining_tokens"]`, `data["active_concurrency"]`, and `data["remaining_tasks"]` access. If the limiter's callback contract ever changes (e.g., a new event variant omits a field), this will raise `KeyError` and the limiter's `_emit_metrics` wrapper will swallow it with a warning log -- silently losing all metrics for that event.

The `expired` and `success` fields are already accessed safely via `data.get(...)`. The gauge fields should follow the same pattern, or the contract should be documented as strict (and tested for the KeyError case).

**Suggested fix:** Either use `.get()` with sensible defaults, or add a defensive guard at the top of `_handle_consume`:

```python
required = {"remaining_tokens", "active_concurrency", "remaining_tasks"}
if not required.issubset(data):
    logger.warning("Incomplete consume event data: %s", data.keys())
    return
```

---

### CONCERN-3: No `Histogram` for latency or window reset timing

**Severity:** Improvement

**Location:** `prometheus.py` (general)

The `consume` event data includes `reset_in_ms` (visible in test payloads), but it is completely ignored by the exporter. A `Histogram` tracking the time-until-reset or consume latency would be the most operationally useful metric for capacity planning and alerting. The docstring at line 1 even mentions "observability through standard Prometheus/Grafana tooling", but the most useful operational metric is not captured.

Not a bug, but a missed opportunity for the integration's stated purpose.

---

### IMPROVEMENT-1: `__init__.py` exports nothing

**Severity:** Note

**Location:** `src/redis_rate_limiter/integrations/__init__.py`

The package `__init__.py` is empty. Users must import `PrometheusMetricsExporter` from the full submodule path. This is fine as a deliberate design choice (keeps the optional dependency lazy), but it means the integration is not discoverable via `from redis_rate_limiter.integrations import PrometheusMetricsExporter`. Consider whether a conditional re-export would be user-friendly.

---

### NOTE-1: Outcome priority -- `expired` wins over `success`

**Severity:** Note

**Location:** `prometheus.py` lines 158-163

The outcome logic checks `expired` before `success`. In the actual Lua script, `success = (result[0] == 1)` and `expired = (result[0] == -1)`, so they are mutually exclusive. The priority ordering is therefore safe but untested -- there is no test for the case where both `expired=True` and `success=True` are passed. This is acceptable since the combination is impossible from the real limiter, but the implicit invariant is worth noting.

---

### NOTE-2: Test coverage is solid but missing two edge cases

**Severity:** Improvement

**Location:** `tests/integrations/test_prometheus.py`

The test suite covers counters, gauges, label names, registry isolation, unknown events, and signature defaults. Two gaps:

1. **No test for the duplicate-registration crash** (CONCERN-1). Creating two exporters on the same registry should be tested to document the current behavior or confirm a fix.
2. **No test for missing keys in `data`** (CONCERN-2). A test calling `exporter("consume", {"success": True})` (omitting gauge fields) would document whether the exporter raises or degrades gracefully.

---

## Metric Type Assessment

| Metric | Type | Correct? | Notes |
|--------|------|----------|-------|
| `consume_total` | Counter | Yes | Monotonically increasing event count. |
| `schedule_total` | Counter | Yes | Monotonically increasing event count. |
| `remaining_tokens` | Gauge | Yes | Point-in-time value that goes up and down. |
| `active_concurrency` | Gauge | Yes | Point-in-time value that goes up and down. |
| `buffer_depth` | Gauge | Yes | Point-in-time value that goes up and down. |

All metric types are correctly chosen.

## Label Cardinality Assessment

| Label | Bounded? | Values |
|-------|----------|--------|
| `limiter_id` | Depends on usage | One per limiter instance -- bounded if users create a fixed set. |
| `outcome` | Yes | `{"success", "rejected", "expired"}` -- 3 values. |
| `scheduled` | Yes | `{"true", "false"}` -- 2 values. |

No risk of cardinality explosion from the labels themselves. The `limiter_id` label is safe as long as users do not generate dynamic IDs (which would be a misuse of the limiter itself).

## Thread Safety Assessment

Prometheus `Counter` and `Gauge` objects from `prometheus_client` are internally thread-safe (they use locks for `.inc()` and `.set()`). The exporter holds no mutable state beyond the metric references, which are set once in `__init__`. The design is thread-safe.
