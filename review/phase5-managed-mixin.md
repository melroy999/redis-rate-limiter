# Phase 5: Managed Mixin Review

File: core/managed.py (667 lines)
Three classes: ManagedRateLimiterMixin, SyncManagedRateLimiter, AsyncManagedRateLimiter.

## Architecture
- Singleton-per-ID pattern with class-level `_instances` dict
- `__init_subclass__` gives each concrete subclass isolated state (good)
- Sentinel pattern prevents direct construction — must use `create()`/`get()`
- Config persisted to Redis hash (`rl:registry:configs`), version counter (`rl:registry:versions`)
- `refresh_config()` polls version, applies overrides if newer version found

## Findings

### CONCERN: _persist_config is not atomic (managed.py:360-369, 610-630)
Both sync and async `_persist_config` do three separate Redis calls:
1. `HSET` the config
2. `HINCRBY` the version
3. `HGET` the version back

If the process crashes between steps 1 and 2, the config is updated but the version
is not incremented, so other workers never pick up the change. A Redis pipeline or
Lua script would make this atomic.

**Impact**: Low probability in practice (crash window is microseconds), but violates
the "single source of truth" guarantee documented in the docstring.

### CONCERN: refresh_config TOCTOU between version check and config read (managed.py:371-406, 632-667)
`refresh_config()` does:
1. Read version → see it's newer
2. Read config → apply it

Between steps 1 and 2, another `update()` could change the config again. The worker
would apply a config that doesn't match the version it recorded. On the next refresh,
it would skip the intermediate version.

**Impact**: Transient inconsistency. The worker would eventually converge to the latest
config on the following refresh cycle. Acceptable for a distributed system, but worth
documenting as a known limitation.

### NOTE: _config_version initialization
The `_config_version` attribute is referenced in `refresh_config()` but never explicitly
initialized in the mixin or the managed classes. It must be set by the concrete backend's
`__init__` chain (via `AbstractRateLimiter` or similar). If a backend forgets to initialize
it, `refresh_config()` would raise `AttributeError`.

Looking at base.py would confirm where this is initialized — but it's declared in
TYPE_CHECKING blocks (managed.py:186, 422) which suggests it's set elsewhere in the MRO.

### NOTE: Sync/async mirror is clean
The async version correctly adds `await` and `cast(Awaitable, ...)` wrappers for all
Redis calls. The `create()` and `get()` methods also call `await instance.start()` for
deferred async init. No structural divergences found.

### NOTE: assert statements used for internal invariants
Lines 248, 297, 362, 487, 540, 613 use `assert cls._redis_client is not None`. These
are used after `_require_configured()` has already verified the state, so they serve as
type narrowing hints for mypy rather than runtime guards. Acceptable pattern, but they
would be stripped with `python -O`.

### IMPROVEMENT: No shutdown of replaced instances on override
When `create(override=True)` replaces an existing instance (managed.py:243-256,
482-496), the old instance is simply overwritten in `_instances`. If the old instance
had background threads/tasks (drain loop, pubsub subscriber), they continue running
as orphans. The caller is responsible for shutting down the old instance first, but
this isn't documented or enforced.

## Verdict
Well-structured mixin with clean sync/async separation. The non-atomic persist and
TOCTOU in refresh are the main concerns, both with low practical impact. The override
orphan issue should be documented.
