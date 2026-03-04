"""Tests for the rate limiter class-level API behavior.

This module validates class-level lifecycle and dynamic-configuration behavior,
including ``configure()``, ``create()``, ``get()``, ``update()``,
``refresh_config()``, and ``_reset()``.

The current implementation under test is ``ManagedTestRateLimiter``, a test-only
backend that extends ``SyncManagedRateLimiter``.

Fixture dependencies:
    - ``redis_client``, ``limiter_id``: from ``tests/conftest.py``.
"""

import inspect
import json
import logging
import time
from typing import Any, ClassVar, Optional

import pytest

from celery_rate_limiter.core import (
    AbstractDistributedRateLimiter,
    SyncManagedRateLimiter,
)
from tests.helpers.utils import assert_log_emitted


class ManagedTestRateLimiter(SyncManagedRateLimiter, AbstractDistributedRateLimiter):
    """Test-only managed limiter used for backend-agnostic class API tests."""

    _backend_label: ClassVar[Optional[str]] = None

    @classmethod
    def _configure_backend(cls, **backend_context: Any) -> None:
        backend_label = backend_context.get("backend_label")
        if backend_label is None:
            raise RuntimeError(
                "ManagedTestRateLimiter.configure(redis_client, backend_label) "
                "must be called before create() or get()."
            )
        cls._backend_label = str(backend_label)

    @classmethod
    def _has_backend_context(cls) -> bool:
        return cls._backend_label is not None

    @classmethod
    def _get_instance_context(cls) -> dict[str, Any]:
        return {}

    @classmethod
    def _reset_backend_context(cls) -> None:
        cls._backend_label = None

    @classmethod
    def _configure_hint(cls) -> str:
        return "ManagedTestRateLimiter.configure(redis_client, backend_label)"

    def _dispatch_task(self, func_path: str, payload: dict, task_id: str) -> None:
        return None

    def _schedule_drain(self, delay: float = 0.0) -> None:
        return None


@pytest.fixture(autouse=True)
def _reset_managed_limiter_class_state(redis_client):
    """Ensure that the managed class API tests begin from a clean, configured state."""
    ManagedTestRateLimiter._reset()
    ManagedTestRateLimiter.configure(redis_client, backend_label="test")
    yield
    ManagedTestRateLimiter._reset()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def create_test_limiter(
    limiter_id: str,
    **overrides,
) -> ManagedTestRateLimiter:
    """Create a limiter instance with stable defaults and optional overrides."""
    params = {
        "limit": 10,
        "window": 60,
        "max_concurrency": 5,
        "override": True,
        "drain_enabled": False,
    }
    params.update(overrides)
    return ManagedTestRateLimiter.create(limiter_id=limiter_id, **params)


def read_registry_config(redis_client, limiter_id: str) -> dict:
    """Read and decode the persisted limiter configuration from the Redis registry."""
    raw_config = redis_client.hget(ManagedTestRateLimiter._REGISTRY_KEY, limiter_id)
    assert raw_config is not None, (
        f"expected persisted config for limiter '{limiter_id}'"
    )
    return json.loads(raw_config)


# ---------------------------------------------------------------------------
# Behavioral tests
# ---------------------------------------------------------------------------


class TestConfigure:
    """Test suite for the ``configure()`` class method."""

    @staticmethod
    def test_configure_sets_class_state(redis_client):
        """Verify that ``configure()`` stores the shared Redis client and backend context."""
        # Arrange
        ManagedTestRateLimiter._reset()

        # Act
        ManagedTestRateLimiter.configure(redis_client, backend_label="custom")

        # Assert
        assert ManagedTestRateLimiter._redis_client is redis_client, (
            "configure should store the provided Redis client"
        )
        assert ManagedTestRateLimiter._backend_label == "custom", (
            "configure should store backend-specific context"
        )

    @staticmethod
    def test_configure_without_backend_label_raises_error(redis_client):
        """Verify that ``configure()`` fails when the required backend context is missing."""
        # Arrange
        ManagedTestRateLimiter._reset()

        # Act & Assert
        with pytest.raises(RuntimeError, match="backend_label"):
            ManagedTestRateLimiter.configure(redis_client)

        # Cleanup for test isolation.
        ManagedTestRateLimiter._reset()


class TestCreate:
    """Test suite for the ``create()`` class method."""

    @staticmethod
    def test_create_without_configure_raises(limiter_id):
        """Verify that ``create()`` fails when ``configure()`` has not been called."""
        # Arrange
        ManagedTestRateLimiter._reset()

        # Act & Assert
        with pytest.raises(RuntimeError, match="configure"):
            ManagedTestRateLimiter.create(
                limiter_id, limit=1, window=1, max_concurrency=1
            )

    @staticmethod
    def test_create_returns_configured_instance(limiter_id):
        """Verify that ``create()`` returns an instance configured with the requested values."""
        # Act
        limiter = create_test_limiter(limiter_id)

        # Assert
        assert limiter.id == limiter_id, "created limiter should keep requested id"
        assert limiter.limit == 10, "created limiter should keep requested limit"
        assert limiter.window == 60, "created limiter should keep requested window"
        assert limiter.max_concurrency == 5, (
            "created limiter should keep requested max_concurrency"
        )

    @staticmethod
    def test_create_caches_instance_locally(limiter_id):
        """Verify that ``create()`` stores the instance in the class-level cache."""
        # Act
        limiter = create_test_limiter(limiter_id)

        # Assert
        assert limiter_id in ManagedTestRateLimiter._instances, (
            "created limiter should be present in local cache"
        )
        assert ManagedTestRateLimiter._instances[limiter_id] is limiter, (
            "cached limiter should be the created instance"
        )

    @staticmethod
    def test_create_duplicate_without_override_raises(limiter_id):
        """Verify that ``create()`` rejects duplicate IDs unless ``override=True``."""
        # Arrange
        create_test_limiter(limiter_id)

        # Act & Assert
        with pytest.raises(ValueError, match="already exists"):
            ManagedTestRateLimiter.create(
                limiter_id,
                limit=10,
                window=60,
                max_concurrency=5,
                override=False,
            )

    @staticmethod
    def test_create_duplicate_with_override_replaces_cached_instance(
        limiter_id,
    ):
        """Verify that ``override=True`` replaces the cached limiter for the same ID."""
        # Arrange
        first = create_test_limiter(limiter_id)

        # Act
        second = ManagedTestRateLimiter.create(
            limiter_id,
            limit=20,
            window=30,
            max_concurrency=3,
            override=True,
            drain_enabled=False,
        )

        # Assert
        assert second is not first, "override create should return a new instance"
        assert second.limit == 20, "override create should apply new limit"
        assert second.window == 30, "override create should apply new window"
        assert second.max_concurrency == 3, (
            "override create should apply new max_concurrency"
        )
        assert ManagedTestRateLimiter._instances[limiter_id] is second, (
            "local cache should point to replacement instance"
        )

    @staticmethod
    def test_create_persist_writes_registry_config(redis_client, limiter_id):
        """Verify that ``create()`` with ``persist=True`` writes the limiter configuration to the Redis registry."""
        # Act
        create_test_limiter(limiter_id)

        # Assert
        config = read_registry_config(redis_client, limiter_id)
        assert config["limit"] == 10, "persisted config should include limit"
        assert config["window"] == 60, "persisted config should include window"
        assert config["max_concurrency"] == 5, (
            "persisted config should include max_concurrency"
        )

    @staticmethod
    def test_create_persist_increments_version(redis_client, limiter_id):
        """Verify that repeated ``create()`` calls with ``override`` increment the Redis version counter."""
        # Arrange
        create_test_limiter(limiter_id)
        initial_version = int(
            redis_client.hget(ManagedTestRateLimiter._VERSION_KEY, limiter_id)
        )

        # Act
        ManagedTestRateLimiter.create(
            limiter_id,
            limit=20,
            window=60,
            max_concurrency=5,
            override=True,
            drain_enabled=False,
        )

        # Assert
        updated_version = int(
            redis_client.hget(ManagedTestRateLimiter._VERSION_KEY, limiter_id)
        )
        assert updated_version == initial_version + 1, (
            "override create should bump config version by one"
        )

    @staticmethod
    def test_create_without_persist_skips_registry_write(redis_client, limiter_id):
        """Verify that ``create()`` with ``persist=False`` does not write to the Redis registry."""
        # Act
        create_test_limiter(limiter_id, persist=False)

        # Assert
        assert (
            redis_client.hget(ManagedTestRateLimiter._REGISTRY_KEY, limiter_id) is None
        ), "persist=False should skip config registry write"


class TestGet:
    """Test suite for the ``get()`` class method."""

    @staticmethod
    def test_get_without_configure_raises(limiter_id):
        """Verify that ``get()`` fails when ``configure()`` has not been called and the cache misses."""
        # Arrange
        ManagedTestRateLimiter._reset()

        # Act & Assert
        with pytest.raises(RuntimeError, match="configure"):
            ManagedTestRateLimiter.get(limiter_id)

    @staticmethod
    def test_get_returns_cached_instance(limiter_id):
        """Verify that ``get()`` returns the cached instance without rehydration."""
        # Arrange
        created = create_test_limiter(limiter_id)

        # Act
        fetched = ManagedTestRateLimiter.get(limiter_id)

        # Assert
        assert fetched is created, "get should return the cached limiter instance"

    @staticmethod
    def test_get_hydrates_from_redis_on_cache_miss(limiter_id):
        """Verify that ``get()`` hydrates the limiter configuration from Redis when the cache misses."""
        # Arrange
        create_test_limiter(limiter_id)
        ManagedTestRateLimiter._instances.clear()

        # Act
        hydrated = ManagedTestRateLimiter.get(limiter_id)

        # Assert
        assert hydrated.id == limiter_id, "hydrated limiter should retain persisted id"
        assert hydrated.limit == 10, "hydrated limiter should retain persisted limit"
        assert hydrated.window == 60, "hydrated limiter should retain persisted window"
        assert hydrated.max_concurrency == 5, (
            "hydrated limiter should retain persisted max_concurrency"
        )

    @staticmethod
    def test_get_hydration_loads_current_config_version(redis_client, limiter_id):
        """Verify that the hydrated limiter tracks the current persisted configuration version."""
        # Arrange
        create_test_limiter(limiter_id)
        expected_version = int(
            redis_client.hget(ManagedTestRateLimiter._VERSION_KEY, limiter_id)
        )
        ManagedTestRateLimiter._instances.clear()

        # Act
        hydrated = ManagedTestRateLimiter.get(limiter_id)

        # Assert
        assert hydrated._config_version == expected_version, (
            "hydrated limiter should load current config version"
        )

    @staticmethod
    def test_get_nonexistent_limiter_raises_value_error():
        """Verify that ``get()`` raises a ValueError when the limiter does not exist in any location."""
        missing_limiter_id = "limiter_id_for_get_nonexistent_limiter_test"

        # Act & Assert
        with pytest.raises(ValueError, match="not found"):
            ManagedTestRateLimiter.get(missing_limiter_id)


class TestUpdate:
    """Test suite for the ``update()`` class method."""

    @staticmethod
    def test_update_can_change_limit(limiter_id):
        """Verify that ``update()`` can change only the rate limit value."""
        # Arrange
        create_test_limiter(limiter_id)

        # Act
        updated = ManagedTestRateLimiter.update(limiter_id, limit=50)

        # Assert
        assert updated.limit == 50, "updated limiter should reflect new limit"

    @staticmethod
    def test_update_can_change_max_concurrency(limiter_id):
        """Verify that ``update()`` can change only the max_concurrency value."""
        # Arrange
        create_test_limiter(limiter_id)

        # Act
        updated = ManagedTestRateLimiter.update(limiter_id, max_concurrency=20)

        # Assert
        assert updated.max_concurrency == 20, (
            "updated limiter should reflect new max_concurrency"
        )

    @staticmethod
    def test_update_can_change_max_age_and_lease_duration(limiter_id):
        """Verify that ``update()`` can change the task max_age and lease_duration values."""
        # Arrange
        create_test_limiter(limiter_id, max_age=3600, lease_duration=30)

        # Act
        updated = ManagedTestRateLimiter.update(
            limiter_id,
            max_age=120,
            lease_duration=9,
        )

        # Assert
        assert updated.max_age == 120, "updated limiter should reflect new max_age"
        assert updated.lease_duration == 9, (
            "updated limiter should reflect new lease_duration"
        )

    @staticmethod
    def test_update_persists_new_config_and_bumps_version(redis_client, limiter_id):
        """Verify that ``update()`` writes the new configuration and increments the version counter."""
        # Arrange
        create_test_limiter(limiter_id)
        initial_version = int(
            redis_client.hget(ManagedTestRateLimiter._VERSION_KEY, limiter_id)
        )

        # Act
        ManagedTestRateLimiter.update(limiter_id, limit=50)

        # Assert
        updated_version = int(
            redis_client.hget(ManagedTestRateLimiter._VERSION_KEY, limiter_id)
        )
        assert updated_version == initial_version + 1, (
            "update should bump config version by one"
        )

        config = read_registry_config(redis_client, limiter_id)
        assert config["limit"] == 50, "update should persist new limit"

    @staticmethod
    def test_update_window_change_sets_pause_until(limiter_id):
        """Verify that changing the window sets a transition pause for safe rollover."""
        # Arrange
        limiter = create_test_limiter(limiter_id)
        assert limiter._paused_until == 0.0, "pause should start disabled"
        previous_window = limiter.window

        # Act
        ManagedTestRateLimiter.update(limiter_id, window=30)

        # Assert
        assert limiter.window == 30, "window should update to requested value"
        assert limiter._paused_until > time.time(), (
            "window update should set a future pause timestamp"
        )
        assert limiter._paused_until <= time.time() + previous_window + 1, (
            "window update pause should not exceed old window plus small tolerance"
        )

    @staticmethod
    def test_update_preserves_unspecified_fields(limiter_id):
        """Verify that ``update()`` keeps fields unchanged when no override is provided for them."""
        # Arrange
        create_test_limiter(
            limiter_id,
            max_age=7200,
            lease_duration=45,
        )

        # Act
        ManagedTestRateLimiter.update(limiter_id, limit=50)
        limiter = ManagedTestRateLimiter.get(limiter_id)

        # Assert
        assert limiter.limit == 50, "limit should be updated"
        assert limiter.window == 60, "window should remain unchanged"
        assert limiter.max_concurrency == 5, "max_concurrency should remain unchanged"
        assert limiter.max_age == 7200, "max_age should remain unchanged"
        assert limiter.lease_duration == 45, "lease_duration should remain unchanged"

    @staticmethod
    def test_update_preserves_jitter_settings(limiter_id):
        """Verify that ``update()`` does not override the existing jitter configuration."""
        # Arrange
        create_test_limiter(
            limiter_id,
            jitter_enabled=False,
            jitter_min_pct=0.11,
            jitter_max_pct=0.22,
        )

        # Act
        ManagedTestRateLimiter.update(limiter_id, limit=99)
        limiter = ManagedTestRateLimiter.get(limiter_id)

        # Assert
        assert limiter.limit == 99, "limit should be updated"
        assert limiter.jitter_enabled is False, "jitter_enabled should remain unchanged"
        assert limiter.jitter_min_pct == 0.11, "jitter_min_pct should remain unchanged"
        assert limiter.jitter_max_pct == 0.22, "jitter_max_pct should remain unchanged"

    @staticmethod
    def test_get_status_reflects_updated_config(limiter_id):
        """Verify that ``get_status()`` returns updated values after ``update()`` modifies the configuration."""
        # Arrange
        create_test_limiter(limiter_id, limit=10, max_concurrency=5)

        # Act
        ManagedTestRateLimiter.update(limiter_id, limit=20, max_concurrency=8)
        limiter = ManagedTestRateLimiter.get(limiter_id)
        status = limiter.get_status()

        # Assert
        assert status["rate_limit"]["limit"] == 20, (
            "get_status should reflect updated limit"
        )
        assert status["concurrency"]["max"] == 8, (
            "get_status should reflect updated max_concurrency"
        )


class TestRefreshConfig:
    """Test suite for the ``refresh_config()`` instance method."""

    @staticmethod
    def test_refresh_config_noop_when_version_unchanged(limiter_id):
        """Verify that ``refresh_config()`` is a no-op when the versions already match."""
        # Arrange
        limiter = create_test_limiter(limiter_id)

        # Act
        changed = limiter.refresh_config()

        # Assert
        assert changed is False, (
            "refresh should report no change when version is current"
        )

    @staticmethod
    def test_refresh_config_applies_remote_change(redis_client, limiter_id):
        """Verify that ``refresh_config()`` applies a newer configuration written by another worker."""
        # Arrange
        limiter = create_test_limiter(limiter_id)
        new_config = {
            "limit": 50,
            "window": 60,
            "max_concurrency": 10,
            "max_age": 3600,
            "lease_duration": 30,
        }
        redis_client.hset(
            ManagedTestRateLimiter._REGISTRY_KEY,
            limiter_id,
            json.dumps(new_config),
        )
        redis_client.hincrby(ManagedTestRateLimiter._VERSION_KEY, limiter_id, 1)

        # Act
        changed = limiter.refresh_config()

        # Assert
        assert changed is True, "refresh should report config change"
        assert limiter.limit == 50, "refresh should apply updated limit"
        assert limiter.max_concurrency == 10, (
            "refresh should apply updated max_concurrency"
        )

    @staticmethod
    def test_refresh_config_window_change_sets_pause_until(redis_client, limiter_id):
        """Verify that ``refresh_config()`` sets a pause when the remote configuration changes the window size."""
        # Arrange
        limiter = create_test_limiter(limiter_id)
        new_config = {
            "limit": 10,
            "window": 30,
            "max_concurrency": 5,
            "max_age": 3600,
            "lease_duration": 30,
        }
        redis_client.hset(
            ManagedTestRateLimiter._REGISTRY_KEY,
            limiter_id,
            json.dumps(new_config),
        )
        redis_client.hincrby(ManagedTestRateLimiter._VERSION_KEY, limiter_id, 1)

        # Act
        changed = limiter.refresh_config()

        # Assert
        assert changed is True, "refresh should report config change"
        assert limiter.window == 30, "refresh should apply updated window"
        assert limiter._paused_until > time.time(), (
            "window change via refresh should set a future pause timestamp"
        )

    @staticmethod
    def test_refresh_config_returns_false_when_version_hash_missing(
        limiter_id,
    ):
        """Verify that ``refresh_config()`` returns False when no version entry exists."""
        # Arrange
        limiter = create_test_limiter(limiter_id, persist=False)

        # Act
        changed = limiter.refresh_config()

        # Assert
        assert changed is False, (
            "refresh should return False when version tracking does not exist"
        )

    @staticmethod
    def test_refresh_config_handles_corrupted_redis_data(redis_client, limiter_id):
        """Verify that ``refresh_config()`` handles malformed persisted JSON gracefully."""
        # Arrange
        limiter = create_test_limiter(limiter_id)
        original_state = (
            limiter.limit,
            limiter.window,
            limiter.max_concurrency,
            limiter.max_age,
            limiter.lease_duration,
        )
        redis_client.hset(
            ManagedTestRateLimiter._REGISTRY_KEY, limiter_id, "{invalid_json"
        )
        redis_client.hincrby(ManagedTestRateLimiter._VERSION_KEY, limiter_id, 1)

        # Act
        changed = limiter.refresh_config()

        # Assert
        assert changed is False, "malformed config should not be applied"
        assert (
            limiter.limit,
            limiter.window,
            limiter.max_concurrency,
            limiter.max_age,
            limiter.lease_duration,
        ) == original_state, "limiter config should remain unchanged on malformed data"

    @staticmethod
    def test_refresh_config_returns_false_when_registry_config_missing(
        redis_client, limiter_id
    ):
        """Verify that ``refresh_config()`` returns False when the version exists but the registry configuration is missing."""
        # Arrange
        limiter = create_test_limiter(limiter_id)
        original_version = limiter._config_version
        original_state = (
            limiter.limit,
            limiter.window,
            limiter.max_concurrency,
            limiter.max_age,
            limiter.lease_duration,
        )

        redis_client.hdel(ManagedTestRateLimiter._REGISTRY_KEY, limiter_id)
        redis_client.hincrby(ManagedTestRateLimiter._VERSION_KEY, limiter_id, 1)

        # Act
        changed = limiter.refresh_config()

        # Assert
        assert changed is False, (
            "refresh should return False when no persisted config payload exists"
        )
        assert limiter._config_version == original_version, (
            "local config version should not advance when config payload is missing"
        )
        assert (
            limiter.limit,
            limiter.window,
            limiter.max_concurrency,
            limiter.max_age,
            limiter.lease_duration,
        ) == original_state, (
            "config fields should remain unchanged when payload is missing"
        )


class TestResetAndConstruction:
    """Test suite for ``_reset()`` and direct construction guards."""

    @staticmethod
    def test_reset_clears_cached_instances_and_configuration(limiter_id):
        """Verify that ``_reset()`` clears the class cache and the shared configuration."""
        # Arrange
        create_test_limiter(limiter_id)

        # Act
        ManagedTestRateLimiter._reset()

        # Assert
        assert len(ManagedTestRateLimiter._instances) == 0, (
            "reset should clear local limiter cache"
        )
        assert ManagedTestRateLimiter._redis_client is None, (
            "reset should clear shared redis client"
        )
        assert ManagedTestRateLimiter._backend_label is None, (
            "reset should clear shared backend context"
        )

    @staticmethod
    def test_direct_construction_raises_runtime_error(redis_client):
        """Verify that direct ``__init__`` invocation, bypassing ``create()``/``get()``, raises a ``RuntimeError``."""
        # Act & Assert
        with pytest.raises(RuntimeError, match="Direct.*construction is not supported"):
            ManagedTestRateLimiter(
                redis_client=redis_client,
                limiter_id="direct_construction_test",
                limit=5,
                window=1.0,
                max_concurrency=2,
            )

    @staticmethod
    def test_subclass_isolation_separate_instances(redis_client):
        """Verify that ``__init_subclass__`` isolates ``_instances`` and ``_redis_client`` per subclass."""

        class IsolatedLimiterA(SyncManagedRateLimiter, AbstractDistributedRateLimiter):
            @classmethod
            def _configure_backend(cls, **backend_context):
                return None

            @classmethod
            def _has_backend_context(cls) -> bool:
                return True

            @classmethod
            def _get_instance_context(cls) -> dict[str, object]:
                return {}

            @classmethod
            def _reset_backend_context(cls) -> None:
                return None

            @classmethod
            def _configure_hint(cls) -> str:
                return "isolated configure hint"

            def _dispatch_task(
                self, func_path: str, payload: dict, task_id: str
            ) -> None:
                return None

            def _schedule_drain(self, delay: float = 0.0) -> None:
                return None

        class IsolatedLimiterB(SyncManagedRateLimiter, AbstractDistributedRateLimiter):
            @classmethod
            def _configure_backend(cls, **backend_context):
                return None

            @classmethod
            def _has_backend_context(cls) -> bool:
                return True

            @classmethod
            def _get_instance_context(cls) -> dict[str, object]:
                return {}

            @classmethod
            def _reset_backend_context(cls) -> None:
                return None

            @classmethod
            def _configure_hint(cls) -> str:
                return "isolated configure hint"

            def _dispatch_task(
                self, func_path: str, payload: dict, task_id: str
            ) -> None:
                return None

            def _schedule_drain(self, delay: float = 0.0) -> None:
                return None

        # Arrange
        isolated_limiter_a_cache_key = (
            "isolated_limiter_a_instance_for_subclass_isolation_test"
        )
        IsolatedLimiterA._instances[isolated_limiter_a_cache_key] = (
            IsolatedLimiterA.__new__(IsolatedLimiterA)
        )
        IsolatedLimiterA._redis_client = redis_client

        # Assert
        assert IsolatedLimiterA._instances is not IsolatedLimiterB._instances, (
            "subclasses should not share _instances mapping"
        )
        assert IsolatedLimiterB._instances == {}, (
            "second subclass should start with empty _instances"
        )
        assert IsolatedLimiterA._redis_client is not IsolatedLimiterB._redis_client, (
            "subclasses should not share _redis_client"
        )
        assert IsolatedLimiterB._redis_client is None, (
            "second subclass should start with no redis client"
        )


class TestManagedMixinInternals:
    """Tests for internal class machinery in ``ManagedRateLimiterMixin``."""

    @staticmethod
    def test_sentinel_is_unique_object_not_none():
        """Verify that ``_SENTINEL`` is a unique ``object()`` instance, not ``None``."""
        # Assert
        assert ManagedTestRateLimiter._SENTINEL is not None, (
            "_SENTINEL must not be None to prevent collisions with explicit None arguments"
        )

    @staticmethod
    def test_init_subclass_assigns_fresh_sentinel_not_none():
        """Verify that ``__init_subclass__`` assigns a non-None sentinel to each new subclass.

        The subclass is defined inside the test body so that ``__init_subclass__``
        executes at test time, when the mutmut trampoline is active. Module-level
        subclasses (e.g., ``ManagedTestRateLimiter``) are defined at import time
        before the trampoline activates, so they cannot catch this mutation.
        """

        # Act
        class FreshSubclass(SyncManagedRateLimiter, AbstractDistributedRateLimiter):
            @classmethod
            def _configure_backend(cls, **backend_context):
                return None

            @classmethod
            def _has_backend_context(cls) -> bool:
                return True

            @classmethod
            def _get_instance_context(cls) -> dict:
                return {}

            @classmethod
            def _reset_backend_context(cls) -> None:
                return None

            @classmethod
            def _configure_hint(cls) -> str:
                return "FreshSubclass.configure(redis_client)"

            def _dispatch_task(self, func_path, payload, task_id):
                return None

            def _schedule_drain(self, delay=0.0):
                return None

        # Assert
        assert FreshSubclass._SENTINEL is not None, (
            "__init_subclass__ must assign a unique object() sentinel, not None"
        )

    @staticmethod
    def test_parse_raw_config_decodes_bytes_with_utf8():
        """Verify that ``_parse_raw_config`` correctly decodes bytes input via UTF-8."""
        # Arrange
        raw_bytes = b'{"limit": 10, "window": 60}'

        # Act
        result = ManagedTestRateLimiter._parse_raw_config(raw_bytes)

        # Assert
        assert result == {"limit": 10, "window": 60}, (
            "_parse_raw_config should decode bytes via utf-8 and return a dict"
        )


# ---------------------------------------------------------------------------
# Observability tests
# ---------------------------------------------------------------------------


class TestConfigureObservability:
    """Observability tests for the ``configure()`` class method log emission."""

    @staticmethod
    def test_configure_emits_info_log(redis_client, caplog):
        """Verify that ``configure()`` emits an INFO log with the class name."""
        # Arrange
        ManagedTestRateLimiter._reset()

        # Act
        with caplog.at_level(logging.INFO, logger="celery_rate_limiter.core.managed"):
            ManagedTestRateLimiter.configure(redis_client, backend_label="test")

        # Assert
        assert_log_emitted(
            caplog.records,
            level="INFO",
            required_fragments=["ManagedTestRateLimiter", "configured"],
            message="should emit an info log confirming configuration",
        )


class TestCreateObservability:
    """Observability tests for the ``create()`` class method log emission."""

    @staticmethod
    def test_create_emits_info_log(limiter_id, caplog):
        """Verify that ``create()`` emits an INFO log with the limiter id and persist flag."""
        # Act
        with caplog.at_level(logging.INFO, logger="celery_rate_limiter.core.managed"):
            create_test_limiter(limiter_id)

        # Assert
        assert_log_emitted(
            caplog.records,
            level="INFO",
            required_fragments=[
                "ManagedTestRateLimiter",
                "created",
                f"limiter={limiter_id}",
                "persist=True",
            ],
            message="should emit an info log with class name, limiter id, and persist flag",
        )


class TestGetObservability:
    """Observability tests for the ``get()`` class method log emission."""

    @staticmethod
    def test_get_cache_hit_emits_debug_log(limiter_id, caplog):
        """Verify that ``get()`` emits a DEBUG log when the instance is resolved from the local cache."""
        # Arrange
        create_test_limiter(limiter_id)

        # Act
        with caplog.at_level(logging.DEBUG, logger="celery_rate_limiter.core.managed"):
            ManagedTestRateLimiter.get(limiter_id)

        # Assert
        assert_log_emitted(
            caplog.records,
            level="DEBUG",
            required_fragments=[
                "ManagedTestRateLimiter",
                "resolved from local cache",
                f"limiter={limiter_id}",
            ],
            message="should emit a debug log for local cache resolution",
        )

    @staticmethod
    def test_get_hydration_emits_debug_log(limiter_id, caplog):
        """Verify that ``get()`` emits a DEBUG log when the instance is hydrated from Redis."""
        # Arrange
        create_test_limiter(limiter_id)
        ManagedTestRateLimiter._instances.clear()

        # Act
        with caplog.at_level(logging.DEBUG, logger="celery_rate_limiter.core.managed"):
            ManagedTestRateLimiter.get(limiter_id)

        # Assert
        assert_log_emitted(
            caplog.records,
            level="DEBUG",
            required_fragments=[
                "ManagedTestRateLimiter",
                "hydrated from Redis",
                f"limiter={limiter_id}",
            ],
            message="should emit a debug log for Redis hydration",
        )


class TestUpdateObservability:
    """Observability tests for the ``update()`` class method log emission."""

    @staticmethod
    def test_update_emits_info_log(limiter_id, caplog):
        """Verify that ``update()`` emits an INFO log with the limiter id and overrides."""
        # Arrange
        create_test_limiter(limiter_id)

        # Act
        with caplog.at_level(logging.INFO, logger="celery_rate_limiter.core.managed"):
            ManagedTestRateLimiter.update(limiter_id, limit=50)

        # Assert
        assert_log_emitted(
            caplog.records,
            level="INFO",
            required_fragments=[
                "ManagedTestRateLimiter",
                "updated",
                f"limiter={limiter_id}",
                "overrides={'limit': 50}",
            ],
            message="should emit an info log with class name, limiter id, and overrides on update",
        )


class TestRefreshConfigObservability:
    """Observability tests for the ``refresh_config()`` instance method log emission."""

    @staticmethod
    def test_refresh_config_success_emits_info_log(redis_client, limiter_id, caplog):
        """Verify that a successful ``refresh_config()`` emits an INFO log with the limiter id and version."""
        # Arrange
        limiter = create_test_limiter(limiter_id)
        new_config = {
            "limit": 50,
            "window": 60,
            "max_concurrency": 10,
            "max_age": 3600,
            "lease_duration": 30,
        }
        redis_client.hset(
            ManagedTestRateLimiter._REGISTRY_KEY,
            limiter_id,
            json.dumps(new_config),
        )
        redis_client.hincrby(ManagedTestRateLimiter._VERSION_KEY, limiter_id, 1)

        # Act
        with caplog.at_level(logging.INFO, logger="celery_rate_limiter.core.managed"):
            limiter.refresh_config()

        # Assert
        assert_log_emitted(
            caplog.records,
            level="INFO",
            required_fragments=[f"limiter={limiter_id}", "version=2"],
            message="should emit an info log with limiter id and version on successful refresh",
        )

    @staticmethod
    def test_refresh_config_corrupted_data_emits_warning_log(
        redis_client, limiter_id, caplog
    ):
        """Verify that ``refresh_config()`` emits a WARNING log when the persisted configuration is malformed."""
        # Arrange
        limiter = create_test_limiter(limiter_id)
        redis_client.hset(
            ManagedTestRateLimiter._REGISTRY_KEY, limiter_id, "{invalid_json"
        )
        redis_client.hincrby(ManagedTestRateLimiter._VERSION_KEY, limiter_id, 1)

        # Act
        with caplog.at_level(
            logging.WARNING, logger="celery_rate_limiter.core.managed"
        ):
            limiter.refresh_config()

        # Assert
        assert_log_emitted(
            caplog.records,
            level="WARNING",
            required_fragments=[f"limiter={limiter_id}", "error="],
            message="should emit a warning log with limiter id and error details on malformed config",
        )


# ---------------------------------------------------------------------------
# Signature tests
# ---------------------------------------------------------------------------


class TestCreateSignatures:
    """Signature tests for the ``create()`` class method default parameter values."""

    @staticmethod
    def test_create_default_parameters():
        """Verify that ``persist`` and ``override`` have the expected defaults.

        Mutation target: ``persist`` and ``override`` default values in
        ``SyncManagedRateLimiter.create``.
        """
        # Arrange & Act
        sig = inspect.signature(SyncManagedRateLimiter.create)

        # Assert
        assert sig.parameters["persist"].default is True, (
            "persist parameter should default to True"
        )
        assert sig.parameters["override"].default is False, (
            "override parameter should default to False"
        )
