"""Tests for rate limiter class-level API behavior.

This module validates class-level lifecycle and dynamic-config behavior:
``configure()``, ``create()``, ``get()``, ``update()``, ``refresh_config()``,
and ``_reset()``.

Current implementation under test: ``ManagedTestRateLimiter`` (test-only backend).
"""

import json
import time
from typing import Any, ClassVar, Optional

import pytest

from celery_rate_limiter.core.limiters import AbstractRedisManagedRateLimiter


class ManagedTestRateLimiter(AbstractRedisManagedRateLimiter):
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
    """Ensure managed class API tests start from clean configured state."""
    ManagedTestRateLimiter._reset()
    ManagedTestRateLimiter.configure(redis_client, backend_label="test")
    yield
    ManagedTestRateLimiter._reset()


class TestRateLimiterClassApi:
    """Test suite for class-level rate limiter API behavior."""

    @staticmethod
    def _create_limiter(
        limiter_id: str,
        **overrides,
    ) -> ManagedTestRateLimiter:
        """Create a limiter with stable defaults and optional overrides."""
        params = {
            "limit": 10,
            "window": 60,
            "max_concurrency": 5,
            "override": True,
        }
        params.update(overrides)
        return ManagedTestRateLimiter.create(limiter_id=limiter_id, **params)

    @staticmethod
    def _read_registry_config(redis_client, limiter_id: str) -> dict:
        """Read and decode persisted limiter config from Redis registry."""
        raw_config = redis_client.hget(ManagedTestRateLimiter._REGISTRY_KEY, limiter_id)
        assert raw_config is not None, (
            f"expected persisted config for limiter '{limiter_id}'"
        )
        return json.loads(raw_config)

    # ==================== configure() ====================

    def test_configure_sets_class_state(self, redis_client):
        """Verify configure stores shared Redis and backend context."""
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

    def test_configure_without_backend_label_raises_error(self, redis_client):
        """Verify configure() fails when backend context is missing."""
        # Arrange
        ManagedTestRateLimiter._reset()

        # Act & Assert
        with pytest.raises(RuntimeError, match="backend_label"):
            ManagedTestRateLimiter.configure(redis_client)

        # Cleanup for test isolation.
        ManagedTestRateLimiter._reset()

    def test_create_without_configure_raises(self, default_limiter_id):
        """Verify create fails when configure has not been called."""
        # Arrange
        ManagedTestRateLimiter._reset()

        # Act & Assert
        with pytest.raises(RuntimeError, match="configure"):
            ManagedTestRateLimiter.create(
                default_limiter_id, limit=1, window=1, max_concurrency=1
            )

    def test_get_without_configure_raises(self, default_limiter_id):
        """Verify get fails when configure has not been called and cache misses."""
        # Arrange
        ManagedTestRateLimiter._reset()

        # Act & Assert
        with pytest.raises(RuntimeError, match="configure"):
            ManagedTestRateLimiter.get(default_limiter_id)

    # ==================== create() ====================

    def test_create_returns_configured_instance(self, default_limiter_id):
        """Verify create returns an instance configured with requested values."""
        # Act
        limiter = self._create_limiter(default_limiter_id)

        # Assert
        assert limiter.id == default_limiter_id, (
            "created limiter should keep requested id"
        )
        assert limiter.limit == 10, "created limiter should keep requested limit"
        assert limiter.window == 60, "created limiter should keep requested window"
        assert limiter.max_concurrency == 5, (
            "created limiter should keep requested max_concurrency"
        )

    def test_create_caches_instance_locally(self, default_limiter_id):
        """Verify create stores the instance in class-level cache."""
        # Act
        limiter = self._create_limiter(default_limiter_id)

        # Assert
        assert default_limiter_id in ManagedTestRateLimiter._instances, (
            "created limiter should be present in local cache"
        )
        assert ManagedTestRateLimiter._instances[default_limiter_id] is limiter, (
            "cached limiter should be the created instance"
        )

    def test_create_duplicate_without_override_raises(self, default_limiter_id):
        """Verify create rejects duplicate IDs unless override=True."""
        # Arrange
        self._create_limiter(default_limiter_id)

        # Act & Assert
        with pytest.raises(ValueError, match="already exists"):
            ManagedTestRateLimiter.create(
                default_limiter_id,
                limit=10,
                window=60,
                max_concurrency=5,
                override=False,
            )

    def test_create_duplicate_with_override_replaces_cached_instance(
        self, default_limiter_id
    ):
        """Verify override=True replaces the cached limiter for the same ID."""
        # Arrange
        first = self._create_limiter(default_limiter_id)

        # Act
        second = ManagedTestRateLimiter.create(
            default_limiter_id,
            limit=20,
            window=30,
            max_concurrency=3,
            override=True,
        )

        # Assert
        assert second is not first, "override create should return a new instance"
        assert second.limit == 20, "override create should apply new limit"
        assert second.window == 30, "override create should apply new window"
        assert second.max_concurrency == 3, (
            "override create should apply new max_concurrency"
        )
        assert ManagedTestRateLimiter._instances[default_limiter_id] is second, (
            "local cache should point to replacement instance"
        )

    def test_create_persist_writes_registry_config(
        self, redis_client, default_limiter_id
    ):
        """Verify create persist=True writes limiter config to Redis registry."""
        # Act
        self._create_limiter(default_limiter_id)

        # Assert
        config = self._read_registry_config(redis_client, default_limiter_id)
        assert config["limit"] == 10, "persisted config should include limit"
        assert config["window"] == 60, "persisted config should include window"
        assert config["max_concurrency"] == 5, (
            "persisted config should include max_concurrency"
        )

    def test_create_persist_increments_version(self, redis_client, default_limiter_id):
        """Verify repeated create with override increments Redis version counter."""
        # Arrange
        self._create_limiter(default_limiter_id)
        initial_version = int(
            redis_client.hget(ManagedTestRateLimiter._VERSION_KEY, default_limiter_id)
        )

        # Act
        ManagedTestRateLimiter.create(
            default_limiter_id,
            limit=20,
            window=60,
            max_concurrency=5,
            override=True,
        )

        # Assert
        updated_version = int(
            redis_client.hget(ManagedTestRateLimiter._VERSION_KEY, default_limiter_id)
        )
        assert updated_version == initial_version + 1, (
            "override create should bump config version by one"
        )

    def test_create_without_persist_skips_registry_write(
        self, redis_client, default_limiter_id
    ):
        """Verify create persist=False does not write to Redis registry."""
        # Act
        self._create_limiter(default_limiter_id, persist=False)

        # Assert
        assert (
            redis_client.hget(ManagedTestRateLimiter._REGISTRY_KEY, default_limiter_id)
            is None
        ), "persist=False should skip config registry write"

    # ==================== get() ====================

    def test_get_returns_cached_instance(self, default_limiter_id):
        """Verify get returns cached instance without rehydration."""
        # Arrange
        created = self._create_limiter(default_limiter_id)

        # Act
        fetched = ManagedTestRateLimiter.get(default_limiter_id)

        # Assert
        assert fetched is created, "get should return the cached limiter instance"

    def test_get_hydrates_from_redis_on_cache_miss(self, default_limiter_id):
        """Verify get hydrates limiter config from Redis when cache misses."""
        # Arrange
        self._create_limiter(default_limiter_id)
        ManagedTestRateLimiter._instances.clear()

        # Act
        hydrated = ManagedTestRateLimiter.get(default_limiter_id)

        # Assert
        assert hydrated.id == default_limiter_id, (
            "hydrated limiter should retain persisted id"
        )
        assert hydrated.limit == 10, "hydrated limiter should retain persisted limit"
        assert hydrated.window == 60, "hydrated limiter should retain persisted window"
        assert hydrated.max_concurrency == 5, (
            "hydrated limiter should retain persisted max_concurrency"
        )

    def test_get_hydration_loads_current_config_version(
        self, redis_client, default_limiter_id
    ):
        """Verify hydrated limiter tracks the current persisted config version."""
        # Arrange
        self._create_limiter(default_limiter_id)
        expected_version = int(
            redis_client.hget(ManagedTestRateLimiter._VERSION_KEY, default_limiter_id)
        )
        ManagedTestRateLimiter._instances.clear()

        # Act
        hydrated = ManagedTestRateLimiter.get(default_limiter_id)

        # Assert
        assert hydrated._config_version == expected_version, (
            "hydrated limiter should load current config version"
        )

    def test_get_nonexistent_limiter_raises_value_error(self):
        """Verify get raises ValueError when limiter does not exist anywhere."""
        missing_limiter_id = "limiter_id_for_get_nonexistent_limiter_test"

        # Act & Assert
        with pytest.raises(ValueError, match="not found"):
            ManagedTestRateLimiter.get(missing_limiter_id)

    # ==================== update() ====================

    def test_update_can_change_limit(self, default_limiter_id):
        """Verify update can change only the rate limit value."""
        # Arrange
        self._create_limiter(default_limiter_id)

        # Act
        updated = ManagedTestRateLimiter.update(default_limiter_id, limit=50)

        # Assert
        assert updated.limit == 50, "updated limiter should reflect new limit"

    def test_update_can_change_max_concurrency(self, default_limiter_id):
        """Verify update can change only the max_concurrency value."""
        # Arrange
        self._create_limiter(default_limiter_id)

        # Act
        updated = ManagedTestRateLimiter.update(default_limiter_id, max_concurrency=20)

        # Assert
        assert updated.max_concurrency == 20, (
            "updated limiter should reflect new max_concurrency"
        )

    def test_update_can_change_max_age_and_lease_duration(self, default_limiter_id):
        """Verify update can change task max_age and lease_duration values."""
        # Arrange
        self._create_limiter(default_limiter_id, max_age=3600, lease_duration=30)

        # Act
        updated = ManagedTestRateLimiter.update(
            default_limiter_id,
            max_age=120,
            lease_duration=9,
        )

        # Assert
        assert updated.max_age == 120, "updated limiter should reflect new max_age"
        assert updated.lease_duration == 9, (
            "updated limiter should reflect new lease_duration"
        )

    def test_update_persists_new_config_and_bumps_version(
        self, redis_client, default_limiter_id
    ):
        """Verify update writes new config and increments version counter."""
        # Arrange
        self._create_limiter(default_limiter_id)
        initial_version = int(
            redis_client.hget(ManagedTestRateLimiter._VERSION_KEY, default_limiter_id)
        )

        # Act
        ManagedTestRateLimiter.update(default_limiter_id, limit=50)

        # Assert
        updated_version = int(
            redis_client.hget(ManagedTestRateLimiter._VERSION_KEY, default_limiter_id)
        )
        assert updated_version == initial_version + 1, (
            "update should bump config version by one"
        )

        config = self._read_registry_config(redis_client, default_limiter_id)
        assert config["limit"] == 50, "update should persist new limit"

    def test_update_window_change_sets_pause_until(self, default_limiter_id):
        """Verify changing window sets a transition pause for safe rollover."""
        # Arrange
        limiter = self._create_limiter(default_limiter_id)
        assert limiter._paused_until == 0.0, "pause should start disabled"
        previous_window = limiter.window

        # Act
        ManagedTestRateLimiter.update(default_limiter_id, window=30)

        # Assert
        assert limiter.window == 30, "window should update to requested value"
        assert limiter._paused_until > time.time(), (
            "window update should set a future pause timestamp"
        )
        assert limiter._paused_until <= time.time() + previous_window + 1, (
            "window update pause should not exceed old window plus small tolerance"
        )

    def test_update_preserves_unspecified_fields(self, default_limiter_id):
        """Verify update keeps fields unchanged when no override is provided."""
        # Arrange
        self._create_limiter(
            default_limiter_id,
            max_age=7200,
            lease_duration=45,
        )

        # Act
        ManagedTestRateLimiter.update(default_limiter_id, limit=50)
        limiter = ManagedTestRateLimiter.get(default_limiter_id)

        # Assert
        assert limiter.limit == 50, "limit should be updated"
        assert limiter.window == 60, "window should remain unchanged"
        assert limiter.max_concurrency == 5, "max_concurrency should remain unchanged"
        assert limiter.max_age == 7200, "max_age should remain unchanged"
        assert limiter.lease_duration == 45, "lease_duration should remain unchanged"

    def test_update_preserves_jitter_settings(self, default_limiter_id):
        """Verify update does not override existing jitter configuration."""
        # Arrange
        self._create_limiter(
            default_limiter_id,
            jitter_enabled=False,
            jitter_min_pct=0.11,
            jitter_max_pct=0.22,
        )

        # Act
        ManagedTestRateLimiter.update(default_limiter_id, limit=99)
        limiter = ManagedTestRateLimiter.get(default_limiter_id)

        # Assert
        assert limiter.limit == 99, "limit should be updated"
        assert limiter.jitter_enabled is False, "jitter_enabled should remain unchanged"
        assert limiter.jitter_min_pct == 0.11, "jitter_min_pct should remain unchanged"
        assert limiter.jitter_max_pct == 0.22, "jitter_max_pct should remain unchanged"

    def test_get_status_reflects_updated_config(self, default_limiter_id):
        """Verify get_status() returns updated values after update() changes config."""
        # Arrange
        self._create_limiter(default_limiter_id, limit=10, max_concurrency=5)

        # Act
        ManagedTestRateLimiter.update(default_limiter_id, limit=20, max_concurrency=8)
        limiter = ManagedTestRateLimiter.get(default_limiter_id)
        status = limiter.get_status()

        # Assert
        assert status["rate_limit"]["limit"] == 20, (
            "get_status should reflect updated limit"
        )
        assert status["concurrency"]["max"] == 8, (
            "get_status should reflect updated max_concurrency"
        )

    # ==================== refresh_config() ====================

    def test_refresh_config_noop_when_version_unchanged(self, default_limiter_id):
        """Verify refresh_config is a no-op when versions already match."""
        # Arrange
        limiter = self._create_limiter(default_limiter_id)

        # Act
        changed = limiter.refresh_config()

        # Assert
        assert changed is False, (
            "refresh should report no change when version is current"
        )

    def test_refresh_config_applies_remote_change(
        self, redis_client, default_limiter_id
    ):
        """Verify refresh_config applies newer config written by another worker."""
        # Arrange
        limiter = self._create_limiter(default_limiter_id)
        new_config = {
            "limit": 50,
            "window": 60,
            "max_concurrency": 10,
            "max_age": 3600,
            "lease_duration": 30,
        }
        redis_client.hset(
            ManagedTestRateLimiter._REGISTRY_KEY, default_limiter_id, json.dumps(new_config)
        )
        redis_client.hincrby(ManagedTestRateLimiter._VERSION_KEY, default_limiter_id, 1)

        # Act
        changed = limiter.refresh_config()

        # Assert
        assert changed is True, "refresh should report config change"
        assert limiter.limit == 50, "refresh should apply updated limit"
        assert limiter.max_concurrency == 10, (
            "refresh should apply updated max_concurrency"
        )

    def test_refresh_config_window_change_sets_pause_until(
        self, redis_client, default_limiter_id
    ):
        """Verify refresh_config sets pause when remote config changes window size."""
        # Arrange
        limiter = self._create_limiter(default_limiter_id)
        new_config = {
            "limit": 10,
            "window": 30,
            "max_concurrency": 5,
            "max_age": 3600,
            "lease_duration": 30,
        }
        redis_client.hset(
            ManagedTestRateLimiter._REGISTRY_KEY, default_limiter_id, json.dumps(new_config)
        )
        redis_client.hincrby(ManagedTestRateLimiter._VERSION_KEY, default_limiter_id, 1)

        # Act
        changed = limiter.refresh_config()

        # Assert
        assert changed is True, "refresh should report config change"
        assert limiter.window == 30, "refresh should apply updated window"
        assert limiter._paused_until > time.time(), (
            "window change via refresh should set a future pause timestamp"
        )

    def test_refresh_config_returns_false_when_version_hash_missing(
        self, default_limiter_id
    ):
        """Verify refresh_config returns False when no version entry exists."""
        # Arrange
        limiter = self._create_limiter(default_limiter_id, persist=False)

        # Act
        changed = limiter.refresh_config()

        # Assert
        assert changed is False, (
            "refresh should return False when version tracking does not exist"
        )

    def test_refresh_config_handles_corrupted_redis_data(
        self, redis_client, default_limiter_id
    ):
        """Verify refresh_config handles malformed persisted JSON gracefully."""
        # Arrange
        limiter = self._create_limiter(default_limiter_id)
        original_state = (
            limiter.limit,
            limiter.window,
            limiter.max_concurrency,
            limiter.max_age,
            limiter.lease_duration,
        )
        redis_client.hset(
            ManagedTestRateLimiter._REGISTRY_KEY, default_limiter_id, "{invalid_json"
        )
        redis_client.hincrby(ManagedTestRateLimiter._VERSION_KEY, default_limiter_id, 1)

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

    def test_refresh_config_returns_false_when_registry_config_missing(
        self, redis_client, default_limiter_id
    ):
        """Verify refresh_config returns False when version exists but registry config is missing."""
        # Arrange
        limiter = self._create_limiter(default_limiter_id)
        original_version = limiter._config_version
        original_state = (
            limiter.limit,
            limiter.window,
            limiter.max_concurrency,
            limiter.max_age,
            limiter.lease_duration,
        )

        redis_client.hdel(ManagedTestRateLimiter._REGISTRY_KEY, default_limiter_id)
        redis_client.hincrby(ManagedTestRateLimiter._VERSION_KEY, default_limiter_id, 1)

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

    # ==================== _reset() ====================

    def test_reset_clears_cached_instances_and_configuration(self, default_limiter_id):
        """Verify _reset clears class cache and shared configuration."""
        # Arrange
        self._create_limiter(default_limiter_id)

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

    def test_subclass_isolation_separate_instances(self, redis_client):
        """Verify __init_subclass__ isolates _instances and _redis_client per subclass."""

        class IsolatedLimiterA(AbstractRedisManagedRateLimiter):
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

        class IsolatedLimiterB(AbstractRedisManagedRateLimiter):
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
