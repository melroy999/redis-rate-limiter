"""Tests for rate limiter class-level API behavior.

This module validates class-level lifecycle and dynamic-config behavior:
``configure()``, ``create()``, ``get()``, ``update()``, ``refresh_config()``,
``_reset()``, and direct-construction deprecation warnings.

Current implementation under test: ``CeleryRateLimiter``.
"""

import json
import time
import warnings

import pytest

from celery_rate_limiter.limiters import (
    AbstractRedisManagedRateLimiter,
    CeleryRateLimiter,
)


class TestRateLimiterClassApi:
    """Test suite for class-level rate limiter API behavior."""

    @staticmethod
    def _create_limiter(
        limiter_id: str,
        **overrides,
    ) -> CeleryRateLimiter:
        """Create a limiter with stable defaults and optional overrides."""
        params = {
            "limit": 10,
            "window": 60,
            "max_concurrency": 5,
            "override": True,
        }
        params.update(overrides)
        return CeleryRateLimiter.create(limiter_id=limiter_id, **params)

    @staticmethod
    def _read_registry_config(redis_client, limiter_id: str) -> dict:
        """Read and decode persisted limiter config from Redis registry."""
        raw_config = redis_client.hget(CeleryRateLimiter._REGISTRY_KEY, limiter_id)
        assert raw_config is not None, (
            f"expected persisted config for limiter '{limiter_id}'"
        )
        return json.loads(raw_config)

    # ==================== configure() ====================

    def test_configure_sets_class_state(self, redis_client, celery_app):
        """Verify configure stores shared Redis and Celery references."""
        # Arrange
        CeleryRateLimiter._reset()

        # Act
        CeleryRateLimiter.configure(redis_client, celery_app=celery_app)

        # Assert
        assert CeleryRateLimiter._redis_client is redis_client, (
            "configure should store the provided Redis client"
        )
        assert CeleryRateLimiter._celery_app is celery_app, (
            "configure should store the provided Celery app"
        )

    def test_configure_without_celery_app_raises_error(self, redis_client):
        """Verify CeleryRateLimiter.configure() fails when celery_app is missing."""
        # Arrange
        CeleryRateLimiter._reset()

        # Act & Assert
        with pytest.raises(RuntimeError, match="celery_app"):
            CeleryRateLimiter.configure(redis_client)

        # Cleanup for test isolation.
        CeleryRateLimiter._reset()

    def test_create_without_configure_raises(self):
        """Verify create fails when configure has not been called."""
        # Arrange
        CeleryRateLimiter._reset()

        # Act & Assert
        with pytest.raises(RuntimeError, match="configure"):
            CeleryRateLimiter.create("api_unconfigured", limit=1, window=1, max_concurrency=1)

    def test_get_without_configure_raises(self):
        """Verify get fails when configure has not been called and cache misses."""
        # Arrange
        CeleryRateLimiter._reset()

        # Act & Assert
        with pytest.raises(RuntimeError, match="configure"):
            CeleryRateLimiter.get("api_unconfigured")

    # ==================== create() ====================

    def test_create_returns_configured_instance(self):
        """Verify create returns an instance configured with requested values."""
        # Act
        limiter = self._create_limiter("api_a")

        # Assert
        assert limiter.id == "api_a", "created limiter should keep requested id"
        assert limiter.limit == 10, "created limiter should keep requested limit"
        assert limiter.window == 60, "created limiter should keep requested window"
        assert limiter.max_concurrency == 5, (
            "created limiter should keep requested max_concurrency"
        )

    def test_create_caches_instance_locally(self):
        """Verify create stores the instance in class-level cache."""
        # Act
        limiter = self._create_limiter("api_b")

        # Assert
        assert "api_b" in CeleryRateLimiter._instances, (
            "created limiter should be present in local cache"
        )
        assert CeleryRateLimiter._instances["api_b"] is limiter, (
            "cached limiter should be the created instance"
        )

    def test_create_duplicate_without_override_raises(self):
        """Verify create rejects duplicate IDs unless override=True."""
        # Arrange
        self._create_limiter("api_c")

        # Act & Assert
        with pytest.raises(ValueError, match="already exists"):
            CeleryRateLimiter.create(
                "api_c",
                limit=10,
                window=60,
                max_concurrency=5,
                override=False,
            )

    def test_create_duplicate_with_override_replaces_cached_instance(self):
        """Verify override=True replaces the cached limiter for the same ID."""
        # Arrange
        first = self._create_limiter("api_d")

        # Act
        second = CeleryRateLimiter.create(
            "api_d",
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
        assert CeleryRateLimiter._instances["api_d"] is second, (
            "local cache should point to replacement instance"
        )

    def test_create_persist_writes_registry_config(self, redis_client):
        """Verify create persist=True writes limiter config to Redis registry."""
        # Act
        self._create_limiter("api_e")

        # Assert
        config = self._read_registry_config(redis_client, "api_e")
        assert config["limit"] == 10, "persisted config should include limit"
        assert config["window"] == 60, "persisted config should include window"
        assert config["max_concurrency"] == 5, (
            "persisted config should include max_concurrency"
        )

    def test_create_persist_increments_version(self, redis_client):
        """Verify repeated create with override increments Redis version counter."""
        # Arrange
        self._create_limiter("api_f")
        initial_version = int(redis_client.hget(CeleryRateLimiter._VERSION_KEY, "api_f"))

        # Act
        CeleryRateLimiter.create(
            "api_f",
            limit=20,
            window=60,
            max_concurrency=5,
            override=True,
        )

        # Assert
        updated_version = int(redis_client.hget(CeleryRateLimiter._VERSION_KEY, "api_f"))
        assert updated_version == initial_version + 1, (
            "override create should bump config version by one"
        )

    def test_create_without_persist_skips_registry_write(self, redis_client):
        """Verify create persist=False does not write to Redis registry."""
        # Act
        self._create_limiter("api_g", persist=False)

        # Assert
        assert redis_client.hget(CeleryRateLimiter._REGISTRY_KEY, "api_g") is None, (
            "persist=False should skip config registry write"
        )

    # ==================== get() ====================

    def test_get_returns_cached_instance(self):
        """Verify get returns cached instance without rehydration."""
        # Arrange
        created = self._create_limiter("api_h")

        # Act
        fetched = CeleryRateLimiter.get("api_h")

        # Assert
        assert fetched is created, "get should return the cached limiter instance"

    def test_get_hydrates_from_redis_on_cache_miss(self):
        """Verify get hydrates limiter config from Redis when cache misses."""
        # Arrange
        self._create_limiter("api_i")
        CeleryRateLimiter._instances.clear()

        # Act
        hydrated = CeleryRateLimiter.get("api_i")

        # Assert
        assert hydrated.id == "api_i", "hydrated limiter should retain persisted id"
        assert hydrated.limit == 10, "hydrated limiter should retain persisted limit"
        assert hydrated.window == 60, "hydrated limiter should retain persisted window"
        assert hydrated.max_concurrency == 5, (
            "hydrated limiter should retain persisted max_concurrency"
        )

    def test_get_hydration_loads_current_config_version(self, redis_client):
        """Verify hydrated limiter tracks the current persisted config version."""
        # Arrange
        self._create_limiter("api_i2")
        expected_version = int(redis_client.hget(CeleryRateLimiter._VERSION_KEY, "api_i2"))
        CeleryRateLimiter._instances.clear()

        # Act
        hydrated = CeleryRateLimiter.get("api_i2")

        # Assert
        assert hydrated._config_version == expected_version, (
            "hydrated limiter should load current config version"
        )

    def test_get_nonexistent_limiter_raises_value_error(self):
        """Verify get raises ValueError when limiter does not exist anywhere."""
        # Act & Assert
        with pytest.raises(ValueError, match="not found"):
            CeleryRateLimiter.get("api_nonexistent")

    # ==================== update() ====================

    def test_update_can_change_limit(self):
        """Verify update can change only the rate limit value."""
        # Arrange
        self._create_limiter("api_j")

        # Act
        updated = CeleryRateLimiter.update("api_j", limit=50)

        # Assert
        assert updated.limit == 50, "updated limiter should reflect new limit"

    def test_update_can_change_max_concurrency(self):
        """Verify update can change only the max_concurrency value."""
        # Arrange
        self._create_limiter("api_k")

        # Act
        updated = CeleryRateLimiter.update("api_k", max_concurrency=20)

        # Assert
        assert updated.max_concurrency == 20, (
            "updated limiter should reflect new max_concurrency"
        )

    def test_update_persists_new_config_and_bumps_version(self, redis_client):
        """Verify update writes new config and increments version counter."""
        # Arrange
        self._create_limiter("api_l")
        initial_version = int(redis_client.hget(CeleryRateLimiter._VERSION_KEY, "api_l"))

        # Act
        CeleryRateLimiter.update("api_l", limit=50)

        # Assert
        updated_version = int(redis_client.hget(CeleryRateLimiter._VERSION_KEY, "api_l"))
        assert updated_version == initial_version + 1, (
            "update should bump config version by one"
        )

        config = self._read_registry_config(redis_client, "api_l")
        assert config["limit"] == 50, "update should persist new limit"

    def test_update_window_change_sets_pause_until(self):
        """Verify changing window sets a transition pause for safe rollover."""
        # Arrange
        limiter = self._create_limiter("api_m")
        assert limiter._paused_until == 0.0, "pause should start disabled"
        previous_window = limiter.window

        # Act
        CeleryRateLimiter.update("api_m", window=30)

        # Assert
        assert limiter.window == 30, "window should update to requested value"
        assert limiter._paused_until > time.time(), (
            "window update should set a future pause timestamp"
        )
        assert limiter._paused_until <= time.time() + previous_window + 1, (
            "window update pause should not exceed old window plus small tolerance"
        )

    def test_update_preserves_unspecified_fields(self):
        """Verify update keeps fields unchanged when no override is provided."""
        # Arrange
        self._create_limiter(
            "api_n",
            max_age=7200,
            lease_duration=45,
        )

        # Act
        CeleryRateLimiter.update("api_n", limit=50)
        limiter = CeleryRateLimiter.get("api_n")

        # Assert
        assert limiter.limit == 50, "limit should be updated"
        assert limiter.window == 60, "window should remain unchanged"
        assert limiter.max_concurrency == 5, "max_concurrency should remain unchanged"
        assert limiter.max_age == 7200, "max_age should remain unchanged"
        assert limiter.lease_duration == 45, "lease_duration should remain unchanged"

    # ==================== refresh_config() ====================

    def test_refresh_config_noop_when_version_unchanged(self):
        """Verify refresh_config is a no-op when versions already match."""
        # Arrange
        limiter = self._create_limiter("api_o")

        # Act
        changed = limiter.refresh_config()

        # Assert
        assert changed is False, "refresh should report no change when version is current"

    def test_refresh_config_applies_remote_change(self, redis_client):
        """Verify refresh_config applies newer config written by another worker."""
        # Arrange
        limiter = self._create_limiter("api_p")
        new_config = {
            "limit": 50,
            "window": 60,
            "max_concurrency": 10,
            "max_age": 3600,
            "lease_duration": 30,
        }
        redis_client.hset(CeleryRateLimiter._REGISTRY_KEY, "api_p", json.dumps(new_config))
        redis_client.hincrby(CeleryRateLimiter._VERSION_KEY, "api_p", 1)

        # Act
        changed = limiter.refresh_config()

        # Assert
        assert changed is True, "refresh should report config change"
        assert limiter.limit == 50, "refresh should apply updated limit"
        assert limiter.max_concurrency == 10, (
            "refresh should apply updated max_concurrency"
        )

    def test_refresh_config_window_change_sets_pause_until(self, redis_client):
        """Verify refresh_config sets pause when remote config changes window size."""
        # Arrange
        limiter = self._create_limiter("api_q")
        new_config = {
            "limit": 10,
            "window": 30,
            "max_concurrency": 5,
            "max_age": 3600,
            "lease_duration": 30,
        }
        redis_client.hset(CeleryRateLimiter._REGISTRY_KEY, "api_q", json.dumps(new_config))
        redis_client.hincrby(CeleryRateLimiter._VERSION_KEY, "api_q", 1)

        # Act
        changed = limiter.refresh_config()

        # Assert
        assert changed is True, "refresh should report config change"
        assert limiter.window == 30, "refresh should apply updated window"
        assert limiter._paused_until > time.time(), (
            "window change via refresh should set a future pause timestamp"
        )

    def test_refresh_config_returns_false_when_version_hash_missing(self):
        """Verify refresh_config returns False when no version entry exists."""
        # Arrange
        limiter = self._create_limiter("api_r", persist=False)

        # Act
        changed = limiter.refresh_config()

        # Assert
        assert changed is False, (
            "refresh should return False when version tracking does not exist"
        )

    # ==================== _reset() ====================

    def test_reset_clears_cached_instances_and_configuration(self):
        """Verify _reset clears class cache and shared configuration."""
        # Arrange
        self._create_limiter("api_s")

        # Act
        CeleryRateLimiter._reset()

        # Assert
        assert len(CeleryRateLimiter._instances) == 0, (
            "reset should clear local limiter cache"
        )
        assert CeleryRateLimiter._redis_client is None, (
            "reset should clear shared redis client"
        )
        assert CeleryRateLimiter._celery_app is None, (
            "reset should clear shared celery app"
        )

    # ==================== deprecation warnings ====================

    def test_direct_construction_emits_deprecation_warning(
        self,
        redis_client,
        celery_app,
    ):
        """Verify direct construction emits a deprecation warning."""
        # Act & Assert
        with pytest.warns(DeprecationWarning, match="deprecated"):
            CeleryRateLimiter(
                redis_client=redis_client,
                celery_app=celery_app,
                limiter_id="api_t",
                limit=5,
                window=60,
                max_concurrency=2,
            )

    def test_class_api_create_does_not_emit_deprecation_warning(self):
        """Verify class API create path does not emit deprecation warnings."""
        # Act
        with warnings.catch_warnings(record=True) as captured:
            warnings.simplefilter("always")
            self._create_limiter("api_u")

        # Assert
        deprecation_warnings = [
            warning
            for warning in captured
            if issubclass(warning.category, DeprecationWarning)
        ]
        assert not deprecation_warnings, (
            "class API create should not emit deprecation warnings"
        )

    def test_subclass_isolation_separate_instances(self):
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

            def _dispatch_task(self, func_path: str, payload: dict, task_id: str) -> None:
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

            def _dispatch_task(self, func_path: str, payload: dict, task_id: str) -> None:
                return None

            def _schedule_drain(self, delay: float = 0.0) -> None:
                return None

        # Arrange
        IsolatedLimiterA._instances["a"] = object()
        IsolatedLimiterA._redis_client = object()

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
