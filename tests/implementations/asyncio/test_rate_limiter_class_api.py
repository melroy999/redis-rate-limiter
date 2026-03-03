"""Tests for the async rate limiter class-level API behavior.

This module validates class-level lifecycle and dynamic-configuration behavior,
including ``configure()``, ``create()``, ``get()``, ``update()``,
``refresh_config()``, and ``_reset()``.

The current implementation under test is ``AsyncManagedTestRateLimiter``, a
test-only async backend.

Fixture dependencies:
    - ``async_redis_client``, ``limiter_id``: from ``tests/conftest.py``.
    - ``_reset_asyncio_limiter_class_state``: from ``tests/implementations/asyncio/conftest.py``.
"""

import inspect
import json
import logging
import time
from typing import Any, ClassVar, Optional

import pytest

from celery_rate_limiter.core import (
    AbstractAsyncDistributedRateLimiter,
    AsyncManagedRateLimiter,
)
from tests.helpers.utils import assert_log_emitted


class AsyncManagedTestRateLimiter(
    AsyncManagedRateLimiter, AbstractAsyncDistributedRateLimiter
):
    """Test-only managed limiter used for backend-agnostic async class API tests."""

    _backend_label: ClassVar[Optional[str]] = None

    @classmethod
    def _configure_backend(cls, **backend_context: Any) -> None:
        backend_label = backend_context.get("backend_label")
        if backend_label is None:
            raise RuntimeError(
                "AsyncManagedTestRateLimiter.configure(redis_client, backend_label) "
                "must be called before create() or get()."
            )
        cls._backend_label = str(backend_label)

    @classmethod
    def _has_backend_context(cls) -> bool:
        return cls._backend_label is not None

    @classmethod
    def _get_instance_context(cls) -> dict[str, Any]:
        return {"drain_enabled": False}

    @classmethod
    def _reset_backend_context(cls) -> None:
        cls._backend_label = None

    @classmethod
    def _configure_hint(cls) -> str:
        return "AsyncManagedTestRateLimiter.configure(redis_client, backend_label)"

    async def _dispatch_task(self, func_path: str, payload: dict, task_id: str) -> None:
        return None

    def _has_local_capacity(self) -> bool:
        return True


@pytest.fixture(autouse=True)
async def _reset_managed_limiter_class_state(async_redis_client):
    """Ensure that the managed class API tests begin from a clean, configured state."""
    AsyncManagedTestRateLimiter._reset()
    AsyncManagedTestRateLimiter.configure(async_redis_client, backend_label="test")
    yield
    AsyncManagedTestRateLimiter._reset()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def create_test_limiter(
    limiter_id: str,
    **overrides: Any,
) -> AsyncManagedTestRateLimiter:
    """Create a limiter instance with stable defaults and optional overrides."""
    params: dict[str, Any] = {
        "limit": 10,
        "window": 60,
        "max_concurrency": 5,
        "override": True,
    }
    params.update(overrides)
    return await AsyncManagedTestRateLimiter.create(  # type: ignore[return-value]
        limiter_id=limiter_id, **params
    )


async def read_registry_config(redis_client: Any, limiter_id: str) -> dict:
    """Read and decode the persisted limiter configuration from the Redis registry."""
    raw_config = await redis_client.hget(
        AsyncManagedTestRateLimiter._REGISTRY_KEY, limiter_id
    )
    assert raw_config is not None, (
        f"expected persisted config for limiter '{limiter_id}'"
    )
    return json.loads(raw_config)


class TestConfigure:
    """Test suite for the ``configure()`` class method."""

    @staticmethod
    async def test_configure_sets_class_state(async_redis_client):
        """Verify that ``configure()`` stores the shared Redis client and backend context."""
        # Arrange
        AsyncManagedTestRateLimiter._reset()

        # Act
        AsyncManagedTestRateLimiter.configure(
            async_redis_client, backend_label="custom"
        )

        # Assert
        assert AsyncManagedTestRateLimiter._redis_client is async_redis_client, (
            "configure should store the provided Redis client"
        )
        assert AsyncManagedTestRateLimiter._backend_label == "custom", (
            "configure should store backend-specific context"
        )

    @staticmethod
    async def test_configure_without_backend_label_raises_error(
        async_redis_client,
    ):
        """Verify that ``configure()`` fails when the required backend context is missing."""
        # Arrange
        AsyncManagedTestRateLimiter._reset()

        # Act & Assert
        with pytest.raises(RuntimeError, match="backend_label"):
            AsyncManagedTestRateLimiter.configure(async_redis_client)

        # Cleanup for test isolation.
        AsyncManagedTestRateLimiter._reset()


class TestCreate:
    """Test suite for the ``create()`` class method."""

    @staticmethod
    async def test_create_without_configure_raises(limiter_id):
        """Verify that ``create()`` fails when ``configure()`` has not been called."""
        # Arrange
        AsyncManagedTestRateLimiter._reset()

        # Act & Assert
        with pytest.raises(RuntimeError, match="configure"):
            await AsyncManagedTestRateLimiter.create(
                limiter_id, limit=1, window=1, max_concurrency=1
            )

    @staticmethod
    async def test_create_returns_configured_instance(limiter_id):
        """Verify that ``create()`` returns an instance configured with the requested values."""
        # Act
        limiter = await create_test_limiter(limiter_id)

        # Assert
        assert limiter.id == limiter_id, "created limiter should keep requested id"
        assert limiter.limit == 10, "created limiter should keep requested limit"
        assert limiter.window == 60, "created limiter should keep requested window"
        assert limiter.max_concurrency == 5, (
            "created limiter should keep requested max_concurrency"
        )

    @staticmethod
    async def test_create_caches_instance_locally(limiter_id):
        """Verify that ``create()`` stores the instance in the class-level cache."""
        # Act
        limiter = await create_test_limiter(limiter_id)

        # Assert
        assert limiter_id in AsyncManagedTestRateLimiter._instances, (
            "created limiter should be present in local cache"
        )
        assert AsyncManagedTestRateLimiter._instances[limiter_id] is limiter, (
            "cached limiter should be the created instance"
        )

    @staticmethod
    async def test_create_duplicate_without_override_raises(limiter_id):
        """Verify that ``create()`` rejects duplicate IDs unless ``override=True``."""
        # Arrange
        await create_test_limiter(limiter_id)

        # Act & Assert
        with pytest.raises(ValueError, match="already exists"):
            await AsyncManagedTestRateLimiter.create(
                limiter_id,
                limit=10,
                window=60,
                max_concurrency=5,
                override=False,
            )

    @staticmethod
    async def test_create_duplicate_with_override_replaces_cached_instance(
        limiter_id,
    ):
        """Verify that ``override=True`` replaces the cached limiter for the same ID."""
        # Arrange
        first = await create_test_limiter(limiter_id)

        # Act
        second = await AsyncManagedTestRateLimiter.create(
            limiter_id,
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
        assert AsyncManagedTestRateLimiter._instances[limiter_id] is second, (
            "local cache should point to replacement instance"
        )

    @staticmethod
    async def test_create_persist_writes_registry_config(
        async_redis_client, limiter_id
    ):
        """Verify that ``create()`` with ``persist=True`` writes the limiter configuration to the Redis registry."""
        # Act
        await create_test_limiter(limiter_id)

        # Assert
        config = await read_registry_config(async_redis_client, limiter_id)
        assert config["limit"] == 10, "persisted config should include limit"
        assert config["window"] == 60, "persisted config should include window"
        assert config["max_concurrency"] == 5, (
            "persisted config should include max_concurrency"
        )

    @staticmethod
    async def test_create_persist_increments_version(async_redis_client, limiter_id):
        """Verify that repeated ``create()`` calls with ``override`` increment the Redis version counter."""
        # Arrange
        await create_test_limiter(limiter_id)
        initial_version = int(
            await async_redis_client.hget(
                AsyncManagedTestRateLimiter._VERSION_KEY, limiter_id
            )
        )

        # Act
        await AsyncManagedTestRateLimiter.create(
            limiter_id,
            limit=20,
            window=60,
            max_concurrency=5,
            override=True,
        )

        # Assert
        updated_version = int(
            await async_redis_client.hget(
                AsyncManagedTestRateLimiter._VERSION_KEY, limiter_id
            )
        )
        assert updated_version == initial_version + 1, (
            "override create should bump config version by one"
        )

    @staticmethod
    async def test_create_without_persist_skips_registry_write(
        async_redis_client, limiter_id
    ):
        """Verify that ``create()`` with ``persist=False`` does not write to the Redis registry."""
        # Act
        await create_test_limiter(limiter_id, persist=False)

        # Assert
        assert (
            await async_redis_client.hget(
                AsyncManagedTestRateLimiter._REGISTRY_KEY, limiter_id
            )
            is None
        ), "persist=False should skip config registry write"


class TestGet:
    """Test suite for the ``get()`` class method."""

    @staticmethod
    async def test_get_without_configure_raises(limiter_id):
        """Verify that ``get()`` fails when ``configure()`` has not been called and the cache misses."""
        # Arrange
        AsyncManagedTestRateLimiter._reset()

        # Act & Assert
        with pytest.raises(RuntimeError, match="configure"):
            await AsyncManagedTestRateLimiter.get(limiter_id)

    @staticmethod
    async def test_get_returns_cached_instance(limiter_id):
        """Verify that ``get()`` returns the cached instance without rehydration."""
        # Arrange
        created = await create_test_limiter(limiter_id)

        # Act
        fetched = await AsyncManagedTestRateLimiter.get(limiter_id)

        # Assert
        assert fetched is created, "get should return the cached limiter instance"

    @staticmethod
    async def test_get_hydrates_from_redis_on_cache_miss(limiter_id):
        """Verify that ``get()`` hydrates the limiter configuration from Redis when the cache misses."""
        # Arrange
        await create_test_limiter(limiter_id)
        AsyncManagedTestRateLimiter._instances.clear()

        # Act
        hydrated = await AsyncManagedTestRateLimiter.get(limiter_id)

        # Assert
        assert hydrated.id == limiter_id, "hydrated limiter should retain persisted id"
        assert hydrated.limit == 10, "hydrated limiter should retain persisted limit"
        assert hydrated.window == 60, "hydrated limiter should retain persisted window"
        assert hydrated.max_concurrency == 5, (
            "hydrated limiter should retain persisted max_concurrency"
        )

    @staticmethod
    async def test_get_hydration_loads_current_config_version(
        async_redis_client, limiter_id
    ):
        """Verify that the hydrated limiter tracks the current persisted configuration version."""
        # Arrange
        await create_test_limiter(limiter_id)
        expected_version = int(
            await async_redis_client.hget(
                AsyncManagedTestRateLimiter._VERSION_KEY, limiter_id
            )
        )
        AsyncManagedTestRateLimiter._instances.clear()

        # Act
        hydrated = await AsyncManagedTestRateLimiter.get(limiter_id)

        # Assert
        assert hydrated._config_version == expected_version, (
            "hydrated limiter should load current config version"
        )

    @staticmethod
    async def test_get_nonexistent_limiter_raises_value_error():
        """Verify that ``get()`` raises a ValueError when the limiter does not exist in any location."""
        missing_limiter_id = "limiter_id_for_get_nonexistent_limiter_test"

        # Act & Assert
        with pytest.raises(ValueError, match="not found"):
            await AsyncManagedTestRateLimiter.get(missing_limiter_id)


class TestUpdate:
    """Test suite for the ``update()`` class method."""

    @staticmethod
    async def test_update_can_change_limit(limiter_id):
        """Verify that ``update()`` can change only the rate limit value."""
        # Arrange
        await create_test_limiter(limiter_id)

        # Act
        updated = await AsyncManagedTestRateLimiter.update(limiter_id, limit=50)

        # Assert
        assert updated.limit == 50, "updated limiter should reflect new limit"

    @staticmethod
    async def test_update_can_change_max_concurrency(limiter_id):
        """Verify that ``update()`` can change only the max_concurrency value."""
        # Arrange
        await create_test_limiter(limiter_id)

        # Act
        updated = await AsyncManagedTestRateLimiter.update(
            limiter_id, max_concurrency=20
        )

        # Assert
        assert updated.max_concurrency == 20, (
            "updated limiter should reflect new max_concurrency"
        )

    @staticmethod
    async def test_update_can_change_max_age_and_lease_duration(limiter_id):
        """Verify that ``update()`` can change the task max_age and lease_duration values."""
        # Arrange
        await create_test_limiter(limiter_id, max_age=3600, lease_duration=30)

        # Act
        updated = await AsyncManagedTestRateLimiter.update(
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
    async def test_update_persists_new_config_and_bumps_version(
        async_redis_client, limiter_id
    ):
        """Verify that ``update()`` writes the new configuration and increments the version counter."""
        # Arrange
        await create_test_limiter(limiter_id)
        initial_version = int(
            await async_redis_client.hget(
                AsyncManagedTestRateLimiter._VERSION_KEY, limiter_id
            )
        )

        # Act
        await AsyncManagedTestRateLimiter.update(limiter_id, limit=50)

        # Assert
        updated_version = int(
            await async_redis_client.hget(
                AsyncManagedTestRateLimiter._VERSION_KEY, limiter_id
            )
        )
        assert updated_version == initial_version + 1, (
            "update should bump config version by one"
        )

        config = await read_registry_config(async_redis_client, limiter_id)
        assert config["limit"] == 50, "update should persist new limit"

    @staticmethod
    async def test_update_window_change_sets_pause_until(limiter_id):
        """Verify that changing the window sets a transition pause for safe rollover."""
        # Arrange
        limiter = await create_test_limiter(limiter_id)
        assert limiter._paused_until == 0.0, "pause should start disabled"
        previous_window = limiter.window

        # Act
        await AsyncManagedTestRateLimiter.update(limiter_id, window=30)

        # Assert
        assert limiter.window == 30, "window should update to requested value"
        assert limiter._paused_until > time.time(), (
            "window update should set a future pause timestamp"
        )
        assert limiter._paused_until <= time.time() + previous_window + 1, (
            "window update pause should not exceed old window plus small tolerance"
        )

    @staticmethod
    async def test_update_preserves_unspecified_fields(limiter_id):
        """Verify that ``update()`` keeps fields unchanged when no override is provided for them."""
        # Arrange
        await create_test_limiter(
            limiter_id,
            max_age=7200,
            lease_duration=45,
        )

        # Act
        await AsyncManagedTestRateLimiter.update(limiter_id, limit=50)
        limiter = await AsyncManagedTestRateLimiter.get(limiter_id)

        # Assert
        assert limiter.limit == 50, "limit should be updated"
        assert limiter.window == 60, "window should remain unchanged"
        assert limiter.max_concurrency == 5, "max_concurrency should remain unchanged"
        assert limiter.max_age == 7200, "max_age should remain unchanged"
        assert limiter.lease_duration == 45, "lease_duration should remain unchanged"

    @staticmethod
    async def test_update_preserves_jitter_settings(limiter_id):
        """Verify that ``update()`` does not override the existing jitter configuration."""
        # Arrange
        await create_test_limiter(
            limiter_id,
            jitter_enabled=False,
            jitter_min_pct=0.11,
            jitter_max_pct=0.22,
        )

        # Act
        await AsyncManagedTestRateLimiter.update(limiter_id, limit=99)
        limiter = await AsyncManagedTestRateLimiter.get(limiter_id)

        # Assert
        assert limiter.limit == 99, "limit should be updated"
        assert limiter.jitter_enabled is False, "jitter_enabled should remain unchanged"
        assert limiter.jitter_min_pct == 0.11, "jitter_min_pct should remain unchanged"
        assert limiter.jitter_max_pct == 0.22, "jitter_max_pct should remain unchanged"

    @staticmethod
    async def test_get_status_reflects_updated_config(limiter_id):
        """Verify that ``get_status()`` returns updated values after ``update()`` modifies the configuration."""
        # Arrange
        await create_test_limiter(limiter_id, limit=10, max_concurrency=5)

        # Act
        await AsyncManagedTestRateLimiter.update(
            limiter_id, limit=20, max_concurrency=8
        )
        limiter = await AsyncManagedTestRateLimiter.get(limiter_id)
        status = await limiter.get_status()

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
    async def test_refresh_config_noop_when_version_unchanged(limiter_id):
        """Verify that ``refresh_config()`` is a no-op when the versions already match."""
        # Arrange
        limiter = await create_test_limiter(limiter_id)

        # Act
        changed = await limiter.refresh_config()

        # Assert
        assert changed is False, (
            "refresh should report no change when version is current"
        )

    @staticmethod
    async def test_refresh_config_applies_remote_change(async_redis_client, limiter_id):
        """Verify that ``refresh_config()`` applies a newer configuration written by another worker."""
        # Arrange
        limiter = await create_test_limiter(limiter_id)
        new_config = {
            "limit": 50,
            "window": 60,
            "max_concurrency": 10,
            "max_age": 3600,
            "lease_duration": 30,
        }
        await async_redis_client.hset(
            AsyncManagedTestRateLimiter._REGISTRY_KEY,
            limiter_id,
            json.dumps(new_config),
        )
        await async_redis_client.hincrby(
            AsyncManagedTestRateLimiter._VERSION_KEY, limiter_id, 1
        )

        # Act
        changed = await limiter.refresh_config()

        # Assert
        assert changed is True, "refresh should report config change"
        assert limiter.limit == 50, "refresh should apply updated limit"
        assert limiter.max_concurrency == 10, (
            "refresh should apply updated max_concurrency"
        )

    @staticmethod
    async def test_refresh_config_window_change_sets_pause_until(
        async_redis_client, limiter_id
    ):
        """Verify that ``refresh_config()`` sets a pause when the remote configuration changes the window size."""
        # Arrange
        limiter = await create_test_limiter(limiter_id)
        new_config = {
            "limit": 10,
            "window": 30,
            "max_concurrency": 5,
            "max_age": 3600,
            "lease_duration": 30,
        }
        await async_redis_client.hset(
            AsyncManagedTestRateLimiter._REGISTRY_KEY,
            limiter_id,
            json.dumps(new_config),
        )
        await async_redis_client.hincrby(
            AsyncManagedTestRateLimiter._VERSION_KEY, limiter_id, 1
        )

        # Act
        changed = await limiter.refresh_config()

        # Assert
        assert changed is True, "refresh should report config change"
        assert limiter.window == 30, "refresh should apply updated window"
        assert limiter._paused_until > time.time(), (
            "window change via refresh should set a future pause timestamp"
        )

    @staticmethod
    async def test_refresh_config_returns_false_when_version_hash_missing(
        limiter_id,
    ):
        """Verify that ``refresh_config()`` returns False when no version entry exists."""
        # Arrange
        limiter = await create_test_limiter(limiter_id, persist=False)

        # Act
        changed = await limiter.refresh_config()

        # Assert
        assert changed is False, (
            "refresh should return False when version tracking does not exist"
        )

    @staticmethod
    async def test_refresh_config_handles_corrupted_redis_data(
        async_redis_client, limiter_id
    ):
        """Verify that ``refresh_config()`` handles malformed persisted JSON gracefully."""
        # Arrange
        limiter = await create_test_limiter(limiter_id)
        original_state = (
            limiter.limit,
            limiter.window,
            limiter.max_concurrency,
            limiter.max_age,
            limiter.lease_duration,
        )
        await async_redis_client.hset(
            AsyncManagedTestRateLimiter._REGISTRY_KEY,
            limiter_id,
            "{invalid_json",
        )
        await async_redis_client.hincrby(
            AsyncManagedTestRateLimiter._VERSION_KEY, limiter_id, 1
        )

        # Act
        changed = await limiter.refresh_config()

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
    async def test_refresh_config_returns_false_when_registry_config_missing(
        async_redis_client, limiter_id
    ):
        """Verify that ``refresh_config()`` returns False when the version exists but the registry configuration is missing."""
        # Arrange
        limiter = await create_test_limiter(limiter_id)
        original_version = limiter._config_version
        original_state = (
            limiter.limit,
            limiter.window,
            limiter.max_concurrency,
            limiter.max_age,
            limiter.lease_duration,
        )

        await async_redis_client.hdel(
            AsyncManagedTestRateLimiter._REGISTRY_KEY, limiter_id
        )
        await async_redis_client.hincrby(
            AsyncManagedTestRateLimiter._VERSION_KEY, limiter_id, 1
        )

        # Act
        changed = await limiter.refresh_config()

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
    async def test_reset_clears_cached_instances_and_configuration(
        limiter_id,
    ):
        """Verify that ``_reset()`` clears the class cache and the shared configuration."""
        # Arrange
        await create_test_limiter(limiter_id)

        # Act
        AsyncManagedTestRateLimiter._reset()

        # Assert
        assert len(AsyncManagedTestRateLimiter._instances) == 0, (
            "reset should clear local limiter cache"
        )
        assert AsyncManagedTestRateLimiter._redis_client is None, (
            "reset should clear shared redis client"
        )
        assert AsyncManagedTestRateLimiter._backend_label is None, (
            "reset should clear shared backend context"
        )

    @staticmethod
    async def test_direct_construction_raises_runtime_error(
        async_redis_client,
    ):
        """Verify that direct ``__init__`` invocation, bypassing ``create()``/``get()``, raises a ``RuntimeError``."""
        # Act & Assert
        with pytest.raises(RuntimeError, match="Direct.*construction is not supported"):
            AsyncManagedTestRateLimiter(
                redis_client=async_redis_client,
                limiter_id="direct_construction_test",
                limit=5,
                window=1.0,
                max_concurrency=2,
            )

    @staticmethod
    async def test_subclass_isolation_separate_instances(async_redis_client):
        """Verify that ``__init_subclass__`` isolates ``_instances`` and ``_redis_client`` per subclass."""

        class IsolatedLimiterA(
            AsyncManagedRateLimiter, AbstractAsyncDistributedRateLimiter
        ):
            @classmethod
            def _configure_backend(cls, **backend_context: Any) -> None:
                return None

            @classmethod
            def _has_backend_context(cls) -> bool:
                return True

            @classmethod
            def _get_instance_context(cls) -> dict[str, Any]:
                return {"drain_enabled": False}

            @classmethod
            def _reset_backend_context(cls) -> None:
                return None

            @classmethod
            def _configure_hint(cls) -> str:
                return "isolated configure hint"

            async def _dispatch_task(
                self, func_path: str, payload: dict, task_id: str
            ) -> None:
                return None

            def _has_local_capacity(self) -> bool:
                return True

        class IsolatedLimiterB(
            AsyncManagedRateLimiter, AbstractAsyncDistributedRateLimiter
        ):
            @classmethod
            def _configure_backend(cls, **backend_context: Any) -> None:
                return None

            @classmethod
            def _has_backend_context(cls) -> bool:
                return True

            @classmethod
            def _get_instance_context(cls) -> dict[str, Any]:
                return {"drain_enabled": False}

            @classmethod
            def _reset_backend_context(cls) -> None:
                return None

            @classmethod
            def _configure_hint(cls) -> str:
                return "isolated configure hint"

            async def _dispatch_task(
                self, func_path: str, payload: dict, task_id: str
            ) -> None:
                return None

            def _has_local_capacity(self) -> bool:
                return True

        # Arrange
        isolated_limiter_a_cache_key = (
            "isolated_limiter_a_instance_for_subclass_isolation_test"
        )
        IsolatedLimiterA._instances[isolated_limiter_a_cache_key] = (
            IsolatedLimiterA.__new__(IsolatedLimiterA)
        )
        IsolatedLimiterA._redis_client = async_redis_client

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


# ---------------------------------------------------------------------------
# Observability tests
# ---------------------------------------------------------------------------


class TestConfigureObservability:
    """Observability tests for the ``configure()`` class method log emission."""

    @staticmethod
    async def test_configure_emits_info_log(async_redis_client, caplog):
        """Verify that ``configure()`` emits an INFO log with the class name."""
        # Arrange
        AsyncManagedTestRateLimiter._reset()

        # Act
        with caplog.at_level(logging.INFO, logger="celery_rate_limiter.core.managed"):
            AsyncManagedTestRateLimiter.configure(
                async_redis_client, backend_label="test"
            )

        # Assert
        assert_log_emitted(
            caplog.records,
            level="INFO",
            required_fragments=["AsyncManagedTestRateLimiter", "configured"],
            message="should emit an info log confirming configuration",
        )


class TestCreateObservability:
    """Observability tests for the ``create()`` class method log emission."""

    @staticmethod
    async def test_create_emits_info_log(limiter_id, caplog):
        """Verify that ``create()`` emits an INFO log with the limiter id and persist flag."""
        # Act
        with caplog.at_level(logging.INFO, logger="celery_rate_limiter.core.managed"):
            await create_test_limiter(limiter_id)

        # Assert
        assert_log_emitted(
            caplog.records,
            level="INFO",
            required_fragments=[
                "AsyncManagedTestRateLimiter",
                "created",
                f"limiter={limiter_id}",
                "persist=True",
            ],
            message="should emit an info log with class name, limiter id, and persist flag",
        )


class TestGetObservability:
    """Observability tests for the ``get()`` class method log emission."""

    @staticmethod
    async def test_get_cache_hit_emits_debug_log(limiter_id, caplog):
        """Verify that ``get()`` emits a DEBUG log when the instance is resolved from the local cache."""
        # Arrange
        await create_test_limiter(limiter_id)

        # Act
        with caplog.at_level(logging.DEBUG, logger="celery_rate_limiter.core.managed"):
            await AsyncManagedTestRateLimiter.get(limiter_id)

        # Assert
        assert_log_emitted(
            caplog.records,
            level="DEBUG",
            required_fragments=[
                "AsyncManagedTestRateLimiter",
                "resolved from local cache",
                f"limiter={limiter_id}",
            ],
            message="should emit a debug log for local cache resolution",
        )

    @staticmethod
    async def test_get_hydration_emits_debug_log(limiter_id, caplog):
        """Verify that ``get()`` emits a DEBUG log when the instance is hydrated from Redis."""
        # Arrange
        await create_test_limiter(limiter_id)
        AsyncManagedTestRateLimiter._instances.clear()

        # Act
        with caplog.at_level(logging.DEBUG, logger="celery_rate_limiter.core.managed"):
            await AsyncManagedTestRateLimiter.get(limiter_id)

        # Assert
        assert_log_emitted(
            caplog.records,
            level="DEBUG",
            required_fragments=[
                "AsyncManagedTestRateLimiter",
                "hydrated from Redis",
                f"limiter={limiter_id}",
            ],
            message="should emit a debug log for Redis hydration",
        )


class TestUpdateObservability:
    """Observability tests for the ``update()`` class method log emission."""

    @staticmethod
    async def test_update_emits_info_log(limiter_id, caplog):
        """Verify that ``update()`` emits an INFO log with the limiter id and overrides."""
        # Arrange
        await create_test_limiter(limiter_id)

        # Act
        with caplog.at_level(logging.INFO, logger="celery_rate_limiter.core.managed"):
            await AsyncManagedTestRateLimiter.update(limiter_id, limit=50)

        # Assert
        assert_log_emitted(
            caplog.records,
            level="INFO",
            required_fragments=[
                "AsyncManagedTestRateLimiter",
                "updated",
                f"limiter={limiter_id}",
                "overrides={'limit': 50}",
            ],
            message="should emit an info log with class name, limiter id, and overrides on update",
        )


class TestRefreshConfigObservability:
    """Observability tests for the ``refresh_config()`` instance method log emission."""

    @staticmethod
    async def test_refresh_config_success_emits_info_log(
        async_redis_client, limiter_id, caplog
    ):
        """Verify that a successful ``refresh_config()`` emits an INFO log with the limiter id and version."""
        # Arrange
        limiter = await create_test_limiter(limiter_id)
        new_config = {
            "limit": 50,
            "window": 60,
            "max_concurrency": 10,
            "max_age": 3600,
            "lease_duration": 30,
        }
        await async_redis_client.hset(
            AsyncManagedTestRateLimiter._REGISTRY_KEY,
            limiter_id,
            json.dumps(new_config),
        )
        await async_redis_client.hincrby(
            AsyncManagedTestRateLimiter._VERSION_KEY, limiter_id, 1
        )

        # Act
        with caplog.at_level(logging.INFO, logger="celery_rate_limiter.core.managed"):
            await limiter.refresh_config()

        # Assert
        assert_log_emitted(
            caplog.records,
            level="INFO",
            required_fragments=[f"limiter={limiter_id}", "version=2"],
            message="should emit an info log with limiter id and version on successful refresh",
        )

    @staticmethod
    async def test_refresh_config_corrupted_data_emits_warning_log(
        async_redis_client, limiter_id, caplog
    ):
        """Verify that ``refresh_config()`` emits a WARNING log when the persisted configuration is malformed."""
        # Arrange
        limiter = await create_test_limiter(limiter_id)
        await async_redis_client.hset(
            AsyncManagedTestRateLimiter._REGISTRY_KEY, limiter_id, "{invalid_json"
        )
        await async_redis_client.hincrby(
            AsyncManagedTestRateLimiter._VERSION_KEY, limiter_id, 1
        )

        # Act
        with caplog.at_level(
            logging.WARNING, logger="celery_rate_limiter.core.managed"
        ):
            await limiter.refresh_config()

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
        ``AsyncManagedRateLimiter.create``.
        """
        # Arrange & Act
        sig = inspect.signature(AsyncManagedRateLimiter.create)

        # Assert
        assert sig.parameters["persist"].default is True, (
            "persist default should be True so that limiter config is stored in Redis"
        )
        assert sig.parameters["override"].default is False, (
            "override default should be False to prevent accidental limiter replacement"
        )
