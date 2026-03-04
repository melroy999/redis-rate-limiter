"""Tests for task data helper methods: signature, in-flight TTL, and cleanup.

This module covers ``_get_task_signature_str`` determinism,
``_get_inflight_ttl`` calculation, and ``_cleanup_inflight_key`` behavior
for both sync and async implementations.

Fixture dependencies:
    - ``redis_client``, ``async_redis_client``: from ``tests/conftest.py``.
    - ``generic_limiter``, ``async_generic_limiter``: from ``tests/implementations/conftest.py``.
"""

import inspect
import logging
from unittest.mock import patch

from redis_rate_limiter.core.limiters import DistributedRateLimiterMixin
from tests.helpers.utils import assert_log_emitted

# ---------------------------------------------------------------------------
# Behavioral tests
# ---------------------------------------------------------------------------


class TestTaskSignature:
    """Tests for the task-signature helper behavior."""

    @staticmethod
    def test_task_signature_is_deterministic_across_key_orders(generic_limiter):
        """Verify that ``_get_task_signature_str`` is deterministic regardless of dictionary key order."""
        # Arrange
        payload_a = {"user_id": 123, "flags": {"vip": True, "beta": False}}
        payload_b = {"flags": {"beta": False, "vip": True}, "user_id": 123}

        # Act
        signature_a = generic_limiter._get_task_signature_str(
            "myapp.tasks.process", payload_a
        )
        signature_b = generic_limiter._get_task_signature_str(
            "myapp.tasks.process", payload_b
        )

        # Assert
        assert signature_a == signature_b, (
            "task signature should be identical regardless of key insertion order"
        )


class TestInflightTtl:
    """Tests for the in-flight TTL calculation."""

    @staticmethod
    def test_inflight_ttl_defaults_to_limiter_max_age(generic_limiter):
        """Verify that the in-flight TTL includes max_age plus the lease and window slack."""
        # Arrange
        expected = (
            generic_limiter.max_age
            + generic_limiter.lease_duration
            + generic_limiter.window
        )

        # Act
        ttl = generic_limiter._get_inflight_ttl()

        # Assert
        assert ttl == expected, (
            "default inflight TTL should be max_age + lease_duration + window"
        )

    @staticmethod
    def test_inflight_ttl_uses_per_task_override(generic_limiter):
        """Verify that a per-task max_age override drives the in-flight TTL calculation."""
        # Arrange
        override = 7
        expected = override + generic_limiter.lease_duration + generic_limiter.window

        # Act
        ttl = generic_limiter._get_inflight_ttl(max_age_override=override)

        # Assert
        assert ttl == expected, (
            "override inflight TTL should use task max_age + lease_duration + window"
        )


class TestCleanupInflightKey:
    """Tests for the best-effort in-flight key cleanup on scheduling failures."""

    @staticmethod
    def test_cleanup_inflight_key_suppresses_redis_failure(generic_limiter, caplog):
        """Verify that ``_cleanup_inflight_key`` does not propagate Redis exceptions.

        The warning log is the only observable proof of suppression (Section 4.3 exception).
        """
        # Arrange
        inflight_key = f"{generic_limiter.id}:inflight:cleanup-test"

        # Act & Assert
        with caplog.at_level(
            logging.WARNING, logger="redis_rate_limiter.core.limiters"
        ):
            with patch.object(
                generic_limiter.redis,
                "delete",
                side_effect=ConnectionError("redis down"),
            ):
                # This invocation must not raise.
                generic_limiter._cleanup_inflight_key(inflight_key, "cleanup-test")

        # Assert
        assert_log_emitted(
            caplog.records,
            level="WARNING",
            required_fragments=[
                f"limiter={generic_limiter.id}",
                "task_id=cleanup-test",
                f"inflight_key={inflight_key}",
                "redis down",
            ],
            message="should emit a warning log containing the limiter id, task id, inflight key, and error",
        )

    @staticmethod
    def test_cleanup_inflight_key_deletes_redis_key(generic_limiter, redis_client):
        """Verify that ``_cleanup_inflight_key`` removes the in-flight key from Redis."""
        # Arrange
        inflight_key = f"{generic_limiter.id}:inflight:cleanup-del"
        redis_client.set(inflight_key, "1")
        assert redis_client.exists(inflight_key) == 1, (
            "precondition: inflight key must exist before cleanup"
        )

        # Act
        generic_limiter._cleanup_inflight_key(inflight_key, "cleanup-del")

        # Assert
        assert redis_client.exists(inflight_key) == 0, (
            "inflight key should be removed after cleanup"
        )

    @staticmethod
    def test_cleanup_inflight_key_handles_missing_key_gracefully(generic_limiter):
        """Verify that ``_cleanup_inflight_key`` does not raise when the key does not exist."""
        # Arrange
        inflight_key = f"{generic_limiter.id}:inflight:nonexistent"

        # Act & Assert
        # This invocation must not raise.
        generic_limiter._cleanup_inflight_key(inflight_key, "nonexistent")


class TestAsyncCleanupInflightKey:
    """Tests for the async best-effort in-flight key cleanup on scheduling failures."""

    @staticmethod
    async def test_cleanup_inflight_key_suppresses_redis_failure(
        async_generic_limiter, caplog
    ):
        """Verify that the async ``_cleanup_inflight_key`` does not propagate Redis exceptions.

        The warning log is the only observable proof of suppression (Section 4.3 exception).
        """
        # Arrange
        inflight_key = f"{async_generic_limiter.id}:inflight:cleanup-test"

        # Act & Assert
        with caplog.at_level(
            logging.WARNING, logger="redis_rate_limiter.core.async_limiters"
        ):
            with patch.object(
                async_generic_limiter.redis,
                "delete",
                side_effect=ConnectionError("redis down"),
            ):
                # This invocation must not raise.
                await async_generic_limiter._cleanup_inflight_key(
                    inflight_key, "cleanup-test"
                )

        # Assert
        assert_log_emitted(
            caplog.records,
            level="WARNING",
            required_fragments=[
                f"limiter={async_generic_limiter.id}",
                "task_id=cleanup-test",
                "redis down",
            ],
            message="should emit a warning log containing the limiter id, task id, and error",
        )

    @staticmethod
    async def test_cleanup_inflight_key_deletes_redis_key(
        async_generic_limiter, async_redis_client
    ):
        """Verify that the async ``_cleanup_inflight_key`` removes the in-flight key from Redis."""
        # Arrange
        inflight_key = f"{async_generic_limiter.id}:inflight:cleanup-del"
        await async_redis_client.set(inflight_key, "1")
        assert await async_redis_client.exists(inflight_key) == 1, (
            "precondition: inflight key must exist before cleanup"
        )

        # Act
        await async_generic_limiter._cleanup_inflight_key(inflight_key, "cleanup-del")

        # Assert
        assert await async_redis_client.exists(inflight_key) == 0, (
            "inflight key should be removed after cleanup"
        )

    @staticmethod
    async def test_cleanup_inflight_key_handles_missing_key_gracefully(
        async_generic_limiter,
    ):
        """Verify that the async ``_cleanup_inflight_key`` does not raise when the key does not exist."""
        # Arrange
        inflight_key = f"{async_generic_limiter.id}:inflight:nonexistent"

        # Act & Assert
        # This invocation must not raise.
        await async_generic_limiter._cleanup_inflight_key(inflight_key, "nonexistent")


# ---------------------------------------------------------------------------
# Observability tests
# ---------------------------------------------------------------------------


class TestCleanupInflightKeyObservability:
    """Observability tests for the ``_cleanup_inflight_key`` debug log emission."""

    @staticmethod
    def test_cleanup_inflight_key_emits_debug_log(
        generic_limiter, redis_client, caplog
    ):
        """Verify that ``_cleanup_inflight_key`` emits a DEBUG log with the removal result."""
        # Arrange
        inflight_key = f"{generic_limiter.id}:inflight:cleanup-del"
        redis_client.set(inflight_key, "1")

        # Act
        with caplog.at_level(logging.DEBUG, logger="redis_rate_limiter.core.limiters"):
            generic_limiter._cleanup_inflight_key(inflight_key, "cleanup-del")

        # Assert
        assert_log_emitted(
            caplog.records,
            level="DEBUG",
            required_fragments=[
                f"limiter={generic_limiter.id}",
                "task_id=cleanup-del",
                f"inflight_key={inflight_key}",
                "removed=1",
            ],
            message="should emit a debug log containing the limiter id, task id, inflight key, and removal result",
        )


class TestAsyncCleanupInflightKeyObservability:
    """Observability tests for the async ``_cleanup_inflight_key`` debug log emission."""

    @staticmethod
    async def test_cleanup_inflight_key_emits_debug_log(
        async_generic_limiter, async_redis_client, caplog
    ):
        """Verify that the async ``_cleanup_inflight_key`` emits a DEBUG log with the removal result."""
        # Arrange
        inflight_key = f"{async_generic_limiter.id}:inflight:cleanup-del"
        await async_redis_client.set(inflight_key, "1")

        # Act
        with caplog.at_level(
            logging.DEBUG, logger="redis_rate_limiter.core.async_limiters"
        ):
            await async_generic_limiter._cleanup_inflight_key(
                inflight_key, "cleanup-del"
            )

        # Assert
        assert_log_emitted(
            caplog.records,
            level="DEBUG",
            required_fragments=[
                f"limiter={async_generic_limiter.id}",
                "task_id=cleanup-del",
                f"inflight_key={inflight_key}",
                "removed=1",
            ],
            message="should emit a debug log containing the limiter id, task id, inflight key, and removal result",
        )


# ---------------------------------------------------------------------------
# Signature tests
# ---------------------------------------------------------------------------


class TestTaskDataHelperSignatures:
    """Signature tests for task data helper default parameter values."""

    @staticmethod
    def test_get_inflight_ttl_max_age_override_defaults_to_none():
        """Verify that the ``max_age_override`` parameter defaults to ``None``.

        Mutation target: ``max_age_override`` default value in ``DistributedRateLimiterMixin._get_inflight_ttl``.
        """
        # Arrange & Act
        sig = inspect.signature(DistributedRateLimiterMixin._get_inflight_ttl)

        # Assert
        assert sig.parameters["max_age_override"].default is None, (
            "max_age_override default must be None"
        )
