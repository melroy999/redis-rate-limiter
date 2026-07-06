"""Dramatiq-specific behavioural tests for the ``DramatiqRateLimiter`` implementation.

Fixture dependencies:
    - ``redis_client``, ``func_path``, ``payload``: from ``tests/conftest.py``.
    - ``limiter``, ``task_id``: from ``tests/implementations/dramatiq/conftest.py``
      and ``tests/implementations/conftest.py``.
"""

import inspect
import logging
from unittest.mock import MagicMock, patch

import pytest

from redis_rate_limiter import DramatiqRateLimiter
from redis_rate_limiter.core.limiters import build_enhanced_payload
from tests.helpers.utils import assert_log_emitted, find_task_in_buffer


@pytest.mark.behavior
class TestDramatiqRateLimiter:
    """Tests that are specific to the Dramatiq backend dispatch and payload logic."""

    @staticmethod
    def test_constructor_stores_broker_instance(limiter, dramatiq_broker):
        """Verify that the constructor stores the provided broker instance."""
        # Assert
        assert limiter.broker is dramatiq_broker, (
            "limiter must store the broker instance passed during construction"
        )

    @staticmethod
    def test_schedule_task_defaults_use_executor_to_true(
        limiter, redis_client, func_path, payload
    ):
        """Verify that ``schedule_task`` defaults
        ``use_executor`` to ``True`` when not specified."""
        # Arrange
        limiter._drain_paused_until = 5_000_000_000.0

        # Act
        success, task_id = limiter.schedule_task(func_path, payload)

        # Assert
        assert success is True, "scheduling should succeed"
        task_data = find_task_in_buffer(redis_client, limiter.buffer_key, task_id)
        assert task_data is not None, "scheduled task should exist in buffer"
        assert task_data["payload"]["meta"]["use_executor"] is True, (
            "task metadata should default use_executor to true"
        )
        assert task_data["payload"]["data"] == payload, (
            "original payload should be forwarded as the data field"
        )

    @staticmethod
    def test_schedule_task_with_use_executor_false_stores_meta(
        limiter, redis_client, func_path, payload
    ):
        """Verify that ``schedule_task`` stores
        ``use_executor=False`` in the task payload metadata."""
        # Arrange
        limiter._drain_paused_until = 5_000_000_000.0

        # Act
        success, task_id = limiter.schedule_task(func_path, payload, use_executor=False)

        # Assert
        assert success is True, "scheduling should succeed"
        task_data = find_task_in_buffer(redis_client, limiter.buffer_key, task_id)
        assert task_data is not None, "scheduled task should exist in buffer"
        assert task_data["payload"]["meta"]["use_executor"] is False, (
            "task metadata should store use_executor as false"
        )
        assert task_data["func_path"] == func_path, (
            "stored task data should contain the original func_path"
        )

    @staticmethod
    def test_dispatch_task_use_executor_true_sends_generic_worker(
        limiter, payload, task_id
    ):
        """Verify that ``_dispatch_task`` sends a message to the generic
        worker when ``use_executor`` is true."""
        # Arrange
        enhanced_payload = build_enhanced_payload(payload, use_executor=True)

        # Act
        with patch(
            "redis_rate_limiter.backends.dramatiq.limiter.DramatiqRateLimiter._dispatch_task.__module__",
            create=True,
        ):
            from redis_rate_limiter.backends.dramatiq.tasks.worker import (
                generic_rate_limited_worker,
            )

            with patch.object(generic_rate_limited_worker, "send") as mock_send:
                limiter._dispatch_task("myapp.tasks.process", enhanced_payload, task_id)

        # Assert
        mock_send.assert_called_once_with(
            limiter_id=limiter.id,
            func_path="myapp.tasks.process",
            payload=payload,
            _rate_limit_task_id=task_id,
        )

    @staticmethod
    def test_dispatch_task_use_executor_false_sends_custom_actor(
        limiter, payload, task_id
    ):
        """Verify that ``_dispatch_task`` sends a message to a custom actor
        directly when ``use_executor`` is false."""
        # Arrange
        func_path = "myapp.tasks.custom"
        enhanced_payload = build_enhanced_payload(payload, use_executor=False)
        mock_actor = MagicMock()

        # Act
        with patch(
            "redis_rate_limiter.core.importing.import_string",
            return_value=mock_actor,
        ):
            limiter._dispatch_task(func_path, enhanced_payload, task_id)

        # Assert
        mock_actor.send.assert_called_once_with(payload, _rate_limit_task_id=task_id)

    @staticmethod
    def test_schedule_drain_wakes_drain_loop(limiter):
        """Verify that ``_schedule_drain`` delegates to the base-class ``DrainLoop``."""
        # Arrange
        delay = 1.75

        # Act
        with patch.object(limiter._drain_loop, "wake") as mock_wake:
            limiter._schedule_drain(delay=delay)

        # Assert
        mock_wake.assert_called_once_with(delay)

    @staticmethod
    def test_dispatch_task_send_failure_propagates(limiter, payload, task_id):
        """Verify that a ``send()`` failure
        propagates from ``_dispatch_task()``."""
        # Arrange
        enhanced_payload = build_enhanced_payload(payload, use_executor=True)

        # Act & Assert
        from redis_rate_limiter.backends.dramatiq.tasks.worker import (
            generic_rate_limited_worker,
        )

        with patch.object(
            generic_rate_limited_worker,
            "send",
            side_effect=ConnectionError("redis down"),
        ):
            with pytest.raises(ConnectionError, match="redis down"):
                limiter._dispatch_task("myapp.tasks.process", enhanced_payload, task_id)

    @staticmethod
    def test_enhanced_payload_structure(limiter, payload):
        """Verify that ``build_enhanced_payload`` wraps the
        payload in the expected data/meta structure."""
        # Act
        enhanced_payload = build_enhanced_payload(payload, use_executor=False)

        # Assert
        assert enhanced_payload["data"] == payload, (
            "enhanced payload should preserve original data"
        )
        assert enhanced_payload["meta"] == {"use_executor": False}, (
            "enhanced payload meta should contain use_executor flag"
        )

    @staticmethod
    def test_dispatch_task_missing_meta_uses_default_executor(limiter, task_id):
        """Verify that ``_dispatch_task`` defaults to the generic
        worker when the meta key is absent."""
        # Arrange
        # A raw payload without the ``meta`` wrapper triggers the default path.
        payload_without_meta = {"data": {"key": "value"}}

        # Act
        from redis_rate_limiter.backends.dramatiq.tasks.worker import (
            generic_rate_limited_worker,
        )

        with patch.object(generic_rate_limited_worker, "send") as mock_send:
            limiter._dispatch_task("myapp.tasks.process", payload_without_meta, task_id)

        # Assert
        mock_send.assert_called_once()

    @staticmethod
    def test_dispatch_task_missing_data_uses_empty_dict(limiter, task_id):
        """Verify that ``_dispatch_task`` uses an empty dict
        when the data key is absent."""
        # Arrange
        payload_without_data = {"meta": {"use_executor": True}}

        # Act
        from redis_rate_limiter.backends.dramatiq.tasks.worker import (
            generic_rate_limited_worker,
        )

        with patch.object(generic_rate_limited_worker, "send") as mock_send:
            limiter._dispatch_task("myapp.tasks.process", payload_without_data, task_id)

        # Assert
        mock_send.assert_called_once()
        call_kwargs = mock_send.call_args[1]
        assert call_kwargs["payload"] == {}, (
            "missing data key should result in an empty dict payload"
        )

    @staticmethod
    def test_schedule_task_forwards_max_age_override(limiter, func_path, payload):
        """Verify that ``schedule_task`` passes the ``max_age``
        override through to the parent scheduler."""
        # Arrange
        max_age_override = 60

        # Act
        # Spy on _get_inflight_ttl to verify the max_age argument is forwarded
        # to the parent scheduler. This avoids a race with the drain loop, which
        # can consume the task and delete the inflight key before a TTL check.
        with patch.object(
            limiter, "_get_inflight_ttl", wraps=limiter._get_inflight_ttl
        ) as mock_ttl:
            success, _ = limiter.schedule_task(
                func_path, payload, max_age=max_age_override
            )

        # Assert
        assert success is True, "scheduling should succeed"
        mock_ttl.assert_called_once_with(max_age_override=max_age_override)


# ---------------------------------------------------------------------------
# Observability tests
# ---------------------------------------------------------------------------


@pytest.mark.observability
class TestDramatiqDispatchObservability:
    """Observability tests for the ``_dispatch_task`` log emissions."""

    @staticmethod
    def test_dispatch_generic_worker_emits_debug_log(limiter, payload, task_id, caplog):
        """Verify that dispatching via the generic worker emits
        a DEBUG log with limiter id, task id, and func path."""
        # Arrange
        enhanced_payload = build_enhanced_payload(payload, use_executor=True)

        # Act
        with caplog.at_level(
            logging.DEBUG, logger="redis_rate_limiter.backends.dramatiq.limiter"
        ):
            from redis_rate_limiter.backends.dramatiq.tasks.worker import (
                generic_rate_limited_worker,
            )

            with patch.object(generic_rate_limited_worker, "send"):
                limiter._dispatch_task("myapp.tasks.process", enhanced_payload, task_id)

        # Assert
        assert_log_emitted(
            caplog.records,
            level="DEBUG",
            label="[DramatiqRateLimiter]",
            required_fragments=[
                f"limiter={limiter.id}",
                f"task_id={task_id}",
                "func_path=myapp.tasks.process",
            ],
            message="should emit a debug log containing the limiter id, task id, and func path",
        )

    @staticmethod
    def test_dispatch_custom_actor_emits_debug_log(limiter, payload, task_id, caplog):
        """Verify that dispatching via a custom actor path emits
        a DEBUG log with limiter id, task id, and func path."""
        # Arrange
        func_path = "myapp.tasks.custom"
        enhanced_payload = build_enhanced_payload(payload, use_executor=False)
        mock_actor = MagicMock()

        # Act
        with caplog.at_level(
            logging.DEBUG, logger="redis_rate_limiter.backends.dramatiq.limiter"
        ):
            with patch(
                "redis_rate_limiter.core.importing.import_string",
                return_value=mock_actor,
            ):
                limiter._dispatch_task(func_path, enhanced_payload, task_id)

        # Assert
        assert_log_emitted(
            caplog.records,
            level="DEBUG",
            label="[DramatiqRateLimiter]",
            required_fragments=[
                f"limiter={limiter.id}",
                f"task_id={task_id}",
                f"func_path={func_path}",
            ],
            message="should emit a debug log containing the limiter id, task id, and func path",
        )


# ---------------------------------------------------------------------------
# Signature tests
# ---------------------------------------------------------------------------


@pytest.mark.signature
class TestDramatiqScheduleTaskSignatures:
    """Signature tests for ``DramatiqRateLimiter.schedule_task()`` default parameter values."""

    @staticmethod
    def test_schedule_task_use_executor_defaults_to_true():
        """Verify that the ``use_executor`` parameter defaults to ``True``.

        Mutation target: default value of ``use_executor`` in ``DramatiqRateLimiter.schedule_task()``.
        """
        # Arrange & Act
        sig = inspect.signature(DramatiqRateLimiter.schedule_task)

        # Assert
        assert sig.parameters["use_executor"].default is True, (
            "use_executor default should be True for generic worker dispatch"
        )
