"""Huey-specific behavioural tests for the ``HueyRateLimiter`` implementation.

Fixture dependencies:
    - ``redis_client``, ``func_path``, ``payload``: from ``tests/conftest.py``.
    - ``limiter``, ``task_id``: from ``tests/implementations/huey/conftest.py``
      and ``tests/implementations/conftest.py``.
"""

import inspect
import logging
from unittest.mock import MagicMock, patch

import pytest
from huey import RedisHuey

from redis_rate_limiter import HueyRateLimiter
from redis_rate_limiter.core.limiters import build_enhanced_payload
from tests.helpers.utils import assert_log_emitted, find_task_in_buffer


@pytest.mark.behavior
class TestHueyRateLimiter:
    """Tests that are specific to the Huey backend dispatch and payload logic."""

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
    def test_dispatch_task_use_executor_true_calls_generic_worker(
        limiter, payload, task_id
    ):
        """Verify that ``_dispatch_task`` calls the generic
        worker when ``use_executor`` is true."""
        # Arrange
        enhanced_payload = build_enhanced_payload(payload, use_executor=True)

        # Act
        import redis_rate_limiter.backends.huey.tasks.worker as worker_module

        with patch.object(worker_module, "generic_rate_limited_worker") as mock_worker:
            limiter._dispatch_task("myapp.tasks.process", enhanced_payload, task_id)

        # Assert
        mock_worker.assert_called_once_with(
            limiter_id=limiter.id,
            func_path="myapp.tasks.process",
            payload=payload,
            _rate_limit_task_id=task_id,
        )

    @staticmethod
    def test_dispatch_task_use_executor_false_calls_custom_task(
        limiter, payload, task_id
    ):
        """Verify that ``_dispatch_task`` calls a custom task function
        directly when ``use_executor`` is false."""
        # Arrange
        func_path = "myapp.tasks.custom"
        enhanced_payload = build_enhanced_payload(payload, use_executor=False)
        mock_task = MagicMock()

        # Act
        with patch(
            "redis_rate_limiter.core.importing.import_string",
            return_value=mock_task,
        ):
            limiter._dispatch_task(func_path, enhanced_payload, task_id)

        # Assert
        mock_task.assert_called_once_with(payload, _rate_limit_task_id=task_id)

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
    def test_dispatch_task_call_failure_propagates(limiter, payload, task_id):
        """Verify that a task call failure
        propagates from ``_dispatch_task()``."""
        # Arrange
        enhanced_payload = build_enhanced_payload(payload, use_executor=True)

        # Act & Assert
        import redis_rate_limiter.backends.huey.tasks.worker as worker_module

        with patch.object(
            worker_module,
            "generic_rate_limited_worker",
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
        import redis_rate_limiter.backends.huey.tasks.worker as worker_module

        with patch.object(worker_module, "generic_rate_limited_worker") as mock_worker:
            limiter._dispatch_task("myapp.tasks.process", payload_without_meta, task_id)

        # Assert
        mock_worker.assert_called_once()

    @staticmethod
    def test_dispatch_task_missing_data_uses_empty_dict(limiter, task_id):
        """Verify that ``_dispatch_task`` uses an empty dict
        when the data key is absent."""
        # Arrange
        payload_without_data = {"meta": {"use_executor": True}}

        # Act
        import redis_rate_limiter.backends.huey.tasks.worker as worker_module

        with patch.object(worker_module, "generic_rate_limited_worker") as mock_worker:
            limiter._dispatch_task("myapp.tasks.process", payload_without_data, task_id)

        # Assert
        mock_worker.assert_called_once()
        call_kwargs = mock_worker.call_args[1]
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

    @staticmethod
    def test_register_worker_sets_module_variable(huey_instance):
        """Verify that ``register_worker`` sets the module-level
        ``generic_rate_limited_worker`` variable."""
        # Act
        from redis_rate_limiter.backends.huey.tasks.worker import register_worker

        register_worker(huey_instance)

        # Assert
        # Re-import to read the updated module-level variable.
        import redis_rate_limiter.backends.huey.tasks.worker as worker_module

        assert worker_module.generic_rate_limited_worker is not None, (
            "generic_rate_limited_worker should be set after register_worker"
        )

    @staticmethod
    def test_register_worker_skips_reregistration_for_same_instance(huey_instance):
        """Verify that ``register_worker`` skips re-registration
        when called again with the same Huey instance."""
        # Arrange
        import redis_rate_limiter.backends.huey.tasks.worker as worker_module
        from redis_rate_limiter.backends.huey.tasks.worker import register_worker

        register_worker(huey_instance)
        first_wrapper = worker_module.generic_rate_limited_worker

        # Act
        register_worker(huey_instance)

        # Assert
        assert worker_module.generic_rate_limited_worker is first_wrapper, (
            "same-instance call should reuse the existing task wrapper"
        )

    @staticmethod
    def test_register_worker_replaces_registration_for_different_instance():
        """Verify that ``register_worker`` clears the stale registration
        and re-registers when called with a different Huey instance."""
        # Arrange
        import redis_rate_limiter.backends.huey.tasks.worker as worker_module
        from redis_rate_limiter.backends.huey.tasks.worker import register_worker

        first_huey = RedisHuey("first", immediate=True)
        register_worker(first_huey)
        first_wrapper = worker_module.generic_rate_limited_worker

        # Act
        second_huey = RedisHuey("second", immediate=True)
        register_worker(second_huey)

        # Assert
        assert worker_module.generic_rate_limited_worker is not first_wrapper, (
            "different-instance call should produce a new task wrapper"
        )
        assert worker_module.generic_rate_limited_worker is not None, (
            "new task wrapper should not be None"
        )


# ---------------------------------------------------------------------------
# Observability tests
# ---------------------------------------------------------------------------


@pytest.mark.observability
class TestHueyDispatchObservability:
    """Observability tests for the ``_dispatch_task`` log emissions."""

    @staticmethod
    def test_dispatch_generic_worker_emits_debug_log(limiter, payload, task_id, caplog):
        """Verify that dispatching via the generic worker emits
        a DEBUG log with limiter id, task id, and func path."""
        # Arrange
        enhanced_payload = build_enhanced_payload(payload, use_executor=True)

        # Act
        with caplog.at_level(
            logging.DEBUG, logger="redis_rate_limiter.backends.huey.limiter"
        ):
            import redis_rate_limiter.backends.huey.tasks.worker as worker_module

            with patch.object(worker_module, "generic_rate_limited_worker"):
                limiter._dispatch_task("myapp.tasks.process", enhanced_payload, task_id)

        # Assert
        assert_log_emitted(
            caplog.records,
            level="DEBUG",
            label="[HueyRateLimiter]",
            required_fragments=[
                f"limiter={limiter.id}",
                f"task_id={task_id}",
                "func_path=myapp.tasks.process",
            ],
            message="should emit a debug log containing the limiter id, task id, and func path",
        )

    @staticmethod
    def test_dispatch_custom_task_emits_debug_log(limiter, payload, task_id, caplog):
        """Verify that dispatching via a custom task path emits
        a DEBUG log with limiter id, task id, and func path."""
        # Arrange
        func_path = "myapp.tasks.custom"
        enhanced_payload = build_enhanced_payload(payload, use_executor=False)
        mock_task = MagicMock()

        # Act
        with caplog.at_level(
            logging.DEBUG, logger="redis_rate_limiter.backends.huey.limiter"
        ):
            with patch(
                "redis_rate_limiter.core.importing.import_string",
                return_value=mock_task,
            ):
                limiter._dispatch_task(func_path, enhanced_payload, task_id)

        # Assert
        assert_log_emitted(
            caplog.records,
            level="DEBUG",
            label="[HueyRateLimiter]",
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
class TestHueyScheduleTaskSignatures:
    """Signature tests for ``HueyRateLimiter.schedule_task()`` default parameter values."""

    @staticmethod
    def test_schedule_task_use_executor_defaults_to_true():
        """Verify that the ``use_executor`` parameter defaults to ``True``.

        Mutation target: default value of ``use_executor`` in ``HueyRateLimiter.schedule_task()``.
        """
        # Arrange & Act
        sig = inspect.signature(HueyRateLimiter.schedule_task)

        # Assert
        assert sig.parameters["use_executor"].default is True, (
            "use_executor default should be True for generic worker dispatch"
        )
