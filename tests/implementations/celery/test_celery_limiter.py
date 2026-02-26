"""Celery-specific behavioural tests for the ``CeleryRateLimiter`` implementation.

Fixture dependencies:
    - ``redis_client``, ``func_path``, ``payload``: from ``tests/conftest.py``.
    - ``limiter``, ``task_id``: from ``tests/implementations/celery/conftest.py``
      and ``tests/implementations/conftest.py``.
"""

import inspect
import json
import logging
from unittest.mock import patch

import pytest

from celery_rate_limiter import CeleryRateLimiter
from tests.helpers.utils import assert_log_emitted


class TestCeleryRateLimiter:
    """Tests that are specific to the Celery backend dispatch and payload logic."""

    @staticmethod
    def test_schedule_task_defaults_use_executor_to_true(
        limiter, redis_client, func_path, payload
    ):
        """Verify that ``schedule_task`` defaults ``use_executor`` to ``True`` when not specified."""
        # Act
        success, task_id = limiter.schedule_task(func_path, payload)

        # Assert
        assert success is True, "scheduling should succeed"
        all_members = redis_client.zrange(limiter.buffer_key, 0, -1)
        results = [m for m in all_members if f'"{task_id}"' in m]
        assert len(results) == 1, "scheduled task should exist in buffer exactly once"
        task_data = json.loads(results[0])
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
        """Verify that ``schedule_task`` stores ``use_executor=False`` in the task payload metadata."""
        # Act
        success, task_id = limiter.schedule_task(func_path, payload, use_executor=False)

        # Assert
        assert success is True, "scheduling should succeed"
        all_members = redis_client.zrange(limiter.buffer_key, 0, -1)
        results = [m for m in all_members if f'"{task_id}"' in m]
        assert len(results) == 1, "scheduled task should exist in buffer exactly once"
        task_data = json.loads(results[0])
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
        """Verify that ``_dispatch_task`` sends the generic worker task when ``use_executor`` is true."""
        # Arrange
        enhanced_payload = limiter._get_enhanced_payload(payload, use_executor=True)

        # Act
        with patch.object(limiter.app, "send_task") as mock_send_task:
            limiter._dispatch_task("myapp.tasks.process", enhanced_payload, task_id)

        # Assert
        mock_send_task.assert_called_once_with(
            "celery_rate_limiter.generic_worker",
            kwargs={
                "limiter_id": limiter.id,
                "func_path": "myapp.tasks.process",
                "payload": payload,
                "_rate_limit_task_id": task_id,
            },
        )

    @staticmethod
    def test_dispatch_task_use_executor_false_sends_custom_task(
        limiter, payload, task_id
    ):
        """Verify that ``_dispatch_task`` sends a custom task directly when ``use_executor`` is false."""
        # Arrange
        func_path = "myapp.tasks.custom"
        enhanced_payload = limiter._get_enhanced_payload(payload, use_executor=False)

        # Act
        with patch.object(limiter.app, "send_task") as mock_send_task:
            limiter._dispatch_task(func_path, enhanced_payload, task_id)

        # Assert
        mock_send_task.assert_called_once_with(
            func_path,
            args=[payload],
            kwargs={"_rate_limit_task_id": task_id},
        )

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
    def test_dispatch_task_send_task_failure_propagates(limiter, payload, task_id):
        """Verify that a ``send_task()`` failure propagates from ``_dispatch_task()``."""
        # Arrange
        enhanced_payload = limiter._get_enhanced_payload(payload, use_executor=True)

        # Act & Assert
        with patch.object(
            limiter.app, "send_task", side_effect=Exception("broker down")
        ):
            with pytest.raises(Exception, match="broker down"):
                limiter._dispatch_task("myapp.tasks.process", enhanced_payload, task_id)

    @staticmethod
    def test_enhanced_payload_structure(limiter, payload):
        """Verify that ``_get_enhanced_payload`` wraps the payload in the expected data/meta structure."""
        # Act
        enhanced_payload = limiter._get_enhanced_payload(payload, use_executor=False)

        # Assert
        assert enhanced_payload["data"] == payload, (
            "enhanced payload should preserve original data"
        )
        assert enhanced_payload["meta"] == {"use_executor": False}, (
            "enhanced payload meta should contain use_executor flag"
        )

    @staticmethod
    def test_dispatch_task_missing_meta_uses_default_executor(limiter, task_id):
        """Verify that ``_dispatch_task`` defaults to the generic worker when the meta key is absent."""
        # Arrange
        # A raw payload without the ``meta`` wrapper triggers the default path.
        payload_without_meta = {"data": {"key": "value"}}

        # Act
        with patch.object(limiter.app, "send_task") as mock_send_task:
            limiter._dispatch_task("myapp.tasks.process", payload_without_meta, task_id)

        # Assert
        mock_send_task.assert_called_once()
        call_args = mock_send_task.call_args
        assert call_args[0][0] == "celery_rate_limiter.generic_worker", (
            "missing meta should default to the generic worker task name"
        )

    @staticmethod
    def test_dispatch_task_missing_data_uses_empty_dict(limiter, task_id):
        """Verify that ``_dispatch_task`` uses an empty dict when the data key is absent."""
        # Arrange
        payload_without_data = {"meta": {"use_executor": True}}

        # Act
        with patch.object(limiter.app, "send_task") as mock_send_task:
            limiter._dispatch_task("myapp.tasks.process", payload_without_data, task_id)

        # Assert
        mock_send_task.assert_called_once()
        call_kwargs = mock_send_task.call_args[1]
        assert call_kwargs["kwargs"]["payload"] == {}, (
            "missing data key should result in an empty dict payload"
        )

    @staticmethod
    def test_schedule_task_forwards_max_age_override(limiter, func_path, payload):
        """Verify that ``schedule_task`` passes the ``max_age`` override through to the parent scheduler."""
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
    def test_dispatch_task_custom_path_sends_data_as_list_arg(
        limiter, payload, task_id
    ):
        """Verify that the custom task path sends data wrapped in a single-element list."""
        # Arrange
        enhanced_payload = limiter._get_enhanced_payload(payload, use_executor=False)

        # Act
        with patch.object(limiter.app, "send_task") as mock_send_task:
            limiter._dispatch_task("myapp.tasks.custom", enhanced_payload, task_id)

        # Assert
        call_kwargs = mock_send_task.call_args[1]
        assert "args" in call_kwargs, (
            "custom task path should pass args keyword argument"
        )
        assert isinstance(call_kwargs["args"], list), (
            "args should be a list, not a tuple or other sequence"
        )
        assert len(call_kwargs["args"]) == 1, (
            "args should contain exactly one element (the data dict)"
        )
        assert call_kwargs["args"][0] == payload, (
            "args should contain the original payload data"
        )


# ---------------------------------------------------------------------------
# Observability tests
# ---------------------------------------------------------------------------


class TestCeleryDispatchObservability:
    """Observability tests for the ``_dispatch_task`` log emissions."""

    @staticmethod
    def test_dispatch_generic_worker_emits_debug_log(limiter, payload, task_id, caplog):
        """Verify that dispatching via the generic worker emits a DEBUG log with limiter id, task id, and func path."""
        # Arrange
        enhanced_payload = limiter._get_enhanced_payload(payload, use_executor=True)

        # Act
        with caplog.at_level(
            logging.DEBUG, logger="celery_rate_limiter.backends.celery.limiter"
        ):
            with patch.object(limiter.app, "send_task"):
                limiter._dispatch_task("myapp.tasks.process", enhanced_payload, task_id)

        # Assert
        assert_log_emitted(
            caplog.records,
            level="DEBUG",
            required_fragments=[
                f"limiter={limiter.id}",
                f"task_id={task_id}",
                "func_path=myapp.tasks.process",
            ],
            message="should emit a debug log containing the limiter id, task id, and func path",
        )

    @staticmethod
    def test_dispatch_custom_task_emits_debug_log(limiter, payload, task_id, caplog):
        """Verify that dispatching via a custom task path emits a DEBUG log with limiter id, task id, and func path."""
        # Arrange
        func_path = "myapp.tasks.custom"
        enhanced_payload = limiter._get_enhanced_payload(payload, use_executor=False)

        # Act
        with caplog.at_level(
            logging.DEBUG, logger="celery_rate_limiter.backends.celery.limiter"
        ):
            with patch.object(limiter.app, "send_task"):
                limiter._dispatch_task(func_path, enhanced_payload, task_id)

        # Assert
        assert_log_emitted(
            caplog.records,
            level="DEBUG",
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


class TestCeleryScheduleTaskSignatures:
    """Signature tests for ``CeleryRateLimiter.schedule_task()`` default parameter values."""

    @staticmethod
    def test_schedule_task_use_executor_defaults_to_true():
        """Verify that the ``use_executor`` parameter defaults to ``True``.

        Mutation target: default value of ``use_executor`` in ``CeleryRateLimiter.schedule_task()``.
        """
        # Arrange & Act
        sig = inspect.signature(CeleryRateLimiter.schedule_task)

        # Assert
        assert sig.parameters["use_executor"].default is True, (
            "use_executor default should be True for generic worker dispatch"
        )
