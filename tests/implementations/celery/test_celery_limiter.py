"""Celery-specific behavioural tests for the ``CeleryRateLimiter`` implementation."""

import inspect
import json
import logging
from unittest.mock import patch

import pytest

from celery_rate_limiter import CeleryRateLimiter


class TestCeleryRateLimiter:
    """Tests that are specific to the Celery backend dispatch and payload logic."""

    @staticmethod
    def test_schedule_task_use_executor_default_is_true():
        """Verify that the ``use_executor`` parameter defaults to ``True`` via signature inspection."""
        # Arrange
        sig = inspect.signature(CeleryRateLimiter.schedule_task)

        # Assert
        assert sig.parameters["use_executor"].default is True, (
            "use_executor default should be True for generic worker dispatch"
        )

    @staticmethod
    def test_schedule_task_defaults_use_executor_to_true(
        limiter, redis_client, func_path, payload
    ):
        """Verify that ``schedule_task`` defaults ``use_executor`` to ``True`` when not specified."""
        # Act
        success, task_id = limiter.schedule_task(func_path, payload)

        # Assert
        assert success is True, "scheduling should succeed"
        _, results = redis_client.zscan(limiter.buffer_key, match=f'*"{task_id}"*')
        assert len(results) == 1, "scheduled task should exist in buffer exactly once"
        task_data = json.loads(results[0][0])
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
        success, task_id = limiter.schedule_task(
            func_path, payload, use_executor=False
        )

        # Assert
        assert success is True, "scheduling should succeed"
        _, results = redis_client.zscan(limiter.buffer_key, match=f'*"{task_id}"*')
        assert len(results) == 1, "scheduled task should exist in buffer exactly once"
        task_data = json.loads(results[0][0])
        assert task_data["payload"]["meta"]["use_executor"] is False, (
            "task metadata should store use_executor as false"
        )
        assert task_data["func_path"] == func_path, (
            "stored task data should contain the original func_path"
        )

    @staticmethod
    def test_dispatch_task_use_executor_true_sends_generic_worker(
        limiter, payload, task_id, caplog
    ):
        """Verify that ``_dispatch_task`` sends the generic worker task when ``use_executor`` is true."""
        # Arrange
        enhanced_payload = limiter._get_enhanced_payload(payload, use_executor=True)

        # Act
        with caplog.at_level(logging.DEBUG, logger="celery_rate_limiter.backends.celery.limiter"):
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
        assert any(
            record.levelname == "DEBUG"
            and limiter.id in record.message
            and task_id in record.message
            and "myapp.tasks.process" in record.message
            for record in caplog.records
        ), "should emit a debug log containing the limiter id, task id, and func path"

    @staticmethod
    def test_dispatch_task_use_executor_false_sends_custom_task(
        limiter, payload, task_id, caplog
    ):
        """Verify that ``_dispatch_task`` sends a custom task directly when ``use_executor`` is false."""
        # Arrange
        func_path = "myapp.tasks.custom"
        enhanced_payload = limiter._get_enhanced_payload(payload, use_executor=False)

        # Act
        with caplog.at_level(logging.DEBUG, logger="celery_rate_limiter.backends.celery.limiter"):
            with patch.object(limiter.app, "send_task") as mock_send_task:
                limiter._dispatch_task(func_path, enhanced_payload, task_id)

        # Assert
        mock_send_task.assert_called_once_with(
            func_path,
            args=[payload],
            kwargs={"_rate_limit_task_id": task_id},
        )
        assert any(
            record.levelname == "DEBUG"
            and limiter.id in record.message
            and task_id in record.message
            and func_path in record.message
            for record in caplog.records
        ), "should emit a debug log containing the limiter id, task id, and func path"

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
    def test_dispatch_task_send_task_failure_propagates(
        limiter, payload, task_id
    ):
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
        enhanced_payload = limiter._get_enhanced_payload(
            payload, use_executor=False
        )

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
    def test_schedule_task_forwards_max_age_override(
        limiter, redis_client, func_path, payload
    ):
        """Verify that ``schedule_task`` passes the ``max_age`` override through to the parent scheduler."""
        # Arrange
        # The fixture limiter has max_age=3600. A much smaller override should
        # produce a noticeably lower TTL on the inflight deduplication key.
        max_age_override = 60

        # Act
        success, task_id = limiter.schedule_task(
            func_path, payload, max_age=max_age_override
        )

        # Assert
        assert success is True, "scheduling should succeed"
        inflight_key = limiter.get_inflight_key(task_id)
        ttl = redis_client.ttl(inflight_key)
        # With max_age=60, lease_duration=30, window=60: TTL = ceil(60+30+60) = 150.
        # If the override were ignored (None -> default 3600): TTL = ceil(3600+30+60) = 3690.
        assert ttl <= 200, (
            f"inflight key TTL should reflect the max_age override of {max_age_override}, got ttl={ttl}"
        )

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
