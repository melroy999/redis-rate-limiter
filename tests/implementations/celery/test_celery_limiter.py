"""Celery-specific behavior tests for CeleryRateLimiter."""

import json
from unittest.mock import patch


class TestCeleryRateLimiter:
    """Tests that are specific to the Celery backend implementation."""

    def test_schedule_task_with_use_executor_false_stores_meta(
        self, limiter, redis_client, func_path, default_payload
    ):
        """Verify schedule_task stores use_executor=False in task payload metadata."""
        # Act
        success, task_id = limiter.schedule_task(
            func_path, default_payload, use_executor=False
        )

        # Assert
        assert success is True, "scheduling should succeed"
        _, results = redis_client.zscan(limiter.buffer_key, match=f'*"{task_id}"*')
        assert len(results) == 1, "scheduled task should exist in buffer exactly once"
        task_data = json.loads(results[0][0])
        assert task_data["payload"]["meta"]["use_executor"] is False, (
            "task metadata should store use_executor as false"
        )

    def test_dispatch_task_use_executor_true_sends_generic_worker(
        self, limiter, default_payload, task_id
    ):
        """Verify _dispatch_task sends generic worker when use_executor is true."""
        # Arrange
        payload = limiter._get_enhanced_payload(default_payload, use_executor=True)

        # Act
        with patch.object(limiter.app, "send_task") as mock_send_task:
            limiter._dispatch_task("myapp.tasks.process", payload, task_id)

            # Assert
            mock_send_task.assert_called_once_with(
                "celery_rate_limiter.generic_worker",
                kwargs={
                    "limiter_id": limiter.id,
                    "func_path": "myapp.tasks.process",
                    "payload": default_payload,
                    "_rate_limit_task_id": task_id,
                },
            )

    def test_dispatch_task_use_executor_false_sends_custom_task(
        self, limiter, default_payload, task_id
    ):
        """Verify _dispatch_task sends custom task when use_executor is false."""
        # Arrange
        func_path = "myapp.tasks.custom"
        payload = limiter._get_enhanced_payload(default_payload, use_executor=False)

        # Act
        with patch.object(limiter.app, "send_task") as mock_send_task:
            limiter._dispatch_task(func_path, payload, task_id)

            # Assert
            mock_send_task.assert_called_once_with(
                func_path,
                args=[default_payload],
                kwargs={"_rate_limit_task_id": task_id},
            )

    def test_schedule_drain_wakes_drain_loop(self, limiter):
        """Verify _schedule_drain delegates to the base class DrainLoop."""
        # Arrange
        delay = 1.75

        # Act
        with patch.object(limiter._drain_loop, "wake") as mock_wake:
            limiter._schedule_drain(delay=delay)

        # Assert
        mock_wake.assert_called_once_with(delay)

    def test_enhanced_payload_structure(self, limiter, default_payload):
        """Verify _get_enhanced_payload wraps payload in data/meta structure."""
        # Act
        enhanced_payload = limiter._get_enhanced_payload(
            default_payload, use_executor=False
        )

        # Assert
        assert enhanced_payload["data"] == default_payload, (
            "enhanced payload should preserve original data"
        )
        assert enhanced_payload["meta"] == {"use_executor": False}, (
            "enhanced payload meta should contain use_executor flag"
        )
