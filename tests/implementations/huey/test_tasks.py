"""Tests for the Huey task helper modules.

Fixture dependencies:
    - ``_reset_limiter_class_state``: from
      ``tests/implementations/huey/conftest.py``.
"""

import logging
from unittest.mock import MagicMock, patch

import pytest

from redis_rate_limiter.backends.huey.limiter import HueyRateLimiter
from tests.helpers.utils import assert_log_emitted


@pytest.mark.behavior
class TestGenericWorkerTask:
    """Test suite for the ``generic_rate_limited_worker`` task behaviour."""

    @staticmethod
    def test_generic_worker_resolves_and_executes_function():
        """Verify that the generic worker resolves the callable
        and executes it with the payload keyword arguments."""
        # Arrange
        limiter = MagicMock()
        lifecycle_context = MagicMock()
        lifecycle_context.__enter__.return_value = lifecycle_context
        lifecycle_context.__exit__.return_value = False
        limiter.task_lifecycle.return_value = lifecycle_context
        target_func = MagicMock(return_value={"ok": True})
        payload = {"alpha": 1, "beta": 2}

        # Act
        # The @rate_limited decorator captures HueyRateLimiter.get via
        # _get_limiter at call time. We inject the mock limiter directly
        # into the managed class instance cache.
        #
        # Access the inner implementation function directly via the Huey
        # TaskWrapper to bypass the enqueue mechanism.
        from redis_rate_limiter.backends.huey.tasks import worker as worker_module

        task_wrapper = worker_module.generic_rate_limited_worker
        assert task_wrapper is not None, (
            "generic_rate_limited_worker must be registered before testing"
        )

        with patch.dict(HueyRateLimiter._instances, {"worker_limiter": limiter}):
            with patch(
                "redis_rate_limiter.backends.huey.tasks.worker.import_string",
                return_value=target_func,
            ) as mock_import:
                # Call the inner function directly (bypassing Huey enqueue).
                result = task_wrapper.call_local(
                    limiter_id="worker_limiter",
                    func_path="json.dumps",
                    payload=payload,
                    _rate_limit_task_id="task-42",
                )

        # Assert
        assert result == {"ok": True}, "generic worker should return target result"
        limiter.task_lifecycle.assert_called_once_with("task-42")
        mock_import.assert_called_once_with("json.dumps")
        target_func.assert_called_once_with(**payload)


# ---------------------------------------------------------------------------
# Observability tests
# ---------------------------------------------------------------------------


@pytest.mark.observability
class TestGenericWorkerTaskObservability:
    """Observability tests for the ``generic_rate_limited_worker`` log emissions."""

    @staticmethod
    def test_generic_worker_emits_debug_log(caplog):
        """Verify that the generic worker emits a DEBUG log
        with the limiter id and func path."""
        # Arrange
        limiter = MagicMock()
        lifecycle_context = MagicMock()
        lifecycle_context.__enter__.return_value = lifecycle_context
        lifecycle_context.__exit__.return_value = False
        limiter.task_lifecycle.return_value = lifecycle_context
        target_func = MagicMock(return_value={"ok": True})

        # Act
        from redis_rate_limiter.backends.huey.tasks import worker as worker_module

        task_wrapper = worker_module.generic_rate_limited_worker
        assert task_wrapper is not None, (
            "generic_rate_limited_worker must be registered before testing"
        )

        with patch.dict(HueyRateLimiter._instances, {"worker_limiter": limiter}):
            with caplog.at_level(
                logging.DEBUG,
                logger="redis_rate_limiter.backends.huey.tasks.worker",
            ):
                with patch(
                    "redis_rate_limiter.backends.huey.tasks.worker.import_string",
                    return_value=target_func,
                ):
                    task_wrapper.call_local(
                        limiter_id="worker_limiter",
                        func_path="json.dumps",
                        payload={"alpha": 1},
                        _rate_limit_task_id="task-42",
                    )

        # Assert
        assert_log_emitted(
            caplog.records,
            level="DEBUG",
            label="[HueyGenericWorker]",
            required_fragments=[
                "limiter=worker_limiter",
                "func_path=json.dumps",
            ],
            message="should emit a debug log containing the limiter id and func path",
        )
