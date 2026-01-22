import json
from unittest.mock import MagicMock

import pytest
import redis


def is_subset(subset, superset):
    for key, value in subset.items():
        if key not in superset:
            return False
        if isinstance(value, dict):
            if not is_subset(value, superset.get(key, {})):
                return False
        elif value != superset[key]:
            return False
    return True


class TestScheduleTask:
    @staticmethod
    def assert_task_existence(limiter, redis_client, func_path: str, payload: dict, task_id: str) -> None:
        # Gather data needed to verify assertions.
        enhanced_payload = limiter._get_enhanced_payload(payload, True)
        full_data = limiter._get_task_data(task_id, func_path, enhanced_payload)
        active_key = limiter._get_active_key(task_id)

        # Assert that the task is marked as active.
        assert bool(redis_client.exists(active_key)) == 1

        # Assert that the task is in the buffer only once.
        cursor, results = redis_client.zscan(limiter.buffer_key, match=f'*"{task_id}"*')
        assert len(results) > 0, f"task with ID {task_id} not found in buffer"
        assert len(results) == 1, f"task with ID {task_id} has been found more than once in the buffer"

        # Assert that the task data remains correct.
        full_data_server_str, score = results[0]
        full_data_server = json.loads(full_data_server_str)

        assert is_subset(full_data, full_data_server), f"task with ID {task_id} has a data mismatch."
        assert "_arrived_at" in full_data_server
        assert isinstance(full_data_server["_arrived_at"], int)

    def test_schedule_single_task(self, limiter, redis_client):
        payload = {"user_id": 123}
        func_path = "myapp.tasks.process_data"

        # Schedule the same task twice.
        success, task_id = limiter.schedule_task(func_path, payload)

        # Do the common task existence and integrity assertions.
        self.assert_task_existence(limiter, redis_client, func_path, payload, task_id)

        # Assert that there is only a single task in the buffer.
        assert redis_client.zcard(limiter.buffer_key) == 1

    def test_schedule_task_skip_existing(self, limiter, redis_client):
        payload = {"user_id": 123}
        func_path = "myapp.tasks.process_data"

        # Schedule the same task twice.
        limiter.schedule_task(func_path, payload)
        success, task_id = limiter.schedule_task(func_path, payload)

        # Do the common task existence and integrity assertions.
        self.assert_task_existence(limiter, redis_client, func_path, payload, task_id)

        # Assert that the second task didn't get scheduled.
        assert success is False

    def test_schedule_tasks_multiple_with_duplicate(self, limiter, redis_client):
        payload = {"user_id": 123}
        payload_2 = {"user_id": 456}
        func_path = "myapp.tasks.process_data"

        # Schedule the same task twice.
        limiter.schedule_task(func_path, payload)
        success, task_id = limiter.schedule_task(func_path, payload)
        success_2, task_id_2 = limiter.schedule_task(func_path, payload_2)

        # Assert that the second task didn't get scheduled and the last one did.
        assert success is False
        assert success_2 is True

        # Do the common task existence and integrity assertions.
        self.assert_task_existence(limiter, redis_client, func_path, payload, task_id)
        self.assert_task_existence(limiter, redis_client, func_path, payload_2, task_id_2)

        # Assert that there is only two tasks in the buffer.
        assert redis_client.zcard(limiter.buffer_key) == 2

    def test_schedule_tasks_no_script_recovery(self, limiter, redis_client):
        payload = {"user_id": 123}
        func_path = "myapp.tasks.process_data"

        # Mock the eval function and have it return to the original after the failure.
        real_evalsha = redis_client.evalsha
        real_script_load = redis_client.script_load

        def mocked_evalsha_func(*args, **kwargs):
            # On the VERY FIRST call, we raise the NoScriptError
            if mocked_evalsha_func.call_count == 0:
                mocked_evalsha_func.call_count += 1
                raise redis.exceptions.NoScriptError("NOSCRIPT")

            # On subsequent calls, we call the REAL redis method
            return real_evalsha(*args, **kwargs)

        mocked_evalsha_func.call_count = 0

        mock_evalsha = MagicMock(side_effect=mocked_evalsha_func)
        mock_script_load = MagicMock(side_effect=real_script_load)

        limiter.redis.evalsha = mock_evalsha
        limiter.redis.script_load = mock_script_load

        success, task_id = limiter.schedule_task(func_path, payload)

        # Expected outcome is true.
        assert success is True

        # Do the common task existence and integrity assertions.
        self.assert_task_existence(limiter, redis_client, func_path, payload, task_id)

        # Verify whether the recovery took the expected path.
        assert mock_evalsha.call_count == 2, "evalsha should have been called twice (fail then retry)"
        assert mock_script_load.call_count == 1, "script_load should have been called to recover"

        # Verify the script was actually reloaded into the class attribute.
        assert limiter.schedule_script_sha is not None

    def test_schedule_tasks_no_script_failure(self, limiter, redis_client):
        # Force evalsha to always fail.
        limiter.redis.evalsha = MagicMock(
            side_effect=redis.exceptions.NoScriptError("Permanent Failure")
        )

        with pytest.raises(RuntimeError, match="Redis failed to retain the Lua script"):
            limiter.schedule_task("path", {}, retry=True)