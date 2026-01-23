import json
import time
from unittest.mock import patch, MagicMock

import pytest
import redis

from celery_rate_limiter.limiters import DistributedLock, TaskLifecycle


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


@pytest.mark.parametrize("lua_script, target_key", [
    # Use existing files.
    ("schedule.lua", "_SCHEDULE_LUA_SCRIPT"),
    # Use fictional not existing file.
    ("missing.lua", "_MISSING_LUA_SCRIPT"),
])
class TestInternalHelpers:
    def test_load_lua_script_imports_only_once(self, limiter, lua_script, target_key):
        # The key must already exist beforehand.
        existing_content = "return 1"
        setattr(limiter, target_key, existing_content)

        # Mock the resource loader to track the number of calls.
        with patch("src.celery_rate_limiter.limiters.resources.files") as mock_files:
            limiter._load_lua_script(lua_script=lua_script, key=target_key)

        # The script should never load in the file since it already exists.
        mock_files.assert_not_called()

        # The script should remain unchanged.
        assert getattr(limiter, target_key) == existing_content

    def test_load_lua_script_raises_import_error(self, limiter, lua_script, target_key):
        # Ensure the attribute doesn't exist.
        if hasattr(limiter, target_key):
            delattr(limiter, target_key)

        # Mock the resource loader and see if the ImportError is thrown correctly.
        with patch("src.celery_rate_limiter.limiters.resources.files",
                   side_effect=Exception("File system error")) as mock_files:
            with pytest.raises(ImportError, match=f"Could not load {lua_script}"):
                limiter._load_lua_script(lua_script=lua_script, key=target_key)

            # Only allow one call.
            mock_files.assert_called_once()


@pytest.mark.parametrize("lock_key", ["test_lock"])
class TestDistributedLock:
    def test_lock_lifecycle(self, redis_client, lock_key):
        # Create the lock object separately so we can look into its values.
        lock = DistributedLock(redis_client, lock_key, timeout_ms=1000)
        with lock as acquired:
            assert acquired is True
            assert redis_client.exists(lock_key)
            assert redis_client.get(lock_key) == lock.token

        # Ensure the lock gets released upon context window departure.
        assert redis_client.exists(lock_key) == 0

    def test_lock_mutual_exclusion(self, redis_client, lock_key):
        # Create the lock objects separately so we can look into its values.
        lock_1 = DistributedLock(redis_client, lock_key, timeout_ms=1000)
        lock_2 = DistributedLock(redis_client, lock_key, timeout_ms=1000)

        # Assert that lock_1 is acquired, but lock 2 is not to show mutual exclusivity.
        with lock_1 as acquired_1:
            assert acquired_1 is True
            with lock_2 as acquired_2:
                assert acquired_2 is False

                # Additionally, assert that lock_1 is still holding the lock.
                assert redis_client.get(lock_key) == lock_1.token

    def test_lock_expiration(self, redis_client, lock_key):
        lock_1 = DistributedLock(redis_client, lock_key, timeout_ms=10)
        lock_2 = DistributedLock(redis_client, lock_key, timeout_ms=5000)

        # Assert that lock_1 is acquired, but lock_2 is not to show mutual exclusivity.
        with lock_1 as acquired_1:
            assert acquired_1 is True

            # Verify that the lock automatically expires.
            time.sleep(0.02)
            assert redis_client.exists(lock_key) == 0

            # Verify that lock_2 can now acquire the lock.
            with lock_2 as acquired_2:
                assert acquired_2 is True
                assert redis_client.get(lock_key) == lock_2.token

                # Manually release lock_1 and check if lock_2 still holds the lock.
                lock_1.__exit__(None, None, None)

                assert redis_client.exists(lock_key) == 1
                assert redis_client.get(lock_key) == lock_2.token

    def test_lock_released_on_exception(self, redis_client, lock_key):
        lock = DistributedLock(redis_client, lock_key, timeout_ms=5000)

        try:
            with lock:
                raise ValueError("Work failed!")
        except ValueError:
            pass

        # Lock should be released even if the code within the context window crashes.
        assert redis_client.exists(lock_key) == 0


@pytest.mark.parametrize("task_id", ["task123"])
class TestTaskLifecycle:
    """
    Context manager that handles concurrency slot cleanup.
    """

    @pytest.fixture
    def mock_limiter_for_lifecycle(self, redis_client, task_id):
        """
        Create a mock limiter that uses the real redis client.
        """
        limiter = MagicMock()
        limiter.redis = redis_client
        limiter.concurrency_key = "test:concurrency"
        limiter.get_active_key.side_effect = lambda _: f"test:active:{task_id}"
        return limiter

    @pytest.fixture
    def active_key(self, mock_limiter_for_lifecycle, task_id):
        """
        Provide the active key to all tests.
        """
        return mock_limiter_for_lifecycle.get_active_key(task_id)

    def test_task_lifecycle(self, redis_client, mock_limiter_for_lifecycle, task_id, active_key):
        # Simulate a running task.
        redis_client.set(mock_limiter_for_lifecycle.concurrency_key, 5)
        redis_client.set(active_key, "1")

        with TaskLifecycle(mock_limiter_for_lifecycle, task_id):
            assert int(redis_client.get(mock_limiter_for_lifecycle.concurrency_key)) == 5

        # Assert that the concurrency key has reduced and that the task is no longer active.
        assert int(redis_client.get(mock_limiter_for_lifecycle.concurrency_key)) == 4
        assert redis_client.exists(active_key) == 0

        # The trigger_consume function must be called to ensure the processing doesn't stall.
        mock_limiter_for_lifecycle.trigger_consume.assert_called_once()

    def test_task_lifecycle_cleanup_on_exception(self, redis_client, mock_limiter_for_lifecycle, task_id, active_key):
        # Simulate a running task.
        redis_client.set(mock_limiter_for_lifecycle.concurrency_key, 1)
        redis_client.set(active_key, "1")

        # Verify that cleanup happens appropriately after an error.
        with pytest.raises(ValueError, match="Worker crashed"):
            with TaskLifecycle(mock_limiter_for_lifecycle, task_id):
                raise ValueError("Worker crashed")

        # Check if concurrency has decremented and that the task is no longer active.
        assert int(redis_client.get(mock_limiter_for_lifecycle.concurrency_key)) == 0
        assert redis_client.exists(active_key) == 0

        # The trigger_consume function must be called to ensure the processing doesn't stall.
        mock_limiter_for_lifecycle.trigger_consume.assert_called_once()

    def test_task_lifecycle_cleanup_on_redis_failure(self, redis_client, mock_limiter_for_lifecycle, task_id,
                                                     active_key):
        with patch.object(mock_limiter_for_lifecycle.redis, 'decr',
                          side_effect=Exception("Redis connection lost")) as mock_decr:
            # Do not simulate a running task here.
            with pytest.raises(Exception, match="Redis connection lost"):
                with TaskLifecycle(mock_limiter_for_lifecycle, task_id):
                    _ = ""

            # Verify the exception got caused by the decr function.
            mock_decr.assert_called_once()

        # The trigger_consume function must be called to ensure the processing doesn't stall.
        mock_limiter_for_lifecycle.trigger_consume.assert_called_once()


# TODO: Create a reconcile concurrency task that "recovers" redis issues.


class TestScheduleTask:
    @staticmethod
    def assert_task_existence(limiter, redis_client, func_path: str, payload: dict, task_id: str) -> None:
        # Gather data needed to verify assertions.
        enhanced_payload = limiter._get_enhanced_payload(payload, True)
        full_data = limiter._get_task_data(task_id, func_path, enhanced_payload)
        active_key = limiter.get_active_key(task_id)

        # Assert that the task is marked as active.
        assert redis_client.exists(active_key) == 1

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

    @pytest.mark.parametrize("payload", [
        {"user_id": 123},
        {},
        {"a": [1, 2], "b": {"c": 3}},
        {"msg": "✅ unicode"}
    ])
    def test_payload_serialization_integrity(self, limiter, redis_client, payload):
        func_path = "myapp.tasks.process_data"
        success, task_id = limiter.schedule_task(func_path, payload)

        # Verify whether the data survived the trip.
        self.assert_task_existence(limiter, redis_client, func_path, payload, task_id)

    def test_schedule_tasks_no_script_recovery(self, limiter, redis_client):
        payload = {"user_id": 123}
        func_path = "myapp.tasks.process_data"

        # Mock the eval function and have it return to the original after the failure.
        real_evalsha = redis_client.evalsha
        real_script_load = redis_client.script_load

        # Create a function that raises a NoScriptError only on the very first call.
        def mocked_evalsha_func(*args, **kwargs):
            if mocked_evalsha_func.call_count == 0:
                mocked_evalsha_func.call_count += 1
                raise redis.exceptions.NoScriptError("NOSCRIPT")
            return real_evalsha(*args, **kwargs)

        mocked_evalsha_func.call_count = 0

        # Use with here to ensure the mock is reverted post execution.
        with patch.object(limiter.redis, 'evalsha', side_effect=mocked_evalsha_func) as mock_eval, \
                patch.object(limiter.redis, 'script_load', side_effect=real_script_load) as mock_load:
            success, task_id = limiter.schedule_task(func_path, payload)

            # Expected outcome is true.
            assert success is True

            # Do the common task existence and integrity assertions.
            self.assert_task_existence(limiter, redis_client, func_path, payload, task_id)

            # Verify whether the recovery took the expected path.
            assert mock_eval.call_count == 2, "evalsha should have been called twice (fail then retry)"
            assert mock_load.call_count == 1, "script_load should have been called to recover"

        # Verify the script was actually reloaded into the class attribute.
        assert limiter.schedule_script_sha is not None

    def test_schedule_tasks_no_script_permanent_failure(self, limiter, redis_client):
        # Force evalsha to always fail.
        with patch.object(
                limiter.redis, 'evalsha', side_effect=redis.exceptions.NoScriptError("Permanent Failure")
        ) as mock_eval:
            with pytest.raises(RuntimeError, match="Redis failed to retain the Lua script"):
                limiter.schedule_task("path", {})

            # Assert that a re-attempt was performed.
            assert mock_eval.call_count == 2

        # Assert no tasks have been added and that the task is no longer marked active.
        task_wildcard = limiter.get_active_key("*")
        active_keys = redis_client.keys(task_wildcard)
        assert len(active_keys) == 0
        assert redis_client.zcard(limiter.buffer_key) == 0
