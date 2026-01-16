import hashlib
import uuid

import redis
import json
from contextlib import contextmanager
from importlib import resources
from typing import TypedDict, Optional, cast


class TaskData(TypedDict):
    """
    A class that holds the task data format.
    """
    id: str
    task: str
    payload: dict


class CeleryConsumeResult(TypedDict):
    """
    A class to hold the result of a consume.lua call.
    """
    task: Optional[TaskData]  # The raw JSON string from Redis.
    success: bool  # Whether a task was actually consumed.
    remaining_tokens: int  # Rate limit telemetry.
    active_concurrency: int  # Concurrency telemetry.
    reset_in: int  # Time until window shift.


class CeleryRateLimiter:
    """
    A class that rate limits celery tasks.
    """
    _CONSUME_LUA_SCRIPT = None
    _SCHEDULE_LUA_SCRIPT = None
    _HEALTH_LUA_SCRIPT = None

    resource_package = "src.rate_limiter.lua"
    consume_script_name = "consume.lua"
    schedule_script_name = "schedule_task.lua"
    health_script_name = "health.lua"

    def __init__(self, redis_client, base_key: str, limit: int, window: int, max_concurrency: int):
        self.redis = redis_client
        self.base_key = base_key
        self.buffer_key = f"{base_key}:buffer"
        self.concurrency_key = f"{base_key}:concurrency"
        self.lock_key = f"{self.base_key}:dispatch_lock"
        self.limit = limit
        self.window = window
        self.max_concurrency = max_concurrency

        # Import the scripts.
        if CeleryRateLimiter._CONSUME_LUA_SCRIPT is None:
            try:
                source = resources.files(self.resource_package).joinpath(self.consume_script_name)
                CeleryRateLimiter._CONSUME_LUA_SCRIPT = source.read_text(encoding="utf-8")
            except Exception as e:
                raise ImportError(f"Could not load {self.consume_script_name} from {self.resource_package}: {e}")

        if CeleryRateLimiter._SCHEDULE_LUA_SCRIPT is None:
            try:
                source = resources.files(self.resource_package).joinpath(self.schedule_script_name)
                CeleryRateLimiter._SCHEDULE_LUA_SCRIPT = source.read_text(encoding="utf-8")
            except Exception as e:
                raise ImportError(f"Could not load {self.schedule_script_name} from {self.resource_package}: {e}")

        if CeleryRateLimiter._HEALTH_LUA_SCRIPT is None:
            try:
                source = resources.files(self.resource_package).joinpath(self.health_script_name)
                CeleryRateLimiter._HEALTH_LUA_SCRIPT = source.read_text(encoding="utf-8")
            except Exception as e:
                raise ImportError(f"Could not load {self.health_script_name} from {self.resource_package}: {e}")

        # Optimize performance by caching the scripts on the server.
        self.consume_script_sha = self.redis.script_load(self._CONSUME_LUA_SCRIPT)
        self.schedule_script_sha = self.redis.script_load(self._SCHEDULE_LUA_SCRIPT)
        self.health_script_sha = self.redis.script_load(self._HEALTH_LUA_SCRIPT)


    def schedule_task(self, func_path: str, payload: dict, priority: int = 100, retry: bool = True) -> bool:
        """
        Schedule a task to run once rate limiting allows for it.
        :param func_path: A path to the function to execute.
        :param payload: The payload for the task in question.
        :param priority: The priority of the task (100 default).
        :param retry: Whether to retry the scheduling on no script error (Redis outage).
        :return: Whether the task got skipped or not.
        """
        # Generate a unique id for the task name and payload.
        task_signature = json.dumps({"path": func_path, "payload": payload}, sort_keys=True)
        task_id = hashlib.md5(task_signature.encode()).hexdigest()

        # Track active tasks--skip if it is already active.
        active_key = f"{self.base_key}:active:{task_id}"
        if self.redis.exists(active_key):
            print(f"DEBUG: Task {task_id} is already in-flight. Skipping.")
            return False

        # Mark as active.
        self.redis.set(active_key, "1", ex=3600)

        # Add a priority for priority queue behavior.
        full_data = json.dumps({
            "id": task_id,
            "func_path": func_path,
            "payload": payload
        }, sort_keys=True)

        try:
            self.redis.evalsha(
                self.schedule_script_sha, 1,
                # KEYS: [buffer]
                # ARGV: [task_json, limit]
                self.buffer_key, full_data, priority
            )
        except redis.exceptions.NoScriptError:
            # Redis cache is volatile, and hence, the sha may become invalid unexpectedly.
            # Check if we should retry or not; throw a runtime error if not.
            if not retry:
                raise RuntimeError("Redis failed to retain the Lua script after a reload attempt.")

            # Fetch the script sha again and reattempt.
            self.schedule_script_sha = self.redis.script_load(self._SCHEDULE_LUA_SCRIPT)
            return self.schedule_task(func_path, payload, priority, retry=False)

        # Add the task with the given priority. The nx=True parameter ensures existing tasks are not overwritten.
        # self.redis.zadd(self.buffer_key, {full_data: priority}, nx=True)

        # Attempt a consume.
        self.trigger_consume()
        return True

    def consume(self, retry: bool = True) -> CeleryConsumeResult:
        """
        Attempt to consume a task from the queue.
        :param retry: Whether to retry the consumption on no script error.
        :return: A payload with a task if successfully consumed. Empty payload otherwise.
        """
        try:
            result = self.redis.evalsha(
                self.consume_script_sha, 3,
                # KEYS: [base, buffer, concurrency]
                # ARGV: [window, limit, max_concurrency]
                self.base_key, self.buffer_key, self.concurrency_key,
                self.window, self.limit, self.max_concurrency
            )
            if not result or len(result) < 5:
                return {
                    "task": None,
                    "success": False,
                    "remaining_tokens": 0,
                    "active_concurrency": 0,
                    "reset_in": self.window
                }

            return {
                "task": cast(TaskData, json.loads(result[0])) if result[0] else None,
                "success": bool(result[1]),
                "remaining_tokens": int(result[2]),
                "active_concurrency": int(result[3]),
                "reset_in": int(result[4])
            }
        except redis.exceptions.NoScriptError:
            # Redis cache is volatile, and hence, the sha may become invalid unexpectedly.
            # Check if we should retry or not; throw a runtime error if not.
            if not retry:
                raise RuntimeError("Redis failed to retain the Lua script after a reload attempt.")

            # Fetch the script sha again and reattempt.
            self.consume_script_sha = self.redis.script_load(self._CONSUME_LUA_SCRIPT)
            return self.consume(retry=False)

    def get_buffer_count(self):
        """Get the number of items in the buffer."""
        return self.redis.zcard(self.buffer_key)

    def trigger_consume(self):
        """Trigger the consumption of the task queue."""
        if self.redis.exists(self.lock_key):
            return

        from config import app
        app.send_task("rate_limiter.attempt_consume", args=[self.base_key])

    @contextmanager
    def execution_lock(self, timeout=30):
        """
        Request the dispatch lock and perform cleanup after task completion.
        :param timeout: The timeout in seconds.
        :return: The status of the lock such that the task knows if it should proceed.
        """
        # Generate a unique id so we can detect timeouts.
        token = str(uuid.uuid4())

        # Acquire the lock.
        acquired = self.redis.set(self.lock_key, token, ex=timeout, nx=True)

        try:
            # Yield the lock status.
            yield acquired
        finally:
            # This runs even if the task crashes.
            if acquired:
                # Only delete if locks match.
                # This prevents deleting locks created after a timeout.
                script = """
                if redis.call("get", KEYS[1]) == ARGV[1] then
                    return redis.call("del", KEYS[1])
                else
                    return 0
                end
                """
                self.redis.eval(script, 1, self.lock_key, token)

    @contextmanager
    def task_lifecycle(self, task_id: str = None):
        """
        A context manager to ensure the concurrency slot is released
        no matter what happens during task execution.
        """
        try:
            yield
        finally:
            # Always decrement the counter when leaving the 'with' block.
            self.redis.decr(self.concurrency_key)

            # Clear the active lock of the task.
            if task_id:
                active_key = f"{self.base_key}:active:{task_id}"
                self.redis.delete(active_key)

            # Re-trigger the dispatcher to fill the empty slot.
            self.trigger_consume()

    def get_status(self, retry: bool = True):
        """
        Returns a snapshot of the current state of the limiter.
        :return: A json formatted result containing all status information.
        """
        try:
            result = self.redis.evalsha(
                self.health_script_sha, 3,
                # KEYS: [base, buffer, concurrency]
                # ARGV: [window, limit, max_concurrency]
                self.base_key, self.buffer_key, self.concurrency_key,
                self.window, self.limit, self.max_concurrency
            )

            # Map the list to our dictionary.
            return {
                "limiter_id": self.base_key,
                "concurrency": {
                    "current": result[3],
                    "max": self.max_concurrency,
                    "available": max(0, self.max_concurrency - result[3])
                },
                "buffer": {
                    "count": result[5],
                },
                "rate_limit": {
                    "val_previous": result[0],
                    "val_current": result[1],
                    "tokens_used": float(result[2]),  # Estimated count is a float
                    "limit": self.limit,
                    "window": self.window,
                    "reset_in_seconds": result[4]
                },
                "dispatcher": {
                    "is_locked": self.redis.exists(f"{self.base_key}:dispatch_lock")
                }
            }
        except redis.exceptions.NoScriptError:
            # Redis cache is volatile, and hence, the sha may become invalid unexpectedly.
            # Check if we should retry or not; throw a runtime error if not.
            if not retry:
                raise RuntimeError("Redis failed to retain the Lua script after a reload attempt.")

            # Fetch the script sha again and reattempt.
            self.consume_script_sha = self.redis.script_load(self._CONSUME_LUA_SCRIPT)
            return self.get_status(retry=False)
