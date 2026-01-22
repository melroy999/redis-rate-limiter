import hashlib
import uuid
import redis
import json

from importlib import resources
from typing import TypedDict, Optional, cast, ContextManager
from redis import Redis
from celery import Celery
from abc import ABC, abstractmethod


class TaskData(TypedDict):
    """
    A class that holds the task data format.
    """
    id: str
    task: str
    payload: dict


class ConsumeResult(TypedDict):
    """
    A class to hold the result of a consume.lua call.
    """
    task: Optional[TaskData]  # The raw JSON string from Redis.
    success: bool  # Whether a task was actually consumed.
    remaining_tokens: int  # Rate limit telemetry.
    active_concurrency: int  # Concurrency telemetry.
    reset_in_ms: int  # Time until window shift.
    remaining_tasks: int  # The number of tasks that remain to be processed.


class DistributedLock:
    """
    A dedicated class for the execution lock (instead of a @contextmanager) to keep the IDE happy.
    Requests a dispatch lock and perform cleanup after task completion.
    """

    def __init__(self, redis_client: Redis, lock_key: str, timeout_ms: int):
        """
        Initialize the lock manager.
        :param redis_client: The client to use to connect to the redis server.
        :param lock_key: The name of the key the lock is stored under.
        :param timeout_ms: The timeout for the lock in milliseconds.
        """
        self.redis = redis_client
        self.lock_key = lock_key
        self.timeout_ms = timeout_ms
        self.token = str(uuid.uuid4())
        self.acquired = False

    def __enter__(self) -> bool:
        """
        Enter the context manager.
        :return: The status of the lock such that the task knows if it should proceed.
        """
        # Acquire the lock.
        self.acquired = self.redis.set(self.lock_key, self.token, px=self.timeout_ms, nx=True)
        return self.acquired

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Leave the context manager.
        """
        # This runs even if the task crashes.
        if self.acquired:
            # Only delete if locks match.
            # This prevents deleting locks created after a timeout.
            script = """
                if redis.call("get", KEYS[1]) == ARGV[1] then
                    return redis.call("del", KEYS[1])
                else
                    return 0
                end
                """
            self.redis.eval(script, 1, self.lock_key, self.token)


class TaskLifecycle:
    """
    Context manager that handles concurrency slot cleanup.
    """

    def __init__(self, limiter, task_id: str):
        """
        Create a lifecycle context manager that cleans up concurrency slots.
        :param limiter: The limiter to observe.
        :param task_id: The id of the task to clear the active state for.
        """
        self.limiter = limiter
        self.task_id = task_id

    def __enter__(self):
        """
        Enter the context manager.
        """
        # No on-enter behavior required.
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Leave the context manager.
        """
        try:
            # Always decrement the counter when leaving the 'with' block.
            self.limiter.redis.decr(self.limiter.concurrency_key)

            # Clear the active lock of the task.
            if self.task_id:
                active_key = f"{self.limiter.id}:active:{self.task_id}"
                self.limiter.redis.delete(active_key)
        finally:
            # Re-trigger the dispatcher to fill the empty slot.
            self.limiter.trigger_consume()


class AbstractDistributedRateLimiter(ABC):
    """
        A class that rate limits celery tasks.
        """
    _CONSUME_LUA_SCRIPT = None
    _SCHEDULE_LUA_SCRIPT = None
    _HEALTH_LUA_SCRIPT = None

    # Get the location of the lua package.
    resource_package = "src.celery_rate_limiter.lua"

    def __init__(
            self,
            redis_client: Redis,
            limiter_id: str,
            limit: int,
            window: int,
            max_concurrency: int,
            max_age: int
    ):
        """
        Create a Celery rate limiter instance with the given parameters and import the appropriate lua scripts.
        :param limiter_id: The id of the rate limiter to create.
        :param window: The time window in seconds that the limit is applied to.
        :param limit: The maximum number of tasks per time window.
        :param max_concurrency: The maximum number of concurrent tasks.
        :param max_age: The maximum time a task may exist in the queue before it expires.
        :return: A rate limiter using the desired parameters.
        """
        self.redis = redis_client
        self.id = limiter_id
        self.buffer_key = f"{self.id}:buffer"
        self.concurrency_key = f"{self.id}:concurrency"
        self.lock_key = f"{self.id}:dispatch_lock"
        self.limit = limit
        self.window = window
        self.max_concurrency = max_concurrency
        self.max_age = max_age

        # Import the scripts.
        self._load_lua_script("consume.lua", "_CONSUME_LUA_SCRIPT")
        self._load_lua_script("schedule.lua", "_SCHEDULE_LUA_SCRIPT")
        self._load_lua_script("health.lua", "_HEALTH_LUA_SCRIPT")

        # Optimize performance by caching the scripts on the server.
        self.consume_script_sha = self.redis.script_load(self._CONSUME_LUA_SCRIPT)
        self.schedule_script_sha = self.redis.script_load(self._SCHEDULE_LUA_SCRIPT)
        self.health_script_sha = self.redis.script_load(self._HEALTH_LUA_SCRIPT)

    def _load_lua_script(self, lua_script: str, key: str) -> None:
        """
        Load a lua script from disk.
        :param lua_script: The name of the script to load.
        :param key: The attribute key to store the script under.
        """
        if getattr(self, key, None) is None:
            try:
                source = resources.files(self.resource_package).joinpath(lua_script)
                setattr(self, key, source.read_text(encoding="utf-8"))
            except Exception as e:
                raise ImportError(f"Could not load {lua_script} from {self.resource_package}: {e}")

    @staticmethod
    def _get_task_signature_str(func_path: str, payload: dict) -> str:
        """Get the signature of a task as a JSON string."""
        return json.dumps({"path": func_path, "payload": payload}, sort_keys=True)

    @staticmethod
    def _get_task_data(task_id: str, func_path: str, payload: dict):
        """Get the data of a task."""
        return {
            "id": task_id,
            "func_path": func_path,
            "payload": payload
        }

    def _get_task_data_str(self, task_id: str, func_path: str, payload: dict) -> str:
        """Get the data of a task as a JSON string."""
        return json.dumps(self._get_task_data(task_id, func_path, payload), sort_keys=True)

    def _get_active_key(self, task_id: str):
        """Get the active key for the given task."""
        return f"{self.id}:active:{task_id}"

    def schedule_task(
            self, func_path: str, payload: dict, priority: int = 100, retry: bool = True
    ) -> tuple[bool, str]:
        """
        Schedule a task to run once rate limiting allows for it.
        :param func_path: The name of the function to schedule.
        :param payload: The payload for the task in question.
        :param priority: The priority of the task (100 default).
        :param retry: Whether to retry the scheduling on no script error (Redis outage).
        :return: Whether the task got skipped or not and its task id.
        :exception RuntimeError: if the necessary lua scripts cannot be (re)loaded.
        """
        # Generate a unique id for the task name and payload.
        task_signature = self._get_task_signature_str(func_path, payload)
        task_id = hashlib.md5(task_signature.encode()).hexdigest()

        # Track active tasks--skip if it is already active.
        active_key = self._get_active_key(task_id)
        if self.redis.exists(active_key):
            print(f"DEBUG: Task {task_id} is already in-flight. Skipping.")
            return False, task_id

        # Add a priority for priority queue behavior.
        full_data = self._get_task_data_str(task_id, func_path, payload)

        try:
            # Attempt to schedule.
            self.redis.evalsha(
                self.schedule_script_sha, 1,
                # KEYS: [buffer]
                # ARGV: [task_json, limit]
                self.buffer_key, full_data, priority
            )

            # Mark as active only after scheduling.
            self.redis.set(active_key, "1", ex=3600)

        except redis.exceptions.NoScriptError:
            # Redis cache is volatile, and hence, the sha may become invalid unexpectedly.
            # Check if we should retry or not; throw a runtime error if not.
            if not retry:
                raise RuntimeError("Redis failed to retain the Lua script after a reload attempt.")

            # Fetch the script sha again and reattempt.
            self.schedule_script_sha = self.redis.script_load(self._SCHEDULE_LUA_SCRIPT)
            return self.schedule_task(func_path, payload, priority, retry=False)

        # Attempt a consume.
        self.trigger_consume()
        return True, task_id

    def consume(self, retry: bool = True) -> ConsumeResult:
        """
        Attempt to consume a task from the queue.
        :param retry: Whether to retry the consumption on no script error.
        :return: A payload with a task if successfully consumed. Empty payload otherwise.
        :exception RuntimeError: if the necessary lua scripts cannot be (re)loaded.
        """
        try:
            # Fetch the result.
            result = self.redis.evalsha(
                self.consume_script_sha, 3,
                # KEYS: [base, buffer, concurrency]
                # ARGV: [window, limit, max_concurrency]
                self.id, self.buffer_key, self.concurrency_key,
                self.window, self.limit, self.max_concurrency, self.max_age
            )

            # Attempt to parse the result.
            return {
                "success": bool(result[0]),
                "task": cast(TaskData, json.loads(result[1])) if result[1] else None,
                "remaining_tokens": int(result[2]),
                "active_concurrency": int(result[3]),
                "reset_in_ms": int(result[4]),
                "remaining_tasks": int(result[5]),
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

    def drain(self):
        """
        Attempt to drain an item from the queue.
        """
        # Lock the execution to avoid the thundering herd problem.
        with self.execution_lock() as acquired:
            if not acquired:
                # Someone is already executing an attempt; hence skip.
                return

            # Perform a consume.
            result = self.consume()

            # Execute the task if the green light is given.
            if result["success"] and result["task"]:
                task = result["task"]

                # Send to the generic worker.
                self._dispatch_task(
                    func_path=task["func_path"],
                    payload=task["payload"],
                    task_id=task.get("id")
                )

                # ONLY pulse if there are still items waiting in the buffer.
                # This prevents the dispatcher from running forever.
                if result["remaining_tasks"] > 0:
                    self._schedule_drain()

            elif result["remaining_tasks"] == 0:
                # Stop: No remaining tasks. Next drain will be triggered by a new task being added.
                pass

            elif result["active_concurrency"] >= self.max_concurrency:
                # Stop: The next drain will be triggered by worker completion.
                pass

            elif result["remaining_tokens"] <= 0:
                # Wait for the rate window to reset.
                ms_to_reset = result.get("reset_in_ms", 0)
                delay_seconds = round(max(0.001, (ms_to_reset / 1000.0) + 0.001), 3)
                self._schedule_drain(delay=delay_seconds)

    @abstractmethod
    def _dispatch_task(self, func_path: str, payload: dict, task_id: str):
        """
        Send the task to the actual worker (Celery worker, Thread, etc.)
        """
        pass

    @abstractmethod
    def _schedule_drain(self, delay: float = 0.0):
        """
        Schedule the `drain` method to run again after `delay` seconds.
        :param delay: The amount of time to sleep before scheduling.
        """
        pass

    def trigger_consume(self):
        """Trigger the consumption of the task queue."""
        if self.redis.exists(self.lock_key):
            return

        self._schedule_drain()

    def execution_lock(self, timeout_ms=5000) -> ContextManager[bool]:
        """
        Request the dispatch lock and perform cleanup after task completion.
        :param timeout_ms: The timeout in milliseconds.
        :return: The status of the lock such that the task knows if it should proceed.
        """
        return DistributedLock(redis_client=self.redis, lock_key=self.lock_key, timeout_ms=timeout_ms)

    def task_lifecycle(self, task_id: str):
        """
        A context manager to ensure the concurrency slot is released
        no matter what happens during task execution.
        """
        return TaskLifecycle(limiter=self, task_id=task_id)

    def get_status(self, retry: bool = True):
        """
        Returns a snapshot of the current state of the limiter.
        :return: A JSON formatted result containing all status information.
        :exception RuntimeError: if the necessary lua scripts cannot be (re)loaded.
        """
        try:
            result = self.redis.evalsha(
                self.health_script_sha, 3,
                # KEYS: [base, buffer, concurrency]
                # ARGV: [window, limit, max_concurrency]
                self.id, self.buffer_key, self.concurrency_key,
                self.window, self.limit, self.max_concurrency
            )

            # Map the list to our dictionary.
            return {
                "limiter_id": self.id,
                "concurrency": {
                    "current": result[3],
                    "max": self.max_concurrency,
                    "available": max(0, self.max_concurrency - int(result[3]))
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
                    "reset_in_ms": result[4]
                },
                "dispatcher": {
                    "is_locked": self.redis.exists(f"{self.id}:dispatch_lock")
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


class CeleryRateLimiter(AbstractDistributedRateLimiter):
    def __init__(
            self,
            redis_client,
            celery_app: Celery,
            *args, **kwargs
    ):
        super().__init__(redis_client, *args, **kwargs)
        self.app = celery_app

    @staticmethod
    def _get_enhanced_payload(payload: dict, use_executor: bool):
        """Get the enhanced payload."""
        return {
            "data": payload,
            "meta": {"use_executor": use_executor}
        }

    def schedule_task(
            self, func_path: str, payload: dict, priority: int = 100, retry: bool = True, use_executor: bool = True
    ) -> tuple[bool, str]:
        """
        :param func_path:
            **use_executor=True**: Dot-path to the python function.
            **use_executor=False**: The Celery task name.
        :param payload: The payload for the task in question.
        :param priority: The priority of the task (100 default).
        :param retry: Whether to retry the scheduling on no script error (Redis outage).
        :param use_executor: Whether to use the generic worker or direct Celery worker dispatch.
        :return: Whether the task got skipped or not and its task id.
        :exception RuntimeError: if the necessary lua scripts cannot be (re)loaded.
        """
        # Add the use executor flag to the payload.
        enhanced_payload = self._get_enhanced_payload(payload, use_executor)

        # Call the parent scheduler.
        return super().schedule_task(func_path, enhanced_payload, priority, retry)

    def _dispatch_task(self, func_path: str, payload: dict, task_id: str):
        # Check if the built-in worker should be used.
        use_executor = payload.get("meta", {}).get("use_executor", True)
        data = payload.get("data", {})

        if use_executor:
            # Dispatch the task to the generic worker.
            self.app.send_task(
                "celery_rate_limiter.generic_worker",
                kwargs={
                    "limiter_id": self.id,
                    "func_path": func_path,
                    "payload": data,
                    "_rate_limit_task_id": task_id
                }
            )
        else:
            # Use the custom user task.
            self.app.send_task(
                func_path,
                args=[data],
                kwargs={"_rate_limit_task_id": task_id}
            )

    def _schedule_drain(self, delay: float = 0.0):
        # Schedule an attempt at consuming a token.
        self.app.send_task(
            "celery_rate_limiter.attempt_consume",
            args=[self.id],
            countdown=delay
        )
