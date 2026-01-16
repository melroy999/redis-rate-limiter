import redis
from importlib import resources
from typing import TypedDict


class RateLimitCheckResult(TypedDict):
    """
    A class to hold the result of a check.lua call.
    """
    success: bool  # Whether a free token was consumed.
    remaining_tokens: int  # Rate limit telemetry.
    reset_in: int  # Time until window shift.


class SlidingWindowRateLimiter:
    """
    A class to hold the global sliding window rate limiter.
    """
    _LUA_SCRIPT = None
    resource_package = "src.rate_limiter.lua"
    script_name = "check.lua"

    def __init__(self, redis_client: redis.Redis, base_key: str, limit: int, window: int):
        self.redis = redis_client
        self.base_key = base_key
        self.limit = limit
        self.window = window

        # Import the script.
        if SlidingWindowRateLimiter._LUA_SCRIPT is None:
            try:
                source = resources.files(self.resource_package).joinpath(self.script_name)
                SlidingWindowRateLimiter._LUA_SCRIPT = source.read_text(encoding="utf-8")
            except Exception as e:
                raise ImportError(f"Could not load {self.script_name} from {self.resource_package}: {e}")

        # Optimize performance by caching the script on the server.
        self.script_sha = self.redis.script_load(self._LUA_SCRIPT)

    def check(self, retry=True) -> RateLimitCheckResult:
        """
        Checks the rate limit and returns detailed telemetry.
        """
        try:
            # Execute the lua script on the redis server and wait for the result.
            result = self.redis.evalsha(
                self.script_sha, 1, self.base_key, self.window, self.limit
            )
            return {
                "success": bool(result[0]),
                "remaining_tokens": int(result[1]),
                "reset_in": int(result[2])
            }
        except redis.exceptions.NoScriptError:
            # Redis cache is volatile, and hence, the sha may become invalid unexpectedly.
            # Check if we should retry or not; throw a runtime error if not.
            if not retry:
                raise RuntimeError("Redis failed to retain the Lua script after a reload attempt.")

            # Fetch the script sha again and reattempt.
            self.script_sha = self.redis.script_load(self._LUA_SCRIPT)
            return self.check(retry=False)
