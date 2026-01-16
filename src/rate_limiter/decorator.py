from functools import wraps
from typing import Callable, Any

from src.rate_limiter.base import SlidingWindowRateLimiter


def ratelimit(limiter: SlidingWindowRateLimiter):
    """
    A decorator to rate limit functions globally.
    Raises a ConnectionError or custom Exception if blocked.
    """

    def decorator(func: Callable[..., Any]):
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            result = limiter.check()

            if result["success"]:
                return func(*args, **kwargs)
            else:
                print(f"Blocked! Retrying possible in {result['reset_in']}s")
                raise Exception(f"Rate limit exceeded. Retry in {result['reset_in']}s")

        return wrapper

    return decorator