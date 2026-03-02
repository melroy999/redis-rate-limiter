"""Pure Python implementation of the sliding window counter algorithm for use in tests.

This module mirrors the Lua logic defined in consume.lua, thereby enabling
deterministic testing of the algorithm independently of the Redis runtime.
"""


def sliding_window_estimate(
    previous_count: int,
    current_count: int,
    window_ms: int,
    elapsed_ms: int,
) -> float:
    """Calculate the estimated request count using the sliding window counter algorithm.

    This is a pure Python implementation of the algorithm defined in consume.lua.
    The previous window's count is weighted according to the proportion of overlap
    with the current sliding window.

    Args:
        previous_count: The number of requests recorded in the previous fixed window.
        current_count: The number of requests recorded in the current fixed window.
        window_ms: The window size in milliseconds.
        elapsed_ms: The time elapsed since the current window started, in milliseconds.

    Returns:
        The estimated request count for the sliding window.
    """
    weight = (window_ms - elapsed_ms) / window_ms
    return current_count + (previous_count * weight)


def is_allowed(
    previous_count: int,
    current_count: int,
    window_ms: int,
    elapsed_ms: int,
    limit: int,
) -> bool:
    """Determine whether a request is permitted under the configured rate limit.

    Args:
        previous_count: The number of requests recorded in the previous fixed window.
        current_count: The number of requests recorded in the current fixed window.
        window_ms: The window size in milliseconds.
        elapsed_ms: The time elapsed since the current window started, in milliseconds.
        limit: The maximum number of requests permitted per window.

    Returns:
        True if the request is permitted, False otherwise.
    """
    estimated = sliding_window_estimate(
        previous_count, current_count, window_ms, elapsed_ms
    )
    return estimated < limit
