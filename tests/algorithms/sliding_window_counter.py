"""Pure Python sliding window counter algorithm used by tests.

This mirrors the Lua logic in consume.lua to enable deterministic testing.
"""


def sliding_window_estimate(
    previous_count: int,
    current_count: int,
    window_ms: int,
    elapsed_ms: int,
) -> float:
    """Calculate the estimated request count using sliding window counter algorithm.

    Pure Python implementation of the algorithm from consume.lua. Weights the
    previous window's count based on how much of it overlaps the sliding window.

    Args:
        previous_count: Number of requests in the previous fixed window.
        current_count: Number of requests in the current fixed window.
        window_ms: Window size in milliseconds.
        elapsed_ms: Time elapsed since current window started.

    Returns:
        Estimated request count for the sliding window.
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
    """Determine if a request is allowed under the rate limit.

    Args:
        previous_count: Number of requests in the previous fixed window.
        current_count: Number of requests in the current fixed window.
        window_ms: Window size in milliseconds.
        elapsed_ms: Time elapsed since current window started.
        limit: Maximum requests allowed per window.

    Returns:
        True if a request can be made, False otherwise.
    """
    estimated = sliding_window_estimate(
        previous_count, current_count, window_ms, elapsed_ms
    )
    return estimated < limit
