-- acquire.lua
--
-- Lightweight admission check for request-oriented rate limiting,
-- intended for the ASGI middleware and other synchronous request
-- handlers that do not buffer work. Atomically estimates the current
-- rate, increments the counter when below the limit, and returns the
-- decision together with telemetry.
--
-- This is the request-path counterpart to consume.lua: it mirrors the
-- sliding window estimation logic but omits the buffer, concurrency
-- tracking, dead letter queue, and task management.
--
-- KEYS:
--     [1] base_key  Base key for the per-window counters; the actual
--                   subkeys take the form "<base_key>:<window_start_ms>".
--
-- ARGV:
--     [1] window_size  Sliding window size in seconds.
--     [2] rate_limit   Maximum requests permitted per sliding window.
--
-- Returns:
--     {allowed, remaining, reset_in_ms, previous_count, current_count}
-- where allowed is 1 (admitted) or 0 (rejected). On admission the
-- current window counter has already been incremented.
local base_key = KEYS[1]
local window_size_ms = tonumber(ARGV[1]) * 1000
local rate_limit = tonumber(ARGV[2])

-- All time arithmetic uses the Redis server clock so that calculations
-- remain consistent regardless of client clock skew.
local redis_time = redis.call('TIME')
local now_ms = (tonumber(redis_time[1]) * 1000) + math.floor(tonumber(redis_time[2]) / 1000)

local current_window_start = math.floor(now_ms / window_size_ms) * window_size_ms
local current_key = base_key .. ':' .. current_window_start
local previous_window_start = current_window_start - window_size_ms
local previous_key = base_key .. ':' .. previous_window_start

local current_count = tonumber(redis.call('GET', current_key) or 0)
local previous_count = tonumber(redis.call('GET', previous_key) or 0)

-- Sliding window counter estimate. The weight decays linearly from 1 at
-- the start of the current window to 0 at the end, fading out the
-- previous window's contribution as the current window fills in.
local time_passed_in_current = now_ms - current_window_start
local weight = (window_size_ms - time_passed_in_current) / window_size_ms
local estimated_count = current_count + (previous_count * weight)

local remaining = math.max(0, math.floor(rate_limit - estimated_count))
local reset_in_ms = math.max(0, current_window_start + window_size_ms - now_ms)

if estimated_count < rate_limit then
    redis.call('INCR', current_key)

    -- The current window's counter must outlive its own window, because
    -- the next window will read it as the "previous" counter when
    -- computing its weighted estimate. The extra 10 seconds is a
    -- defensive margin on top of the strict 2 * window_size minimum.
    if current_count == 0 then
        redis.call('PEXPIRE', current_key, 2 * window_size_ms + 10000)
    end

    return {1, remaining - 1, reset_in_ms, previous_count, current_count + 1}
end

return {0, remaining, reset_in_ms, previous_count, current_count}
