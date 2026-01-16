-- A global rate limiter to reliably limit calls to an external API.
-- The implementation uses a sliding window counter algorithm.
-- KEYS[1]: Base key name (e.g., "rate_limit:api_global")
-- ARGV[1]: Window size in seconds (e.g., 60)
-- ARGV[2]: Max requests allowed (e.g., 100)
local base_key = KEYS[1]
local window_size = tonumber(ARGV[1])
local rate_limit = tonumber(ARGV[2])

-- Use the Redis time to avoid clock skew in distributed systems.
local time_info = redis.call('TIME')
local now_sec = tonumber(time_info[1])
-- local now_micro = tonumber(time_info[2])

-- Find the right keys for our current and previous windows.
local current_window_start = math.floor(now_sec / window_size) * window_size
local current_key = base_key .. ':' .. current_window_start
local previous_window_start = current_window_start - window_size
local previous_key = base_key .. ':' .. previous_window_start

-- Get the count in the previous and current windows.
local current_count = tonumber(redis.call('GET', current_key) or 0)
local previous_count = tonumber(redis.call('GET', previous_key) or 0)

-- Approximate the count using the previous and current window and the weight rate.
-- Estimated Rate = (Previous_Window_Count * Weight) + Current_Window_Count
-- Weight = (Window_Size - Time_Elapsed_In_Current_Window) / Window_Size
local time_passed_in_current = now_sec - current_window_start
local weight = (window_size - time_passed_in_current) / window_size
local estimated_count = current_count + (previous_count * weight)

-- Decide whether free tokens are available. Use denied as the default state.
local allowed = 0
if estimated_count < rate_limit then
    -- Consume a token by incrementing the window counter.
    redis.call('INCR', current_key)

    -- Ensure the window will expire and clean itself up.
    -- Note that the current window needs to be available for the next.
    -- Hence, ensure it is available at least two windows, with some jitter.
    if current_count == 0 then
        redis.call('EXPIRE', current_key, window_size * 2 + 10)
    end

    -- Token consumed. Proceed.
    allowed = 1
    estimated_count = estimated_count + 1
end

-- Telemetry calculations.
local remaining = math.max(0, math.floor(rate_limit - estimated_count))

-- Use the Redis server time to calculate a relative 'TTL' for the window.
-- This ensures that clock skew will not be an issue.
local reset_in = current_window_start + window_size - now_sec
reset_in = math.max(0, reset_in)

-- Return allowed, remaining, and relative seconds until reset.
return {allowed, remaining, reset_in}