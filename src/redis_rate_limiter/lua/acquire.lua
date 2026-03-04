-- Lightweight sliding window counter check for request-oriented rate limiting.
-- This script mirrors the sliding window estimation logic from consume.lua
-- without buffer, concurrency, DLQ, or task management.
--
-- KEYS[1]: Base key (e.g., "rl:api:user_123")
-- ARGV[1]: The window size in seconds (e.g., 60)
-- ARGV[2]: The maximum number of requests permitted (e.g., 100)
local base_key = KEYS[1]
local window_size_ms = tonumber(ARGV[1]) * 1000
local rate_limit = tonumber(ARGV[2])

-- Retrieve the Redis server time to avoid clock skew in distributed systems.
local redis_time = redis.call('TIME')
local now_ms = (tonumber(redis_time[1]) * 1000) + math.floor(tonumber(redis_time[2]) / 1000)

-- Determine the keys corresponding to the current and previous windows.
local current_window_start = math.floor(now_ms / window_size_ms) * window_size_ms
local current_key = base_key .. ':' .. current_window_start
local previous_window_start = current_window_start - window_size_ms
local previous_key = base_key .. ':' .. previous_window_start

-- Retrieve the request counts for the previous and current windows.
local current_count = tonumber(redis.call('GET', current_key) or 0)
local previous_count = tonumber(redis.call('GET', previous_key) or 0)

-- Approximate the total count using a weighted combination of the previous and current windows.
local time_passed_in_current = now_ms - current_window_start
local weight = (window_size_ms - time_passed_in_current) / window_size_ms
local estimated_count = current_count + (previous_count * weight)

-- Calculate telemetry values.
local remaining = math.max(0, math.floor(rate_limit - estimated_count))
local reset_in_ms = math.max(0, current_window_start + window_size_ms - now_ms)

-- Determine whether the request is permitted.
if estimated_count < rate_limit then
    -- Consume a token by incrementing the current window counter.
    redis.call('INCR', current_key)

    -- Ensure the window key will expire and be cleaned up automatically.
    if current_count == 0 then
        redis.call('PEXPIRE', current_key, 2 * window_size_ms + 10000)
    end

    -- The request is allowed; return the updated telemetry.
    return {1, remaining - 1, reset_in_ms, previous_count, current_count + 1}
end

-- The rate limit has been reached; deny the request.
return {0, remaining, reset_in_ms, previous_count, current_count}
