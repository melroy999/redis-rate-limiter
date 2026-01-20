-- A global rate limiter to reliably limit calls to an external API.
-- The implementation uses a sliding window counter algorithm.
-- KEYS[1]: Base key name (e.g., "rate_limit:api_global")
-- KEYS[2]: Buffer key name (e.g., "rate_limit:api_buffer")
-- KEYS[3]: Concurrency key name (e.g., "rate_limit:api_concurrency")
-- ARGV[1]: Window size in seconds (e.g., 60)
-- ARGV[2]: Max requests allowed (e.g., 100)
-- ARGV[3]: Max simultaneously running tasks allowed (e.g., 10)
local base_key = KEYS[1]
local buffer_key = KEYS[2]
local concurrency_key = KEYS[3]
local window_size = tonumber(ARGV[1])
local rate_limit = tonumber(ARGV[2])
local max_concurrency = tonumber(ARGV[3])

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

-- Get number of currently running tasks.
local active_now = tonumber(redis.call('GET', concurrency_key) or 0)
local buffer_count = tonumber(redis.call('ZCARD', buffer_key) or 0)

-- Use the Redis server time to calculate a relative 'TTL' for the window.
-- This ensures that clock skew will not be an issue.
local reset_in = current_window_start + window_size - now_sec
reset_in = math.max(0, reset_in)

-- Return all useful information.
return {previous_count, current_count, estimated_count, active_now, reset_in, buffer_count}