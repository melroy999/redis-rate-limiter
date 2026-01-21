-- A global rate limiter to reliably limit calls to an external API.
-- This specific script consumes 'tasks' from a buffer.
-- The implementation uses a sliding window counter algorithm.
-- KEYS[1]: Base key name (e.g., "rate_limit:api_global")
-- KEYS[2]: Buffer key name (e.g., "rate_limit:api_buffer")
-- KEYS[3]: Concurrency key name (e.g., "rate_limit:api_concurrency")
-- ARGV[1]: Window size in seconds (e.g., 60)
-- ARGV[2]: Max requests allowed (e.g., 100)
-- ARGV[3]: Max simultaneously running tasks allowed (e.g., 10)
-- ARGV[4]: Max Age (seconds)
local base_key = KEYS[1]
local buffer_key = KEYS[2]
local concurrency_key = KEYS[3]
local window_size = tonumber(ARGV[1])
local rate_limit = tonumber(ARGV[2])
local max_concurrency = tonumber(ARGV[3])
local max_age = tonumber(ARGV[4]) or 3600

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

-- Telemetry calculations.
local remaining = math.max(0, math.floor(rate_limit - estimated_count))
local buffer_count = tonumber(redis.call('ZCARD', buffer_key) or 0)

-- Use the Redis server time to calculate a relative 'TTL' for the window.
-- This ensures that clock skew will not be an issue.
local reset_in = current_window_start + window_size - now_sec
reset_in = math.max(0, reset_in)

-- Check if a task can be consumed or not.
if estimated_count < rate_limit and active_now < max_concurrency then
    -- Get the task at the top of the priority queue.
    local tasks = redis.call('ZRANGE', buffer_key, 0, 0)

    -- Do all of the bookkeeping if a the task exists.
    if #tasks > 0 then
        -- Remove the task.
        redis.call('ZREM', buffer_key, tasks[1])

        -- Consume a token by incrementing the window counter.
        redis.call('INCR', current_key)

        -- Increase the concurrently running tasks counter.
        redis.call('INCR', concurrency_key)

        -- Ensure the window will expire and clean itself up.
        -- Note that the current window needs to be available for the next.
        -- Hence, ensure it is available at least two windows, with some jitter.
        if current_count == 0 then
            redis.call('EXPIRE', current_key, window_size * 2 + 10)
        end

        -- Return the task and telemetry information.
        return {1, tasks[1], remaining - 1, active_now + 1, reset_in, buffer_count - 1}
    end
end

-- Return nothing and deny the request.
return {0, false, remaining, active_now, reset_in, buffer_count}