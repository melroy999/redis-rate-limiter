-- A global rate limiter to reliably limit calls to an external API.
-- This specific script consumes 'tasks' from a buffer.
-- The implementation uses a sliding window counter algorithm.
-- KEYS[1]: Base key name (e.g., "rate_limit:api_global")
-- KEYS[2]: Buffer key name (e.g., "rate_limit:api_buffer")
-- KEYS[3]: Concurrency key name (e.g., "rate_limit:api_concurrency")
-- KEYS[4]: Dead letter queue key name (e.g., "rate_limit:dlq")
-- ARGV[1]: Window size in seconds (e.g., 60)
-- ARGV[2]: Max requests allowed (e.g., 100)
-- ARGV[3]: Max simultaneously running tasks allowed (e.g., 10)
-- ARGV[4]: Max Age (DLQ/Queue TTL, seconds)
-- ARGV[5]: Lease Duration (Lock TTL, seconds)
local base_key = KEYS[1]
local buffer_key = KEYS[2]
local concurrency_key = KEYS[3]
local dlq_key = KEYS[4]
local window_size_ms = tonumber(ARGV[1]) * 1000
local rate_limit = tonumber(ARGV[2])
local max_concurrency = tonumber(ARGV[3])
local max_age = tonumber(ARGV[4]) or 3600
local lease_duration = tonumber(ARGV[5]) or 30

-- Use the Redis time to avoid clock skew in distributed systems.
local redis_time = redis.call('TIME')
local timestamp = tonumber(redis_time[1])
local now_ms = (tonumber(redis_time[1]) * 1000) + math.floor(tonumber(redis_time[2]) / 1000)

-- Perform the self-healing concurrency consistency check and remove expired leases.
redis.call('ZREMRANGEBYSCORE', concurrency_key, "-inf", timestamp - 1)

-- Get number of currently running tasks.
local active_now = tonumber(redis.call('ZCARD', concurrency_key) or 0)

-- Find the right keys for our current and previous windows.
local current_window_start = math.floor(now_ms / window_size_ms) * window_size_ms
local current_key = base_key .. ':' .. current_window_start
local previous_window_start = current_window_start - window_size_ms
local previous_key = base_key .. ':' .. previous_window_start

-- Get the count in the previous and current windows.
local current_count = tonumber(redis.call('GET', current_key) or 0)
local previous_count = tonumber(redis.call('GET', previous_key) or 0)

-- Approximate the count using the previous and current window and the weight rate.
-- Estimated Rate = (Previous_Window_Count * Weight) + Current_Window_Count
-- Weight = (window_size_ms - Time_Elapsed_In_Current_Window) / window_size_ms
local time_passed_in_current = now_ms - current_window_start
local weight = (window_size_ms - time_passed_in_current) / window_size_ms
local estimated_count = current_count + (previous_count * weight)

-- Telemetry calculations.
local remaining = math.max(0, math.floor(rate_limit - estimated_count))
local buffer_count = tonumber(redis.call('ZCARD', buffer_key) or 0)

-- Use the Redis server time to calculate a relative 'TTL' for the window.
-- This ensures that clock skew will not be an issue.
local reset_in_ms = current_window_start + window_size_ms - now_ms
reset_in_ms = math.max(0, reset_in_ms)

-- Check if a task can be consumed or not.
if estimated_count < rate_limit and active_now < max_concurrency then
    -- Get the task at the top of the priority queue.
    local tasks = redis.call('ZRANGE', buffer_key, 0, 0)

    -- Do all of the bookkeeping if the task exists.
    if #tasks > 0 then
        -- Get the task's data.
        local raw_task_json = tasks[1]
        local task_data = cjson.decode(raw_task_json)

        -- Remove the task.
        redis.call('ZREM', buffer_key, raw_task_json)

        local task_id = task_data.id

        -- Check if the task has expired.
        local effective_max_age = task_data['__meta_max_age'] or max_age
        local task_age = timestamp - math.floor(task_data['__meta_arrived_at'] / 1000)
        if task_age > effective_max_age then
            -- If it has, add the task to the DLQ.
            redis.call('RPUSH', dlq_key, raw_task_json)
            -- Expired tasks will never execute, so clear their dedupe marker now.
            local inflight_key = task_data['inflight_key']
            redis.call('DEL', inflight_key)

            -- Signify expiration with a -1 value.
            return {-1, false, remaining, active_now, reset_in_ms, buffer_count - 1, previous_count, current_count}
        end

        -- Consume a token by incrementing the window counter.
        redis.call('INCR', current_key)

        -- Ensure the window will expire and clean itself up.
        -- Note that the current window needs to be available for the next.
        -- Hence, ensure it is available at least two windows, with some jitter.
        if current_count == 0 then
            redis.call('PEXPIRE', current_key, 2 * window_size_ms + 10000)
        end

        -- Register the task in the concurrency set and set its expiration time.
        local lease_expiry = timestamp + lease_duration
        redis.call('ZADD', concurrency_key, lease_expiry, task_id)

        -- Return the task and telemetry information.
        return {1, tasks[1], remaining - 1, active_now + 1, reset_in_ms, buffer_count - 1, previous_count, current_count + 1}
    end
end

-- Return nothing and deny the request.
return {0, false, remaining, active_now, reset_in_ms, buffer_count, previous_count, current_count}
