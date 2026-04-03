-- A global rate limiter designed to reliably constrain calls to an external API.
-- This script is responsible for consuming tasks from the buffer.
-- The implementation employs a sliding window counter algorithm.
-- KEYS[1]: Base key name (e.g., "rate_limit:api_global")
-- KEYS[2]: Buffer key name (e.g., "rate_limit:api_buffer")
-- KEYS[3]: Concurrency key name (e.g., "rate_limit:api_concurrency")
-- KEYS[4]: Dead letter queue key name (e.g., "rate_limit:dlq")
-- ARGV[1]: The window size in seconds (e.g., 60)
-- ARGV[2]: The maximum number of requests permitted (e.g., 100)
-- ARGV[3]: The maximum number of simultaneously running tasks permitted (e.g., 10)
-- ARGV[4]: The maximum age in seconds, used as the TTL for the queue.
-- ARGV[5]: The lease duration in seconds, used as the TTL for concurrency locks.
local base_key = KEYS[1]
local buffer_key = KEYS[2]
local concurrency_key = KEYS[3]
local dlq_key = KEYS[4]
local window_size_ms = tonumber(ARGV[1]) * 1000
local rate_limit = tonumber(ARGV[2])
local max_concurrency = tonumber(ARGV[3])
local max_age = tonumber(ARGV[4]) or 3600
local lease_duration = tonumber(ARGV[5]) or 30

-- Retrieve the Redis server time to avoid clock skew in distributed systems.
local redis_time = redis.call('TIME')
local timestamp = tonumber(redis_time[1])
local now_ms = (tonumber(redis_time[1]) * 1000) + math.floor(tonumber(redis_time[2]) / 1000)

-- Perform a self-healing concurrency consistency check by removing expired leases.
redis.call('ZREMRANGEBYSCORE', concurrency_key, "-inf", timestamp - 1)

-- Retrieve the number of currently active tasks.
local active_now = tonumber(redis.call('ZCARD', concurrency_key) or 0)

-- Determine the keys corresponding to the current and previous windows.
local current_window_start = math.floor(now_ms / window_size_ms) * window_size_ms
local current_key = base_key .. ':' .. current_window_start
local previous_window_start = current_window_start - window_size_ms
local previous_key = base_key .. ':' .. previous_window_start

-- Retrieve the request counts for the previous and current windows.
local current_count = tonumber(redis.call('GET', current_key) or 0)
local previous_count = tonumber(redis.call('GET', previous_key) or 0)

-- Approximate the total count using a weighted combination of the previous and current windows.
-- Estimated_Rate = (Previous_Window_Count * Weight) + Current_Window_Count
-- Weight = (window_size_ms - Time_Elapsed_In_Current_Window) / window_size_ms
local time_passed_in_current = now_ms - current_window_start
local weight = (window_size_ms - time_passed_in_current) / window_size_ms
local estimated_count = current_count + (previous_count * weight)

-- Compute telemetry values.
local remaining = math.max(0, math.floor(rate_limit - estimated_count))
local buffer_count = tonumber(redis.call('ZCARD', buffer_key) or 0)

-- Calculate the time remaining in the current window relative to the Redis server clock.
-- This approach ensures that clock skew does not affect the result.
local reset_in_ms = current_window_start + window_size_ms - now_ms
reset_in_ms = math.max(0, reset_in_ms)

-- Determine whether a task may be consumed.
if estimated_count < rate_limit and active_now < max_concurrency then
    -- Retrieve the task at the head of the priority queue.
    local tasks = redis.call('ZRANGE', buffer_key, 0, 0)

    -- Perform all necessary bookkeeping if the task exists.
    if #tasks > 0 then
        -- Deserialize the task data.
        local raw_task_json = tasks[1]
        local task_data = cjson.decode(raw_task_json)

        -- Remove the task from the buffer.
        redis.call('ZREM', buffer_key, raw_task_json)

        local task_id = task_data.id

        -- Verify whether the task has exceeded its maximum age.
        local effective_max_age = task_data['__meta_max_age'] or max_age
        local task_age = timestamp - math.floor(task_data['__meta_arrived_at'] / 1000)
        if task_age > effective_max_age then
            -- The task has expired; transfer it to the dead letter queue.
            redis.call('RPUSH', dlq_key, raw_task_json)
            -- Expired tasks will not be executed; therefore, clear the deduplication marker immediately.
            local inflight_key = task_data['inflight_key']
            redis.call('DEL', inflight_key)

            -- Return a status code of -1 to indicate that the task has expired.
            return {-1, false, remaining, active_now, reset_in_ms, buffer_count - 1, previous_count, current_count}
        end

        -- Consume a token by incrementing the current window counter.
        redis.call('INCR', current_key)

        -- Ensure the window key will expire and be cleaned up automatically.
        -- The current window must remain available for the subsequent window's weight calculation.
        -- Hence, set an expiration of at least two window periods, with additional jitter.
        if current_count == 0 then
            redis.call('PEXPIRE', current_key, 2 * window_size_ms + 10000)
        end

        -- Register the task in the concurrency set and assign its lease expiration time.
        local lease_expiry = timestamp + lease_duration
        redis.call('ZADD', concurrency_key, lease_expiry, task_id)

        -- Return the consumed task along with the telemetry information.
        return {1, tasks[1], remaining - 1, active_now + 1, reset_in_ms, buffer_count - 1, previous_count, current_count + 1}
    end
end

-- The rate limit or concurrency limit has been reached; deny the request.
return {0, false, remaining, active_now, reset_in_ms, buffer_count, previous_count, current_count}
