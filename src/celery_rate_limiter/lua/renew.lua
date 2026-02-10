-- Renew the lease over a concurrency slot.
-- KEYS[1]: Concurrency key name (e.g., "rate_limit:api_concurrency")
-- ARGV[1]: The id of the task for which the lease needs to be renewed.
-- ARGV[2]: The duration of the lease in seconds.
local concurrency_key = KEYS[1]
local task_id = ARGV[1]
local lease_duration = tonumber(ARGV[2])

-- Check if the task exists in the concurrency set.
local score = redis.call("ZSCORE", concurrency_key, task_id)

if score then
    -- Get the new expiry time and update the lease.
    local now = redis.call("TIME")
    local timestamp = tonumber(now[1])
    local new_expiry = timestamp + lease_duration
    redis.call("ZADD", concurrency_key, new_expiry, task_id)
    return 1
else
    return 0
end
