-- Renew the lease on a concurrency slot for a given task.
-- KEYS[1]: Concurrency key name (e.g., "rate_limit:api_concurrency")
-- ARGV[1]: The identifier of the task whose lease is to be renewed.
-- ARGV[2]: The duration of the lease in seconds.
local concurrency_key = KEYS[1]
local task_id = ARGV[1]
local lease_duration = tonumber(ARGV[2])

-- Verify that the task exists in the concurrency set before renewing.
local score = redis.call("ZSCORE", concurrency_key, task_id)

if score then
    -- Compute the new expiration time and update the lease accordingly.
    local now = redis.call("TIME")
    local timestamp = tonumber(now[1])
    local new_expiry = timestamp + lease_duration
    redis.call("ZADD", concurrency_key, new_expiry, task_id)
    return 1
else
    return 0
end
