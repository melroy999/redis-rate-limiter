-- renew.lua
--
-- Extends the lease on a concurrency slot for a still-running task,
-- preventing the self-healing pass in consume.lua from reclaiming the
-- slot prematurely. Used by long-running tasks that approach their
-- original lease expiry.
--
-- KEYS:
--     [1] concurrency_key  Sorted set tracking in-flight task IDs scored
--                          by lease expiry timestamps in seconds.
--
-- ARGV:
--     [1] task_id         Identifier of the task whose lease should be
--                         extended.
--     [2] lease_duration  Replacement lease duration in seconds, applied
--                         relative to the current Redis server time.
--
-- Returns 1 if the lease was renewed, or 0 if the task is no longer
-- present in the concurrency set (e.g., already reclaimed or completed).
local concurrency_key = KEYS[1]
local task_id = ARGV[1]
local lease_duration = tonumber(ARGV[2])

local score = redis.call("ZSCORE", concurrency_key, task_id)

if score then
    local now = redis.call("TIME")
    local timestamp = tonumber(now[1])
    local new_expiry = timestamp + lease_duration
    redis.call("ZADD", concurrency_key, new_expiry, task_id)
    return 1
else
    return 0
end
