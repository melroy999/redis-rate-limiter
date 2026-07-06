-- release.lua
--
-- Atomically releases a concurrency slot, clears the in-flight
-- deduplication marker, and publishes a drain signal so that other
-- workers can attempt to consume the freed slot. Combining all three
-- operations into one EVALSHA saves two Redis round trips on every
-- consumed task compared to issuing them separately.
--
-- KEYS:
--     [1] concurrency_key  Sorted set of in-flight task IDs, scored by
--                          lease expiry timestamps in seconds.
--     [2] inflight_key     Per-task deduplication marker key.
--     [3] drain_channel    Pub/Sub channel used to notify other workers
--                          that a slot has been freed and they should
--                          attempt to consume.
--
-- ARGV:
--     [1] task_id    Identifier of the task to release. An empty string
--                    is accepted and results in a ZREM/DEL no-op, but
--                    the drain signal is still published so that
--                    callers managing tasks that never reached the
--                    in-flight stage still wake other workers.
--     [2] worker_id  Sender identifier embedded in the published
--                    message; subscribers use this to filter out
--                    self-notifications.
--
-- Returns:
--     {removed_concurrency, removed_inflight}
-- where each element is 0 or 1, mirroring the ZREM and DEL return
-- values. Both are 0 when task_id is empty.
local concurrency_key = KEYS[1]
local inflight_key = KEYS[2]
local drain_channel = KEYS[3]
local task_id = ARGV[1]
local worker_id = ARGV[2]

local removed_concurrency = 0
local removed_inflight = 0

if task_id and task_id ~= "" then
    removed_concurrency = redis.call("ZREM", concurrency_key, task_id)
    removed_inflight = redis.call("DEL", inflight_key)
end

redis.call("PUBLISH", drain_channel, worker_id)

return {removed_concurrency, removed_inflight}
