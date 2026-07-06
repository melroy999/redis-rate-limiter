-- schedule.lua
--
-- Inserts a serialized task into the priority buffer with the metadata
-- that consume.lua needs for age-based dead letter queue support. The
-- task is stored as a member of a sorted set, scored by priority (lower
-- scores are dequeued first).
--
-- KEYS:
--     [1] buffer_key  Sorted set holding pending tasks ordered by priority.
--
-- ARGV:
--     [1] task_json         JSON-encoded task payload, expected to end in '}'.
--     [2] priority          Numeric priority used as the sorted-set score.
--     [3] override_max_age  Optional per-task max age in seconds, which
--                           overrides the limiter default in consume.lua.
local buffer_key = KEYS[1]
local task_json = ARGV[1]
local priority = tonumber(ARGV[2])
local override_max_age = tonumber(ARGV[3])

local redis_time = redis.call('TIME')
local now_ms = (tonumber(redis_time[1]) * 1000) + math.floor(tonumber(redis_time[2]) / 1000)

-- Metadata is injected by string substitution rather than a cjson
-- decode/encode round-trip, because cjson coerces empty arrays ([]) to
-- empty objects ({}), which the Python deserializer would then reject.
local metadata_fields = ',"__meta_arrived_at":' .. now_ms
if override_max_age then
    metadata_fields = metadata_fields .. ',"__meta_max_age":' .. override_max_age
end
local final_json = string.gsub(task_json, '}$', metadata_fields .. '}', 1)

redis.call('ZADD', buffer_key, priority, final_json)