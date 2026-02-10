-- Schedule a given task and track creation time for DLQ support.
-- KEYS[1]: Buffer key name (e.g., "rate_limit:api_buffer")
-- ARGV[1]: A json string defining the parameters of the task.
-- ARGV[2]: The priority of the task.
-- ARGV[3]: An optional override value for the max age of a task.
local buffer_key = KEYS[1]
local task_json = ARGV[1]
local priority = tonumber(ARGV[2])
local override_max_age = tonumber(ARGV[3])

-- Get the time.
local redis_time = redis.call('TIME')
local now_ms = (tonumber(redis_time[1]) * 1000) + math.floor(tonumber(redis_time[2]) / 1000)

-- Inject metadata into JSON using string manipulation to preserve array/object types.
-- This avoids cjson decode/encode cycle which converts empty arrays [] to empty objects {}.
local metadata_fields = ',"__meta_arrived_at":' .. now_ms
if override_max_age then
    metadata_fields = metadata_fields .. ',"__meta_max_age":' .. override_max_age
end

-- Inject before the final closing brace.
local final_json = string.gsub(task_json, '}$', metadata_fields .. '}', 1)

-- Queue the task.
redis.call('ZADD', buffer_key, priority, final_json)