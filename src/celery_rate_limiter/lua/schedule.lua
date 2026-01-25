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

-- Inject the arrival time into the json.
local task = cjson.decode(task_json)
task['_arrived_at'] = now_ms
if override_max_age then
    -- Store the max age override if provided.
    task['_max_age'] = override_max_age
end
local final_json = cjson.encode(task)

-- Queue the task.
redis.call('ZADD', buffer_key, priority, final_json)