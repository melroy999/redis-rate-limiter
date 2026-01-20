-- Schedule a given task and track creation time for DLQ support.
-- KEYS[1]: Buffer key name (e.g., "rate_limit:api_buffer")
-- ARGV[1]: A json string defining the parameters of the task.
-- ARGV[2]: The priority of the task.
local buffer_key = KEYS[1]
local task_json = ARGV[1]
local priority = tonumber(ARGV[2])

-- Check if task exists already.
local exists = redis.call('ZSCORE', buffer_key, task_json)
if exists then
    -- Task already queued, do nothing (Mimics NX=True).
    return 0
end

-- Get the time.
local time_info = redis.call('TIME')
local now_sec = tonumber(time_info[1])

-- Inject the arrival time into the json.
local task = cjson.decode(task_json)
task['_arrived_at'] = now_sec
local final_json = cjson.encode(task)

-- Queue the task.
redis.call('ZADD', buffer_key, priority, final_json)