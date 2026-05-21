-- consume.lua
--
-- Atomically dequeues at most one task from the priority buffer and
-- admits it for execution if both the rate limit and the concurrency
-- limit allow. This is the hot path for backends that route work
-- through a buffered queue (Celery, Dramatiq, Huey, ThreadPool,
-- AsyncIO).
--
-- Algorithm: sliding window counter. Two adjacent fixed windows are
-- maintained in Redis, and the effective rate is approximated as a
-- weighted blend of the two. See docs/architecture/sliding-window.md
-- for the full derivation; tests/algorithms/sliding_window_counter.py
-- contains a Python reference implementation kept in lockstep with
-- this script.
--
-- KEYS:
--     [1] base_key         Base key for the per-window counters; the
--                          actual subkeys take the form
--                          "<base_key>:<window_start_ms>".
--     [2] buffer_key       Sorted set of pending tasks, scored by
--                          priority (lower scores are dequeued first).
--     [3] concurrency_key  Sorted set of in-flight task IDs, scored by
--                          lease expiry timestamps in seconds.
--     [4] dlq_key          List used as the dead letter queue for tasks
--                          that have exceeded their maximum age.
--
-- ARGV:
--     [1] window_size      Sliding window size in seconds.
--     [2] rate_limit       Maximum tasks permitted per sliding window.
--     [3] max_concurrency  Maximum tasks permitted to run simultaneously.
--     [4] max_age          Default maximum task age in seconds; defaults
--                          to 3600 if absent.
--     [5] lease_duration   Concurrency lease lifetime in seconds;
--                          defaults to 30 if absent.
--
-- Returns a table whose first element is a status code:
--      1: task admitted; second element is the JSON-encoded task.
--      0: blocked by rate or concurrency limit, or buffer was empty.
--     -1: head-of-buffer task expired and was moved to the DLQ; the
--         caller should retry to inspect the next task.
--     -2: head-of-buffer was an acquire marker whose deadline had elapsed;
--         it was silently dropped (no DLQ, no signal). The caller should
--         retry to inspect the next task.
-- The remaining elements expose telemetry, in order:
--     {status, task_json_or_false, remaining, active_now, reset_in_ms,
--      buffer_count, previous_count, current_count}
local base_key = KEYS[1]
local buffer_key = KEYS[2]
local concurrency_key = KEYS[3]
local dlq_key = KEYS[4]
local window_size_ms = tonumber(ARGV[1]) * 1000
local rate_limit = tonumber(ARGV[2])
local max_concurrency = tonumber(ARGV[3])
local max_age = tonumber(ARGV[4]) or 3600
local lease_duration = tonumber(ARGV[5]) or 30

-- All time arithmetic uses the Redis server clock so that calculations
-- remain consistent regardless of client clock skew.
local redis_time = redis.call('TIME')
local timestamp = tonumber(redis_time[1])
local now_ms = (tonumber(redis_time[1]) * 1000) + math.floor(tonumber(redis_time[2]) / 1000)

-- Self-heal: reclaim concurrency slots whose leases have already expired,
-- recovering capacity held by tasks that crashed before releasing.
redis.call('ZREMRANGEBYSCORE', concurrency_key, "-inf", timestamp - 1)
local active_now = tonumber(redis.call('ZCARD', concurrency_key) or 0)

local current_window_start = math.floor(now_ms / window_size_ms) * window_size_ms
local current_key = base_key .. ':' .. current_window_start
local previous_window_start = current_window_start - window_size_ms
local previous_key = base_key .. ':' .. previous_window_start

local current_count = tonumber(redis.call('GET', current_key) or 0)
local previous_count = tonumber(redis.call('GET', previous_key) or 0)

-- Sliding window counter estimate. The weight decays linearly from 1 at
-- the start of the current window to 0 at the end, fading out the
-- previous window's contribution as the current window fills in.
local time_passed_in_current = now_ms - current_window_start
local weight = (window_size_ms - time_passed_in_current) / window_size_ms
local estimated_count = current_count + (previous_count * weight)

local remaining = math.max(0, math.floor(rate_limit - estimated_count))
local buffer_count = tonumber(redis.call('ZCARD', buffer_key) or 0)
local reset_in_ms = math.max(0, current_window_start + window_size_ms - now_ms)

if estimated_count < rate_limit and active_now < max_concurrency then
    local tasks = redis.call('ZRANGE', buffer_key, 0, 0)

    if #tasks > 0 then
        local raw_task_json = tasks[1]
        local task_data = cjson.decode(raw_task_json)
        redis.call('ZREM', buffer_key, raw_task_json)

        local task_id = task_data.id

        -- A per-task override may be set in schedule.lua via __meta_max_age.
        local effective_max_age = task_data['__meta_max_age'] or max_age
        local task_age = timestamp - math.floor(task_data['__meta_arrived_at'] / 1000)
        if task_age > effective_max_age then
            -- Acquire markers represent abandoned admission attempts, not
            -- failed task work; silently drop instead of polluting the DLQ.
            if task_data.func_path ~= '__redis_rate_limiter_acquire_marker__' then
                redis.call('RPUSH', dlq_key, raw_task_json)
            end
            -- Clear the deduplication marker to allow re-submission.
            local inflight_key = task_data['inflight_key']
            redis.call('DEL', inflight_key)

            return {-1, false, remaining, active_now, reset_in_ms, buffer_count - 1, previous_count, current_count}
        end

        -- Acquire markers carry an absolute deadline (arrived_at + timeout).
        -- Skip admission if the deadline has already passed: the caller's
        -- BLPOP has already timed out, so reserving a slot would leak
        -- capacity until the lease expires. Returns status -2 so the drain
        -- loop can retry the next task without conflating this with the
        -- expired-to-DLQ path (status -1).
        if task_data.func_path == '__redis_rate_limiter_acquire_marker__' then
            local timeout_ms = task_data.payload and task_data.payload._acquire_timeout_ms
            if timeout_ms then
                local deadline_ms = task_data['__meta_arrived_at'] + timeout_ms
                if now_ms >= deadline_ms then
                    redis.call('DEL', task_data['inflight_key'])
                    return {-2, false, remaining, active_now, reset_in_ms, buffer_count - 1, previous_count, current_count}
                end
            end
        end

        redis.call('INCR', current_key)

        -- The current window's counter must outlive its own window,
        -- because the next window will read it as the "previous" counter
        -- when computing its weighted estimate. The extra 10 seconds is
        -- a defensive margin on top of the strict 2 * window_size minimum.
        if current_count == 0 then
            redis.call('PEXPIRE', current_key, 2 * window_size_ms + 10000)
        end

        local lease_expiry = timestamp + lease_duration
        redis.call('ZADD', concurrency_key, lease_expiry, task_id)

        -- Acquire markers signal their waiting caller via BLPOP on a
        -- per-call list. The RPUSH is atomic with the lease ZADD: when
        -- the caller's BLPOP wakes, the slot is already reserved. The
        -- TTL is derived from the acquire timeout so the key does not
        -- expire while the caller's BLPOP is still waiting.
        if task_data.func_path == '__redis_rate_limiter_acquire_marker__' then
            local signal_key = base_key .. ':acquire:' .. task_id
            redis.call('RPUSH', signal_key, task_id)
            redis.call('PEXPIRE', signal_key, task_data.payload._acquire_timeout_ms)
        end

        return {1, tasks[1], remaining - 1, active_now + 1, reset_in_ms, buffer_count - 1, previous_count, current_count + 1}
    end
end

return {0, false, remaining, active_now, reset_in_ms, buffer_count, previous_count, current_count}
