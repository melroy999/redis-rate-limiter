-- health.lua
--
-- Returns a read-only snapshot of all observable rate limiter state.
-- Used by the diagnostics and Prometheus integrations to expose limiter
-- health without mutating any of the underlying keys. The estimation
-- logic mirrors consume.lua so that reported values agree with what
-- consume.lua would compute on the same call.
--
-- KEYS:
--     [1] base_key         Base key for the per-window counters.
--     [2] buffer_key       Sorted set holding pending tasks.
--     [3] concurrency_key  Sorted set tracking in-flight task leases.
--
-- ARGV:
--     [1] window_size  Sliding window size in seconds.
--
-- Returns:
--     {previous_count, current_count, estimated_count,
--      active_now, reset_in_ms, buffer_count}
local base_key = KEYS[1]
local buffer_key = KEYS[2]
local concurrency_key = KEYS[3]
local window_size_ms = tonumber(ARGV[1]) * 1000

-- All time arithmetic uses the Redis server clock so that calculations
-- remain consistent regardless of client clock skew.
local redis_time = redis.call('TIME')
local now_ms = (tonumber(redis_time[1]) * 1000) + math.floor(tonumber(redis_time[2]) / 1000)

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

local active_now = tonumber(redis.call('ZCARD', concurrency_key) or 0)
local buffer_count = tonumber(redis.call('ZCARD', buffer_key) or 0)
local reset_in_ms = math.max(0, current_window_start + window_size_ms - now_ms)

return {previous_count, current_count, estimated_count, active_now, reset_in_ms, buffer_count}