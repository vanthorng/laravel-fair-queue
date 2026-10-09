-- KEYS: jobs, partitions, reserved, leases, running, global-running, starts, metrics,
--       delayed, ring, members, wfq, depths
-- ARGV: id, token, now-ms, release-at-ms (0 means acknowledge), ready-prefix
if redis.call('HGET', KEYS[4], ARGV[1]) ~= ARGV[2] then return 0 end
local partition = redis.call('HGET', KEYS[2], ARGV[1])
if not partition then return 0 end
local ready_prefix = string.sub(KEYS[1], 1, -6) .. ':ready:'
redis.call('ZREM', KEYS[3], ARGV[1])
redis.call('HDEL', KEYS[4], ARGV[1])
local running = tonumber(redis.call('HINCRBY', KEYS[5], partition, -1))
if running <= 0 then redis.call('HDEL', KEYS[5], partition) end
local global = tonumber(redis.call('DECR', KEYS[6]))
if global <= 0 then redis.call('DEL', KEYS[6]) end
local started = tonumber(redis.call('HGET', KEYS[7], ARGV[1]) or ARGV[3])
local duration = math.max(0, tonumber(ARGV[3]) - started)
redis.call('HDEL', KEYS[7], ARGV[1])
redis.call('HINCRBY', KEYS[8], 'duration_ms', duration)
if tonumber(ARGV[4]) > 0 then
    redis.call('HINCRBY', KEYS[8], 'released', 1)
    redis.call('HINCRBY', KEYS[13], partition, 1)
    if tonumber(ARGV[4]) > tonumber(ARGV[3]) then
        redis.call('ZADD', KEYS[9], ARGV[4], ARGV[1])
    else
        redis.call('RPUSH', ready_prefix .. partition, ARGV[1])
        if redis.call('SADD', KEYS[11], partition) == 1 then
            redis.call('RPUSH', KEYS[10], partition)
            local head = redis.call('ZRANGE', KEYS[12], 0, 0, 'WITHSCORES')
            redis.call('ZADD', KEYS[12], tonumber(head[2] or '0'), partition)
        end
    end
else
    redis.call('HDEL', KEYS[1], ARGV[1])
    redis.call('HDEL', KEYS[2], ARGV[1])
    redis.call('HINCRBY', KEYS[8], 'completed', 1)
end
return {1, duration, partition}
