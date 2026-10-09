-- KEYS: reserved, leases, partitions, running, global-running, starts,
--       ring, members, wfq, depths, metrics
-- ARGV: now-ms, batch-size, ready-prefix
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
local ready_prefix = string.sub(KEYS[1], 1, -10) .. ':ready:'
local reclaimed = 0
for _, id in ipairs(ids) do
    local partition = redis.call('HGET', KEYS[3], id)
    if redis.call('HGET', KEYS[2], id) then
        if partition then
            redis.call('RPUSH', ready_prefix .. partition, id)
            if redis.call('SADD', KEYS[8], partition) == 1 then
                redis.call('RPUSH', KEYS[7], partition)
                local head = redis.call('ZRANGE', KEYS[9], 0, 0, 'WITHSCORES')
                redis.call('ZADD', KEYS[9], tonumber(head[2] or '0'), partition)
            end
            redis.call('HINCRBY', KEYS[10], partition, 1)
            local running = tonumber(redis.call('HINCRBY', KEYS[4], partition, -1))
            if running <= 0 then redis.call('HDEL', KEYS[4], partition) end
        end
        local global = tonumber(redis.call('DECR', KEYS[5]))
        if global <= 0 then redis.call('DEL', KEYS[5]) end
        redis.call('HDEL', KEYS[6], id)
        redis.call('HINCRBY', KEYS[11], 'reclaimed', 1)
        redis.call('HDEL', KEYS[2], id)
        reclaimed = reclaimed + 1
    end
    redis.call('ZREM', KEYS[1], id)
end
return reclaimed
