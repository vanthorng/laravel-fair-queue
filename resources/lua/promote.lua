-- KEYS: delayed, partitions, ring, members, wfq
-- ARGV: now-ms, batch-size, ready-prefix
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
local ready_prefix = string.sub(KEYS[1], 1, -9) .. ':ready:'
for _, id in ipairs(ids) do
    local partition = redis.call('HGET', KEYS[2], id)
    if partition then
        redis.call('RPUSH', ready_prefix .. partition, id)
        if redis.call('SADD', KEYS[4], partition) == 1 then
            redis.call('RPUSH', KEYS[3], partition)
            local head = redis.call('ZRANGE', KEYS[5], 0, 0, 'WITHSCORES')
            redis.call('ZADD', KEYS[5], tonumber(head[2] or '0'), partition)
        end
    end
    redis.call('ZREM', KEYS[1], id)
end
return #ids
