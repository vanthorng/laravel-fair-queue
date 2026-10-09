-- KEYS: ring, members, wfq, jobs, partitions, reserved, leases, running,
--       global-running, depths, metrics, process-next, deficit, weights, penalties, starts,
--       wfq-cursor, max-concurrent, process-rates, process-bursts, wait-ms, reserved-count, delayed
-- ARGV: ready-prefix, strategy, now-ms, lease-until-ms, max-global, max-partition,
--       max-scan, rate, burst, token, max-weight
local now = tonumber(ARGV[3])
local global_limit = tonumber(ARGV[5])
if global_limit > 0 and tonumber(redis.call('GET', KEYS[9]) or '0') >= global_limit then return nil end
local scan = math.max(1, tonumber(ARGV[7]))
local strategy = ARGV[2]
local ready_prefix = string.sub(KEYS[1], 1, -6) .. ':ready:'
local candidates
if strategy == 'wfq' then
    local count = redis.call('ZCARD', KEYS[3])
    local offset = 0
    if count > 0 then offset = tonumber(redis.call('GET', KEYS[17]) or '0') % count end
    candidates = redis.call('ZRANGE', KEYS[3], offset, offset + scan - 1)
    if count > 0 and #candidates < scan then
        local wrapped = redis.call('ZRANGE', KEYS[3], 0, scan - #candidates - 1)
        for _, member in ipairs(wrapped) do table.insert(candidates, member) end
    end
    if count > 0 then redis.call('SET', KEYS[17], (offset + scan) % count) end
else
    candidates = redis.call('LRANGE', KEYS[1], 0, scan - 1)
end
for _, partition in ipairs(candidates) do
    local ready = ready_prefix .. partition
    if redis.call('LLEN', ready) == 0 then
        redis.call('SREM', KEYS[2], partition)
        redis.call('ZREM', KEYS[3], partition)
        redis.call('LREM', KEYS[1], 1, partition)
        redis.call('HDEL', KEYS[13], partition)
    else
        local running = tonumber(redis.call('HGET', KEYS[8], partition) or '0')
        local limit = tonumber(redis.call('HGET', KEYS[18], partition) or ARGV[6])
        local penalty = tonumber(redis.call('HGET', KEYS[15], partition) or '100')
        local rate = tonumber(redis.call('HGET', KEYS[19], partition) or ARGV[8]) * penalty / 100
        local burst = math.max(1, tonumber(redis.call('HGET', KEYS[20], partition) or ARGV[9]))
        local permitted = limit <= 0 or running < limit
        local next_time = 0
        local interval = 0
        if permitted and rate > 0 then
            interval = 1000 / rate
            next_time = tonumber(redis.call('HGET', KEYS[12], partition) or '0')
            permitted = next_time - ((burst - 1) * interval) <= now
        end
        if permitted then
            local weight = math.max(1, math.min(tonumber(ARGV[11]), tonumber(redis.call('HGET', KEYS[14], partition) or '1')))
            weight = math.max(1, math.floor(weight * penalty / 100))
            local deficit = tonumber(redis.call('HGET', KEYS[13], partition) or '0')
            if strategy == 'drr' and deficit <= 0 then deficit = deficit + weight end
            local id = redis.call('LPOP', ready)
            if id then
                local raw = redis.call('HGET', KEYS[4], id)
                if raw and redis.call('HEXISTS', KEYS[7], id) == 0
                    and redis.call('ZSCORE', KEYS[6], id) == false
                    and redis.call('ZSCORE', KEYS[23], id) == false then
                    local decoded = cjson.decode(raw)
                    decoded.attempts = (tonumber(decoded.attempts) or 0) + 1
                    redis.call('HSET', KEYS[4], id, cjson.encode(decoded))
                    redis.call('ZADD', KEYS[6], ARGV[4], id)
                    redis.call('HSET', KEYS[7], id, ARGV[10])
                    redis.call('HSET', KEYS[16], id, now)
                    redis.call('HINCRBY', KEYS[8], partition, 1)
                    redis.call('INCR', KEYS[9])
                    redis.call('HINCRBY', KEYS[10], partition, -1)
                    redis.call('HINCRBY', KEYS[11], 'reserved', 1)
                    local enqueued = tonumber((decoded.fair or {}).enqueued_at_ms or now)
                    redis.call('HINCRBY', KEYS[11], 'wait_ms', math.max(0, now - enqueued))
                    redis.call('HINCRBY', KEYS[21], partition, math.max(0, now - enqueued))
                    redis.call('HINCRBY', KEYS[22], partition, 1)
                    if rate > 0 then
                        redis.call('HSET', KEYS[12], partition, math.max(next_time, now - ((burst - 1) * interval)) + interval)
                    end
                    if strategy == 'wfq' then
                        redis.call('ZINCRBY', KEYS[3], 1 / weight, partition)
                    else
                        redis.call('LREM', KEYS[1], 1, partition)
                        if strategy == 'drr' then
                            deficit = deficit - 1
                            redis.call('HSET', KEYS[13], partition, deficit)
                        end
                        if strategy == 'drr' and deficit > 0 and redis.call('LLEN', ready) > 0 then
                            redis.call('LPUSH', KEYS[1], partition)
                        else
                            redis.call('RPUSH', KEYS[1], partition)
                        end
                    end
                    if redis.call('LLEN', ready) == 0 then
                        redis.call('SREM', KEYS[2], partition)
                        redis.call('ZREM', KEYS[3], partition)
                        redis.call('LREM', KEYS[1], 1, partition)
                        redis.call('HDEL', KEYS[13], partition)
                    end
                    if strategy == 'wfq' then redis.call('SET', KEYS[17], 0) end
                    return {raw, ARGV[10], id, partition}
                end
            end
        end
        if strategy ~= 'wfq' then
            redis.call('LREM', KEYS[1], 1, partition)
            redis.call('RPUSH', KEYS[1], partition)
        end
    end
end
return nil
