-- KEYS: jobs, partitions, delayed, ring, members, wfq, depths, dispatch-next, metrics,
--       weights, max-concurrent, process-rates, process-bursts
-- ARGV: id, payload, partition, available-at-ms, now-ms, rate, burst, weight, ready-key,
--       max-concurrent, process-rate, process-burst
if redis.call('HEXISTS', KEYS[1], ARGV[1]) == 1 then return 0 end
local at = tonumber(ARGV[4])
local now = tonumber(ARGV[5])
local rate = tonumber(ARGV[6])
local burst = math.max(1, tonumber(ARGV[7]))
if rate > 0 then
    local interval = 1000 / rate
    local previous = tonumber(redis.call('HGET', KEYS[8], ARGV[3]) or '0')
    local scheduled = math.max(at, previous - ((burst - 1) * interval))
    at = math.max(at, scheduled)
    redis.call('HSET', KEYS[8], ARGV[3], math.max(previous, at - ((burst - 1) * interval)) + interval)
end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
redis.call('HSET', KEYS[2], ARGV[1], ARGV[3])
redis.call('HSET', KEYS[10], ARGV[3], ARGV[8])
redis.call('HSET', KEYS[11], ARGV[3], ARGV[10])
redis.call('HSET', KEYS[12], ARGV[3], ARGV[11])
redis.call('HSET', KEYS[13], ARGV[3], ARGV[12])
redis.call('HINCRBY', KEYS[7], ARGV[3], 1)
redis.call('HINCRBY', KEYS[9], 'dispatched', 1)
if at > now then
    redis.call('ZADD', KEYS[3], at, ARGV[1])
else
    local ready = string.sub(KEYS[1], 1, -6) .. ':ready:' .. ARGV[3]
    redis.call('RPUSH', ready, ARGV[1])
    if redis.call('SADD', KEYS[5], ARGV[3]) == 1 then
        redis.call('RPUSH', KEYS[4], ARGV[3])
        local head = redis.call('ZRANGE', KEYS[6], 0, 0, 'WITHSCORES')
        redis.call('ZADD', KEYS[6], tonumber(head[2] or '0'), ARGV[3])
    end
end
return at
