-- KEYS: short-lived guard
if redis.call('SET', KEYS[1], '1', 'NX', 'PX', 1000) then return 1 end
return 0
