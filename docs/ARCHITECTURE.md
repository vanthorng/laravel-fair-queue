# Architecture and operational contract

Each named fair queue uses a separate Redis key namespace, `fairq:{sha1(queue-name)}` by default. All workers for one queue share this namespace. The core keys hold a job payload hash, job-to-partition hash, per-partition ready lists, delayed and reserved sorted sets, a round-robin ring, an active-members set, a weighted scheduler sorted set, per-partition depths, and lease tokens. A reservation script claims the job and global/partition concurrency slots in one Redis operation. Release and acknowledgement compare the token before changing state. Expiry recovery runs in bounded batches.

The queue is at-least-once. A worker crash leaves its reservation until `retry_after`; a later worker reclaims and retries it. Set job and worker timeouts shorter than the lease. A paused process can resume after lease expiry, so handlers must make side effects idempotent or use a downstream fencing token. Redis AOF/replication policy remains part of the durability guarantee.

## Scheduling

- **Round robin:** rotate active partitions after each reservation.
- **Deficit round robin:** add the effective weight as quantum and serve one unit per job until the partition's deficit is spent.
- **Weighted fair queuing:** advance a partition's virtual finish score by `1 / effective_weight` for each reservation.

Effective weight is `weight + priority`, capped by `weights.maximum` and reduced by noisy-neighbor penalty with a floor of one. An idle partition leaves the active ring and rejoins at the current weighted virtual time. Workers scan at most `max_scan` candidates per reservation. A full or rate-limited partition is skipped. Large active-partition counts can increase selection latency; split independent workloads into named fair queues to distribute Redis load.

## Custom strategy contract

`SchedulingStrategy::mode()` is passed as reservation argument 2. `reservationScript()` must return a complete Redis Lua script using the same 23 keys and 11 arguments as `resources/lua/reserve.lua`, returning `{raw_payload, lease_token, job_id, partition}` or nil. It must remove the selected ID from ready state, increment attempts in the stored payload, add a reserved expiry, write the lease token, and increment both concurrency counters atomically. It must preserve bounded work and use the token on subsequent completion. Custom scripts require independent race and failure testing.

## Outage and deployment behavior

Redis commands are never silently downgraded to a local queue. Dispatch and reservation failures surface as exceptions. Monitoring errors after a confirmed enqueue or acknowledgement are logged without changing job delivery. Use a dedicated Redis instance or database with `noeviction`, persistence, restricted access, and memory headroom. Keep `retry_after` above the maximum runtime and avoid unbounded Lua work. Redis Cluster is unsupported because scripts derive ready-list keys dynamically.

The package does not use a central scheduling process. Every worker performs bounded maintenance and reservation. The per-queue Redis primary is still the serialization point for atomic global limits; scale independent queues across Redis instances when throughput demands it.
