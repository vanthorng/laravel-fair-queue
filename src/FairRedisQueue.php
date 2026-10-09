<?php

declare(strict_types=1);

namespace Teerex\FairQueue;

use Illuminate\Contracts\Events\Dispatcher;
use Illuminate\Contracts\Redis\Factory as RedisFactory;
use Illuminate\Queue\Jobs\InspectedJob;
use Illuminate\Queue\Jobs\RedisJob;
use Illuminate\Queue\RedisQueue;
use Illuminate\Redis\Connections\PhpRedisClusterConnection;
use Illuminate\Redis\Connections\PredisClusterConnection;
use Illuminate\Support\Collection;
use Psr\Log\LoggerInterface;
use RuntimeException;
use Teerex\FairQueue\Contracts\PartitionResolver;
use Teerex\FairQueue\Contracts\SchedulingStrategy;
use Teerex\FairQueue\Exceptions\MissingPartition;
use Teerex\FairQueue\Redis\Coordinator;
use Teerex\FairQueue\Redis\Keys;
use Teerex\FairQueue\Scheduling\StrategyFactory;
use Throwable;

final class FairRedisQueue extends RedisQueue
{
    private readonly Coordinator $coordinator;

    private readonly NoisyNeighborDetector $detector;

    private readonly SchedulingStrategy $strategy;

    /** @var array<string, int> */
    private array $lastMaintenanceAtMs = [];

    /** @var array<string, bool> */
    private array $registeredQueueNames = [];

    /** @param array<string, mixed> $settings */
    public function __construct(
        RedisFactory $redis,
        private readonly PartitionResolver $resolver,
        private readonly Dispatcher $events,
        private readonly LoggerInterface $logger,
        private readonly array $settings,
        string $default = 'default',
        string $connection = 'default',
        int $retryAfter = 120,
        bool $dispatchAfterCommit = false,
    ) {
        parent::__construct($redis, $default, $connection, $retryAfter, null, $dispatchAfterCommit);
        if ($this->getConnection() instanceof PhpRedisClusterConnection
            || $this->getConnection() instanceof PredisClusterConnection) {
            throw new \InvalidArgumentException('Redis Cluster is not supported by fair-redis.');
        }
        $this->coordinator = new Coordinator($redis, $connection);
        $this->detector = new NoisyNeighborDetector(
            $this->coordinator, $events, $logger, (array) ($settings['noisy_neighbor'] ?? []),
        );
        $this->strategy = StrategyFactory::make($settings['strategy'] ?? 'drr');
    }

    public function getQueue($queue): string
    {
        $name = $this->normalizeQueueName($queue);
        $prefix = (string) ($this->settings['prefix'] ?? 'fairq');

        return $prefix.':{'.sha1($name).'}';
    }

    public function keys(mixed $queue = null): Keys
    {
        return new Keys($this->getQueue($queue));
    }

    public function coordinator(): Coordinator
    {
        return $this->coordinator;
    }

    public function push($job, $data = '', $queue = null)
    {
        $partition = $this->resolvePartition($job, $data);
        $payload = $this->withPartition($this->createPayload($job, $this->getQueue($queue), $data), $partition);

        return $this->enqueueUsing($job, $payload, $queue, null, fn ($payload, $queue) => $this->pushRaw($payload, $queue));
    }

    public function later($delay, $job, $data = '', $queue = null)
    {
        $partition = $this->resolvePartition($job, $data);
        $payload = $this->withPartition($this->createPayload($job, $this->getQueue($queue), $data), $partition);

        return $this->enqueueUsing(
            $job,
            $payload,
            $queue,
            $delay,
            fn ($payload, $queue, $delay) => $this->laterRaw($delay, $payload, $queue),
        );
    }

    /** @param iterable<object|string> $jobs */
    public function bulk($jobs, $data = '', $queue = null)
    {
        foreach ($jobs as $job) {
            $delay = is_object($job) ? ($job->delay ?? null) : null;
            $delay !== null
                ? $this->later($delay, $job, $data, $queue)
                : $this->push($job, $data, $queue);
        }
    }

    /** @param array<string, mixed> $options */
    public function pushRaw($payload, $queue = null, array $options = [])
    {
        $decoded = json_decode($payload, true, 512, JSON_THROW_ON_ERROR);
        $partition = $this->partitionFromPayload($decoded);
        if (! isset($decoded['fair'])) {
            $decoded['fair'] = ['kind' => $partition->kind, 'id' => $partition->id];
            $payload = json_encode($decoded, JSON_THROW_ON_ERROR);
        }
        $id = (string) ($decoded['id'] ?? '');

        if ($id === '') {
            throw new RuntimeException('Fair queue payload is missing a job ID.');
        }

        $keys = $this->keys($queue);
        $now = (int) floor(microtime(true) * 1000);
        $at = (int) ($options['available_at_ms'] ?? $now);
        $dispatch = $this->settings['dispatch_rate'] ?? [];
        $processing = $this->settings['processing_rate'] ?? [];
        $weights = $this->settings['weights'] ?? [];
        $overrides = (array) ($this->settings['partitions'][$partition->key()] ?? []);
        $weight = (int) ($overrides['weight'] ?? $weights['default'] ?? 1)
            + max(0, (int) ($overrides['priority'] ?? 0));

        $this->coordinator->script('enqueue', [
            $keys->key('jobs'), $keys->key('partitions'), $keys->key('delayed'),
            $keys->key('ring'), $keys->key('members'), $keys->key('wfq'),
            $keys->key('depths'), $keys->key('dispatch-next'), $keys->key('metrics'), $keys->key('weights'),
            $keys->key('max-concurrent'), $keys->key('process-rates'), $keys->key('process-bursts'),
        ], [
            $id, $payload, $partition->key(), $at, $now,
            (float) ($overrides['dispatch_rate'] ?? $dispatch['per_second'] ?? 0),
            (int) ($overrides['dispatch_burst'] ?? $dispatch['burst'] ?? 1),
            max(1, $weight), $keys->ready($partition->key()),
            (int) ($overrides['max_concurrent'] ?? $this->settings['tenant_max_concurrent'] ?? 10),
            (float) ($overrides['processing_rate'] ?? $processing['per_second'] ?? 0),
            (int) ($overrides['processing_burst'] ?? $processing['burst'] ?? 1),
        ]);

        $name = $this->normalizeQueueName($queue);
        if (! isset($this->registeredQueueNames[$name])) {
            try {
                $this->getConnection()->sadd($this->registryKey(), $name);
                $this->registeredQueueNames[$name] = true;
            } catch (Throwable $exception) {
                $this->logger->warning('Unable to update fair queue name registry', ['exception' => $exception]);
            }
        }

        $this->assessQuietly($keys, $partition->key());

        return $id;
    }

    protected function laterRaw($delay, $payload, $queue = null)
    {
        return $this->pushRaw($payload, $queue, ['available_at_ms' => $this->availableAt($delay) * 1000]);
    }

    public function pop($queue = null, $index = 0)
    {
        $keys = $this->keys($queue);
        $now = (int) floor(microtime(true) * 1000);
        if ($now - ($this->lastMaintenanceAtMs[$keys->base] ?? 0) >= max(1, (int) ($this->settings['maintenance_interval_ms'] ?? 250))) {
            $this->maintain($queue);
            $this->lastMaintenanceAtMs[$keys->base] = $now;
        }
        $processing = $this->settings['processing_rate'] ?? [];
        $weights = $this->settings['weights'] ?? [];
        $result = $this->coordinator->execute($this->strategy->reservationScript(), [
            $keys->key('ring'), $keys->key('members'), $keys->key('wfq'),
            $keys->key('jobs'), $keys->key('partitions'), $keys->key('reserved'),
            $keys->key('leases'), $keys->key('running'), $keys->key('global-running'),
            $keys->key('depths'), $keys->key('metrics'), $keys->key('process-next'),
            $keys->key('deficit'), $keys->key('weights'), $keys->key('penalties'), $keys->key('starts'),
            $keys->key('wfq-cursor'), $keys->key('max-concurrent'),
            $keys->key('process-rates'), $keys->key('process-bursts'),
            $keys->key('wait-ms'), $keys->key('reserved-count'), $keys->key('delayed'),
        ], [
            $keys->readyPrefix(), $this->strategy->mode(), $now,
            $now + ($this->retryAfter * 1000), (int) ($this->settings['max_concurrent'] ?? 100),
            (int) ($this->settings['tenant_max_concurrent'] ?? 10),
            (int) ($this->settings['max_scan'] ?? 128),
            (float) ($processing['per_second'] ?? 0), (int) ($processing['burst'] ?? 1),
            bin2hex(random_bytes(16)), (int) ($weights['maximum'] ?? 20),
        ], 'reserve');

        if (! is_array($result) || ! isset($result[0], $result[1])) {
            return null;
        }

        return new RedisJob(
            $this->container, $this, (string) $result[0], (string) $result[1],
            $this->connectionName, $this->normalizeQueueName($queue),
        );
    }

    /** @return array{promoted:int,reclaimed:int} */
    public function maintain(mixed $queue = null): array
    {
        $keys = $this->keys($queue);
        $now = (int) floor(microtime(true) * 1000);
        $batch = max(1, (int) ($this->settings['maintenance_batch'] ?? 100));

        $promoted = $this->coordinator->script('promote', [
            $keys->key('delayed'), $keys->key('partitions'), $keys->key('ring'),
            $keys->key('members'), $keys->key('wfq'),
        ], [$now, $batch, $keys->readyPrefix()]);
        $reclaimed = $this->coordinator->script('reclaim', [
            $keys->key('reserved'), $keys->key('leases'), $keys->key('partitions'),
            $keys->key('running'), $keys->key('global-running'), $keys->key('starts'),
            $keys->key('ring'), $keys->key('members'), $keys->key('wfq'),
            $keys->key('depths'), $keys->key('metrics'),
        ], [$now, $batch, $keys->readyPrefix()]);

        return ['promoted' => (int) $promoted, 'reclaimed' => (int) $reclaimed];
    }

    public function deleteReserved($queue, $job)
    {
        $this->finish($queue, $job, 0);
    }

    public function deleteAndRelease($queue, $job, $delay)
    {
        $this->finish($queue, $job, $this->availableAt($delay) * 1000);
    }

    private function finish(string $queue, RedisJob $job, int $releaseAt): void
    {
        $keys = $this->keys($queue);
        $result = $this->coordinator->script('finish', [
            $keys->key('jobs'), $keys->key('partitions'), $keys->key('reserved'),
            $keys->key('leases'), $keys->key('running'), $keys->key('global-running'),
            $keys->key('starts'), $keys->key('metrics'), $keys->key('delayed'),
            $keys->key('ring'), $keys->key('members'), $keys->key('wfq'), $keys->key('depths'),
        ], [
            (string) $job->getJobId(), $job->getReservedJob(),
            (int) floor(microtime(true) * 1000), $releaseAt, $keys->readyPrefix(),
        ]);

        if (is_array($result) && isset($result[1], $result[2])) {
            try {
                $this->coordinator->recordOutcome(
                    $keys,
                    (string) $result[2],
                    (int) $result[1],
                    $releaseAt === 0,
                    max(1, (int) (($this->settings['noisy_neighbor']['window_seconds'] ?? 60))),
                );
                $this->detector->assess($keys, (string) $result[2]);
            } catch (Throwable $exception) {
                $this->logger->warning('Unable to update fair queue observations', ['exception' => $exception]);
            }
        }
    }

    public function size($queue = null)
    {
        $snapshot = $this->snapshot($queue);

        return $snapshot['ready'] + $snapshot['delayed'] + $snapshot['reserved'];
    }

    public function pendingSize($queue = null)
    {
        return $this->snapshot($queue)['ready'];
    }

    public function delayedSize($queue = null)
    {
        return $this->snapshot($queue)['delayed'];
    }

    public function reservedSize($queue = null)
    {
        return $this->snapshot($queue)['reserved'];
    }

    /** @return array<string, mixed> */
    public function snapshot(mixed $queue = null): array
    {
        return $this->coordinator->snapshot(
            $this->keys($queue),
            max(1, (int) ($this->settings['noisy_neighbor']['window_seconds'] ?? 60)),
        );
    }

    public function clear($queue = null)
    {
        throw new RuntimeException('queue:clear is not supported for fair queues; stop workers and use the documented maintenance procedure.');
    }

    public function totalSize(): int
    {
        return $this->allQueueNames()->sum(fn (string $name): int => $this->size($name));
    }

    public function totalPendingSize(): int
    {
        return $this->allQueueNames()->sum(fn (string $name): int => $this->pendingSize($name));
    }

    public function totalDelayedSize(): int
    {
        return $this->allQueueNames()->sum(fn (string $name): int => $this->delayedSize($name));
    }

    public function totalReservedSize(): int
    {
        return $this->allQueueNames()->sum(fn (string $name): int => $this->reservedSize($name));
    }

    public function pendingJobs($queue = null): Collection
    {
        $keys = $this->keys($queue);
        $ids = [];
        foreach ((array) ($this->getConnection()->smembers($keys->key('members')) ?: []) as $partition) {
            foreach ((array) ($this->getConnection()->lrange($keys->ready((string) $partition), 0, -1) ?: []) as $id) {
                $ids[] = $id;
            }
        }

        return $this->inspect($keys, $ids, $this->normalizeQueueName($queue));
    }

    public function delayedJobs($queue = null): Collection
    {
        $keys = $this->keys($queue);

        return $this->inspect($keys, (array) ($this->getConnection()->zrange($keys->key('delayed'), 0, -1) ?: []), $this->normalizeQueueName($queue));
    }

    public function reservedJobs($queue = null): Collection
    {
        $keys = $this->keys($queue);

        return $this->inspect($keys, (array) ($this->getConnection()->zrange($keys->key('reserved'), 0, -1) ?: []), $this->normalizeQueueName($queue));
    }

    public function allPendingJobs(): Collection
    {
        return $this->allQueueNames()->flatMap(fn (string $name): Collection => $this->pendingJobs($name));
    }

    public function allDelayedJobs(): Collection
    {
        return $this->allQueueNames()->flatMap(fn (string $name): Collection => $this->delayedJobs($name));
    }

    public function allReservedJobs(): Collection
    {
        return $this->allQueueNames()->flatMap(fn (string $name): Collection => $this->reservedJobs($name));
    }

    protected function allQueueNames(): Collection
    {
        $names = array_map('strval', array_merge(
            [$this->default],
            (array) ($this->getConnection()->smembers($this->registryKey()) ?: []),
        ));

        return new Collection(array_values(array_unique($names)));
    }

    public function creationTimeOfOldestPendingJob($queue = null)
    {
        $keys = $this->keys($queue);
        $oldest = null;

        foreach ((array) ($this->getConnection()->smembers($keys->key('members')) ?: []) as $partition) {
            $id = $this->getConnection()->lindex($keys->ready((string) $partition), 0);
            if (! $id) {
                continue;
            }
            $payload = $this->getConnection()->hget($keys->key('jobs'), $id);
            $decoded = $payload ? json_decode($payload, true) : null;
            $created = $decoded['fair']['enqueued_at_ms'] ?? null;
            if (is_numeric($created)) {
                $seconds = intdiv((int) $created, 1000);
                $oldest = $oldest === null ? $seconds : min($oldest, $seconds);
            }
        }

        return $oldest;
    }

    private function resolvePartition(mixed $job, mixed $data): Partition
    {
        return $this->resolver->resolve($job, $data)
            ?? throw new MissingPartition('Fair queue dispatch requires a tenant or group partition.');
    }

    private function withPartition(string $payload, Partition $partition): string
    {
        $decoded = json_decode($payload, true, 512, JSON_THROW_ON_ERROR);
        $decoded['fair'] = ['kind' => $partition->kind, 'id' => $partition->id, 'enqueued_at_ms' => (int) floor(microtime(true) * 1000)];

        return json_encode($decoded, JSON_THROW_ON_ERROR);
    }

    /** @param array<string, mixed> $payload */
    private function partitionFromPayload(array $payload): Partition
    {
        $fair = $payload['fair'] ?? null;

        if (is_array($fair) && isset($fair['kind'], $fair['id'])) {
            return new Partition((string) $fair['kind'], (string) $fair['id']);
        }

        return $this->resolver->resolve(null)
            ?? throw new MissingPartition('Raw fair queue dispatch requires fair.kind and fair.id in the payload or an active tenant context.');
    }

    private function assessQuietly(Keys $keys, string $partition): void
    {
        try {
            $this->detector->assess($keys, $partition);
        } catch (Throwable $exception) {
            $this->logger->warning('Unable to assess fair queue tenant health', ['exception' => $exception]);
        }
    }

    private function normalizeQueueName(mixed $queue): string
    {
        if ($queue instanceof \BackedEnum) {
            return (string) $queue->value;
        }
        if ($queue instanceof \UnitEnum) {
            return $queue->name;
        }

        return (string) ($queue ?: $this->default);
    }

    private function registryKey(): string
    {
        return (string) ($this->settings['prefix'] ?? 'fairq').':queue-names';
    }

    /**
     * @param  list<string>  $ids
     * @return Collection<int, mixed>
     */
    private function inspect(Keys $keys, array $ids, string $queueName): Collection
    {
        return collect($ids)->map(function (mixed $id) use ($keys, $queueName): mixed {
            $payload = $this->getConnection()->hget($keys->key('jobs'), (string) $id);
            if (! $payload) {
                return null;
            }

            return class_exists(InspectedJob::class)
                ? InspectedJob::fromPayload($payload, queue: $queueName)
                : json_decode($payload, true);
        })->filter()->values();
    }
}
