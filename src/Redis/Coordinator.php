<?php

declare(strict_types=1);

namespace Teerex\FairQueue\Redis;

use Illuminate\Contracts\Redis\Factory as RedisFactory;
use Illuminate\Redis\Connections\Connection;
use Teerex\FairQueue\Exceptions\RedisUnavailable;
use Throwable;

final class Coordinator
{
    /** @var array<string, string> */
    private static array $scripts = [];

    public function __construct(
        private readonly RedisFactory $redis,
        private readonly string $connection,
    ) {}

    public function connection(): Connection
    {
        return $this->redis->connection($this->connection);
    }

    /**
     * @param  list<string>  $keys
     * @param  list<int|float|string>  $arguments
     */
    public function script(string $name, array $keys, array $arguments): mixed
    {
        if (! isset(self::$scripts[$name])) {
            $path = dirname(__DIR__, 2).'/resources/lua/'.$name.'.lua';
            $contents = file_get_contents($path);

            if ($contents === false) {
                throw new RedisUnavailable("Missing fair queue Redis script: {$name}");
            }

            self::$scripts[$name] = $contents;
        }

        return $this->execute(self::$scripts[$name], $keys, $arguments, $name);
    }

    /**
     * @param  list<string>  $keys
     * @param  list<int|float|string>  $arguments
     */
    public function execute(string $source, array $keys, array $arguments, string $operation = 'custom'): mixed
    {
        try {
            return call_user_func_array(
                [$this->connection(), 'eval'],
                array_merge([$source, count($keys)], $keys, $arguments),
            );
        } catch (Throwable $exception) {
            throw new RedisUnavailable("Fair queue Redis {$operation} operation failed", previous: $exception);
        }
    }

    /** @return array<string, int|float|string> */
    public function metrics(Keys $keys): array
    {
        return $this->connection()->hgetall($keys->key('metrics')) ?: [];
    }

    /** @return array<string, int> */
    public function depths(Keys $keys): array
    {
        $values = $this->connection()->hgetall($keys->key('depths')) ?: [];

        return array_map(static fn (mixed $value): int => (int) $value, $values);
    }

    /** @return array<string, mixed> */
    public function snapshot(Keys $keys, int $windowSeconds = 60): array
    {
        try {
            $depths = $this->depths($keys);
            $runningByPartition = $this->connection()->hgetall($keys->key('running')) ?: [];
            $failures = $this->connection()->hgetall($keys->key('failures')) ?: [];
            $wait = $this->connection()->hgetall($keys->key('wait-ms')) ?: [];
            $reservedCounts = $this->connection()->hgetall($keys->key('reserved-count')) ?: [];
            $throughput = [];
            $epoch = intdiv(time(), max(1, $windowSeconds));
            foreach ([$epoch, $epoch - 1] as $bucket) {
                $window = $this->connection()->hgetall($keys->key('window:'.$bucket)) ?: [];
                foreach ($window as $field => $value) {
                    if (str_ends_with((string) $field, ':completed')) {
                        $partition = substr((string) $field, 0, -strlen(':completed'));
                        $throughput[$partition] = ($throughput[$partition] ?? 0) + (int) $value;
                    }
                }
            }
            $averageWait = [];
            foreach ($wait as $partition => $totalWait) {
                $averageWait[$partition] = intdiv((int) $totalWait, max(1, (int) ($reservedCounts[$partition] ?? 0)));
            }

            return [
                'ready' => max(0, array_sum($depths) - (int) $this->connection()->zcard($keys->key('delayed'))),
                'delayed' => (int) $this->connection()->zcard($keys->key('delayed')),
                'reserved' => (int) $this->connection()->zcard($keys->key('reserved')),
                'running' => (int) ($this->connection()->get($keys->key('global-running')) ?: 0),
                'metrics' => $this->metrics($keys),
                'depths' => $depths,
                'running_by_partition' => array_map('intval', $runningByPartition),
                'failures_by_partition' => array_map('intval', $failures),
                'completed_recent_by_partition' => $throughput,
                'average_wait_ms_by_partition' => $averageWait,
            ];
        } catch (Throwable $exception) {
            throw new RedisUnavailable('Fair queue metrics unavailable', previous: $exception);
        }
    }

    public function recordFailure(Keys $keys, string $partition, int $windowSeconds): void
    {
        try {
            $this->connection()->hincrby($keys->key('metrics'), 'failed', 1);
            $this->connection()->hincrby($keys->key('failures'), $partition, 1);
            $window = $keys->key('window:'.intdiv(time(), $windowSeconds));
            $this->connection()->hincrby($window, $partition.':failed', 1);
            $this->connection()->expire($window, $windowSeconds * 3);
        } catch (Throwable $exception) {
            throw new RedisUnavailable('Fair queue failure metrics unavailable', previous: $exception);
        }
    }

    public function recordOutcome(Keys $keys, string $partition, int $durationMs, bool $completed, int $windowSeconds): void
    {
        try {
            $window = $keys->key('window:'.intdiv(time(), $windowSeconds));
            $this->connection()->hincrby($window, $partition.':attempts', 1);
            $this->connection()->hincrby($window, $partition.':duration_ms', max(0, $durationMs));
            if ($completed) {
                $this->connection()->hincrby($window, $partition.':completed', 1);
            }
            $this->connection()->expire($window, $windowSeconds * 3);
        } catch (Throwable $exception) {
            throw new RedisUnavailable('Fair queue outcome metrics unavailable', previous: $exception);
        }
    }
}
