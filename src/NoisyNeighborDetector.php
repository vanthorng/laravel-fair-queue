<?php

declare(strict_types=1);

namespace Teerex\FairQueue;

use Illuminate\Contracts\Events\Dispatcher;
use Psr\Log\LoggerInterface;
use Teerex\FairQueue\Events\NeighborStateChanged;
use Teerex\FairQueue\Redis\Coordinator;
use Teerex\FairQueue\Redis\Keys;

final readonly class NoisyNeighborDetector
{
    /** @param array<string, mixed> $settings */
    public function __construct(
        private Coordinator $coordinator,
        private Dispatcher $events,
        private LoggerInterface $logger,
        private array $settings,
    ) {}

    public function assess(Keys $keys, string $partition): void
    {
        if (! ($this->settings['enabled'] ?? false)) {
            return;
        }

        $redis = $this->coordinator->connection();
        $windowSeconds = max(1, (int) ($this->settings['window_seconds'] ?? 60));
        $epoch = intdiv(time(), $windowSeconds);
        $guard = $keys->key('assessment:'.$partition);

        if ((int) $this->coordinator->script('assessment-guard', [$guard], []) !== 1) {
            return;
        }

        $depth = (int) ($redis->hget($keys->key('depths'), $partition) ?: 0);
        $attempts = 0;
        $failures = 0;
        $duration = 0;

        foreach ([$epoch, $epoch - 1] as $bucket) {
            $window = $keys->key('window:'.$bucket);
            $attempts += (int) ($redis->hget($window, $partition.':attempts') ?: 0);
            $failures += (int) ($redis->hget($window, $partition.':failed') ?: 0);
            $duration += (int) ($redis->hget($window, $partition.':duration_ms') ?: 0);
        }

        $minimum = max(1, (int) ($this->settings['minimum_samples'] ?? 20));
        $reason = null;
        if ($depth >= (int) ($this->settings['depth_threshold'] ?? PHP_INT_MAX)) {
            $reason = 'depth';
        } elseif ($attempts >= $minimum && $failures / $attempts >= (float) ($this->settings['failure_rate_threshold'] ?? 1)) {
            $reason = 'failure_rate';
        } elseif ($attempts >= $minimum && $duration / $attempts >= (int) ($this->settings['duration_ms_threshold'] ?? PHP_INT_MAX)) {
            $reason = 'duration';
        }

        $penalties = $keys->key('penalties');
        $current = (int) ($redis->hget($penalties, $partition) ?: 100);
        if ($reason !== null) {
            $target = max(1, min(100, (int) ($this->settings['penalty_percent'] ?? 50)));
            $redis->hset($keys->key('recovery'), $partition, 0);
            if ($current !== $target) {
                $redis->hset($penalties, $partition, $target);
                $this->notify($partition, $target, $reason);
            }

            return;
        }

        if ($current === 100) {
            return;
        }

        $last = (int) ($redis->hget($keys->key('recovery-window'), $partition) ?: -1);
        if ($last === $epoch) {
            return;
        }

        $redis->hset($keys->key('recovery-window'), $partition, $epoch);
        $count = (int) $redis->hincrby($keys->key('recovery'), $partition, 1);
        if ($count >= (int) ($this->settings['recovery_windows'] ?? 3)) {
            $redis->hdel($penalties, $partition);
            $redis->hdel($keys->key('recovery'), $partition);
            $this->notify($partition, 100, 'recovered');
        }
    }

    private function notify(string $partition, int $penalty, string $reason): void
    {
        $this->logger->info('Fair queue noisy-neighbor state changed', [
            'partition' => $partition,
            'penalty_percent' => $penalty,
            'reason' => $reason,
        ]);
        $this->events->dispatch(new NeighborStateChanged($partition, $penalty, $reason));
    }
}
