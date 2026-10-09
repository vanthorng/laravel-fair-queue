<?php

declare(strict_types=1);

namespace Teerex\FairQueue\Tests\Integration;

use Illuminate\Contracts\Queue\Factory as QueueFactory;
use Teerex\FairQueue\FairRedisQueue;
use Teerex\FairQueue\Partition;
use Teerex\FairQueue\Tests\Support\ProbeJob;
use Teerex\FairQueue\Tests\TestCase;

final class NoisyNeighborTest extends TestCase
{
    public function test_deep_partition_is_penalized_while_other_partition_stays_healthy(): void
    {
        config()->set('fair-queue.noisy_neighbor.enabled', true);
        config()->set('fair-queue.noisy_neighbor.depth_threshold', 1);
        config()->set('fair-queue.noisy_neighbor.penalty_percent', 25);
        $queue = $this->app->make(QueueFactory::class)->connection('fair');
        self::assertInstanceOf(FairRedisQueue::class, $queue);
        $queue->push(new ProbeJob('noisy'));

        $redis = $queue->getConnection();
        $penalties = $queue->keys()->key('penalties');
        self::assertSame('25', (string) $redis->hget($penalties, (new Partition('tenant', 'noisy'))->key()));
        self::assertFalse((bool) $redis->hget($penalties, (new Partition('tenant', 'healthy'))->key()));
    }
}
