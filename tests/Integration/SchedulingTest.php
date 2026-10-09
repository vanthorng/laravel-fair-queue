<?php

declare(strict_types=1);

namespace Teerex\FairQueue\Tests\Integration;

use Illuminate\Contracts\Queue\Factory as QueueFactory;
use Teerex\FairQueue\FairRedisQueue;
use Teerex\FairQueue\Partition;
use Teerex\FairQueue\Tests\Support\ProbeJob;
use Teerex\FairQueue\Tests\TestCase;

final class SchedulingTest extends TestCase
{
    /** @return array<string, int> */
    private function sample(string $strategy, int $count = 40): array
    {
        config()->set('fair-queue.strategy', $strategy);
        config()->set('fair-queue.partitions', [
            (new Partition('tenant', 'a'))->key() => ['weight' => 3],
            (new Partition('tenant', 'b'))->key() => ['weight' => 1],
        ]);

        $queue = $this->app->make(QueueFactory::class)->connection('fair');
        self::assertInstanceOf(FairRedisQueue::class, $queue);

        for ($i = 0; $i < 100; $i++) {
            $queue->push(new ProbeJob('a'));
            $queue->push(new ProbeJob('b'));
        }

        $counts = ['a' => 0, 'b' => 0];
        for ($i = 0; $i < $count; $i++) {
            $job = $queue->pop();
            self::assertNotNull($job);
            $counts[$job->payload()['fair']['id']]++;
            $job->delete();
        }

        return $counts;
    }

    public function test_deficit_round_robin_respects_weight_without_starving_other_tenant(): void
    {
        $counts = $this->sample('drr');
        self::assertGreaterThanOrEqual(25, $counts['a']);
        self::assertGreaterThanOrEqual(7, $counts['b']);
    }

    public function test_weighted_fair_queuing_respects_weight_without_starving_other_tenant(): void
    {
        $counts = $this->sample('wfq');
        self::assertGreaterThanOrEqual(25, $counts['a']);
        self::assertGreaterThanOrEqual(7, $counts['b']);
    }

    public function test_dispatch_rate_accepts_jobs_and_defers_excess(): void
    {
        config()->set('fair-queue.dispatch_rate', ['per_second' => 1, 'burst' => 1]);
        $queue = $this->app->make(QueueFactory::class)->connection('fair');
        self::assertInstanceOf(FairRedisQueue::class, $queue);
        $queue->push(new ProbeJob('a'));
        $queue->push(new ProbeJob('a'));

        self::assertSame(2, $queue->size());
        self::assertSame(1, $queue->delayedSize());
    }

    public function test_processing_rate_holds_back_ready_jobs(): void
    {
        config()->set('fair-queue.processing_rate', ['per_second' => 1, 'burst' => 1]);
        $queue = $this->app->make(QueueFactory::class)->connection('fair');
        self::assertInstanceOf(FairRedisQueue::class, $queue);
        $queue->push(new ProbeJob('a'));
        $queue->push(new ProbeJob('a'));

        $first = $queue->pop();
        self::assertNotNull($first);
        $first->delete();
        self::assertNull($queue->pop());
        sleep(2);
        $second = $queue->pop();
        self::assertNotNull($second);
        $second->delete();
    }

    public function test_high_volume_reservations_preserve_every_job(): void
    {
        $queue = $this->app->make(QueueFactory::class)->connection('fair');
        self::assertInstanceOf(FairRedisQueue::class, $queue);
        $ids = [];

        for ($i = 0; $i < 1000; $i++) {
            $ids[] = $queue->push(new ProbeJob('tenant-'.$i % 10));
        }

        self::assertCount(1000, array_unique($ids));
        $reserved = [];
        for ($i = 0; $i < 1000; $i++) {
            $job = $queue->pop();
            self::assertNotNull($job);
            $reserved[] = $job->getJobId();
            $job->delete();
        }

        self::assertCount(1000, array_unique($reserved));
        self::assertSame(0, $queue->size());
    }
}
