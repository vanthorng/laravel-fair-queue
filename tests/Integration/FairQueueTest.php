<?php

declare(strict_types=1);

namespace Teerex\FairQueue\Tests\Integration;

use Illuminate\Contracts\Queue\Factory as QueueFactory;
use Illuminate\Support\Facades\Artisan;
use Teerex\FairQueue\Exceptions\MissingPartition;
use Teerex\FairQueue\Exceptions\RedisUnavailable;
use Teerex\FairQueue\FairRedisQueue;
use Teerex\FairQueue\Partition;
use Teerex\FairQueue\TenantContext;
use Teerex\FairQueue\Tests\Support\ProbeJob;
use Teerex\FairQueue\Tests\TestCase;

final class FairQueueTest extends TestCase
{
    private function queue(): FairRedisQueue
    {
        $queue = $this->app->make(QueueFactory::class)->connection('fair');
        self::assertInstanceOf(FairRedisQueue::class, $queue);

        return $queue;
    }

    public function test_missing_partition_is_rejected(): void
    {
        $this->expectException(MissingPartition::class);
        $this->queue()->push('plain-handler');
    }

    public function test_context_can_partition_an_existing_job(): void
    {
        $queue = $this->queue();
        $context = $this->app->make(TenantContext::class);
        $context->runFor(new Partition('tenant', 'legacy'), fn () => $queue->push('plain-handler'));

        $job = $queue->pop();
        self::assertNotNull($job);
        self::assertSame('legacy', $job->payload()['fair']['id']);
        $job->delete();
    }

    public function test_round_robin_does_not_starve_a_tenant(): void
    {
        config()->set('fair-queue.strategy', 'rr');
        $queue = $this->queue();
        for ($i = 0; $i < 20; $i++) {
            $queue->push(new ProbeJob('a'));
            $queue->push(new ProbeJob('b'));
        }

        $seen = [];
        for ($i = 0; $i < 20; $i++) {
            $job = $queue->pop();
            self::assertNotNull($job);
            $seen[] = $job->payload()['fair']['id'];
            $job->delete();
        }

        self::assertSame(['a', 'b', 'a', 'b'], array_slice($seen, 0, 4));
        self::assertSame(10, count(array_filter($seen, static fn ($id) => $id === 'a')));
    }

    public function test_tenant_concurrency_and_release_preserve_attempts(): void
    {
        config()->set('fair-queue.tenant_max_concurrent', 1);
        $queue = $this->queue();
        $queue->push(new ProbeJob('a'));
        $queue->push(new ProbeJob('a'));
        $first = $queue->pop();
        self::assertNotNull($first);
        self::assertNull($queue->pop());
        $first->release(0);
        $second = $queue->pop();
        self::assertNotNull($second);
        self::assertSame(1, $second->attempts());
        $second->delete();
        $retry = $queue->pop();
        self::assertNotNull($retry);
        self::assertSame(2, $retry->attempts());
        $retry->delete();
    }

    public function test_busy_tenant_does_not_block_another_tenant(): void
    {
        config()->set('fair-queue.tenant_max_concurrent', 1);
        $queue = $this->queue();
        $queue->push(new ProbeJob('a'));
        $queue->push(new ProbeJob('a'));
        $queue->push(new ProbeJob('b'));

        $first = $queue->pop();
        self::assertSame('a', $first?->payload()['fair']['id']);
        $second = $queue->pop();
        self::assertSame('b', $second?->payload()['fair']['id']);
        $first?->delete();
        $second?->delete();
    }

    public function test_expired_lease_is_reclaimed_and_stale_ack_is_fenced(): void
    {
        config()->set('queue.connections.fair.retry_after', 1);
        $queue = $this->queue();
        $queue->push(new ProbeJob('a'));
        $old = $queue->pop();
        self::assertNotNull($old);
        sleep(2);
        $new = $queue->pop();
        self::assertNotNull($new);
        self::assertSame($old->getJobId(), $new->getJobId());
        $old->delete();
        self::assertSame(1, $queue->reservedSize());
        $new->delete();
        self::assertSame(0, $queue->size());
    }

    public function test_delayed_jobs_are_promoted_without_losing_their_partition(): void
    {
        $queue = $this->queue();
        $queue->later(1, new ProbeJob('a'));
        self::assertNull($queue->pop());
        sleep(2);
        $job = $queue->pop();
        self::assertNotNull($job);
        self::assertSame('a', $job->payload()['fair']['id']);
        $job->delete();
    }

    public function test_standard_laravel_worker_executes_a_fair_job(): void
    {
        ProbeJob::$executed = 0;
        $queue = $this->queue();
        $queue->push(new ProbeJob('a'));

        $exit = Artisan::call('queue:work', [
            'connection' => 'fair', '--queue' => 'default', '--once' => true,
            '--sleep' => 0, '--timeout' => 1,
        ]);

        self::assertSame(0, $exit);
        self::assertSame(1, ProbeJob::$executed);
        self::assertSame(0, $queue->size());
    }

    public function test_redis_outage_rejects_dispatch(): void
    {
        config()->set('database.redis.down', [
            'host' => '127.0.0.1', 'port' => 1, 'database' => 15,
            'timeout' => 0.2, 'read_timeout' => 0.2,
        ]);
        config()->set('queue.connections.fair-down', [
            'driver' => 'fair-redis', 'connection' => 'down',
            'queue' => 'default', 'retry_after' => 2,
        ]);
        $queue = $this->app->make(QueueFactory::class)->connection('fair-down');
        $this->expectException(RedisUnavailable::class);
        $queue->push(new ProbeJob('a'));
    }
}
