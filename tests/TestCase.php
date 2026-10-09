<?php

declare(strict_types=1);

namespace Teerex\FairQueue\Tests;

use Orchestra\Testbench\TestCase as BaseTestCase;
use Teerex\FairQueue\FairQueueServiceProvider;

abstract class TestCase extends BaseTestCase
{
    protected function tearDown(): void
    {
        if ($this->app !== null) {
            $prefix = (string) $this->app['config']->get('fair-queue.prefix');
            try {
                foreach ($this->app['redis']->connection('default')->keys($prefix.':*') as $key) {
                    $this->app['redis']->connection('default')->del($key);
                }
            } catch (\Throwable) {
                // Unit tests and unavailable Redis hosts have nothing to clean up.
            }
        }

        parent::tearDown();
    }

    protected function getPackageProviders($app): array
    {
        return [FairQueueServiceProvider::class];
    }

    protected function getEnvironmentSetUp($app): void
    {
        $app['config']->set('app.key', 'base64:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=');
        $app['config']->set('queue.failed.driver', 'null');
        $app['config']->set('database.redis.client', 'predis');
        $app['config']->set('database.redis.default', [
            'host' => getenv('REDIS_HOST') ?: '127.0.0.1',
            'port' => getenv('REDIS_PORT') ?: 6379,
            'database' => 15,
        ]);
        $app['config']->set('fair-queue.prefix', 'fairq-test-'.bin2hex(random_bytes(8)));
        $app['config']->set('fair-queue.max_concurrent', 100);
        $app['config']->set('fair-queue.tenant_max_concurrent', 100);
        $app['config']->set('fair-queue.noisy_neighbor.enabled', false);
        $app['config']->set('queue.connections.fair', [
            'driver' => 'fair-redis',
            'connection' => 'default',
            'queue' => 'default',
            'retry_after' => 2,
        ]);
    }
}
