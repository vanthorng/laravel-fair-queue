<?php

declare(strict_types=1);

namespace Teerex\FairQueue;

use Illuminate\Queue\Events\JobExceptionOccurred;
use Illuminate\Queue\Events\JobFailed;
use Illuminate\Queue\Events\JobProcessed;
use Illuminate\Queue\Events\JobProcessing;
use Illuminate\Queue\Jobs\RedisJob;
use Illuminate\Support\ServiceProvider;
use Teerex\FairQueue\Commands\DoctorCommand;
use Teerex\FairQueue\Commands\MaintainCommand;
use Teerex\FairQueue\Commands\StatusCommand;
use Teerex\FairQueue\Contracts\PartitionResolver;

final class FairQueueServiceProvider extends ServiceProvider
{
    public function register(): void
    {
        $this->mergeConfigFrom(__DIR__.'/../config/fair-queue.php', 'fair-queue');

        if (! $this->app->bound(TenantContext::class)) {
            $this->app->scoped(TenantContext::class, fn () => new TenantContext);
        }
        if (! $this->app->bound(PartitionResolver::class)) {
            $this->app->bind(PartitionResolver::class, ContextPartitionResolver::class);
        }
    }

    public function boot(): void
    {
        $this->publishes([
            __DIR__.'/../config/fair-queue.php' => config_path('fair-queue.php'),
        ], 'fair-queue-config');

        if (config('queue.connections.fair') === null) {
            config()->set('queue.connections.fair', [
                'driver' => 'fair-redis',
                'connection' => config('fair-queue.redis_connection', 'default'),
                'queue' => config('fair-queue.queue', 'default'),
                'retry_after' => config('fair-queue.retry_after', 120),
                'after_commit' => false,
            ]);
        }

        $this->app['queue']->extend('fair-redis', function (): FairQueueConnector {
            return new FairQueueConnector(
                $this->app['redis'],
                $this->app->make(PartitionResolver::class),
                $this->app['events'],
                $this->app['log'],
                (array) config('fair-queue', []),
            );
        });

        $this->app['events']->listen(JobProcessing::class, function (JobProcessing $event): void {
            if (! ($event->job instanceof RedisJob) || ! ($event->job->getRedisQueue() instanceof FairRedisQueue)) {
                return;
            }

            $fair = $event->job->payload()['fair'] ?? null;
            if (is_array($fair) && isset($fair['kind'], $fair['id'])) {
                $this->app->make(TenantContext::class)->enter(new Partition((string) $fair['kind'], (string) $fair['id']));
            }
        });

        $clearContext = function (object $event): void {
            if ($event->job instanceof RedisJob && $event->job->getRedisQueue() instanceof FairRedisQueue) {
                $this->app->make(TenantContext::class)->clear();
            }
        };
        $this->app['events']->listen(JobProcessed::class, $clearContext);
        $this->app['events']->listen(JobExceptionOccurred::class, $clearContext);

        $this->app['events']->listen(JobFailed::class, function (JobFailed $event): void {
            if (! ($event->job instanceof RedisJob) || ! ($event->job->getRedisQueue() instanceof FairRedisQueue)) {
                return;
            }

            $payload = $event->job->payload();
            $fair = $payload['fair'] ?? null;

            if (! is_array($fair) || ! isset($fair['kind'], $fair['id'])) {
                return;
            }

            $partition = new Partition((string) $fair['kind'], (string) $fair['id']);
            $queue = $event->job->getRedisQueue();
            $this->app->make(TenantContext::class)->clear();
            try {
                $queue->coordinator()->recordFailure(
                    $queue->keys($event->job->getQueue()),
                    $partition->key(),
                    max(1, (int) config('fair-queue.noisy_neighbor.window_seconds', 60)),
                );
            } catch (\Throwable $exception) {
                $this->app['log']->warning('Unable to record fair queue failure metrics', ['exception' => $exception]);
            }
        });

        if ($this->app->runningInConsole()) {
            $this->commands([StatusCommand::class, DoctorCommand::class, MaintainCommand::class]);
        }

        if ((bool) config('fair-queue.metrics.enabled', false)) {
            $this->loadRoutesFrom(__DIR__.'/../routes/metrics.php');
        }
    }
}
